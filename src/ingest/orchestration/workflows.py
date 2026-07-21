"""Temporal workflows — DETERMINISTIC (decisions only, no IO, no clocks).

PROJECT-NEUTRAL: these workflows orchestrate any project's scrape — the
project name rides along and everything project-specific happens inside the
activity.

ScrapeSource — one source. Fail-loud: attempts=1, no ceiling on a source's
               runtime, no heartbeat (the daily pass is the dead-worker
               detector). The numbers come from config.ACTIVITY_OPTIONS —
               ONE home; this file must never restate them.
PlatformRun  — parent per platform. FANS OUT children CONCURRENTLY (not a
               loop): starts all sources at once, gathers results. The worker
               pool's slot count is the real throughput bound.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from ingest.orchestration.activities import scrape_source
    from ingest.orchestration import naming
    from ingest.orchestration.config import ACTIVITY_OPTIONS

_DURABLE = dict(
    # DOCTRINE (Michael, 2026-07-21): Temporal exists for DURABLE EXECUTION.
    # An infrastructure fault must NOT cost a day of data.
    #
    # History of this setting, because both extremes have already burned us:
    #   2026-07-15  unlimited retries on EVERYTHING -> a permanent retry
    #               population of sources whose gate could never pass; it ate
    #               worker slots and re-OOMed pods.
    #   2026-07-16  attempts=1 on everything -> the opposite failure. On
    #               2026-07-21 a wrong ClickHouse hostname (doubled helm
    #               prefix) was live for ~2h and PERMANENTLY killed 1,186
    #               source scrapes. The fault was fixed minutes later; the
    #               data was still gone until the next daily pass.
    #
    # Neither extreme is right, and the choice was never actually between
    # them: core.errors ALREADY classifies TransientError vs PermanentError.
    # That taxonomy just was not wired to the retry policy. Now it is.
    #
    #   Transient (429, 5xx, timeouts, connection resets, sink unreachable)
    #       -> retry forever. This is durable execution; the world blinking
    #          must not lose the day.
    #   Permanent (403 anti-bot, 404 gone, malformed source, gate refusal)
    #       -> ZERO retries. These never heal, and retrying them is exactly
    #          what created the 07-15 slot-eating population.
    #
    # So retries are unbounded in TIME but bounded in KIND. Slot exhaustion is
    # prevented by never retrying the errors that cannot succeed, not by
    # capping attempts on errors that can.
    start_to_close_timeout=timedelta(seconds=ACTIVITY_OPTIONS["start_to_close_seconds"]),
    retry_policy=RetryPolicy(
        maximum_attempts=ACTIVITY_OPTIONS["maximum_attempts"],
        initial_interval=timedelta(seconds=ACTIVITY_OPTIONS["retry_initial_seconds"]),
        maximum_interval=timedelta(seconds=ACTIVITY_OPTIONS["retry_maximum_seconds"]),
        backoff_coefficient=2.0,
        # Bound by KIND. Anything not named here is treated as transient and
        # retried — the safe default: a NEW unclassified fault costs latency,
        # not a day of data.
        non_retryable_error_types=list(ACTIVITY_OPTIONS["non_retryable_error_types"]),
    ),
)


@workflow.defn
class ScrapeSource:
    @workflow.run
    async def run(self, platform: str, key: str, domain: str = "") -> dict:
        # run_id must be UNIQUE PER EXECUTION, not per day: the workflow_id is
        # date-scoped and a same-day refire (janitor TERMINATE_IF_RUNNING)
        # reuses it — batches from the dead attempt would then share the
        # successful attempt's run_id and pollute its membership rows.
        info = workflow.info()
        run_id = f"{info.workflow_id}/{info.run_id}"
        return await workflow.execute_activity(
            scrape_source, args=[platform, key, run_id, domain], **_DURABLE)


@workflow.defn
class PlatformRun:
    @workflow.run
    async def run(self, platform: str, keys: list, run_date: str,
                  domain: str = "") -> dict:
        # COMPATIBILITY IS FOREVER: this engine's whole point is that
        # workflows retry until deliberately killed (janitor), so an
        # execution can be in flight for DAYS across worker deploys. A
        # signature change that rejects an older caller's recorded input
        # wedges every in-flight execution in an infinite WorkflowTaskFailed
        # retry loop that still reports Running (2026-07-19..21: three days
        # dark, run.<platform>.* stuck on "missing positional argument:
        # 'domain'"). New params MUST default; the default resolves in the
        # ACTIVITY (workflows are deterministic, no env reads here).
        # fan out: every source as a child, ALL in flight at once (bounded by
        # worker slots, not by this workflow).
        async def one(key: str):
            return await workflow.execute_child_workflow(
                ScrapeSource.run, args=[platform, key, domain],
                id=naming.source_scrape(platform, key, run_date),
                id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING)

        results = await asyncio.gather(*[one(s) for s in keys],
                                       return_exceptions=True)
        ok = sum(1 for r in results if isinstance(r, dict) and r.get("list_ok"))
        failed = len(keys) - ok
        # NO FALSE POSITIVE: if ANY source failed, the platform run FAILS (red).
        # A run that didn't land every source's items is a failure, not a pass.
        if failed:
            bad = [s for s, r in zip(keys, results)
                   if not (isinstance(r, dict) and r.get("list_ok"))]
            raise ApplicationError(
                f"{platform}: {failed}/{len(keys)} sources failed "
                f"(e.g. {bad[:5]}) — not all items landed",
                type="PlatformRunIncomplete", non_retryable=True)
        return {"platform": platform, "sources": len(keys), "succeeded": ok,
                "failed": failed}
