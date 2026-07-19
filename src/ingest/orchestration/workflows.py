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

_FAIL_LOUD = dict(
    # DOCTRINE (Michael, re-affirmed 2026-07-16): ONE attempt, fail LOUD.
    # A failed source = a red workflow + an evidence row; the daily pass is the
    # retry mechanism AND the dead-worker detector. Unlimited retries (07-15)
    # created a permanent retry population that ate the worker slots and
    # re-OOMed pods on sources whose gate could never pass.
    # No ceiling on a source: a scrape takes as long as it takes. The SDK
    # requires start_to_close to exist, so config.ACTIVITY_OPTIONS sets it
    # absurdly high rather than to a number that would kill a slow-but-working
    # scrape. DERIVED, not restated: this dict and config.py diverged three
    # ways once (30m docstring / 45m code / 30d config) — never again.
    start_to_close_timeout=timedelta(seconds=ACTIVITY_OPTIONS["start_to_close_seconds"]),
    retry_policy=RetryPolicy(maximum_attempts=ACTIVITY_OPTIONS["maximum_attempts"]),
)


@workflow.defn
class ScrapeSource:
    @workflow.run
    async def run(self, platform: str, key: str, domain: str) -> dict:
        # run_id must be UNIQUE PER EXECUTION, not per day: the workflow_id is
        # date-scoped and a same-day refire (janitor TERMINATE_IF_RUNNING)
        # reuses it — batches from the dead attempt would then share the
        # successful attempt's run_id and pollute its membership rows.
        info = workflow.info()
        run_id = f"{info.workflow_id}/{info.run_id}"
        return await workflow.execute_activity(
            scrape_source, args=[platform, key, run_id, domain], **_FAIL_LOUD)


@workflow.defn
class PlatformRun:
    @workflow.run
    async def run(self, platform: str, keys: list, run_date: str,
                  domain: str) -> dict:
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
