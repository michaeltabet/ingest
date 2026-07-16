"""Temporal workflows — DETERMINISTIC (decisions only, no IO, no clocks).

ScrapeBoard  — one board. Fail-loud: attempts=1, absurd start_to_close, no
               heartbeat (the daily pass is the dead-worker detector).
PlatformRun  — parent per platform. FANS OUT children CONCURRENTLY (not a
               loop): starts all boards at once, gathers results. The worker
               pool's slot count is the real throughput bound.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from ingest.orchestration.activities import scrape_board
    from ingest.orchestration import naming

_FAIL_LOUD = dict(
    # DOCTRINE (Michael): no timeouts, no retry cap. A board keeps retrying
    # until it lands ALL its jobs. Not-all-jobs = the quality gate raises =
    # Temporal retries = try again. Forever, until it's complete.
    start_to_close_timeout=timedelta(days=30),          # never fires; SDK requires a value
    retry_policy=RetryPolicy(maximum_attempts=0),       # 0 = UNLIMITED. no cap. keep retrying, ever.
)


@workflow.defn
class ScrapeBoard:
    @workflow.run
    async def run(self, platform: str, slug: str) -> dict:
        run_id = workflow.info().workflow_id
        return await workflow.execute_activity(
            scrape_board, args=[platform, slug, run_id], **_FAIL_LOUD)


@workflow.defn
class PlatformRun:
    @workflow.run
    async def run(self, platform: str, slugs: list, run_date: str) -> dict:
        # fan out: every board as a child, ALL in flight at once (bounded by
        # worker slots, not by this workflow).
        async def one(slug: str):
            return await workflow.execute_child_workflow(
                ScrapeBoard.run, args=[platform, slug],
                id=naming.board_scrape(platform, slug, run_date),
                id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING)

        results = await asyncio.gather(*[one(s) for s in slugs],
                                       return_exceptions=True)
        ok = sum(1 for r in results if isinstance(r, dict) and r.get("list_ok"))
        failed = len(slugs) - ok
        # NO FALSE POSITIVE: if ANY board failed, the platform run FAILS (red).
        # A run that didn't land every board's jobs is a failure, not a pass.
        if failed:
            bad = [s for s, r in zip(slugs, results)
                   if not (isinstance(r, dict) and r.get("list_ok"))]
            raise ApplicationError(
                f"{platform}: {failed}/{len(slugs)} boards failed "
                f"(e.g. {bad[:5]}) — not all jobs landed",
                type="PlatformRunIncomplete", non_retryable=True)
        return {"platform": platform, "boards": len(slugs), "succeeded": ok,
                "failed": failed}
