"""Temporal workflows — DETERMINISTIC (decisions only, no IO, no clocks).

Real @workflow.defn. Fail-loud doctrine is encoded in the activity options:
attempts=1 (Temporal's default is infinite), start_to_close absurd (exists only
because the SDK requires a value; never meant to fire — the daily Airflow pass
is the real safety net).

ScrapeBoard  — one board (heavy families, and the smoke-test entrypoint).
PlatformRun  — parent per platform (fan-out) — added when the DB plan is wired.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ingest.orchestration.activities import scrape_board


@workflow.defn
class ScrapeBoard:
    @workflow.run
    async def run(self, platform: str, slug: str) -> dict:
        return await workflow.execute_activity(
            scrape_board,
            args=[platform, slug],
            start_to_close_timeout=timedelta(days=30),        # doctrine: never fires
            retry_policy=RetryPolicy(maximum_attempts=1),     # doctrine: fail-loud
        )
