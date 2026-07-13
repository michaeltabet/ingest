"""REAL Temporal run — not a fixture, not a mock.

Spins a real Temporal dev server (start_local downloads the actual temporal
binary), registers a real worker, and executes a real ScrapeBoard workflow.
The activity fetches a LIVE board over the network. If this prints a workflow
result with real job counts, the scraper genuinely runs inside Temporal.

    .venv/bin/python tests/temporal_smoke.py [platform] [slug]
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from temporalio.worker import Worker
from temporalio.testing import WorkflowEnvironment

from ingest.orchestration.activities import scrape_board
from ingest.orchestration.workflows import ScrapeBoard
from ingest.orchestration import naming


async def main(platform: str, slug: str):
    from datetime import date
    env = await WorkflowEnvironment.start_local()
    try:
        async with Worker(env.client, task_queue="scrape-http",
                          workflows=[ScrapeBoard], activities=[scrape_board]):
            result = await env.client.execute_workflow(
                ScrapeBoard.run, args=[platform, slug],
                id=naming.board_scrape(platform, slug, date.today().isoformat()),
                task_queue="scrape-http")
        print("\n=== workflow result (from real Temporal) ===")
        for k, v in result.items():
            print(f"  {k}: {v}")
        ok = result["list_ok"] and result["stubs_seen"] > 0
        print(f"\n{'REAL TEMPORAL RUN OK' if ok else 'FAILED'} — "
              f"{result['stubs_seen']} jobs via workflow->activity->live fetch")
        sys.exit(0 if ok else 1)
    finally:
        await env.shutdown()


if __name__ == "__main__":
    platform = sys.argv[1] if len(sys.argv) > 1 else "greenhouse"
    slug = sys.argv[2] if len(sys.argv) > 2 else "gitlab"
    asyncio.run(main(platform, slug))
