"""Nightly trigger — starts one PlatformRun per platform, then exits.

The CronJob runs this; the worker Deployment processes the resulting children.
Fire-and-return (start_workflow, not execute) — the trigger does not wait.

Env: TEMPORAL_ADDRESS/NAMESPACE, CH_* (board source), BOARDS_LIMIT (optional
cap per platform for a bounded wave; unset = all boards).
"""
from __future__ import annotations

import asyncio
import os
from datetime import date

from temporalio.client import Client

from ingest.orchestration.config import TemporalConfig
from ingest.orchestration import naming
from ingest.orchestration.workflows import PlatformRun
from ingest.scraping import registry
from ingest.boards import source


async def trigger(limit=None, run_date=None):
    cfg = TemporalConfig.from_env()
    client = await Client.connect(cfg.target, namespace=cfg.namespace)
    run_date = run_date or date.today().isoformat()
    started = 0
    for p in registry.all_platforms():
        slugs = source.boards_for(p, limit=limit)
        if not slugs:
            print(f"skip {p}: no boards")
            continue
        await client.start_workflow(
            PlatformRun.run, args=[p, slugs, run_date],
            id=naming.platform_run(p, run_date), task_queue="scrape-http")
        print(f"started {naming.platform_run(p, run_date)} ({len(slugs)} boards)")
        started += 1
    print(f"triggered {started} platform runs for {run_date}")


def main():
    limit = int(os.environ["BOARDS_LIMIT"]) if os.environ.get("BOARDS_LIMIT") else None
    asyncio.run(trigger(limit=limit))


if __name__ == "__main__":
    main()
