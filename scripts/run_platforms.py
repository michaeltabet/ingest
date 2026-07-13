"""Real production run: all 13 platforms, fanned out, against real boards,
landing raw+evidence in ClickHouse.

Reads boards from the real source (runs_board), runs one PlatformRun parent per
platform CONCURRENTLY, each fanning out its boards as child workflows on the
worker pool. Bounded to N boards/platform to prove the pipe (full fleet = same
command, no limit, run in waves).

Blocked platforms (workday/oracle/taleo) will FAIL LOUD here — that is the
doctrine, visible in Temporal + the evidence table, not hidden.

Prereqs: port-forwards to production temporal (7233) + clickhouse (8123);
CH_* env set. Run: .venv/bin/python scripts/run_platforms.py [N]
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from temporalio.client import Client
from temporalio.worker import Worker

from ingest.orchestration.activities import scrape_board
from ingest.orchestration.workflows import ScrapeBoard, PlatformRun
from ingest.orchestration import naming
from ingest.scraping import registry
from ingest.boards import source

TARGET = "127.0.0.1:7233"
QUEUE = "scrape-http"


async def main(n: int):
    client = await Client.connect(TARGET, namespace=naming.NAMESPACE)
    run_date = date.today().isoformat()
    platforms = registry.all_platforms()  # 13
    print(f"platforms: {platforms}\nboards/platform: {n}  run_date: {run_date}\n")

    async with Worker(client, task_queue=QUEUE,
                      workflows=[ScrapeBoard, PlatformRun], activities=[scrape_board]):
        async def run_platform(p):
            slugs = source.boards_for(p, limit=n)
            if not slugs:
                return p, {"boards": 0, "succeeded": 0, "failed": 0, "note": "no boards"}
            try:
                r = await client.execute_workflow(
                    PlatformRun.run, args=[p, slugs, run_date],
                    id=naming.platform_run(p, run_date), task_queue=QUEUE)
                return p, r
            except Exception as e:
                return p, {"boards": len(slugs), "error": str(e)[:60]}

        results = await asyncio.gather(*[run_platform(p) for p in platforms])

    print(f"\n{'platform':16} {'boards':>6} {'ok':>4} {'fail':>4}")
    tb = ts = 0
    for p, r in sorted(results):
        b = r.get("boards", 0); ok = r.get("succeeded", 0)
        tb += b; ts += ok
        note = r.get("error") or r.get("note") or ""
        print(f"{p:16} {b:>6} {ok:>4} {b-ok:>4}  {note}")
    print(f"\n{'TOTAL':16} {tb:>6} {ts:>4} {tb-ts:>4}")
    print(f"\nTemporal: https://temporal.tail05f41d.ts.net  namespace '{naming.NAMESPACE}'")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    asyncio.run(main(n))
