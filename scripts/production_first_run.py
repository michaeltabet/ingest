"""FIRST REAL PRODUCTION RUN — visible in your Temporal UI.

Runs the 3 platforms with REAL recorded fixtures against the cluster's
production Temporal, in the `ingest` namespace, with proper descriptive
workflow IDs. Each activity does a LIVE fetch. Executions persist and are
inspectable at https://temporal.tail05f41d.ts.net (namespace: ingest).

Prereq: a port-forward to production temporal on 127.0.0.1:7233.
This does NOT write to ClickHouse yet (tables not created) and does NOT leave a
permanent worker running — it is the visibility proof, not the deploy.

    .venv/bin/python scripts/production_first_run.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from google.protobuf.duration_pb2 import Duration
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.service import RPCError
import temporalio.api.workflowservice.v1 as wsv1

from ingest.orchestration.activities import scrape_board
from ingest.orchestration.workflows import ScrapeBoard
from ingest.orchestration import naming

TARGET = "127.0.0.1:7233"
NS = naming.NAMESPACE  # "ingest"
QUEUE = "scrape-http"
BOARDS = [("greenhouse", "gitlab"), ("ashby", "openai"), ("lever", "highspot")]


async def ensure_namespace():
    base = await Client.connect(TARGET, namespace="default")
    d = Duration()
    d.FromTimedelta(timedelta(days=3))
    try:
        await base.service_client.workflow_service.register_namespace(
            wsv1.RegisterNamespaceRequest(
                namespace=NS,
                workflow_execution_retention_period=d,
                description="HiredSignal ingest — new Python scraper (parallel to atlas)"))
        print(f"registered namespace '{NS}' (retention 3d) — waiting for propagation")
        await asyncio.sleep(4)
    except RPCError as e:
        if "already exists" in str(e).lower():
            print(f"namespace '{NS}' already exists")
        else:
            raise


async def main():
    await ensure_namespace()
    client = await Client.connect(TARGET, namespace=NS)
    run_date = date.today().isoformat()
    print(f"\nrunning {len(BOARDS)} boards in namespace '{NS}' (run_date={run_date}):\n")
    async with Worker(client, task_queue=QUEUE,
                      workflows=[ScrapeBoard], activities=[scrape_board]):
        for platform, slug in BOARDS:
            wid = naming.board_scrape(platform, slug, run_date)
            res = await client.execute_workflow(
                ScrapeBoard.run, args=[platform, slug], id=wid, task_queue=QUEUE)
            print(f"  {wid}")
            print(f"      status=COMPLETED  stubs_seen={res['stubs_seen']}  "
                  f"bytes_in={res['bytes_in']}  list_status={res['list_status']}")
    print(f"\nView them: https://temporal.tail05f41d.ts.net  →  namespace '{NS}'")


if __name__ == "__main__":
    asyncio.run(main())
