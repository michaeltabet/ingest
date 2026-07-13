"""The real Temporal worker — the process that DOES the scraping.

Connects to the cluster Temporal (env-driven), joins a task queue, and runs
ScrapeBoard workflows + the scrape_board activity. This is the entrypoint the
k8s Deployment runs (one image, --queue selects the pool).

    python -m ingest.orchestration.worker --queue scrape-http

Env: TEMPORAL_TARGET (e.g. production temporal via port-forward or in-cluster
DNS), TEMPORAL_NAMESPACE (default: ingest).
"""
from __future__ import annotations

import argparse
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from ingest.orchestration.config import TemporalConfig
from ingest.orchestration.activities import scrape_board
from ingest.orchestration.workflows import ScrapeBoard, PlatformRun


async def run_worker(queue: str):
    cfg = TemporalConfig.from_env()
    client = await Client.connect(cfg.target, namespace=cfg.namespace)
    print(f"worker up: target={cfg.target} ns={cfg.namespace} queue={queue}")
    worker = Worker(client, task_queue=queue,
                    workflows=[ScrapeBoard, PlatformRun], activities=[scrape_board])
    await worker.run()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", default="scrape-http")
    args = ap.parse_args()
    asyncio.run(run_worker(args.queue))


if __name__ == "__main__":
    main()
