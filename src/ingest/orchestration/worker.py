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
import os

from temporalio.client import Client
from temporalio.worker import Worker

from ingest.orchestration.config import TemporalConfig
from ingest.orchestration.activities import scrape_board
from ingest.orchestration.workflows import ScrapeBoard, PlatformRun


async def run_worker(queue: str):
    cfg = TemporalConfig.from_env()
    # concurrency: scrapes are I/O-bound, but each in-flight board holds real
    # memory until its batches flush, and every co-located activity dies with
    # the pod on an OOM. 16 slots x 2Gi = ~128Mi headroom per slot — failures
    # stay attributable to their own board (2026-07-16 incident).
    slots = int(os.environ.get("WORKER_SLOTS", "16"))
    client = await Client.connect(cfg.target, namespace=cfg.namespace)
    print(f"worker up: target={cfg.target} ns={cfg.namespace} queue={queue} slots={slots}")
    worker = Worker(client, task_queue=queue,
                    workflows=[ScrapeBoard, PlatformRun], activities=[scrape_board],
                    max_concurrent_activities=slots,
                    max_cached_workflows=100)
    await worker.run()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", default="scrape-http")
    args = ap.parse_args()
    asyncio.run(run_worker(args.queue))


if __name__ == "__main__":
    main()
