"""The real Temporal worker — the process that DOES the scraping.

Connects to the cluster Temporal (env-driven), joins a task queue, and runs
ScrapeSource workflows + the scrape_source activity. This is the entrypoint the
k8s Deployment runs (one image, --queue selects the pool).

    INGEST_DOMAIN=<project> python -m ingest.orchestration.worker

Env: TEMPORAL_TARGET (e.g. production temporal via port-forward or in-cluster
DNS), TEMPORAL_NAMESPACE (default: ingest).
"""
from __future__ import annotations

import argparse
import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from ingest.orchestration.config import QUEUE_HTTP, TemporalConfig, WORKER
from ingest.orchestration.activities import scrape_source
from ingest.orchestration.workflows import ScrapeSource, PlatformRun


async def run_worker(queue: str):
    cfg = TemporalConfig.from_env()
    # concurrency: scrapes are I/O-bound, but each in-flight source holds real
    # memory until its batches flush, and every co-located activity dies with
    # the pod on an OOM. 16 slots x 2Gi = ~128Mi headroom per slot — failures
    # stay attributable to their own source (2026-07-16 incident).
    slots = int(os.environ.get("WORKER_SLOTS") or WORKER["slots"])
    client = await Client.connect(cfg.target, namespace=cfg.namespace)
    print(f"worker up: target={cfg.target} ns={cfg.namespace} queue={queue} slots={slots}")
    worker = Worker(client, task_queue=queue,
                    workflows=[ScrapeSource, PlatformRun], activities=[scrape_source],
                    max_concurrent_activities=slots,
                    max_cached_workflows=WORKER["max_cached_workflows"])
    await worker.run()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", default=QUEUE_HTTP.name)
    args = ap.parse_args()
    asyncio.run(run_worker(args.queue))


if __name__ == "__main__":
    main()
