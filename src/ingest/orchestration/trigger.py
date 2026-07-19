"""Nightly trigger — starts one PlatformRun per platform, then exits.

The CronJob runs this; the worker Deployment processes the resulting children.
Fire-and-return (start_workflow, not execute) — the trigger does not wait.

DOMAIN-NEUTRAL: the domain (pipelines/<name>.conf) supplies the platform list
and the source inventory (resolver.keys). One CronJob per domain.

Env: TEMPORAL_ADDRESS/NAMESPACE, CH_* (inventory), INGEST_DOMAIN (required —
which project), SOURCES_LIMIT (optional cap per platform; unset = spec value).
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy

from ingest import domains
from ingest.orchestration.config import TemporalConfig
from ingest.orchestration import naming
from ingest.orchestration.workflows import PlatformRun


async def trigger(domain, limit=None, run_date=None, platforms=None):
    """platforms: None = all (the nightly run); a list = only those (janitor
    refire). Same IDs either way — TERMINATE_IF_RUNNING replaces, by design."""
    dom = domains.get(domain)
    cfg = TemporalConfig.from_env()
    client = await Client.connect(cfg.target, namespace=cfg.namespace)
    run_date = run_date or date.today().isoformat()
    started = 0
    queue = dom.temporal.get("task_queue") or sys.exit(
        f"spec for {domain!r} is missing temporal.task_queue")
    for p in (platforms or dom.enabled_platforms()):
        keys = dom.resolver.keys(p, limit=limit)
        if not keys:
            print(f"skip {p}: no sources")
            continue
        await client.start_workflow(
            PlatformRun.run, args=[p, keys, run_date, domain],
            id=naming.platform_run(p, run_date), task_queue=queue,
            id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING)
        print(f"started {naming.platform_run(p, run_date)} ({len(keys)} sources)")
        started += 1
    print(f"triggered {started} platform runs for {run_date}")


def main():
    from ingest import domains as _domains
    dom_name = os.environ.get("INGEST_DOMAIN") or sys.exit(
        "INGEST_DOMAIN not set — the engine has no default project")
    raw = os.environ.get("SOURCES_LIMIT")
    spec_limit = _domains.get(dom_name).trigger.get("sources_limit")
    limit = int(raw) if raw else (int(spec_limit) if spec_limit else None)
    # RUN_DATE: the scheduler's run window (Airflow's logical date), not
    # date.today(). Unset = today, for a hand-run trigger.
    asyncio.run(trigger(domain=dom_name,
                        limit=limit,
                        run_date=os.environ.get("RUN_DATE") or None))


if __name__ == "__main__":
    main()
