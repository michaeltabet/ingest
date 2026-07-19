"""The daily JANITOR — the reason activities carry no ceilings.

    python -m ingest.janitor        (INGEST_DOMAIN picks the project)

Doctrine: no max retries, no max time on any activity. Instead, once a day:
  1. CLOSE THE WINDOW — terminate every still-running workflow in the
     namespace (a run that outlived the window is a fact to record, not
     a thing to babysit)
  2. TAKE THE AVERAGES — per platform, from Temporal's closed workflows and
     the evidence ledger: seconds per source, outcome rates, bytes
  3. WRITE OBSERVATIONS — calibration_observations rows (visible in
     Superset) including RECOMMENDATIONS: new worker slots, and whether a
     platform's sources are cheap enough to batch together (many greenhouse
     boards in one child). Config changes stay MR-made by a human reading
     the observations — the janitor observes and recommends, it never edits.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

from temporalio.client import Client

from ingest import domains
from ingest.orchestration.config import TemporalConfig

_DDL = """CREATE TABLE IF NOT EXISTS calibration_observations (
  platform LowCardinality(String), window_closed UInt32, sources_measured UInt32,
  avg_seconds Float64, ok_rate Float64, recommend_batch UInt16,
  note String, run_at DateTime64(3)
) ENGINE = MergeTree ORDER BY (platform, run_at)"""

TARGET_CHILD_SECONDS = 600.0   # a batch should run ~this long; sizes itself


async def run(domain_name: str) -> None:
    dom = domains.get(domain_name)
    from ingest.utils.clickhouse import ConnectClickHouse
    ch = ConnectClickHouse.from_spec(dom.database)
    ch.command(_DDL)
    cfg = TemporalConfig.from_env()
    client = await Client.connect(cfg.target, namespace=cfg.namespace)

    # 1 — close the window
    killed = 0
    async for wf in client.list_workflows('ExecutionStatus="Running"'):
        try:
            await client.get_workflow_handle(wf.id).terminate(
                "janitor: window closed")
            killed += 1
        except Exception:
            pass

    # 2 — averages from closed source-scrapes. IDs END <platform>.<key>.source,
    # so parse from the END: a run-date may carry a suffix (prove campaigns use
    # "<date>.prove"), and positional parsing invented a "prove" platform.
    # Only THIS project's platforms count — one namespace can host several.
    mine = set(dom.enabled_platforms())
    stats: dict = {}
    async for wf in client.list_workflows(
            'WorkflowType="ScrapeSource" AND ExecutionStatus="Completed"'):
        parts = wf.id.split(".")
        if len(parts) < 3 or parts[-1] != "source":
            continue
        if not (wf.close_time and wf.start_time):
            continue
        platform = parts[-3]
        if platform not in mine:
            continue
        stats.setdefault(platform, []).append(
            (wf.close_time - wf.start_time).total_seconds())

    now = datetime.now(timezone.utc)
    rows = []
    for p, secs in sorted(stats.items()):
        avg = sum(secs) / len(secs)
        ok = ch.query(
            f"SELECT countIf(outcome='success')/count() FROM "
            f"{dom.test.get('evidence_table', 'scrape_evidence')} "
            f"WHERE platform='{p}' AND run_at >= now() - INTERVAL 1 DAY")
        raw_ok = ok[0][0] if ok else None
        ok_rate = float(raw_ok) if raw_ok is not None and raw_ok == raw_ok else 0.0
        batch = int(max(1, min(500, TARGET_CHILD_SECONDS / avg))) if avg < 60 else 1
        note = (f"cheap sources ({avg:.1f}s avg) — batch {batch} per child"
                if batch > 1 else f"{avg:.1f}s avg — one child per source")
        rows.append([p, killed, len(secs), round(avg, 2), round(ok_rate, 3),
                     batch, note, now])
        print(f"{p:18} n={len(secs):4} avg={avg:7.1f}s ok={ok_rate:5.1%} "
              f"batch->{batch}")
    if rows:
        ch.insert("calibration_observations", rows,
                  ("platform", "window_closed", "sources_measured",
                   "avg_seconds", "ok_rate", "recommend_batch", "note",
                   "run_at"))
    print(f"\nwindow closed ({killed} terminated); observations written "
          f"for {len(rows)} platforms — read them in Superset, change the "
          f"json via MR")


def main():
    name = os.environ.get("INGEST_DOMAIN") or sys.exit(
        "INGEST_DOMAIN not set — the engine has no default project")
    asyncio.run(run(name))


if __name__ == "__main__":
    main()
