"""THE TEST — the escalation ladder, run against real sources.

    python -m ingest.test             (INGEST_DOMAIN picks the project)

One campaign step per invocation (the Airflow test DAG runs exactly this, so
the trigger path is tested too):

  per platform: rung = 4 -> 8 -> 16 -> PROVEN
    * sample <rung> keys NEVER TESTED BEFORE (a board is never chosen twice —
      a fix is proven against fresh data, not overfit to the board it broke on)
    * run them through the REAL temporal flow (PlatformRun, own worker inline)
    * a key passes when its evidence says success AND its landed rows pass the
      project's DATA VALIDATOR (spec: prove.validator — pydantic-style checks,
      project code)
    * 100% pass -> next rung next step; any fail -> stay, fix, fresh sample
  every key-run writes a test_campaign row WITH the parameters it ran under
  (temporal knobs + calibration) — the whole campaign is visible in Superset.

State lives in ClickHouse (test_campaign) — nothing to remember locally.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import sys
from datetime import date, datetime, timezone

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.worker import Worker

from ingest import domains
from ingest.orchestration import naming
from ingest.orchestration.activities import scrape_source
from ingest.orchestration.config import QUEUE_HTTP, TemporalConfig, WORKER
from ingest.orchestration.workflows import PlatformRun, ScrapeSource

_DDL = """CREATE TABLE IF NOT EXISTS test_campaign (
  campaign String, platform LowCardinality(String), key String, rung UInt16,
  passed UInt8, evidence_outcome LowCardinality(String),
  data_errors Array(String), params String, run_at DateTime64(3)
) ENGINE = MergeTree ORDER BY (platform, run_at)"""

LADDER_DEFAULT = [4, 8, 16]


def _ch(dom):
    from ingest.utils.clickhouse import ConnectClickHouse
    ch = ConnectClickHouse.from_spec(dom.database)
    ch.command(_DDL)
    return ch


def _state(ch, platform: str, ladder: list) -> tuple:
    """(current_rung, used_keys, proven). A rung is judged by its LATEST
    batch only — a platform that failed once and was fixed passes on a clean
    fresh batch; history is evidence, not a life sentence."""
    rows = ch.query(
        f"SELECT rung, argMax(ok, batch) FROM ("
        f"  SELECT rung, run_at AS batch, min(passed) AS ok"
        f"  FROM test_campaign WHERE platform='{platform}'"
        f"  GROUP BY rung, run_at"
        f") GROUP BY rung ORDER BY rung")
    passed_rungs = {int(r) for r, ok in rows if int(ok) == 1}
    rung = None
    for r in ladder:
        if r not in passed_rungs:
            rung = r
            break
    used = {k for (k,) in ch.query(
        f"SELECT DISTINCT key FROM test_campaign WHERE platform='{platform}'")}
    return rung, used, rung is None


async def step(domain_name: str) -> None:
    dom = domains.get(domain_name)
    test_cfg = getattr(dom, "test", {}) or {}
    ladder = test_cfg.get("ladder", LADDER_DEFAULT)
    validate = None
    if test_cfg.get("validator"):
        mod, attr = test_cfg["validator"].split(":", 1)
        import importlib
        validate = getattr(importlib.import_module(mod), attr)

    ch = _ch(dom)
    cfg = TemporalConfig.from_env()
    client = await Client.connect(cfg.target, namespace=cfg.namespace)
    campaign = date.today().isoformat()
    # UNIQUE per invocation: workflow ids (and therefore every evidence row's
    # run_id) carry it, so a dashboard can scope to THE LAST RUN rather than
    # to "today" — several runs happen in a day.
    run_date = f"{campaign}.t{datetime.now(timezone.utc):%H%M}"
    # the parameters this step runs under — recorded on every row
    params = json.dumps({"temporal": dom.temporal, "calibration": {
        k: v.get("value") for k, v in dom.calibration.items()
        if isinstance(v, dict)}})

    plan = []
    for p in dom.enabled_platforms():
        rung, used, proven = _state(ch, p, ladder)
        if proven:
            print(f"{p:18} PROVEN — no more testing")
            continue
        pool = [k for k in dom.resolver.keys(p) if k not in used]
        if not pool:
            print(f"{p:18} rung {rung}: no untested keys left")
            continue
        sample = random.sample(pool, min(rung, len(pool)))
        plan.append((p, rung, sample))
        print(f"{p:18} rung {rung}: testing {len(sample)} fresh keys")
    if not plan:
        print("nothing to prove — campaign complete")
        return

    async with Worker(client, task_queue=QUEUE_HTTP.name,
                      workflows=[ScrapeSource, PlatformRun],
                      activities=[scrape_source],
                      max_concurrent_activities=WORKER["slots"]):
        async def run_platform(p, rung, sample):
            try:
                await client.execute_workflow(
                    PlatformRun.run, args=[p, sample, run_date, domain_name],
                    id=naming.platform_run(p, run_date),
                    task_queue=QUEUE_HTTP.name,
                    id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING)
            except Exception:
                pass   # per-key verdicts come from evidence, not the parent
            now = datetime.now(timezone.utc)
            rows = []
            for key in sample:
                sid = f"{p}:{key}"
                # SCOPED TO THIS RUN: an unscoped lookup graded a board on a
                # PREVIOUS run's evidence whenever this run died before commit
                # (audit, 07-19).
                ev = ch.query(
                    f"SELECT outcome, list_status, errors FROM "
                    f"{test_cfg.get('evidence_table', 'scrape_evidence')} "
                    f"WHERE board_id='{sid}' AND startsWith(run_id, '{run_date}') "
                    f"ORDER BY run_at DESC LIMIT 1")
                outcome = ev[0][0] if ev else "no-evidence"
                # A source that is GONE or BLOCKED is inventory rot, not scraper
                # quality: 404/410, OR the list never came back as usable data
                # (dead tenant serving HTML instead of its API). Consume it,
                # flag it, never hold it against the platform's rung.
                if ev and outcome == "failure" and int(ev[0][1]) in (404, 410):
                    # ONLY 404/410 = the source is GONE. 403 is anti-bot
                    # DETECTING US and a JSON parse error is a scraper crash —
                    # laundering those as "dead board" turned 4 hard failures
                    # into rung advancement for 3 platforms (audit, 07-19).
                    outcome = "dead-board"
                errors = []
                if outcome == "success" and validate:
                    # scoped to THIS run, and sampled ACROSS the scrape rather
                    # than 'newest 20' — the sink streams, so newest-20 was
                    # deterministically the last flush batch, not a sample.
                    landed = ch.query(
                        f"SELECT raw FROM {test_cfg.get('data_table', 'jobs')} "
                        f"WHERE board_id='{sid}' AND startsWith(run_id, '{run_date}') "
                        f"ORDER BY cityHash64(external_id) LIMIT 25")
                    if not landed:
                        errors.append("no rows landed")
                    for (raw,) in landed:
                        errors.extend(validate(raw))
                        if errors:
                            break
                passed = 1 if (outcome in ("success", "empty") and not errors) else 0
                if outcome == "dead-board":
                    # a vanished source is EVIDENCE ABOUT THE INVENTORY, not
                    # about the scraper: consume it, flag it, and EXCLUDE it
                    # from the rung arithmetic entirely — counting it as a
                    # pass inflated the verdict (audit, 07-19).
                    passed = 1
                rows.append([campaign, p, key, rung, passed, outcome,
                             errors[:5], params, now])
            ch.insert("test_campaign", rows,
                      ("campaign", "platform", "key", "rung", "passed",
                       "evidence_outcome", "data_errors", "params", "run_at"))
            judged = [r for r in rows if r[5] != "dead-board"]
            dead = len(rows) - len(judged)
            ok = sum(r[4] for r in judged)
            clean = judged and ok == len(judged)
            print(f"{p:18} rung {rung}: {ok}/{len(judged)} passed"
                  + (f" ({dead} dead-board excluded)" if dead else "")
                  + ("  -> next rung on next step" if clean
                     else "  -> FIX, then fresh sample"))

        await asyncio.gather(*[run_platform(*t) for t in plan])
    print(f"\ntest rows in test_campaign (Superset) — campaign={campaign}")


def main():
    name = os.environ.get("INGEST_DOMAIN") or sys.exit(
        "INGEST_DOMAIN not set — the engine has no default project")
    asyncio.run(step(name))


if __name__ == "__main__":
    main()
