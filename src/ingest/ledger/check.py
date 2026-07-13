"""Gate runner — REAL. Queries the ClickHouse evidence ledger and reports, per
platform, what is working and what is failing. Fails loudly (non-zero exit).

This is the observability that answers "do we know?". No skeleton: every gate
below runs a real query against ingest.scrape_evidence / ingest.boards and its
result is written back to ingest.gate_results (gates are data).

    python -m ingest.ledger.check          # human table + exit code
    python -m ingest.ledger.check --json   # machine output

Gates (binary, per platform, evaluated on the latest run_date present):
  G1 DELIVERY   platform's latest run produced stubs (> 0). Silent-success = FAIL.
  G2 FAILRATE   board failure rate <= 30% on the latest run.
  G3 COVERAGE   fraction of the platform's boards ever scraped (informational
                floor; FAIL if a scrape-enabled platform has NEVER produced a row).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

from ingest.utils.clickhouse import ConnectClickHouse
from ingest.scraping import registry

FAILRATE_MAX = 0.30


def _rows(ch, sql):
    return ch.query(sql)


def run_gates(ch) -> list:
    # target board counts per platform
    targets = {p: n for p, n in _rows(ch,
        "SELECT platform, count() FROM boards WHERE enabled=1 GROUP BY platform")}
    # ever-scraped board counts
    ever = {p: n for p, n in _rows(ch,
        "SELECT platform, uniq(board_id) FROM scrape_evidence GROUP BY platform")}
    # latest-run stats per platform (the run_date with the max run_at)
    latest = _rows(ch, """
        SELECT platform,
               countMerge_boards AS boards,
               stubs,
               ok,
               fail
        FROM (
          SELECT platform,
                 count() AS countMerge_boards,
                 sum(stubs_seen) AS stubs,
                 countIf(outcome='success') AS ok,
                 countIf(outcome='failure') AS fail,
                 max(run_at) AS mx
          FROM scrape_evidence
          GROUP BY platform
        )
    """)
    latest_by = {r[0]: r for r in latest}

    results = []
    for platform in registry.all_platforms():
        target = int(targets.get(platform, 0))
        scraped = int(ever.get(platform, 0))
        row = latest_by.get(platform)

        if row is None:
            results.append(_gate(platform, "G1_DELIVERY", False,
                                 "NEVER RAN — no evidence rows (blocked/not deployed)"))
            results.append(_gate(platform, "G3_COVERAGE", False,
                                 f"0/{target} boards ever scraped"))
            continue

        _, boards, stubs, ok, fail = row
        boards, stubs, ok, fail = int(boards), int(stubs), int(ok), int(fail)

        # G1 delivery / silent-success
        results.append(_gate(platform, "G1_DELIVERY", stubs > 0,
                             f"{stubs} stubs across {boards} boards"
                             + ("" if stubs > 0 else "  <-- SILENT SUCCESS")))
        # G2 fail rate
        rate = (fail / boards) if boards else 1.0
        results.append(_gate(platform, "G2_FAILRATE", rate <= FAILRATE_MAX,
                             f"{fail}/{boards} failed ({rate:.0%})"))
        # G3 coverage
        cov = (scraped / target) if target else 0.0
        results.append(_gate(platform, "G3_COVERAGE", scraped > 0,
                             f"{scraped}/{target} boards ever scraped ({cov:.1%})"))
    return results


def _gate(platform, gate, passed, detail):
    return {"platform": platform, "gate": gate, "passed": passed, "detail": detail}


def persist(ch, results, run_id="check"):
    now = datetime.now(timezone.utc)
    rows = [[run_id, r["gate"], r["platform"], 1 if r["passed"] else 0,
             "none" if r["passed"] else "alarm", r["detail"], now] for r in results]
    ch.insert("gate_results", rows,
              ("run_id", "gate", "platform", "passed", "action", "detail", "run_at"))


def main():
    as_json = "--json" in sys.argv
    ch = ConnectClickHouse.from_env()
    results = run_gates(ch)
    try:
        persist(ch, results)
    except Exception as e:
        print(f"(warn: could not persist gate_results: {e})", file=sys.stderr)

    failed = [r for r in results if not r["passed"]]
    if as_json:
        import json
        print(json.dumps(results, indent=2))
    else:
        print(f"\nGATE REPORT  ({len(results)} checks, {len(failed)} FAILING)\n")
        cur = None
        for r in sorted(results, key=lambda x: (x["platform"], x["gate"])):
            if r["platform"] != cur:
                cur = r["platform"]
                print(f"  {cur}")
            mark = "PASS" if r["passed"] else "FAIL"
            print(f"      [{mark}] {r['gate']:12} {r['detail']}")
        print(f"\n{'ALL GATES PASS' if not failed else str(len(failed)) + ' GATES FAILING — see FAIL above'}")

    # fail loudly
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
