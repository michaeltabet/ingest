"""Per-platform tests + the FIXTURE-COVERAGE GATE (CI).

Run offline, stdlib only:  python tests/test_platforms.py

For every REGISTERED platform:
  1. gate: it MUST have fixtures/<platform>/list.json (else CI fails)
  2. run its real scraper against a FixtureClient replaying that fixture
  3. assert it acquired stubs and produced payloads

This is what makes 2am repair work: a platform's whole behavior is exercised
offline, in milliseconds, with no network and no Temporal.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingest.core.context import ScrapeContext
from ingest.core.models import Board, Response
from ingest.scraping import registry
from ingest.utils.http import FixtureClient

FIXTURES = ROOT / "fixtures"
FAILED = []


def check(name, cond):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}")
    if not cond:
        FAILED.append(name)


def _client_for(platform: str) -> FixtureClient:
    list_body = (FIXTURES / platform / "list.json").read_bytes()
    detail_path = FIXTURES / platform / "detail.json"
    detail_body = detail_path.read_bytes() if detail_path.exists() else b"{}"
    return FixtureClient([
        (lambda m, u: u.endswith("/detail"), Response(200, detail_body)),
        (lambda m, u: True, Response(200, list_body)),   # everything else = the list
    ])


async def _run(platform: str):
    scraper = registry.get(platform)
    board = Board(board_id=f"{platform}-test", platform=platform, slug="acme", url="")
    ctx = ScrapeContext(http=_client_for(platform))
    return await scraper.fetch(board, ctx)


def main():
    plats = registry.all_platforms()
    print(f"registered platforms: {plats}\n")

    # --- fixture-coverage gate ---
    print("fixture-coverage gate:")
    for p in plats:
        check(f"{p} has fixtures/list.json", (FIXTURES / p / "list.json").exists())

    # --- run each platform against its fixture ---
    print("\nper-platform runs:")
    for p in plats:
        res = asyncio.run(_run(p))
        check(f"{p}: list_ok", res.list_ok)
        check(f"{p}: stubs_seen > 0", res.stubs_seen > 0)
        check(f"{p}: produced payloads", len(res.payloads) > 0)

    print(f"\n{'ALL PASS' if not FAILED else 'FAILURES: ' + ', '.join(FAILED)}")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
