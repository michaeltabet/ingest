"""CLI: the offline tools. Real HTTP (stdlib), no cluster, no Temporal.

    python -m ingest.cli record <platform> --slug <slug>   # hit a LIVE board, save fixture
    python -m ingest.cli run    <platform> --slug <slug>    # LIVE fetch, print evidence

`record` uses the platform's OWN list_request facts, so the saved fixture is a
real response from a real board — not hand-written JSON.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest.core.context import ScrapeContext
from ingest.core.models import Board
from ingest.scraping import registry
from ingest.utils.http import UrllibClient

ROOT = Path(__file__).resolve().parents[2]


async def _fetch(platform: str, slug: str, url: str):
    scraper = registry.get(platform)
    board = Board(board_id=f"{platform}:{slug}", platform=platform, slug=slug, url=url or "")
    ctx = ScrapeContext(http=UrllibClient())
    return await scraper.fetch(board, ctx)


def cmd_record(args):
    res = asyncio.run(_fetch(args.platform, args.slug, args.url))
    if not res.list_ok or res.stubs_seen == 0:
        print(json.dumps(res.summary(), indent=2))
        print(f"\nREFUSED to save: list_ok={res.list_ok} stubs_seen={res.stubs_seen} "
              f"(bad slug or dead board — pick a live one)")
        sys.exit(1)
    fixdir = ROOT / "fixtures" / args.platform
    fixdir.mkdir(parents=True, exist_ok=True)
    lst = [p for p in res.payloads if p.kind == "list"]
    det = [p for p in res.payloads if p.kind == "detail"]
    if lst:
        (fixdir / "list.json").write_bytes(lst[0].body)
    if det:
        (fixdir / "detail.json").write_bytes(det[0].body)
    print(json.dumps(res.summary(), indent=2))
    print(f"\nsaved {len(lst)} list + {len(det)} detail REAL fixture(s) -> {fixdir}")


def cmd_run(args):
    res = asyncio.run(_fetch(args.platform, args.slug, args.url))
    print(json.dumps(res.summary(), indent=2))


def main():
    ap = argparse.ArgumentParser(prog="ingest.cli")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("record", cmd_record), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("platform")
        p.add_argument("--slug", required=True)
        p.add_argument("--url", default="")
        p.set_defaults(fn=fn)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
