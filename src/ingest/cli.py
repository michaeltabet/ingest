"""CLI: run one source offline. Real HTTP (stdlib), no cluster, no Temporal.

    INGEST_DOMAIN=<project> python -m ingest.cli run <platform> --key <key>

Fetches live using the project's own platform facts (spec-resolved) and
prints the evidence summary. The project comes from INGEST_DOMAIN — the
engine has no default project.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest import domains
from ingest.core.context import ScrapeContext
from ingest.utils.http import UrllibClient


async def _fetch(platform: str, key: str):
    name = os.environ.get("INGEST_DOMAIN") or sys.exit(
        "INGEST_DOMAIN not set — the engine has no default project")
    dom = domains.get(name)
    scraper = dom.registry.get(platform)
    source = dom.resolver.resolve(platform, key)
    return await scraper.fetch(source, ScrapeContext(http=UrllibClient()))


def cmd_run(args):
    res = asyncio.run(_fetch(args.platform, args.key))
    print(json.dumps(res.summary(), indent=2))


def main():
    ap = argparse.ArgumentParser(prog="ingest.cli")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run")
    p.add_argument("platform")
    p.add_argument("--key", required=True)
    p.set_defaults(fn=cmd_run)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
