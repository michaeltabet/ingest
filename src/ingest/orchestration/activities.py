"""Temporal activities — the DIRTY side (network, IO). Real @activity.defn.

scrape_board runs the platform scraper against a live board, writes the raw
payloads to ClickHouse (scrape_raw) and the evidence row (scrape_evidence)
DIRECTLY, and returns ONLY the evidence summary. Raw bytes never travel
through Temporal (the 2MB law).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from temporalio import activity

from ingest.core.context import ScrapeContext
from ingest.core.models import Board
from ingest.scraping import registry
from ingest.utils.http import UrllibClient

_CH = None


def _clickhouse():
    global _CH
    if _CH is None:
        from ingest.utils.clickhouse import ConnectClickHouse
        _CH = ConnectClickHouse.from_env()
    return _CH


def _dt(iso: str) -> datetime:
    try:
        return datetime.fromisoformat(iso)
    except Exception:
        return datetime.now(timezone.utc)


def _persist(result, run_id: str):
    ch = _clickhouse()
    if result.payloads:
        rows = [[p.platform, p.board_id, run_id, p.url, p.kind, p.http_status,
                 p.digest, p.stub_digest or "", p.body, _dt(p.fetched_at)]
                for p in result.payloads]
        ch.insert("scrape_raw", rows,
                  ("platform", "board_id", "run_id", "url", "kind", "http_status",
                   "digest", "stub_digest", "body", "fetched_at"))
    s = result.summary()
    # BINARY — no partial. A board either fully succeeded or it failed.
    outcome = ("success"
               if s["list_ok"] and s["stubs_seen"] > 0 and s["details_failed"] == 0
               else "failure")
    ch.insert("scrape_evidence",
              [[run_id, s["platform"], s["board_id"], s["list_status"], s["pages_fetched"],
                s["stubs_seen"], s["details_ok"], s["details_failed"], s["payloads"],
                s["bytes_in"], outcome, s["errors"], datetime.now(timezone.utc)]],
              ("run_id", "platform", "board_id", "list_status", "pages_fetched",
               "stubs_seen", "details_ok", "details_failed", "payloads", "bytes_in",
               "outcome", "errors", "run_at"))


@activity.defn
async def scrape_board(platform: str, slug: str, run_id: str) -> dict:
    scraper = registry.get(platform)
    board = Board(board_id=f"{platform}:{slug}", platform=platform, slug=slug, url="")
    ctx = ScrapeContext(http=UrllibClient())
    result = await scraper.fetch(board, ctx)
    # blocking CH insert → thread so the worker loop isn't stalled
    await asyncio.to_thread(_persist, result, run_id)
    return result.summary()
