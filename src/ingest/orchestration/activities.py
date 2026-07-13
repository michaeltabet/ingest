"""Temporal activities — the DIRTY side (network, IO). Real @activity.defn.

scrape_board runs the platform scraper against a live board and returns ONLY
the evidence summary (counts). In the full system the raw payloads are written
to ClickHouse from inside here; Temporal never carries the bytes (the 2MB law).
"""
from __future__ import annotations

from temporalio import activity

from ingest.core.context import ScrapeContext
from ingest.core.models import Board
from ingest.scraping import registry
from ingest.utils.http import UrllibClient


@activity.defn
async def scrape_board(platform: str, slug: str) -> dict:
    scraper = registry.get(platform)
    board = Board(board_id=f"{platform}:{slug}", platform=platform, slug=slug, url="")
    ctx = ScrapeContext(http=UrllibClient())
    result = await scraper.fetch(board, ctx)
    # TODO(step: ClickHouse): write result.payloads to scrape_raw here.
    # Return evidence only — never the raw bytes.
    return result.summary()
