"""Workable. Facts ported from atlas-kt WorkableScraper.kt.

One-shot widget fetch (the v3 POST died 2026-03-31; the v1 GET ignores the
company filter). Widget endpoint returns ALL jobs for the slug in one call,
full HTML inline. Envelope: {"name","description","jobs"|"results": [...]}.
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import OneShotScraper
from ingest.utils.normalize import digest_json


class WorkableScraper(OneShotScraper):
    platform = "workable"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET",
            f"https://apply.workable.com/api/v1/widget/accounts/{board.slug}?details=true")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"{}")
        jobs = data.get("jobs") or data.get("results") or []   # fallback preserved
        stubs = [Stub(digest=digest_json({"s": j.get("shortcode") or j.get("id")}),
                      external_id=str(j.get("shortcode") or j.get("id"))) for j in jobs]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)
