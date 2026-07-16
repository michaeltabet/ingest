"""Ashby. Facts ported from atlas-kt AshbyScraper.kt.

One-shot: GET posting-api/job-board/{slug} → {"jobs": [...]}.
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import OneShotScraper
from ingest.utils.normalize import digest_json, raw_json


class AshbyScraper(OneShotScraper):
    platform = "ashby"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://api.ashbyhq.com/posting-api/job-board/{board.slug}")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        jobs = json.loads(body or b"{}").get("jobs", [])
        stubs = [Stub(digest=digest_json({"id": j.get("id"), "p": j.get("publishedDate")}),
                      external_id=str(j.get("id")), raw=raw_json(j)) for j in jobs]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)
