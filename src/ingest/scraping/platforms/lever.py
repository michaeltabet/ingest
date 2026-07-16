"""Lever. Facts ported from atlas-kt LeverScraper.kt.

One-shot: GET postings/{slug}?mode=json (mode=json is REQUIRED — without it
Lever returns HTML). The response is a JSON ARRAY, not an object.
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import OneShotScraper
from ingest.utils.normalize import digest_json, raw_json


class LeverScraper(OneShotScraper):
    platform = "lever"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://api.lever.co/v0/postings/{board.slug}?mode=json")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"[]")
        jobs = data if isinstance(data, list) else []
        stubs = [Stub(digest=digest_json({"id": j.get("id"), "t": j.get("updatedAt")}),
                      external_id=str(j.get("id")), raw=raw_json(j)) for j in jobs]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)
