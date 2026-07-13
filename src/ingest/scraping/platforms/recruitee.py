"""Recruitee. Facts ported from atlas-kt RecruiteeScraper.kt.

One-shot: GET https://{slug}.recruitee.com/api/offers/ → {"offers":[...]},
full descriptions inline.
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import OneShotScraper
from ingest.utils.normalize import digest_json


class RecruiteeScraper(OneShotScraper):
    platform = "recruitee"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://{board.slug}.recruitee.com/api/offers/")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        offers = json.loads(body or b"{}").get("offers", [])
        stubs = [Stub(digest=digest_json({"id": o.get("id"), "u": o.get("updated_at")}),
                      external_id=str(o.get("id"))) for o in offers]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)
