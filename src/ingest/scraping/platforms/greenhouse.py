"""Greenhouse. Facts ported from atlas-kt GreenhouseScraper.kt.

One-shot: GET boards-api once with content=true → every job inline, no paging,
no detail calls. The board `slug` is the Greenhouse org token.
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import OneShotScraper
from ingest.utils.normalize import digest_json, raw_json


class GreenhouseScraper(OneShotScraper):
    platform = "greenhouse"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET",
            f"https://boards-api.greenhouse.io/v1/boards/{board.slug}/jobs?content=true")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        # one_shot: the list already holds each job's full JSON — attach it as
        # Stub.raw so the family lands it. No field parsing here (ELT).
        jobs = json.loads(body or b"{}").get("jobs", [])
        stubs = [Stub(digest=digest_json({"id": j.get("id"), "u": j.get("updated_at")}),
                      external_id=str(j.get("id")), raw=raw_json(j))
                 for j in jobs]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)
