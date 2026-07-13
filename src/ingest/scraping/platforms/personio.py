"""Personio. Facts ported from atlas-kt PersonioScraper.kt.

Career hosts: https://{slug}.jobs.personio.com (international; .de legacy).
We use the JSON path: GET {base}/search.json → list of positions (no JD body;
detail JD lives at {base}/job/{id}.json — fetched per job).
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import PagedDetailScraper
from ingest.utils.normalize import digest_json


class PersonioScraper(PagedDetailScraper):
    platform = "personio"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://{board.slug}.jobs.personio.com/search.json")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"[]")
        jobs = data if isinstance(data, list) else data.get("positions", [])
        stubs = [Stub(digest=digest_json({"id": j.get("id")}),
                      external_id=str(j.get("id")))
                 for j in jobs if j.get("id") is not None]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)  # single page

    def detail_request(self, stub, board: Board) -> Request:
        return Request("GET",
            f"https://{board.slug}.jobs.personio.com/job/{stub.external_id}.json")
