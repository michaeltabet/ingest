"""Personio. Facts ported from atlas-kt PersonioScraper.kt + verified live.

One-shot: GET https://{slug}.jobs.personio.com/search.json returns every
position WITH its `description` inline — no detail call (the /job/{id}.json
path 404s; the JD is already in the list response).
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import OneShotScraper
from ingest.utils.normalize import digest_json


class PersonioScraper(OneShotScraper):
    platform = "personio"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://{board.slug}.jobs.personio.com/search.json")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"[]")
        jobs = data if isinstance(data, list) else data.get("positions", [])
        stubs = [Stub(digest=digest_json({"id": j.get("id")}), external_id=str(j.get("id")))
                 for j in jobs if j.get("id") is not None]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)
