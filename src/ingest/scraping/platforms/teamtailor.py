"""TeamTailor. Facts ported from atlas-kt TeamtailorScraper.kt.

Host: https://{slug}.teamtailor.com. The public career feed is JSON Feed
(jsonfeed.org): GET /jobs.json → {"items": [{id, title, url, content_html,
_jobposting}, ...]}. Full JD inline (content_html) — one-shot, no detail call.
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import OneShotScraper
from ingest.utils.normalize import MIN_JD_CHARS, digest_json, raw_json


class TeamtailorScraper(OneShotScraper):
    platform = "teamtailor"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://{board.slug}.teamtailor.com/jobs.json")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        items = json.loads(body or b"{}").get("items", [])
        stubs = [Stub(digest=digest_json({"id": j.get("id"), "d": j.get("date_published")}),
                      external_id=str(j.get("id")), raw=raw_json(j)) for j in items]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)

    def jd_present(self, raw: str) -> bool:
        try:
            d = json.loads(raw or "{}")
        except Exception:
            return True
        return len(str(d.get("content_html", "")).strip()) > MIN_JD_CHARS
