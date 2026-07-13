"""TEMPLATE — copy this to <platform>.py and fill the FACTS. Delete nothing
structural. A platform file is ~15-40 lines: URLs, cursor shape, quirks. No
loops, no retries, no HTML parsing — those live in the family and utils.

Underscore-prefixed → the registry skips it. Not a real platform.
"""
from __future__ import annotations

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import OneShotScraper  # or Paged / PagedDetail / Browser
from ingest.utils.normalize import digest_json


class TemplateScraper(OneShotScraper):
    platform = "abstract"   # set to the real key, e.g. "greenhouse"

    def list_request(self, board: Board, cursor) -> Request:
        # where the jobs live
        return Request("GET", f"https://api.example.com/boards/{board.slug}/jobs")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        # PEEK only: find stubs (for count + dedup digest), and the next cursor.
        # Digest STABLE fields only.
        import json
        data = json.loads(body or b"{}")
        stubs = [Stub(digest=digest_json({"id": j.get("id")}),
                      external_id=str(j.get("id")))
                 for j in data.get("jobs", [])]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)
