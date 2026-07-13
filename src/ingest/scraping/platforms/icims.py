"""iCIMS. Facts ported from atlas-kt IcimsScraper.kt.

  LIST   GET https://careers-{slug}.icims.com/sitemap.xml
         — <loc> entries matching /jobs/{id}/{job-slug}/job are the postings
  DETAIL GET {loc}?in_iframe=1
         — the shell page is empty; the iframe HTML embeds a JSON-LD JobPosting

parse_list peeks the sitemap with a regex (navigation, not parsing — the raw
XML is what gets dumped).
"""
from __future__ import annotations

import re

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import PagedDetailScraper
from ingest.utils.normalize import digest_json

_LOC = re.compile(r"<loc>\s*(https?://[^<]*/jobs/(\d+)/[^<]*/job/?)\s*</loc>", re.I)


class IcimsScraper(PagedDetailScraper):
    platform = "icims"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://careers-{board.slug}.icims.com/sitemap.xml")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        text = (body or b"").decode("utf-8", errors="replace")
        stubs = [Stub(digest=digest_json({"id": jid}), external_id=jid, detail_url=url)
                 for url, jid in _LOC.findall(text)]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)  # one sitemap

    def detail_request(self, stub, board: Board) -> Request:
        sep = "&" if "?" in stub.detail_url else "?"
        return Request("GET", f"{stub.detail_url}{sep}in_iframe=1")
