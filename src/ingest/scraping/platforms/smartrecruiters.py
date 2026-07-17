"""SmartRecruiters. Facts ported from atlas-kt SmartrecruitersScraper.kt.

paged+detail:
  LIST   GET https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=100&offset=N
         → {"totalFound": N, "content": [...stubs...]}
  DETAIL GET https://api.smartrecruiters.com/v1/companies/{slug}/postings/{id}
Pagination: offset advanced by ACTUAL count returned; stop on empty content
or offset >= totalFound.
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import PagedDetailScraper
from ingest.utils.normalize import MIN_JD_CHARS, digest_json

PAGE = 100


class SmartrecruitersScraper(PagedDetailScraper):
    platform = "smartrecruiters"

    def list_request(self, board: Board, cursor) -> Request:
        offset = cursor or 0
        return Request("GET",
            f"https://api.smartrecruiters.com/v1/companies/{board.slug}/postings"
            f"?limit={PAGE}&offset={offset}")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"{}")
        content = data.get("content", [])
        stubs = [Stub(digest=digest_json({"id": j.get("id"), "r": j.get("releasedDate")}),
                      external_id=str(j.get("id")),
                      detail_url=str(j.get("id")))       # detail built from id
                 for j in content]
        offset = (cursor or 0) + len(content)
        nxt = offset if content and offset < data.get("totalFound", 0) else None
        return ListPage(stubs=stubs, next_cursor=nxt, raw_body=body, status=200)

    def detail_request(self, stub, board: Board) -> Request:
        return Request("GET",
            f"https://api.smartrecruiters.com/v1/companies/{board.slug}/postings/{stub.external_id}")

    def jd_present(self, raw: str) -> bool:
        try:
            d = json.loads(raw or "{}")
        except Exception:
            return True
        secs = (d.get("jobAd") or {}).get("sections") or {}
        return len(str((secs.get("jobDescription") or {}).get("text", "")).strip()) > MIN_JD_CHARS
