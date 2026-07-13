"""BambooHR. Facts ported from atlas-kt BamboohrScraper.kt.

paged+detail (single list page, then a detail call per job):
  LIST   GET https://{slug}.bamboohr.com/careers/list        -> {"result": [...]}
  DETAIL GET https://{slug}.bamboohr.com/careers/{id}/detail

The detail URL is built by detail_request from the stub id + board slug — so
the incremental seen-set skips details we already have (BambooHR list is cheap,
details are the cost).
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import PagedDetailScraper
from ingest.utils.normalize import digest_json


class BamboohrScraper(PagedDetailScraper):
    platform = "bamboohr"

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://{board.slug}.bamboohr.com/careers/list")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        jobs = json.loads(body or b"{}").get("result", [])
        stubs = [Stub(digest=digest_json({"id": j.get("id")}),
                      external_id=str(j.get("id"))) for j in jobs]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)  # single page

    def detail_request(self, stub, board: Board) -> Request:
        return Request("GET",
            f"https://{board.slug}.bamboohr.com/careers/{stub.external_id}/detail")
