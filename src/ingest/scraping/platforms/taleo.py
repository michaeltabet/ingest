"""Taleo. Facts ported from atlas-kt TaleoScraper.kt.

  LIST POST https://{slug}.taleo.net/careersection/rest/jobboard/searchjobs?lang=en&portal=1
       body: pageNo (1-indexed) + sort spec
       → {"pagingData": {"totalCount": N}, "requisitionList": [...]}
Stop: empty requisitionList or past totalCount.

Known atlas quirk NOT replicated: some tenants require a session cookie seeded
by GET-ing the careersection listing page first. Tenants that need it will
fail LOUDLY here (evidence row, status != 200 or 0 stubs) — that is the
fail-loud doctrine; cookie support is a transport-level decision to make from
the evidence, not a silent pre-patch.
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import PagedScraper
from ingest.utils.normalize import digest_json


class TaleoScraper(PagedScraper):
    platform = "taleo"

    def list_request(self, board: Board, cursor) -> Request:
        page = cursor or 1
        return Request("POST",
            f"https://{board.slug}.taleo.net/careersection/rest/jobboard/searchjobs?lang=en&portal=1",
            json={"multilineEnabled": False, "sortingSelection":
                  {"sortBySelectionParam": "3", "ascendingSortingOrder": "false"},
                  "fieldData": {"fields": {}, "valid": True},
                  "filterSelectionParam": {"searchFilterSelections": []},
                  "advancedSearchFiltersSelectionParam":
                  {"searchFilterSelections": []}, "pageNo": page},
            headers={"Content-Type": "application/json"})

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"{}")
        reqs = data.get("requisitionList", [])
        stubs = [Stub(digest=digest_json({"c": r.get("contestNo") or r.get("jobId")}),
                      external_id=str(r.get("contestNo") or r.get("jobId")))
                 for r in reqs]
        total = (data.get("pagingData") or {}).get("totalCount", 0)
        page = cursor or 1
        nxt = page + 1 if reqs and page * max(len(reqs), 1) < total else None
        return ListPage(stubs=stubs, next_cursor=nxt, raw_body=body, status=200)
