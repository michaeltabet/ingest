"""Oracle Recruiting Cloud. Facts ported from atlas-kt OracleScraper.kt.

Public REST API (no auth) on the tenant's Oracle Cloud host:
  LIST GET {base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions
           ?onlyData=true&expand=requisitionList.secondaryLocations
           &finder=findReqs;siteNumber={site},facetsList=LOCATIONS%3BWORK_LOCATIONS,limit={n},offset={o}
       → items[0].requisitionList (stubs) + items[0].TotalJobsCount
  DETAIL GET {base}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails
           ?expand=all&onlyData=true&finder=ById;Id="{id}",siteNumber={site}

Needs the tenant base host + siteNumber — carried on the board
(board.url = https://{tenant-host}/hcmUI/CandidateExperience/en/sites/{site});
not derivable from a bare slug. Missing/odd URL → loud failure.
"""
from __future__ import annotations

import json
from urllib.parse import urlparse

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import PagedDetailScraper
from ingest.utils.normalize import digest_json

PAGE = 25


def _parts(board: Board):
    url = board.url or board.metadata.get("board_url", "")
    u = urlparse(url)
    if not u.netloc or "/sites/" not in u.path:
        raise ValueError(f"oracle: no usable board url for {board.board_id!r} (got {url!r})")
    site = [p for p in u.path.split("/") if p][-1]
    return f"https://{u.netloc}", site


class OracleScraper(PagedDetailScraper):
    platform = "oracle"

    def list_request(self, board: Board, cursor) -> Request:
        base, site = _parts(board)
        offset = cursor or 0
        return Request("GET",
            f"{base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
            f"?onlyData=true&finder=findReqs;siteNumber={site},limit={PAGE},offset={offset}")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"{}")
        items = data.get("items") or [{}]
        reqs = items[0].get("requisitionList", []) if items else []
        total = items[0].get("TotalJobsCount", 0) if items else 0
        stubs = [Stub(digest=digest_json({"id": r.get("Id")}), external_id=str(r.get("Id")))
                 for r in reqs if r.get("Id")]
        offset = (cursor or 0) + len(reqs)
        nxt = offset if reqs and offset < total else None
        return ListPage(stubs=stubs, next_cursor=nxt, raw_body=body, status=200,
                        total=int(total or 0))

    def detail_request(self, stub, board: Board) -> Request:
        base, site = _parts(board)
        return Request("GET",
            f"{base}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
            f'?expand=all&onlyData=true&finder=ById;Id="{stub.external_id}",siteNumber={site}')
