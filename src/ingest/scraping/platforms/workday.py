"""Workday. Facts ported from atlas-kt WorkdayScraper.kt.

paged+detail on tenant-specific hosts:
  LIST   POST https://{host}/wday/cxs/{tenant}/{board}/jobs
              {"limit": N, "offset": N, "searchText": ""}
         → {"total": N, "jobPostings": [{title, externalPath, postedOn, ...}]}
  DETAIL GET  https://{host}/wday/cxs/{tenant}/{board}{externalPath}

Host facts: {tenant}.wd{N}.myworkdayjobs.com — the wd{N} data-centre suffix is
part of the host, NOT of API paths. tenant = first host label; board = first
path segment of the board URL (falls back to tenant).

The board URL must be supplied (board.url or metadata["board_url"]) — the wd{N}
suffix is NOT derivable from the slug. No URL → loud failure, by design.

Quirks preserved: offset advances by len(postings) (last page is short); empty
jobPostings terminates (guards against total>0 with no rows); Accept-Language
header is required by some Cloudflare configs (set at transport level for all
requests — see utils/http defaults when workers are configured).

Deliberate deviation from atlas: atlas used limit=200 with a degrade-to-20 on
HTTP 400 (Ameresco/Circle/FCA reject 200). We use limit=20 ALWAYS — the safe
value for every tenant; costs a few extra list pages, removes the quirk path.
"""
from __future__ import annotations

import json
from urllib.parse import urlparse

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import PagedDetailScraper
from ingest.utils.normalize import digest_json

PAGE = 20


def _parts(board: Board):
    url = board.url or board.metadata.get("board_url", "")
    host = urlparse(url).netloc
    if not host or "myworkdayjobs.com" not in host:
        raise ValueError(f"workday: no usable board url for {board.board_id!r} (got {url!r})")
    tenant = host.split(".")[0]
    path = [p for p in urlparse(url).path.split("/") if p]
    return host, tenant, (path[0] if path else tenant)


class WorkdayScraper(PagedDetailScraper):
    platform = "workday"

    def list_request(self, board: Board, cursor) -> Request:
        host, tenant, bname = _parts(board)
        return Request("POST", f"https://{host}/wday/cxs/{tenant}/{bname}/jobs",
                       json={"limit": PAGE, "offset": cursor or 0, "searchText": ""},
                       headers={"Accept-Language": "en-US,en;q=0.9"})

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"{}")
        postings = data.get("jobPostings") or []
        stubs = [Stub(digest=digest_json({"p": j.get("externalPath")}),
                      external_id=(j.get("externalPath") or "").rsplit("/", 1)[-1],
                      detail_url=j.get("externalPath"))
                 for j in postings if j.get("externalPath")]
        offset = (cursor or 0) + len(postings)
        # Workday's per-page `total` FLICKERS to 0 on later pages, so stopping on
        # `offset < total` quits early (~60/2000). Paginate until an EMPTY page.
        nxt = offset if postings else None
        # items_seen counts EVERY posting the page contained; stubs only the
        # usable ones (externalPath present). Some tenants always carry a few
        # pathless postings — comparing stubs against `total` made the
        # completeness gate impossible for them (the 07-15/16 retry storm).
        return ListPage(stubs=stubs, next_cursor=nxt, raw_body=body, status=200,
                        total=int(data.get("total", 0)), items_seen=len(postings))

    def detail_request(self, stub, board: Board) -> Request:
        host, tenant, bname = _parts(board)
        return Request("GET", f"https://{host}/wday/cxs/{tenant}/{bname}{stub.detail_url}",
                       headers={"Accept-Language": "en-US,en;q=0.9"})
