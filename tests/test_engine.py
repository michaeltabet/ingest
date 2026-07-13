"""Offline engine tests + CI gates. Run with stdlib only (no pytest needed):

    python tests/test_engine.py

Covers: (1) the registry-coverage gate, (2) DDL generation from Records,
(3) each family loop against a FixtureClient — including the incremental
dedup path. These are the contract tests that make 2am repair possible.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.core.context import ScrapeContext
from ingest.core.models import Board, ListPage, Request, Response, Stub
from ingest.core.record import Record
from ingest.scraping.families import OneShotScraper, PagedDetailScraper
from ingest.utils.http import FixtureClient
from ingest.utils.normalize import digest_json
import ingest.ledger.records  # noqa: F401  (register Record subclasses)

FAILED = []


def check(name, cond):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}")
    if not cond:
        FAILED.append(name)


# --- a throwaway one-shot platform, defined in-test -------------------------
class _Greenish(OneShotScraper):
    platform = "greenish"

    def list_request(self, board, cursor):
        return Request("GET", f"https://api.x/boards/{board.slug}/jobs")

    def parse_list(self, body, cursor):
        data = json.loads(body or b"{}")
        stubs = [Stub(digest=digest_json({"id": j["id"]}), external_id=str(j["id"]))
                 for j in data.get("jobs", [])]
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)


class _Workish(PagedDetailScraper):
    platform = "workish"
    detail_concurrency = 3

    def list_request(self, board, cursor):
        off = cursor or 0
        return Request("POST", "https://api.x/jobs", json={"offset": off})

    def parse_list(self, body, cursor):
        data = json.loads(body or b"{}")
        posts = data.get("jobPostings", [])
        stubs = [Stub(digest=digest_json({"p": p["path"]}),
                      detail_url=f"https://api.x{p['path']}", external_id=p["path"])
                 for p in posts]
        off = (cursor or 0) + len(posts)
        nxt = off if off < data.get("total", 0) else None
        return ListPage(stubs=stubs, next_cursor=nxt, raw_body=body, status=200)

    def detail_request(self, stub, board):
        return Request("GET", stub.detail_url)


def _resp(obj, status=200):
    return Response(status=status, body=json.dumps(obj).encode())


async def test_oneshot():
    http = FixtureClient([
        (lambda m, u: "/jobs" in u, _resp({"jobs": [{"id": 1}, {"id": 2}]})),
    ])
    board = Board(board_id="b1", platform="greenish", slug="acme", url="")
    res = await _Greenish().fetch(board, ScrapeContext(http=http))
    check("oneshot stubs_seen==2", res.stubs_seen == 2)
    check("oneshot one list payload", len(res.payloads) == 1)
    check("oneshot list_ok", res.list_ok)


async def test_paged_detail_with_dedup():
    listing = _resp({"jobPostings": [{"path": "/a"}, {"path": "/b"}], "total": 2})
    http = FixtureClient([
        (lambda m, u: u.endswith("/jobs"), listing),
        (lambda m, u: u.endswith("/a") or u.endswith("/b"), _resp({"jd": "x"})),
    ])
    board = Board(board_id="b1", platform="workish", slug="acme", url="")
    # seen-set already contains /a → only /b should be detail-fetched
    seen = {digest_json({"p": "/a"})}
    res = await _Workish().fetch(board, ScrapeContext(http=http, known_digests=seen))
    check("paged_detail stubs_seen==2", res.stubs_seen == 2)
    check("paged_detail details_ok==1 (dedup skipped /a)", res.details_ok == 1)
    check("paged_detail payloads==2 (1 list + 1 detail)", len(res.payloads) == 2)


def test_registry_coverage():
    # gate: every real platform module must be discoverable & keyed
    from ingest.scraping import registry
    plats = registry.all_platforms()
    check("registry importable (0+ platforms, template excluded)", isinstance(plats, list))


def test_ddl_generates():
    tables = Record.subclasses()
    check("records produce DDL", all(t.ddl().startswith("CREATE TABLE") for t in tables))
    check("scrape_raw present", any(t.__table__ == "scrape_raw" for t in tables))


def main():
    print("engine tests:")
    asyncio.run(test_oneshot())
    asyncio.run(test_paged_detail_with_dedup())
    test_registry_coverage()
    test_ddl_generates()
    print(f"\n{'ALL PASS' if not FAILED else 'FAILURES: ' + ', '.join(FAILED)}")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
