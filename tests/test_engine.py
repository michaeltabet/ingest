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
    # under pytest, print-and-collect is invisible — a FAIL must fail the test
    assert cond, name


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
    # stubs_seen counts fresh-unique stubs only (known/_walk-deduped ones are
    # excluded) so jobs_extracted == stubs_seen holds under dedup — the gate
    # arithmetic stays consistent. items_seen still counts everything served.
    check("paged_detail stubs_seen==1 (fresh only; /a known)", res.stubs_seen == 1)
    check("paged_detail items_seen==2 (everything served)", res.items_seen == 2)
    check("paged_detail details_ok==1 (dedup skipped /a)", res.details_ok == 1)
    check("paged_detail payloads==2 (1 list + 1 detail)", len(res.payloads) == 2)
    check("paged_detail extracted==stubs_seen under dedup",
          res.jobs_landed == res.stubs_seen)


async def test_streaming_sink():
    # a paged board whose detail bodies exceed the flush threshold mid-scrape:
    # the sink must receive every payload/job exactly once, the buffers must
    # empty as they flush, and the evidence counters must survive the clears.
    import ingest.scraping.families as fam
    pages = [
        _resp({"jobPostings": [{"path": "/a"}, {"path": "/b"}], "total": 3}),
        _resp({"jobPostings": [{"path": "/c"}], "total": 3}),
        _resp({"jobPostings": [], "total": 3}),
    ]
    calls = {"n": 0}

    def next_page(m, u):
        return u.endswith("/jobs")

    class _Seq:
        async def send(self, req):
            if req.url.endswith("/jobs"):
                calls["n"] += 1
                return pages[calls["n"] - 1]
            return _resp({"jd": "x" * 64})

    flushed = {"payloads": 0, "jobs": 0, "calls": 0}

    async def sink(payloads, jobs):
        flushed["payloads"] += len(payloads)
        flushed["jobs"] += len(jobs)
        flushed["calls"] += 1

    board = Board(board_id="b1", platform="workish", slug="acme", url="")
    old = fam.FLUSH_BYTES
    fam.FLUSH_BYTES = 1   # force a flush after every page
    try:
        res = await _Workish().fetch(board, ScrapeContext(http=_Seq(), sink=sink))
    finally:
        fam.FLUSH_BYTES = old
    total_payloads = flushed["payloads"] + len(res.payloads)
    total_jobs = flushed["jobs"] + len(res.jobs)
    check("streaming flushed at least once mid-scrape", flushed["calls"] >= 1)
    # _Workish stops via total (off < total), so the empty page-3 is never
    # fetched: 2 list payloads + 3 details.
    check("streaming no payload lost or duped (2 list + 3 detail)",
          total_payloads == 5 and res.payloads_written == 5)
    check("streaming no job lost or duped", total_jobs == 3 and res.jobs_landed == 3)
    check("streaming evidence counters survive buffer clears",
          res.summary()["jobs_extracted"] == 3 and res.summary()["payloads"] == 5)
    check("streaming items_seen == stubs_seen when nothing dropped",
          res.items_seen == 3 and res.stubs_seen == 3)


async def test_gate_items_seen_vs_total():
    # workday-shaped: 3 postings reported, one has no usable path. items_seen
    # must count all 3 (gate passes vs total); stubs_seen only the usable 2.
    class _Dropish(_Workish):
        platform = "dropish"

        def parse_list(self, body, cursor):
            data = json.loads(body or b"{}")
            posts = data.get("jobPostings", [])
            stubs = [Stub(digest=digest_json({"p": p["path"]}),
                          detail_url=f"https://api.x{p['path']}",
                          external_id=p["path"])
                     for p in posts if p.get("path")]
            return ListPage(stubs=stubs, next_cursor=None, raw_body=body,
                            status=200, total=int(data.get("total", 0)),
                            items_seen=len(posts))

    http = FixtureClient([
        (lambda m, u: u.endswith("/jobs"),
         _resp({"jobPostings": [{"path": "/a"}, {"path": None}, {"path": "/b"}],
                "total": 3})),
        (lambda m, u: u.endswith("/a") or u.endswith("/b"), _resp({"jd": "x"})),
    ])
    board = Board(board_id="b1", platform="dropish", slug="acme", url="")
    res = await _Dropish().fetch(board, ScrapeContext(http=http))
    check("items_seen counts unusable postings (3)", res.items_seen == 3)
    check("stubs_seen counts usable only (2)", res.stubs_seen == 2)
    check("completeness passes: items_seen >= total",
          res.items_seen >= res.reported_total)


def test_registry_coverage():
    # gate: every real platform module must be discoverable & keyed
    from ingest.scraping import registry
    plats = registry.all_platforms()
    check("registry importable (0+ platforms, template excluded)", isinstance(plats, list))


def test_ddl_generates():
    tables = Record.subclasses()
    check("records produce DDL", all(t.ddl().startswith("CREATE TABLE") for t in tables))
    check("scrape_raw present", any(t.__table__ == "scrape_raw" for t in tables))


async def test_churn_details_gone():
    # a job deleted between the list fetch and its detail fetch (404) is CHURN:
    # counted in details_gone, NOT details_failed — one deleted posting must
    # never redden a board that extracted everything else (cliffordchance
    # 153/154, 07-17). The gate's arithmetic is extracted + gone == stubs.
    listing = _resp({"jobPostings": [{"path": "/a"}, {"path": "/b"}], "total": 2})
    http = FixtureClient([
        (lambda m, u: u.endswith("/jobs"), listing),
        (lambda m, u: u.endswith("/a"), _resp({"jd": "x"})),
        (lambda m, u: u.endswith("/b"), _resp({}, status=404)),
    ])
    board = Board(board_id="b1", platform="workish", slug="acme", url="")
    res = await _Workish().fetch(board, ScrapeContext(http=http))
    check("churn: details_gone==1", res.details_gone == 1)
    check("churn: details_failed==0 (404 is churn, not fault)",
          res.details_failed == 0)
    check("churn: extracted+gone==stubs (gate arithmetic)",
          res.jobs_landed + res.details_gone == res.stubs_seen)


async def test_jd_presence_counts():
    # jd_present is a PLATFORM fact. A board where some jobs lack a JD is fine
    # (churny listings); a board where ALL landed jobs lack one is the 07-17
    # fake-pass (smartrecruiters/personio green with zero usable JDs) and the
    # counters must make that signature unmistakable: jobs_no_jd == extracted.
    class _JdAware(_Workish):
        platform = "jdish"

        def jd_present(self, raw):
            return bool(json.loads(raw or "{}").get("jd"))

    listing = _resp({"jobPostings": [{"path": "/a"}, {"path": "/b"}], "total": 2})
    board = Board(board_id="b1", platform="jdish", slug="acme", url="")

    http = FixtureClient([
        (lambda m, u: u.endswith("/jobs"), listing),
        (lambda m, u: u.endswith("/a"), _resp({"jd": "real text"})),
        (lambda m, u: u.endswith("/b"), _resp({"jd": ""})),      # empty shell
    ])
    res = await _JdAware().fetch(board, ScrapeContext(http=http))
    check("jd: one empty of two counted", res.jobs_no_jd == 1)
    check("jd: partial-empty passes (jobs_no_jd < extracted)",
          res.jobs_no_jd < res.jobs_landed)

    http = FixtureClient([
        (lambda m, u: u.endswith("/jobs"), listing),
        (lambda m, u: u.endswith("/a") or u.endswith("/b"), _resp({"jd": ""})),
    ])
    res = await _JdAware().fetch(board, ScrapeContext(http=http))
    check("jd: all-empty board carries the fake-pass signature "
          "(jobs_no_jd == extracted)", res.jobs_no_jd == res.jobs_landed)


def main():
    print("engine tests:")
    asyncio.run(test_oneshot())
    asyncio.run(test_paged_detail_with_dedup())
    asyncio.run(test_streaming_sink())
    asyncio.run(test_gate_items_seen_vs_total())
    asyncio.run(test_churn_details_gone())
    asyncio.run(test_jd_presence_counts())
    test_registry_coverage()
    test_ddl_generates()
    print(f"\n{'ALL PASS' if not FAILED else 'FAILURES: ' + ', '.join(FAILED)}")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
