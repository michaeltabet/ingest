"""The scraping families — factored to the common denominator.

Hierarchy, most-common → most-specific:

    Scraper                     contract: fetch() -> RawResult
      _HttpFamily               HTTP hooks (list_request, parse_list) + the
                                page-walk (_walk) + job landing (_land)   ← COMMON
        ListScraper             land jobs straight from the list           ← shape A
          PagedDetailScraper    + follow each stub to a detail body        ← shape B
        BrowserScraper          render first (reserved)                    ← shape C

Only TWO real HTTP shapes: the job is either IN the list (ListScraper — covers
one-shot as a single page AND paged, same loop) or BEHIND a detail fetch
(PagedDetailScraper). A platform is just FACTS (which URL, how to read it);
"where the raw comes from" is the only thing a shape adds. Fix the loop here →
every platform in that shape heals.
"""
from __future__ import annotations

import asyncio
from abc import abstractmethod

from ingest.core.models import (Board, Job, ListPage, RawPayload, RawResult,
                                Request, Response, now_iso)
from ingest.core.scraper import Scraper
from ingest.core.context import ScrapeContext
from ingest.utils.normalize import sha256_hex


def _payload(board: Board, kind: str, url: str, resp: Response,
             stub_digest=None) -> RawPayload:
    return RawPayload(platform=board.platform, board_id=board.board_id, url=url,
                      kind=kind, http_status=resp.status, body=resp.body,
                      digest=sha256_hex(resp.body), fetched_at=now_iso(),
                      stub_digest=stub_digest)


FLUSH_BYTES = 16 * 1024 * 1024   # flush buffers at ~16MB: bounds peak RAM per
FLUSH_JOBS = 500                 # activity AND keeps CH inserts fat (HDD node
                                 # hates many small inserts). Measured flush-
                                 # moment footprint ≈ threshold + ~2.5x insert
                                 # transient; at 16 slots x 2Gi that must stay
                                 # under ~120Mi/slot — 48MB blew it when flushes
                                 # correlated (startup herds).


class _HttpFamily(Scraper):
    """The common denominator for every HTTP scraper: the page-walk and the
    job-landing. Subclasses supply platform FACTS and say what happens per page.
    """

    @abstractmethod
    def list_request(self, board: Board, cursor) -> Request: ...

    @abstractmethod
    def parse_list(self, body: bytes, cursor) -> ListPage:
        """Peek (navigation, not parsing): return stubs + next cursor.

        A list-shape platform attaches each job's raw JSON to its Stub (Stub.raw);
        a detail-shape platform leaves raw empty and the body arrives via detail.
        """

    # Does this landed body carry a job description? The scraper is the ONE
    # Python home for a platform's shape, so the presence check lives here next
    # to list_request/parse_list — not duplicated into the gate. Default True:
    # an un-overridden platform is not blocked (it can still be caught in
    # silver). Platforms whose JD location is known override this so a body with
    # an EMPTY description is counted and, if a whole board is empty, fails loud.
    def jd_present(self, raw: str) -> bool:
        return True

    # --- landing a job (ELT, no parse) — written ONCE, used by every shape ---
    def _land(self, res: RawResult, external_id: str, raw: str, digest: str) -> None:
        if not self.jd_present(raw):
            res.jobs_no_jd += 1
        res.jobs.append(Job(external_id=external_id or "", raw=raw, digest=digest))
        res.jobs_landed += 1
        res.bytes_buffered += len(raw)

    # --- streaming: hand full buffers to the sink so RAM stays bounded -------
    @staticmethod
    async def _maybe_flush(res: RawResult, ctx: ScrapeContext) -> None:
        """If a sink is injected and the buffers are fat enough, hand them off
        and clear them. Counters (jobs_landed, payloads_written, bytes_in, ...)
        survive — evidence and the gate never depend on the buffers. Without a
        sink (tests, offline repair) behavior is exactly the old buffer-all."""
        if ctx.sink is None:
            return
        if res.bytes_buffered < FLUSH_BYTES and len(res.jobs) < FLUSH_JOBS:
            return
        payloads, jobs = res.payloads, res.jobs
        res.payloads, res.jobs = [], []
        res.bytes_buffered = 0
        await ctx.sink(payloads, jobs)

    def _land_stubs(self, res: RawResult, stubs: list) -> None:
        for s in stubs:
            if s.raw:
                self._land(res, s.external_id, s.raw, s.digest)

    # --- the page-walk — written ONCE. one-shot is just a single-page walk. ---
    async def _walk(self, board, ctx: ScrapeContext, res: RawResult, on_page) -> None:
        """Walk list pages until exhausted, invoking `on_page(page)` for each.
        Handles status, byte accounting, raw list payloads, stub counting, and
        cursor advance — the parts every shape shares."""
        cursor = None
        # in-run seen-set, seeded with the caller's known digests: a digest
        # served twice (pagination shifting under a mutating board) is counted
        # as a DUPE and dropped, so it is never landed or detail-fetched twice
        # — and dupes_seen feeds the completeness gate (dup-inflated pages are
        # evidence the walk missed something, not extra coverage).
        seen = set(ctx.known_digests)
        while True:
            req = self.list_request(board, cursor)
            resp = await ctx.http.send(req)
            if res.list_status == 0:
                res.list_status = resp.status
            res.pages_fetched += 1
            res.bytes_in += len(resp.body)
            res.bytes_buffered += len(resp.body)
            res.payloads.append(_payload(board, "list", req.url, resp))
            res.payloads_written += 1
            if not resp.ok:
                break
            page = self.parse_list(resp.body, cursor)
            res.items_seen += page.items_seen or len(page.stubs)
            raw_stub_count = len(page.stubs)
            page.stubs = [s for s in page.stubs if s.digest not in seen]
            seen.update(s.digest for s in page.stubs)
            res.dupes_seen += raw_stub_count - len(page.stubs)
            res.stubs_seen += len(page.stubs)
            if page.total:
                # totals can differ page-to-page (workday's flickers) — keep the max
                res.reported_total = max(res.reported_total, page.total)
            await on_page(page)
            await self._maybe_flush(res, ctx)
            if not raw_stub_count or page.next_cursor is None:   # empty/last page = done
                break
            cursor = page.next_cursor


class ListScraper(_HttpFamily):
    """Shape A: the job is IN the list. Covers one-shot (single page, cursor
    always None) and paged (multi-page) with the SAME loop."""
    family = "list"

    async def fetch(self, board, ctx: ScrapeContext) -> RawResult:
        res = RawResult(board_id=board.board_id, platform=board.platform)

        async def on_page(page):
            self._land_stubs(res, page.stubs)

        await self._walk(board, ctx, res, on_page)
        return res


# Back-compat aliases: platforms still declare their intent (one-shot vs paged),
# but both are the same loop now. No platform file needs to change.
OneShotScraper = ListScraper
PagedScraper = ListScraper


class PagedDetailScraper(ListScraper):
    """Shape B: pages of stubs, then a per-job DETAIL fetch (Workday, iCIMS,
    Oracle, ...). Reuses the page-walk; the only addition is following each
    fresh stub to its detail body, which IS the job."""
    family = "paged_detail"
    detail_concurrency = 50   # per-page cap; was 10000 ("no cap") — a latent
                              # socket bomb the day a pooled client replaces
                              # urllib's thread ceiling. 50 = the Hypothesis
                              # physics bound (calibration.DetailConcurrency
                              # hi=50); the weekly calibrate DAG tunes DOWN
                              # from here, never above it.

    @abstractmethod
    def detail_request(self, stub, board: Board) -> Request: ...

    async def _fetch_detail(self, stub, board, ctx, lock, res):
        req = self.detail_request(stub, board)
        try:
            resp = await ctx.http.send(req)
        except Exception as exc:                     # logged-and-skipped, never abort
            async with lock:
                res.details_failed += 1
                res.errors.append(f"detail {stub.external_id}: {exc}")
            return
        async with lock:
            res.bytes_in += len(resp.body)
            if resp.ok:
                res.details_ok += 1
                res.bytes_buffered += len(resp.body)
                res.payloads.append(_payload(board, "detail", req.url, resp,
                                             stub_digest=stub.digest))
                res.payloads_written += 1
                # the DETAIL body IS the job — land it raw.
                self._land(res, stub.external_id or "",
                           resp.body.decode("utf-8", "replace"), stub.digest)
            elif resp.status in (404, 410):
                # The job was pulled between the list fetch and this detail
                # fetch. That is CHURN, not a scrape fault — a live board with
                # 15k postings always loses a few mid-run. Counting it as a
                # failure made details_failed==0 unreachable, so the gate threw
                # away boards that had extracted 99.9% of their jobs.
                res.details_gone += 1
            else:
                res.details_failed += 1
                res.errors.append(
                    f"detail {stub.external_id}: HTTP {resp.status}")

    async def fetch(self, board, ctx: ScrapeContext) -> RawResult:
        res = RawResult(board_id=board.board_id, platform=board.platform)
        lock = asyncio.Lock()

        async def on_page(page):
            # page.stubs arrive already deduped by _walk (in-run seen-set,
            # seeded with ctx.known_digests) — every stub here is fresh.
            for i in range(0, len(page.stubs), self.detail_concurrency):
                chunk = page.stubs[i:i + self.detail_concurrency]
                await asyncio.gather(*[
                    self._fetch_detail(s, board, ctx, lock, res) for s in chunk])

        await self._walk(board, ctx, res, on_page)
        return res


class BrowserScraper(Scraper):
    """Shape C: JS-built page; needs a rendered browser (Paradox). Reserved —
    runs on the scrape-browser queue. Same _land contract once implemented."""
    family = "browser"

    async def fetch(self, board, ctx: ScrapeContext) -> RawResult:
        raise NotImplementedError("browser family not implemented yet")
