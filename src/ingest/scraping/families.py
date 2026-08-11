"""The scraping families — factored to the common denominator.

Hierarchy, most-common → most-specific:

    Scraper                     contract: fetch() -> RawResult
      _HttpFamily               HTTP hooks (list_request, parse_list) + the
                                page-walk (_walk) + item landing (_land)  ← COMMON
        ListScraper             land items straight from the list          ← shape A
          PagedDetailScraper    + follow each stub to a detail body        ← shape B
        BrowserScraper          render first (reserved)                    ← shape C

Only TWO real HTTP shapes: the item is either IN the list (ListScraper — covers
one-shot as a single page AND paged, same loop) or BEHIND a detail fetch
(PagedDetailScraper). A platform is just FACTS (which URL, how to read it);
"where the raw comes from" is the only thing a shape adds. Fix the loop here →
every platform in that shape heals.

THIS FILE IS THE SCRAPER-STYLE LIBRARY: a spec's `[platform.*] type` is one
word from SPEC_TYPES below. A new scrape style tomorrow = a new family class
here + its one-word name in SPEC_TYPES — never logic in a spec.
"""
from __future__ import annotations

import asyncio
from abc import abstractmethod

from ingest.core.models import (Item, ListPage, RawPayload, RawResult,
                                Request, Response, Source, now_iso)
from ingest.core.scraper import Scraper
from ingest.core.context import ScrapeContext
from ingest.utils.normalize import sha256_hex


def _payload(source: Source, kind: str, url: str, resp: Response,
             stub_digest=None) -> RawPayload:
    return RawPayload(platform=source.platform, source_id=source.source_id,
                      url=url, kind=kind, http_status=resp.status,
                      body=resp.body, digest=sha256_hex(resp.body),
                      fetched_at=now_iso(), stub_digest=stub_digest)


# No walk may page forever. A source that re-serves the same page (workday
# aviva: 129 jobs seen 2049 times over ~21 pages) would otherwise loop until
# the 30-day activity timeout, hammering the vendor into rate-limiting us.
MAX_PAGES = 500

FLUSH_BYTES = 16 * 1024 * 1024   # flush buffers at ~16MB: bounds peak RAM per
FLUSH_ITEMS = 500                # activity AND keeps CH inserts fat (HDD node
                                 # hates many small inserts). Measured flush-
                                 # moment footprint ≈ threshold + ~2.5x insert
                                 # transient; at 16 slots x 2Gi that must stay
                                 # under ~120Mi/slot — 48MB blew it when flushes
                                 # correlated (startup herds).


class _HttpFamily(Scraper):
    """The common denominator for every HTTP scraper: the page-walk and the
    item-landing. Subclasses supply platform FACTS and say what happens per page.
    """

    @abstractmethod
    def list_request(self, source: Source, cursor) -> Request: ...

    @abstractmethod
    def parse_list(self, body: bytes, cursor) -> ListPage:
        """Peek (navigation, not parsing): return stubs + next cursor.

        A list-shape platform attaches each item's raw JSON to its Stub
        (Stub.raw); a detail-shape platform leaves raw empty and the body
        arrives via detail.
        """

    # Does this landed body carry the content the project requires? The
    # scraper is the ONE Python home for a platform's
    # shape, so the presence check lives here next to list_request/parse_list —
    # not duplicated into the gate. Default True: an un-overridden platform is
    # not blocked (it can still be caught in silver). Platforms whose content
    # location is known override this so a body with EMPTY content is counted
    # and, if a whole source is empty, fails loud.
    def content_present(self, raw: str) -> bool:
        return True

    # --- landing an item (ELT, no parse) — written ONCE, used by every shape --
    def _land(self, res: RawResult, external_id: str, raw: str, digest: str) -> None:
        if not self.content_present(raw):
            res.items_no_content += 1
        res.items.append(Item(external_id=external_id or "", raw=raw, digest=digest))
        res.items_landed += 1
        res.bytes_buffered += len(raw)

    # --- streaming: hand full buffers to the sink so RAM stays bounded -------
    @staticmethod
    async def _maybe_flush(res: RawResult, ctx: ScrapeContext) -> None:
        """If a sink is injected and the buffers are fat enough, hand them off
        and clear them. Counters (items_landed, payloads_written, bytes_in, ...)
        survive — evidence and the gate never depend on the buffers. Without a
        sink (tests, offline repair) behavior is exactly the old buffer-all."""
        if ctx.sink is None:
            return
        if res.bytes_buffered < FLUSH_BYTES and len(res.items) < FLUSH_ITEMS:
            return
        payloads, items = res.payloads, res.items
        res.payloads, res.items = [], []
        res.bytes_buffered = 0
        await ctx.sink(payloads, items)

    def _land_stubs(self, res: RawResult, stubs: list) -> None:
        for s in stubs:
            if s.raw:
                self._land(res, s.external_id, s.raw, s.digest)

    # --- the page-walk — written ONCE. one-shot is just a single-page walk. ---
    async def _walk(self, source, ctx: ScrapeContext, res: RawResult, on_page) -> None:
        """Walk list pages until exhausted, invoking `on_page(page)` for each.
        Handles status, byte accounting, raw list payloads, stub counting, and
        cursor advance — the parts every shape shares."""
        cursor = None
        # in-run seen-set, seeded with the caller's known digests: a digest
        # served twice (pagination shifting under a mutating source) is counted
        # as a DUPE and dropped, so it is never landed or detail-fetched twice
        # — and dupes_seen feeds the completeness gate (dup-inflated pages are
        # evidence the walk missed something, not extra coverage).
        seen = set(ctx.known_digests)
        while True:
            req = self.list_request(source, cursor)
            resp = await ctx.http.send(req)
            if res.list_status == 0:
                res.list_status = resp.status
            res.pages_fetched += 1
            res.bytes_in += len(resp.body)
            res.bytes_buffered += len(resp.body)
            res.payloads.append(_payload(source, "list", req.url, resp))
            res.payloads_written += 1
            if not resp.ok:
                # A DEEP page failure used to leave list_status at page 1's 200,
                # so list_ok stayed True and a board cut off mid-pagination
                # graded SUCCESS (audit D3, 07-19). Record the failing status:
                # a walk that could not finish is not a healthy list fetch.
                res.errors.append(f"list page {res.pages_fetched} "
                                  f"HTTP {resp.status} (cursor={cursor!r})")
                res.list_status = resp.status
                break
            page = self.parse_list(resp.body, cursor)
            res.items_seen += page.items_seen or len(page.stubs)
            raw_stub_count = len(page.stubs)
            page.stubs = [s for s in page.stubs if s.digest not in seen]
            seen.update(s.digest for s in page.stubs)
            res.dupes_seen += raw_stub_count - len(page.stubs)
            res.stubs_seen += len(page.stubs)
            if page.total:
                # totals can differ page-to-page (some platforms flicker) — keep the max
                res.reported_total = max(res.reported_total, page.total)
            await on_page(page)
            await self._maybe_flush(res, ctx)
            # PROGRESS, reported to whoever is driving this scrape. Every page
            # is at most one 30s HTTP call away from the last one, so a healthy
            # walk reports far more often than any sane heartbeat timeout, and
            # a walk that stops reporting has stopped working.
            ctx.heartbeat(f"page {res.pages_fetched} "
                          f"items {res.items_landed} of {res.reported_total}")
            if not raw_stub_count or page.next_cursor is None:   # empty/last page = done
                break
            if res.pages_fetched >= MAX_PAGES:
                res.errors.append(
                    f"walk stopped at the {MAX_PAGES}-page cap "
                    f"(dupes_seen={res.dupes_seen}) — source is not advancing")
                break
            cursor = page.next_cursor


class ListScraper(_HttpFamily):
    """Shape A: the item is IN the list. Covers one-shot (single page, cursor
    always None) and paged (multi-page) with the SAME loop."""
    family = "list"

    async def fetch(self, source, ctx: ScrapeContext) -> RawResult:
        res = RawResult(source_id=source.source_id, platform=source.platform)

        async def on_page(page):
            self._land_stubs(res, page.stubs)

        await self._walk(source, ctx, res, on_page)
        return res


# Back-compat aliases: platforms still declare their intent (one-shot vs paged),
# but both are the same loop now. No platform file needs to change.
OneShotScraper = ListScraper
PagedScraper = ListScraper


class PagedDetailScraper(ListScraper):
    """Shape B: pages of stubs, then a per-item DETAIL fetch. Reuses the
    page-walk; the only addition is following each
    fresh stub to its detail body, which IS the item."""
    family = "paged_detail"
    detail_concurrency = 50   # per-page cap; was 10000 ("no cap") — a latent
                              # socket bomb the day a pooled client replaces
                              # urllib's thread ceiling. 50 = the Hypothesis
                              # physics bound (calibration.DetailConcurrency
                              # hi=50); the weekly calibrate DAG tunes DOWN
                              # from here, never above it.

    @abstractmethod
    def detail_request(self, stub, source: Source) -> Request: ...

    async def _fetch_detail(self, stub, source, ctx, lock, res):
        req = self.detail_request(stub, source)
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
                res.payloads.append(_payload(source, "detail", req.url, resp,
                                             stub_digest=stub.digest))
                res.payloads_written += 1
                # the DETAIL body IS the item — land it raw.
                self._land(res, stub.external_id or "",
                           resp.body.decode("utf-8", "replace"), stub.digest)
            elif resp.status in (404, 410):
                # The item was pulled between the list fetch and this detail
                # fetch. That is CHURN, not a scrape fault — a live source with
                # 15k postings always loses a few mid-run. Counting it as a
                # failure made details_failed==0 unreachable, so the gate threw
                # away sources that had extracted 99.9% of their items.
                res.details_gone += 1
            else:
                res.details_failed += 1
                res.errors.append(
                    f"detail {stub.external_id}: HTTP {resp.status}")

    async def fetch(self, source, ctx: ScrapeContext) -> RawResult:
        res = RawResult(source_id=source.source_id, platform=source.platform)
        lock = asyncio.Lock()

        async def on_page(page):
            # page.stubs arrive already deduped by _walk (in-run seen-set,
            # seeded with ctx.known_digests) — every stub here is fresh.
            for i in range(0, len(page.stubs), self.detail_concurrency):
                chunk = page.stubs[i:i + self.detail_concurrency]
                await asyncio.gather(*[
                    self._fetch_detail(s, source, ctx, lock, res) for s in chunk])
                # A detail-shape source spends nearly all its time in here: one
                # page of 500 stubs is 10 chunks, and without a report between
                # them the whole page looks like one long silence.
                ctx.heartbeat(f"details {res.details_ok} ok "
                              f"{res.details_failed} failed "
                              f"{res.details_gone} gone")

        await self._walk(source, ctx, res, on_page)
        return res


class BrowserScraper(Scraper):
    """Shape C: JS-built page; needs a rendered browser (Paradox). Reserved —
    runs on the scrape-browser queue. Same _land contract once implemented."""
    family = "browser"

    async def fetch(self, source, ctx: ScrapeContext) -> RawResult:
        raise NotImplementedError("browser family not implemented yet")


# The spec vocabulary: one word in the conf -> a family here. Grows with the
# library; the loader validates every [platform.*] type against this.
SPEC_TYPES = {
    "one_step": ListScraper.family,          # item is IN the list
    "multi_step": PagedDetailScraper.family, # list, then per-item detail
    "browser": BrowserScraper.family,        # rendered page
}
