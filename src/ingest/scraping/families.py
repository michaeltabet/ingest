"""The scraping families — THE LOOPS, each written exactly once.

A family implements Scraper.fetch(). A platform subclass supplies FACTS via
the hooks (list_request, parse_list, and for paged+detail, detail_request).
Fix a loop here → every platform in that family heals. This is the atlas-kt
'42-file edit' collapsed to one.

Incremental extraction lives in PagedDetail: details are fetched only for
stubs whose digest is NOT in ctx.known_digests (the seen-set, passed in).
"""
from __future__ import annotations

import asyncio
from abc import abstractmethod

from ingest.core.models import (Board, ListPage, RawPayload, RawResult, Request,
                                Response, now_iso)
from ingest.core.scraper import Scraper
from ingest.core.context import ScrapeContext
from ingest.utils.normalize import sha256_hex


def _payload(board: Board, kind: str, url: str, resp: Response,
             stub_digest=None) -> RawPayload:
    return RawPayload(platform=board.platform, board_id=board.board_id, url=url,
                      kind=kind, http_status=resp.status, body=resp.body,
                      digest=sha256_hex(resp.body), fetched_at=now_iso(),
                      stub_digest=stub_digest)


class _HttpFamily(Scraper):
    """Shared hooks for all HTTP families. Not a public family itself."""

    @abstractmethod
    def list_request(self, board: Board, cursor) -> Request: ...

    @abstractmethod
    def parse_list(self, body: bytes, cursor) -> ListPage:
        """Peek (navigation, not parsing): return stubs + next cursor."""


class OneShotScraper(_HttpFamily):
    """Family 1: one call returns everything (Greenhouse, Lever)."""
    family = "one_shot"

    async def fetch(self, board, ctx: ScrapeContext) -> RawResult:
        res = RawResult(board_id=board.board_id, platform=board.platform)
        req = self.list_request(board, None)
        resp = await ctx.http.send(req)
        res.list_status = resp.status
        res.pages_fetched = 1
        res.bytes_in += len(resp.body)
        res.payloads.append(_payload(board, "list", req.url, resp))
        if resp.ok:
            page = self.parse_list(resp.body, None)
            res.stubs_seen = len(page.stubs)
        return res


class PagedScraper(_HttpFamily):
    """Family 2: walk pages, each page already complete."""
    family = "paged"

    async def fetch(self, board, ctx: ScrapeContext) -> RawResult:
        res = RawResult(board_id=board.board_id, platform=board.platform)
        cursor = None
        while True:
            req = self.list_request(board, cursor)
            resp = await ctx.http.send(req)
            if res.list_status == 0:
                res.list_status = resp.status
            res.pages_fetched += 1
            res.bytes_in += len(resp.body)
            res.payloads.append(_payload(board, "list", req.url, resp))
            if not resp.ok:
                break
            page = self.parse_list(resp.body, cursor)
            res.stubs_seen += len(page.stubs)
            if not page.stubs or page.next_cursor is None:  # empty page = done
                break
            cursor = page.next_cursor
        return res


class PagedDetailScraper(_HttpFamily):
    """Family 3: pages of stubs, then a per-job detail fetch (Workday, iCIMS).

    Incremental: skip a stub whose digest is already in ctx.known_digests.
    detail_concurrency is a Hypothesis default (calibrated), overridable per
    platform.
    """
    family = "paged_detail"
    detail_concurrency = 10   # hypothesis; calibrated from the ledger

    @abstractmethod
    def detail_request(self, stub, board: Board) -> Request: ...

    async def _fetch_detail(self, stub, board, ctx, res_lock, res):
        req = self.detail_request(stub, board)
        try:
            resp = await ctx.http.send(req)
        except Exception as exc:                     # logged-and-skipped, never abort
            async with res_lock:
                res.details_failed += 1
                res.errors.append(f"detail {stub.external_id}: {exc}")
            return
        async with res_lock:
            res.bytes_in += len(resp.body)
            if resp.ok:
                res.details_ok += 1
                res.payloads.append(_payload(board, "detail", req.url, resp,
                                             stub_digest=stub.digest))
            else:
                res.details_failed += 1

    async def fetch(self, board, ctx: ScrapeContext) -> RawResult:
        res = RawResult(board_id=board.board_id, platform=board.platform)
        lock = asyncio.Lock()
        cursor = None
        while True:
            req = self.list_request(board, cursor)
            resp = await ctx.http.send(req)
            if res.list_status == 0:
                res.list_status = resp.status
            res.pages_fetched += 1
            res.bytes_in += len(resp.body)
            res.payloads.append(_payload(board, "list", req.url, resp))
            if not resp.ok:
                break
            page = self.parse_list(resp.body, cursor)
            fresh = [s for s in page.stubs if s.digest not in ctx.known_digests]
            res.stubs_seen += len(page.stubs)
            for i in range(0, len(fresh), self.detail_concurrency):
                chunk = fresh[i:i + self.detail_concurrency]
                await asyncio.gather(*[
                    self._fetch_detail(s, board, ctx, lock, res) for s in chunk])
            if not page.stubs or page.next_cursor is None:
                break
            cursor = page.next_cursor
        return res


class BrowserScraper(Scraper):
    """Family 5: JS-built page; needs a rendered browser (Paradox).
    Reserved — runs on the scrape-browser queue. Implemented when the first
    browser platform is ported."""
    family = "browser"

    async def fetch(self, board, ctx: ScrapeContext) -> RawResult:
        raise NotImplementedError("browser family not implemented yet")
