"""Transport (L1). The ONE home for retries, backoff, headers, politeness.

Injected into scrapers via ScrapeContext — never constructed by a scraper.
Tests inject FixtureClient and replay saved responses; nothing hits the net.
When *how bytes move* breaks (a header, a rate cap, a TLS quirk), the fix is
here and only here — and it heals every platform at once.
"""
from __future__ import annotations

import asyncio
from typing import Protocol

from ingest.core.models import Request, Response


class HttpClient(Protocol):
    async def send(self, req: Request) -> Response: ...
    async def aclose(self) -> None: ...


class FixtureClient:
    """Replays recorded responses. Powers tests and offline --dry runs.
    Routes: list of ((method, url) -> bool, Response); first match wins."""

    def __init__(self, routes=None):
        self._routes = list(routes or [])
        self.sent = []

    def add(self, predicate, response: Response):
        self._routes.append((predicate, response))
        return self

    async def send(self, req: Request) -> Response:
        self.sent.append(req)
        for predicate, response in self._routes:
            if predicate(req.method, req.url):
                return response
        return Response(status=404, body=b"", headers={})

    async def aclose(self) -> None:
        pass


class UrllibClient:
    """Real HTTP over the stdlib (urllib) — zero dependencies. Blocking work is
    pushed to a thread so it satisfies the async `send` contract. Used by the
    recorder and offline `--dry` runs so they work with no pip installs; the
    production workers use HttpxClient for real concurrency.
    """

    def __init__(self, *, timeout: float = 30.0, default_headers: dict | None = None):
        self._timeout = timeout
        self._headers = default_headers or {"User-Agent": "ingest/0.1 (+hs)"}

    def _blocking(self, req: Request) -> Response:
        import urllib.request
        import urllib.error
        data = None
        headers = dict(self._headers)
        if req.headers:
            headers.update(req.headers)
        if req.json is not None:
            import json as _json
            data = _json.dumps(req.json).encode()
            headers.setdefault("Content-Type", "application/json")
        r = urllib.request.Request(req.url, data=data, headers=headers, method=req.method)
        try:
            with urllib.request.urlopen(r, timeout=self._timeout) as resp:
                return Response(status=resp.status, body=resp.read(),
                                headers=dict(resp.headers))
        except urllib.error.HTTPError as e:
            return Response(status=e.code, body=e.read(), headers=dict(e.headers or {}))

    async def send(self, req: Request) -> Response:
        return await asyncio.to_thread(self._blocking, req)

    async def aclose(self) -> None:
        pass


class HttpxClient:
    """Real client. httpx is imported lazily so core + tests never need it.

    Retries ONLY transport-level errors (conn reset, DNS blip) a couple times —
    this is NOT the HTTP-status retry policy (families handle status; the
    janitor handles cross-run retry). `rps` is a per-client politeness cap that
    binds ALWAYS, independent of the planner.
    """

    def __init__(self, *, timeout: float = 30.0, rps: float | None = None,
                 default_headers: dict | None = None, transport_retries: int = 2):
        import httpx
        self._client = httpx.AsyncClient(timeout=timeout,
                                         headers=default_headers or {},
                                         follow_redirects=True)
        self._min_interval = (1.0 / rps) if rps else 0.0
        self._transport_retries = transport_retries
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def _throttle(self):
        if self._min_interval <= 0:
            return
        async with self._lock:
            loop = asyncio.get_event_loop()
            wait = self._min_interval - (loop.time() - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = loop.time()

    async def send(self, req: Request) -> Response:
        import httpx
        await self._throttle()
        last = None
        for attempt in range(self._transport_retries + 1):
            try:
                r = await self._client.request(req.method, req.url,
                                               json=req.json, headers=req.headers)
                return Response(status=r.status_code, body=r.content,
                                headers=dict(r.headers))
            except httpx.TransportError as exc:
                last = exc
                if attempt < self._transport_retries:
                    await asyncio.sleep(0.5 * (2 ** attempt))
        raise last

    async def aclose(self):
        await self._client.aclose()
