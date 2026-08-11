"""Temporal activities — the DIRTY side (network, IO). Real @activity.defn.

DOMAIN-NEUTRAL: this module is the engine's execution boundary. It resolves a
Domain by name (pipelines/<domain>.conf), runs that domain's scraper, streams
batches through the domain's Sink, judges the run with the domain's Gate, and
commits the evidence. It knows no table names and no gate rules — those live
in the project's own package (referenced by its spec; a new vertical plugs in
without touching this file). Raw bytes never travel through Temporal (the 2MB law): the sink writes
them directly; only the evidence summary is returned.
"""
from __future__ import annotations

import asyncio
import os

from temporalio import activity

from ingest import domains
from ingest.core.context import ScrapeContext
from ingest.core.errors import GateRefused
from ingest.utils.http import UrllibClient


def _http_client():
    """Async httpx in production — 20 details in flight are 20 sockets, not 20
    threads. The threaded urllib fallback (no pip installs; recorder/--dry) can
    NOT meet the activity budget for big sources when many activities share
    one ~32-thread default executor (measured: 10K details alone 26.5m, under
    contention 141m+)."""
    try:
        from ingest.utils.http import HttpxClient
        return HttpxClient()
    except ImportError:
        return UrllibClient()


def _heartbeat():
    """The progress reporter handed to the scraper.

    Inside an activity this is temporalio's activity.heartbeat, which is the
    ONLY thing that makes a stalled scrape visible: without it an activity that
    stops making progress keeps its 'Started' state until start_to_close
    expires, so the board is neither scraped nor rescheduled for as long as
    that timeout is set. Measured 2026-08-11 with no heartbeat configured:
    2,580 of the night's 16,852 ScrapeSource workflows were still Running 8
    hours in, their activity 'Started', and the same shape on the two nights
    before (3,331 and 3,233) — the 15-20% the nightly coverage misses.

    Outside an activity (tests, CLI, offline repair) it is a no-op, so the
    scrapers stay Temporal-free.
    """
    if not activity.in_activity():
        return lambda *_detail: None
    return lambda *detail: activity.heartbeat(*detail)


async def run_scrape(domain_name: str, platform: str, key: str,
                     run_id: str, http=None) -> dict:
    """One source, end to end: fetch -> stream -> gate -> commit -> verdict.
    Plain async function so tests drive it with an injected http client and an
    in-memory domain (domains.register) — no Temporal, no ClickHouse."""
    dom = domains.get(domain_name)
    scraper = dom.registry.get(platform)
    # calibrated knob: detail concurrency (spec value, clamped by the class
    # ceiling — the library's hi bound is physics, calibration tunes DOWN)
    cal = dom.calibration.get("detail_concurrency", {}).get("value")
    if cal and hasattr(scraper, "detail_concurrency"):
        scraper.detail_concurrency = min(int(cal), scraper.detail_concurrency)
    # the key builder — a blocking lookup for domains that store URLs
    source = await asyncio.to_thread(dom.resolver.resolve, platform, key)
    sink = dom.make_sink()

    async def stream(payloads, items):
        # blocking sink write -> thread so the worker loop isn't stalled
        await asyncio.to_thread(sink.flush, source, payloads, items, run_id)

    # sink = STREAMING: the family flushes fat batches mid-scrape, so a big
    # source's memory is bounded by the flush threshold, not its catalog size.
    own_http = http is None
    http = http or _http_client()
    ctx = ScrapeContext(http=http, sink=stream, heartbeat=_heartbeat())
    try:
        result = await scraper.fetch(source, ctx)
    except Exception as exc:
        # A RAISING scraper must still leave an evidence row — a run that
        # vanishes from the ledger is the one failure mode fail-loud cannot
        # tolerate (the prove campaign surfaced 3 invisible runs, 07-18).
        from ingest.core.models import RawResult
        wreck = RawResult(source_id=source.source_id, platform=platform)
        wreck.errors.append(f"scraper raised: {type(exc).__name__}: {exc}")
        verdict = dom.gate.evaluate(wreck.summary())
        if not verdict.failed:
            from ingest.core.domain import Verdict
            verdict = Verdict("failure", wreck.errors[0])
        await asyncio.to_thread(sink.commit, wreck, verdict, run_id)
        raise
    finally:
        if own_http:
            await http.aclose()

    verdict = dom.gate.evaluate(result.summary())
    # commit BEFORE raising: the evidence row must carry the verdict either
    # way — the ledger is the debugging surface, red workflows only point at it.
    await asyncio.to_thread(sink.commit, result, verdict, run_id)
    # Fail LOUD: the workflow goes red so a broken source surfaces, never
    # hides. ("empty" completes quietly — zero records is a fact, not a fault.)
    if verdict.failed:
        # GateRefused, NOT RuntimeError: retries are bounded by KIND now, and
        # an untyped error counts as transient and would be retried forever.
        # A gate refusal is permanent by definition — the scrape succeeded and
        # the data is bad; running it again produces the same bad data.
        raise GateRefused(verdict.reason)
    return result.summary()


@activity.defn
async def scrape_source(platform: str, key: str, run_id: str,
                        domain: str = "") -> dict:
    # arg order is Temporal API surface. The project name normally arrives
    # explicitly; an EMPTY domain means the caller predates the domain param
    # (in-flight executions started before a deploy — see COMPATIBILITY IS
    # FOREVER in workflows.py) and resolves to this worker's own project.
    # Activities may read env; workflows must not.
    if not domain:
        domain = os.environ.get("INGEST_DOMAIN") or ""
        if not domain:
            raise RuntimeError("empty domain and INGEST_DOMAIN unset — "
                               "cannot resolve project for legacy caller")
    return await run_scrape(domain, platform, key, run_id)
