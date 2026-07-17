"""Temporal activities — the DIRTY side (network, IO). Real @activity.defn.

scrape_board runs the platform scraper against a live board, writes the raw
payloads to ClickHouse (scrape_raw) and the evidence row (scrape_evidence)
DIRECTLY, and returns ONLY the evidence summary. Raw bytes never travel
through Temporal (the 2MB law).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from temporalio import activity

from ingest.core.context import ScrapeContext
from ingest.core.models import Board
from ingest.scraping import registry
from ingest.utils.http import UrllibClient


def _http_client():
    """Async httpx in production — 20 details in flight are 20 sockets, not 20
    threads. The threaded urllib fallback (no pip installs; recorder/--dry) can
    NOT meet the 45m activity budget for big boards when 16 activities share
    one ~32-thread default executor (measured: 10K details alone 26.5m, under
    contention 141m+)."""
    try:
        from ingest.utils.http import HttpxClient
        return HttpxClient()
    except ImportError:
        return UrllibClient()

def _clickhouse():
    # fresh client per call — clickhouse-connect sessions can't run concurrent
    # queries, and activities persist in parallel (asyncio.to_thread). A new
    # client per write gets its own session; creation is a cheap HTTP handshake.
    from ingest.utils.clickhouse import ConnectClickHouse
    return ConnectClickHouse.from_env()


def _dt(iso: str) -> datetime:
    try:
        return datetime.fromisoformat(iso)
    except Exception:
        return datetime.now(timezone.utc)


def _insert_payloads(ch, payloads, run_id: str):
    # BRONZE — raw bodies, verbatim. Append-only; safe to write per-batch.
    if not payloads:
        return
    rows = [[p.platform, p.board_id, run_id, p.url, p.kind, p.http_status,
             p.digest, p.stub_digest or "", p.body, _dt(p.fetched_at)]
            for p in payloads]
    ch.insert("scrape_raw", rows,
              ("platform", "board_id", "run_id", "url", "kind", "http_status",
               "digest", "stub_digest", "body", "fetched_at"))


def _insert_jobs(ch, platform: str, board_id: str, jobs, run_id: str,
                 now: datetime):
    # LANDED jobs (ELT) — one raw row per job. Idempotent via ReplacingMergeTree.
    if not jobs:
        return
    jrows = [[platform, board_id, j.external_id, j.raw, j.digest, run_id, now]
             for j in jobs]
    ch.insert("jobs", jrows,
              ("platform", "board_id", "external_id", "raw", "digest",
               "run_id", "fetched_at"))
    # MEMBERSHIP (jobs_runs) — content-free row per (job, run). ReplacingMergeTree
    # collapses `jobs` to latest-writer, so a run that died mid-scrape can
    # overwrite rows from the last GOOD run; membership + the evidence commit
    # marker let silver read "jobs of the latest SUCCESSFUL run" exactly.
    mrows = [[platform, board_id, j.external_id, run_id, now] for j in jobs]
    ch.insert("jobs_runs", mrows,
              ("platform", "board_id", "external_id", "run_id", "fetched_at"))


def _persist(result, run_id: str):
    """Final flush + evidence + gate. The evidence row is the COMMIT MARKER:
    written last, only after every batch landed. Batches from a run whose
    evidence says failure (or never landed) are invisible to silver, which
    gates on evidence.outcome='success'."""
    ch = _clickhouse()
    now = datetime.now(timezone.utc)

    _insert_payloads(ch, result.payloads, run_id)
    _insert_jobs(ch, result.platform, result.board_id, result.jobs, run_id, now)

    # QUALITY GATE — NO FAKE PASS. A board succeeds only if it fetched the list,
    # saw jobs, and EXTRACTED every usable one (count in == count out), with no
    # failed detail fetches. Anything less is a failure, recorded and raised.
    s = result.summary()
    extracted = s["jobs_extracted"]
    # COMPLETENESS: if the board reports a total, we must have SEEN all of it —
    # pagination that stopped short (e.g. workday 60/2000) is a FAILURE. Seen is
    # items_seen (every posting the pages contained), NOT stubs_seen: platforms
    # drop unusable postings (workday: no externalPath) and comparing kept-stubs
    # to total made the gate impossible on those boards (07-15/16 retry storm).
    # Dupes SUBTRACT: a stub served twice means pagination shifted under us and
    # something else was skipped — dup-inflated pages must not count as coverage.
    complete = (s["reported_total"] == 0) or (
        s["items_seen"] - s["dupes_seen"] >= s["reported_total"])
    empty = (s["list_ok"] and s["items_seen"] == 0 and s["reported_total"] == 0
             and s["details_failed"] == 0)
    # Jobs pulled mid-run (detail 404/410) are NOT missing data — they stopped
    # existing. They must count toward coverage or details_failed==0 becomes
    # unreachable on any large live board: cliffordchance landed 153/154 and was
    # thrown away over one deleted posting (07-17). Real faults (5xx/timeout)
    # still redden the board — this narrows the gate, it does not soften it.
    accounted = extracted + s["details_gone"]
    # NO FAKE PASS ON CONTENT: a board that landed jobs but EVERY one of them
    # carries no job description is not a success — it is the exact failure that
    # let smartrecruiters/icims/personio show green with 0 usable JDs for weeks
    # (07-17). One JD-less job is churn; a whole board of them is broken.
    jd_ok = (extracted == 0) or (s["jobs_no_jd"] < extracted)
    ok = (s["list_ok"] and s["stubs_seen"] > 0
          and accounted == s["stubs_seen"] and s["details_failed"] == 0
          and complete and jd_ok)
    # An employer with genuinely zero openings is NOT a failure — recording it
    # as one would put a permanent false alarm in the fail-loud channel.
    outcome = "success" if ok else ("empty" if empty else "failure")

    # The gate's verdict must land IN the evidence row, not only in the raised
    # exception. Without this a gate failure stores errors=[] and the row says
    # "failure" with no reason — the ledger is the debugging surface, and it was
    # blank for every gate failure (329 unexplainable rows on 07-17).
    errors = list(s["errors"])
    if not ok and not empty:
        errors.append(_gate_reason(s, extracted, complete))

    ch.insert("scrape_evidence",
              [[run_id, s["platform"], s["board_id"], s["list_status"], s["pages_fetched"],
                s["stubs_seen"], s["items_seen"], s["dupes_seen"], s["reported_total"],
                extracted, s["details_ok"], s["details_failed"],
                s["details_gone"], s["jobs_no_jd"],
                s["payloads"], s["bytes_in"], outcome, errors, now]],
              ("run_id", "platform", "board_id", "list_status", "pages_fetched",
               "stubs_seen", "items_seen", "dupes_seen", "reported_total",
               "jobs_extracted", "details_ok", "details_failed",
               "details_gone", "jobs_no_jd",
               "payloads", "bytes_in", "outcome", "errors", "run_at"))

    # Fail LOUD: the workflow goes red so a broken board surfaces, never hides.
    # ("empty" completes quietly — zero openings is a fact, not a fault.)
    if not ok and not empty:
        raise RuntimeError(_gate_reason(s, extracted, complete))


def _gate_reason(s: dict, extracted: int, complete: bool) -> str:
    """Why the gate rejected this board. ONE source of truth for the wording:
    the evidence row and the raised error must never disagree."""
    return (f"quality gate FAILED for {s['board_id']}: "
            f"list_ok={s['list_ok']} items_seen={s['items_seen']}"
            f"(-{s['dupes_seen']} dupes)/"
            f"{s['reported_total'] or '?'} stubs_seen={s['stubs_seen']} "
            f"jobs_extracted={extracted} "
            f"details_failed={s['details_failed']} "
            f"details_gone={s['details_gone']} "
            f"jobs_no_jd={s['jobs_no_jd']} complete={complete}")


def _board_url(platform: str, slug: str) -> str:
    """Look up the stored board URL (needed by workday/oracle host resolution)."""
    try:
        ch = _clickhouse()
        rows = ch.query(
            f"SELECT url FROM boards WHERE platform='{platform}' AND slug='{slug}' "
            f"AND url != '' ORDER BY url DESC LIMIT 1")
        return rows[0][0] if rows else ""
    except Exception:
        return ""


@activity.defn
async def scrape_board(platform: str, slug: str, run_id: str) -> dict:
    scraper = registry.get(platform)
    url = await asyncio.to_thread(_board_url, platform, slug)
    board = Board(board_id=f"{platform}:{slug}", platform=platform, slug=slug, url=url)

    def _flush_batch(payloads, jobs):
        # fresh client per flush (sessions can't run concurrent queries)
        ch = _clickhouse()
        _insert_payloads(ch, payloads, run_id)
        _insert_jobs(ch, platform, board.board_id, jobs, run_id,
                     datetime.now(timezone.utc))

    async def sink(payloads, jobs):
        # blocking CH insert → thread so the worker loop isn't stalled
        await asyncio.to_thread(_flush_batch, payloads, jobs)

    # sink = STREAMING: the family flushes fat batches mid-scrape, so a big
    # board's memory is bounded by the flush threshold, not its catalog size.
    http = _http_client()
    ctx = ScrapeContext(http=http, sink=sink)
    try:
        result = await scraper.fetch(board, ctx)
    finally:
        await http.aclose()
    await asyncio.to_thread(_persist, result, run_id)
    return result.summary()
