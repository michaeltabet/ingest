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
    complete = (s["reported_total"] == 0) or (s["items_seen"] >= s["reported_total"])
    ok = (s["list_ok"] and s["stubs_seen"] > 0
          and extracted == s["stubs_seen"] and s["details_failed"] == 0
          and complete)
    outcome = "success" if ok else "failure"

    ch.insert("scrape_evidence",
              [[run_id, s["platform"], s["board_id"], s["list_status"], s["pages_fetched"],
                s["stubs_seen"], extracted, s["details_ok"], s["details_failed"],
                s["payloads"], s["bytes_in"], outcome, s["errors"], now]],
              ("run_id", "platform", "board_id", "list_status", "pages_fetched",
               "stubs_seen", "jobs_extracted", "details_ok", "details_failed",
               "payloads", "bytes_in", "outcome", "errors", "run_at"))

    # Fail LOUD: the workflow goes red so a broken board surfaces, never hides.
    if not ok:
        raise RuntimeError(
            f"quality gate FAILED for {s['board_id']}: "
            f"list_ok={s['list_ok']} items_seen={s['items_seen']}/"
            f"{s['reported_total'] or '?'} stubs_seen={s['stubs_seen']} "
            f"jobs_extracted={extracted} "
            f"details_failed={s['details_failed']} complete={complete}")


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
    ctx = ScrapeContext(http=UrllibClient(), sink=sink)
    result = await scraper.fetch(board, ctx)
    await asyncio.to_thread(_persist, result, run_id)
    return result.summary()
