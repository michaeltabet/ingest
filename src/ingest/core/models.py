"""Contracts (L0). Dumb dataclasses — the shared language every layer speaks.

This module imports nothing from the rest of the system. Nothing here has
logic or IO. See README 'where-is-what': what a scraper returns lives here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- inputs -----------------------------------------------------------------

@dataclass
class Board:
    """One employer board to scrape. A row from boards.Board (the DB)."""
    board_id: str
    platform: str
    slug: str
    url: str
    metadata: dict = field(default_factory=dict)


@dataclass
class Request:
    """A single HTTP request a family loop asks the client to send.

    Method-carrying (not just a URL) because some platforms list via POST
    (Workday) and others via GET (Greenhouse). Families stay method-agnostic.
    """
    method: str
    url: str
    json: dict | None = None
    headers: dict | None = None


@dataclass
class Response:
    status: int
    body: bytes
    headers: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


# --- navigation (platform peeks — NOT parsing) ------------------------------

@dataclass
class Stub:
    """Minimal navigation info a platform extracts from a list response.

    NOT the parsed job — just enough to (a) count, (b) dedup via `digest`,
    (c) find the detail URL for paged+detail families. Job content is already
    in the raw list payload we dump; we never re-store it here.

    `digest` MUST be computed over STABLE fields only. A session id or
    timestamp inside a stub silently kills dedup (every stub looks new).

    `raw` carries this one job's JSON when the LIST already contains it
    (one_shot/paged families) — the family lands it directly. For paged+detail
    families the job body arrives later from the detail fetch, so `raw` stays
    empty here and the family lands the detail body instead.
    """
    digest: str
    detail_url: str | None = None
    external_id: str | None = None
    raw: str = ""


@dataclass
class ListPage:
    """What a platform's parse_list returns for one fetched page.

    `total` = the job count the platform REPORTS for the whole board (e.g.
    workday `total`, oracle `TotalJobsCount`, taleo `pagingData.totalCount`).
    0 = not reported (one-shot lists are complete by definition). The gate
    fails a board whose stubs_seen != a reported total — i.e. pagination that
    didn't get ALL the jobs.
    """
    stubs: list
    next_cursor: object | None
    raw_body: bytes
    status: int
    total: int = 0
    items_seen: int = 0   # postings the page CONTAINED, incl. ones the platform
                          # couldn't turn into stubs (e.g. workday postings with
                          # no externalPath). 0 = same as len(stubs). The
                          # completeness gate compares THIS against `total`;
                          # comparing kept-stubs against total makes the gate
                          # impossible on boards with even one unusable posting.


# --- SILVER: the actual extracted job (this is the objective) ---------------

@dataclass
class Job:
    """One job, LANDED not parsed (ELT). We explode a response into one row per
    job and store that job's raw JSON verbatim. Field extraction (title,
    company, salary, ...) is a later transform over `raw` — NOT done here.

    external_id = the platform's own id (for dedup / idempotent rerun).
    raw         = that single job's JSON, as-is.
    digest      = stable hash for ReplacingMergeTree (rerun replaces, no dupes).
    platform/board_id are stamped at persist time from the Board.
    """
    external_id: str
    raw: str
    digest: str


# --- outputs (evidence-carrying) --------------------------------------------

@dataclass
class RawPayload:
    """One raw response, destined verbatim for ClickHouse scrape_raw (bronze)."""
    platform: str
    board_id: str
    url: str
    kind: str                    # "list" | "detail"
    http_status: int
    body: bytes
    digest: str
    fetched_at: str
    stub_digest: str | None = None


@dataclass
class RawResult:
    """A scraper's return: raw payloads + the run's EVIDENCE.

    Judgment (did this board succeed?) happens OUTSIDE the scraper, against
    this evidence — see the gate chain. Zero rows is distinguishable from
    success at the type level, by construction.
    """
    board_id: str
    platform: str
    payloads: list = field(default_factory=list)   # BUFFER — may be flushed+cleared mid-scrape
    jobs: list = field(default_factory=list)   # SILVER buffer — may be flushed+cleared mid-scrape
    list_status: int = 0
    pages_fetched: int = 0
    stubs_seen: int = 0        # UNIQUE usable stubs (deduped in-run by _walk)
    items_seen: int = 0        # postings seen incl. unusable ones (gate vs reported_total)
    dupes_seen: int = 0        # stubs served more than once (pagination shifted)
    reported_total: int = 0    # what the platform says the board holds (0 = unknown)
    details_ok: int = 0
    details_failed: int = 0    # detail fetch BROKE (5xx/timeout) — a real fault
    details_gone: int = 0      # detail 404/410 — job pulled mid-run; churn, not fault
    bytes_in: int = 0
    errors: list = field(default_factory=list)
    # flush-safe counters: lists above are cleared when a sink flushes them, so
    # evidence NEVER counts from the buffers.
    payloads_written: int = 0  # total payloads produced (buffered + flushed)
    jobs_landed: int = 0       # total jobs landed (buffered + flushed)
    jobs_no_jd: int = 0        # landed jobs whose body carries NO description —
                               # a green board full of these is a fake pass
    bytes_buffered: int = 0    # bytes currently sitting in the buffers

    @property
    def list_ok(self) -> bool:
        return 200 <= self.list_status < 300

    def summary(self) -> dict:
        """The evidence row (minus raw bodies) — what the ledger records.
        Counts come from the flush-safe counters, never the buffers."""
        return {
            "board_id": self.board_id,
            "platform": self.platform,
            "list_status": self.list_status,
            "list_ok": self.list_ok,
            "pages_fetched": self.pages_fetched,
            "stubs_seen": self.stubs_seen,
            "items_seen": self.items_seen,
            "dupes_seen": self.dupes_seen,
            "reported_total": self.reported_total,
            "jobs_extracted": self.jobs_landed,
            "details_ok": self.details_ok,
            "details_failed": self.details_failed,
            "details_gone": self.details_gone,
            "jobs_no_jd": self.jobs_no_jd,
            "payloads": self.payloads_written,
            "bytes_in": self.bytes_in,
            "errors": list(self.errors),
        }
