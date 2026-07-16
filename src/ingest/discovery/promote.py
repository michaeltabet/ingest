"""promote — board_candidates → ingest.boards, gated four times.

  0. recency:   board_candidates must hold rows newer than MAX_AGE_DAYS —
                otherwise cc_scan hasn't landed a crawl and we'd just
                re-litigate a stale pool → exit loudly, promote nothing
  1. anti-join: (platform, slug) already in ingest.boards → status 'known'
  2. shape:     scraping.validation.validate_board (the same gate the worker
                trusts) → status 'rejected' + reason
  3. probe:     replay the platform scraper's FIRST list request — same
                method/URL/body, built by the scraper itself via the registry
                (taleo and workday list via POST; a bare GET of their candidate
                URL proves nothing) — then require the page-1 response shape
                (_SHAPES). A parked tenant that 200s with marketing HTML is
                status 'probe_failed:bad_shape', not a board.

Survivors are inserted into ingest.boards. workday/oracle land with enabled=0:
workday needs per-tenant ramp and oracle is fleet-disabled today — flipping
those on is a deliberate act, not a side effect of discovery.

Bounded, not silent: at most PROBE_LIMIT (default 250) candidates per platform
are probed per run; the remainder stay status 'candidate' and the summary says
exactly how many were deferred. The monthly cadence drains the pool. A tenant
seen in two crawls is probed once per run ('dup_crawl' in the summary).

Env: CH_* (house vars), PROBE_LIMIT, PROBE_CONCURRENCY (default 8),
     MAX_AGE_DAYS (default 40), DRY_RUN=1 → classify + print, write nothing.
"""
from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from ingest.core.models import Board
from ingest.discovery.records import BoardCandidate
from ingest.scraping import registry
from ingest.scraping.validation import validate_board
from ingest.utils.clickhouse import ConnectClickHouse

# discovery finds these; the nightly worker ramps them (BOARDS_LIMIT); humans
# enable workday/oracle deliberately.
_DISABLED_ON_INSERT = frozenset({"workday", "oracle"})

_BOARD_COLS = ("platform", "slug", "company", "board_id", "url", "enabled")
_CAND_COLS = ("source", "crawl", "platform", "slug", "url", "n_urls", "status", "reason")


# what page 1 of a real scrape must contain — keys mirror each platform's
# parse_list in scraping/platforms/*.py. Empty boards pass (key present, zero
# jobs); redirects to a parked/marketing page do not.
def _json(body: bytes):
    return json.loads(body or b"null")


_SHAPES = {
    "greenhouse":      lambda b: "jobs" in _json(b),
    "lever":           lambda b: isinstance(_json(b), list),
    "ashby":           lambda b: "jobs" in _json(b),
    "workable":        lambda b: not {"jobs", "results"}.isdisjoint(_json(b)),
    "smartrecruiters": lambda b: "content" in _json(b),
    "teamtailor":      lambda b: "items" in _json(b),
    "personio":        lambda b: isinstance(_json(b), list) or "positions" in _json(b),
    "recruitee":       lambda b: "offers" in _json(b),
    "bamboohr":        lambda b: "result" in _json(b),
    "icims":           lambda b: b"<urlset" in b,   # sitemap XML, not JSON
    "taleo":           lambda b: "requisitionList" in _json(b),
    "workday":         lambda b: "jobPostings" in _json(b),
    "oracle":          lambda b: "items" in _json(b),
}


def _probe(platform: str, slug: str, url: str) -> tuple[bool, str]:
    """Replay the platform scraper's first list request and shape-check page 1.

    The request comes from the scraper's own list_request (registry lookup),
    so method/URL/body can never drift from what the worker will actually
    send — taleo POSTs a search body, workday POSTs /wday/cxs/..., the rest
    GET. Never raises: one junk candidate is a probe_failed row with the
    exception class as reason, not a dead run.
    """
    import httpx
    try:
        req = registry.get(platform).list_request(
            Board(board_id=f"{platform}:{slug}", platform=platform,
                  slug=slug, url=url), None)
        res = httpx.request(
            req.method, req.url, json=req.json, timeout=10.0,
            follow_redirects=True,
            headers={"User-Agent": "hiredsignal-ingest-discovery/1.0",
                     **(req.headers or {})})
        if res.status_code != 200:
            return False, str(res.status_code)
        try:
            shaped = bool(_SHAPES[platform](res.content))
        except Exception:  # HTML where JSON belongs, wrong envelope, ...
            shaped = False
        return (True, "200") if shaped else (False, "bad_shape")
    except Exception as exc:  # noqa: BLE001 — the reason IS the evidence row
        return False, type(exc).__name__


def run(ch, *, probe_limit: int, concurrency: int, dry_run: bool,
        max_age_days: int = 40) -> dict:
    # gate 0 — stage-1 freshness. Decoupled from the Spark job's wall-clock:
    # all we require is that SOME crawl landed within the window; a dead
    # cc_scan must stop promotion loudly, not let it churn last year's pool.
    recent = ch.query(
        "SELECT count() FROM board_candidates "
        f"WHERE discovered_at >= now() - INTERVAL {int(max_age_days)} DAY")[0][0]
    if not recent:
        raise SystemExit(
            f"promote: no board_candidates rows in the last {max_age_days} days "
            "— cc_scan has not landed a recent crawl; run stage 1 first")

    known = {(p, s) for p, s in ch.query(
        "SELECT platform, slug FROM boards")}

    # argMax over (n_urls, url): a workday tenant with several sites in one
    # crawl keeps separate candidate rows, and the group deterministically
    # picks the most-crawled site (url as tiebreak), never an arbitrary one.
    rows = ch.query("""
        SELECT source, crawl, platform, slug,
               argMax(url, (n_urls, url))    AS url,
               max(n_urls)                   AS n_urls,
               argMax(status, discovered_at) AS status
        FROM board_candidates
        GROUP BY source, crawl, platform, slug
        HAVING status = 'candidate'
        ORDER BY platform, n_urls DESC, slug
    """)
    if not rows and not known:
        raise SystemExit("promote: boards AND board_candidates empty — wrong DB?")

    statuses: list[tuple] = []   # board_candidates status rows to append
    new_boards: list[tuple] = []
    summary = {"candidates": len(rows), "known": 0, "rejected": 0,
               "probe_failed": 0, "promoted": 0, "deferred": 0,
               "dup_crawl": 0, "per_platform_promoted": {}}

    to_probe: list[tuple] = []
    probed_per_platform: dict[str, int] = {}
    handled: set[tuple[str, str]] = set()   # (platform, slug) seen this run
    for source, crawl, platform, slug, url, n_urls, _ in rows:
        if (platform, slug) in handled:
            # same tenant surfaced by another crawl; the first (highest
            # n_urls) row already carries it — probe/insert exactly once.
            summary["dup_crawl"] += 1
            continue
        handled.add((platform, slug))
        if (platform, slug) in known:
            statuses.append((source, crawl, platform, slug, url, n_urls,
                             "known", ""))
            summary["known"] += 1
            continue
        ok, reason = validate_board(platform, slug, url)
        if not ok:
            statuses.append((source, crawl, platform, slug, url, n_urls,
                             "rejected", reason))
            summary["rejected"] += 1
            continue
        if probed_per_platform.get(platform, 0) >= probe_limit:
            summary["deferred"] += 1  # stays 'candidate'; next run picks it up
            continue
        probed_per_platform[platform] = probed_per_platform.get(platform, 0) + 1
        to_probe.append((source, crawl, platform, slug, url, n_urls))

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        verdicts = list(pool.map(lambda c: _probe(c[2], c[3], c[4]), to_probe))

    for (source, crawl, platform, slug, url, n_urls), (alive, code) in zip(to_probe, verdicts):
        if not alive:
            statuses.append((source, crawl, platform, slug, url, n_urls,
                             "probe_failed", code))
            summary["probe_failed"] += 1
            continue
        enabled = 0 if platform in _DISABLED_ON_INSERT else 1
        new_boards.append((platform, slug, slug, f"{platform}:{slug}", url, enabled))
        statuses.append((source, crawl, platform, slug, url, n_urls,
                         "promoted", ""))
        summary["promoted"] += 1
        summary["per_platform_promoted"][platform] = \
            summary["per_platform_promoted"].get(platform, 0) + 1

    if not dry_run:
        if new_boards:
            ch.insert("boards", new_boards, _BOARD_COLS)
        if statuses:
            ch.insert("board_candidates", statuses, _CAND_COLS)
    return summary


def main() -> int:
    dry_run = os.environ.get("DRY_RUN", "0").lower() in {"1", "true", "yes"}
    ch = ConnectClickHouse.from_env()
    summary = run(
        ch,
        probe_limit=int(os.environ.get("PROBE_LIMIT", "250")),
        concurrency=int(os.environ.get("PROBE_CONCURRENCY", "8")),
        dry_run=dry_run,
        max_age_days=int(os.environ.get("MAX_AGE_DAYS", "40")),
    )
    print(json.dumps({"event": "promote_done", "dry_run": dry_run,
                      "ddl_owner": BoardCandidate.__table__, **summary}),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
