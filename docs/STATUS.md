# ingest — STATUS & NEXT STEPS

_Last verified: 2026-07-13, from the ClickHouse ledger (`ingest.scrape_evidence`)._
_Everything below is measured, not asserted. When in doubt, re-query the ledger._

---

## Where it actually is

A full run of all 13 platforms is executing against **production Temporal**
(namespace `ingest`) and **production ClickHouse** (database `ingest`), driven
by a **single local worker on the Mac** (NOT the cluster — see Known Issues).

**Total (this run):** ~**652 boards attempted, 536 succeeded, 116 failed** out
of a ~5,576-board target. That is **~11%** of the fleet. The run is NOT
complete — one worker is slow; it keeps grinding.

### Per platform (target | attempted | ok | fail)

| platform | target | attempted | ok | fail | state |
|---|--:|--:|--:|--:|---|
| ashby | 965 | 68 | 64 | 4 | scraping |
| lever | 864 | 74 | 59 | 15 | scraping |
| smartrecruiters | 679 | 71 | 65 | 6 | scraping |
| greenhouse | 532 | 83 | 72 | 11 | scraping |
| teamtailor | 509 | 80 | 67 | 13 | scraping |
| personio | 508 | 61 | 49 | 12 | scraping (fixed to one-shot) |
| recruitee | 300 | 82 | 71 | 11 | scraping |
| workable | 210 | 44 | 35 | 9 | scraping |
| bamboohr | 152 | 34 | 27 | 7 | scraping |
| icims | 131 | 48 | 27 | 21 | scraping — **~44% fail, investigate** |
| **workday** | 470 | 6 | **0** | 6 | **BLOCKED** (site name) |
| **taleo** | 176 | 1 | **0** | 1 | **BLOCKED** (session cookie) |
| **oracle** | 80 | 0 | **0** | 0 | **BLOCKED** (host + siteNumber) |

**Criteria reminder (Michael):** all 13 platforms scrape ALL their boards' jobs,
no fake positives. Against that: **10 platforms partially proven, 3 at zero,
run ~11% through.** Not met yet.

---

## Blocked platforms — the real fix each needs (deferred, do later)

### workday (470 boards) — needs the career-SITE name, not just the host
- Host is resolvable (`{slug}.wd{N}.myworkdayjobs.com`) — a resolver did 219/470
  (`scripts/resolve_workday_hosts.py`). **But the host alone is not enough.**
- The API path is `/wday/cxs/{tenant}/{SITE}/jobs` and **SITE is almost never
  the slug** (e.g. `abbott.wd1` returns HTTP 422 for `abbott/abbott`). SITE is a
  per-tenant career-site id (`External`, `Careers`, `{tenant}_careers`,
  `en-US/...`).
- ⚠️ The resolver counted 422 responses as "resolved" — so the 219 stored URLs
  include hosts whose SITE path is still wrong. **Those would be fake positives
  if trusted.** Fix the resolver to require a 200 with real `jobPostings`.
- Real fix: resolve SITE per tenant — either from the original full board URL
  (if it exists anywhere) or by parsing each tenant's careers landing page.

### oracle (80) — needs tenant host + siteNumber
- URL is `https://{host}/hcmUI/CandidateExperience/en/sites/{siteNumber}`; the
  bare slug is not enough. Same "load/resolve the full board URL" shape as workday.

### taleo (176) — needs a session cookie
- `searchjobs` POST returns HTTP 200 but empty for every tenant unless a session
  cookie is seeded by first GET-ing the careersection page. The HTTP client has
  no cookie jar. Fix: add cookie support to `utils/http.py` (or a taleo-specific
  session step), then the existing POST works.

---

## Known issues to clean up

1. **workday duplicate board rows.** The resolver re-inserted 219 rows into
   `ingest.boards`; `ReplacingMergeTree` merges lazily, so counts are inflated
   (workday shows 689 not 470). Run `OPTIMIZE TABLE ingest.boards FINAL` or
   dedup-on-read (`GROUP BY platform, slug`).
2. **The run is on the Mac, not the cluster.** The deployed `ingest-worker`
   Deployment (ns `hiredsignal`) has an OLDER bundle missing the fixes below, so
   it cannot carry the run. Ship the current bundle to gitops
   (`apps/ingest-worker/ingest-source.tgz`) so cluster pods take over — that is
   what makes the run fast and unattended.
3. **icims ~44% fail rate** — worth a look; likely some sitemaps 404 or the
   iframe detail path differs per tenant.
4. **No fake-positive audit yet.** `outcome='success'` currently means
   `list_ok AND stubs_seen>0 AND details_failed==0`. Confirm `stubs_seen` counts
   real job entries per platform (a 200 with a non-job JSON shape must not count).

---

## Fixes already shipped this session (in `hs/ingest` main)

- Registered `PlatformRun` on the worker (was only `ScrapeBoard` → all 13
  parents failed instantly).
- Fresh ClickHouse client per write (shared client → concurrent-session errors).
- personio → one-shot (was fetching a 404 detail URL; JD is inline).
- `id_reuse_policy=TERMINATE_IF_RUNNING` so re-runs replace cleanly.
- Activity looks up `board.url` from `ingest.boards` (for workday/oracle).

---

## Next steps (priority order)

1. **Let the current run finish** and re-read the per-platform table — that is
   the real coverage of the 10 working platforms.
2. **Ship the fixed bundle to the cluster** (gitops) so pods (not the Mac) run
   it, at real concurrency. Then raise/remove `BOARDS_LIMIT`.
3. **Fix the workday resolver** to require a real 200 (kill fake positives),
   then resolve the SITE name per tenant.
4. **oracle**: load/resolve full board URLs.
5. **taleo**: add a cookie jar to the HTTP client.
6. **Then** the downstream layers that are still skeleton: Airflow gates/janitor/
   calibration, Kafka+Debezium CDC → `temporal_meta`, and the Bronze→Silver parse.

## How to check state yourself (the observability)

```sql
-- per-platform coverage, latest run
SELECT b.platform, b.t AS target, e.a AS attempted, e.ok, e.fail
FROM (SELECT platform, count() t FROM ingest.boards GROUP BY platform) b
LEFT JOIN (SELECT platform, count() a, countIf(outcome='success') ok,
           countIf(outcome='failure') fail FROM ingest.scrape_evidence
           WHERE run_at > now() - INTERVAL 2 HOUR GROUP BY platform) e
USING platform ORDER BY b.t DESC;
```
Temporal UI: `https://temporal.tail05f41d.ts.net` → namespace `ingest`.
