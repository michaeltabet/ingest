# Fixture provenance — real vs synthetic

A fixture is trustworthy only if RECORDED from a live board via
`python -m ingest.cli record <platform> --slug <slug>`. Synthetic fixtures
prove parsing only and DO NOT satisfy the port gate.

## Recorded from live boards (2026-07-13) — 10/13

| Platform | Family | Live board | stubs |
|---|---|---|---|
| greenhouse | one_shot | samsara | 310 |
| ashby | one_shot | percepta | 13 |
| lever | one_shot | gettyimages | 9 |
| recruitee | one_shot | greatminds | 19 |
| teamtailor | one_shot | xait | 1 |
| workable | one_shot | woozle-research | 2 |
| personio | paged_detail | dci | 25 |
| smartrecruiters | paged_detail | sixt | 552 |
| bamboohr | paged_detail | infinox | 5 |
| icims | paged_detail (sitemap) | axway | 62 |

## Blocked — 3/13 (honest gaps, NOT faked)

| Platform | Blocker | Kind |
|---|---|---|
| workday | needs the `{tenant}.wd{N}.myworkdayjobs.com` host; `wd{N}` varies per tenant and is NOT in runs_board (circle=wd1, verified). Board URL required, or a host-resolution step. | DATA |
| oracle | needs tenant host + `siteNumber` (`/sites/{site}`); not a bare slug. Board URL required. | DATA |
| taleo | `searchjobs` POST returns 200 but empty for every tenant — needs a session cookie seeded by GET-ing the careersection page first (atlas confirmed). Transport has no cookie jar yet. | CAPABILITY |

Blocked platforms FAIL LOUD by doctrine — they produce an evidence row with
0 stubs, not a silent success. Unblocking is tracked work, not a patch.
