# ingest — HiredSignal job-board scraper / ingest

New Python system that acquires job postings from ATS platforms and lands them
raw in ClickHouse. Runs in parallel with the legacy `atlas-kt` (Kotlin) until
per-platform cutover. This repo is **acquisition + ingest only** — parsing,
identity, and serving are downstream.

> Design contract: `docs/SCRAPER-REDESIGN-WHITEPAPER-v3.pdf`
> Build constraints: `docs/BUILD-BRIEF.md`
> **Read both before changing anything.**

## The whole system is a handful of base classes

Everything is a subclass of exactly one of these. Add a subclass file — never
touch plumbing.

| Base (`src/ingest/core/`) | Contract | Subclasses in |
|---|---|---|
| `Record` | owns its ClickHouse schema; DDL is generated from the class | `ledger/records.py` |
| `Scraper` | `(source, ctx) -> RawResult` (raw bytes + evidence counts) | families: `scraping/families.py`; platforms: `scraping/platforms/` |
| `Domain` (+`Gate`/`Sink`/`SourceResolver`) | one vertical: gate rules, landing tables, slug builder | seam: `core/domain.py`; jobs: `domains/jobs/` |
| `Gate` | `check(run) -> GateResult` (validation + a defined failure action) | `ledger/gates.py` |

## Where-is-what (the locality contract — every question, one file)

| "Where is…?" | Home |
|---|---|
| retries / backoff / rate caps / headers / TLS | `utils/http.py` |
| the scraping loop for a family | `scraping/families.py` |
| a platform's URLs, cursor, quirks | `scraping/platforms/<platform>.py` |
| date / HTML / digest utilities | `utils/normalize.py` |
| what a scraper returns | `core/models.py` |
| error taxonomy | `core/errors.py` |
| platform → class mapping | `scraping/registry.py` (auto-discovers) |
| a vertical's wiring (scrapers·gate·sink·slug builder·temporal·k8s) | the SEPARATE `ingest-pipelines` repo, `<project>.json` (see `docs/DOMAINS.md`) |
| every ClickHouse table | `ledger/records.py` (each Record class generates its own DDL) |
| Temporal config / queues / routing | spec `[temporal]` part; precedence env > spec > `orchestration/config.py` defaults |
| per-platform partition (budget, cron, rps, owner) | `boards.Platform` DB row |

## Doctrine (why it's built this way)

- **Extract, don't parse.** Scrapers dump raw bytes + evidence counts. Parsing
  is a later Airflow batch, replayable from raw. Network → scraper; bytes → batch.
- **Fail loud, handle daily.** Temporal: attempts=1, no heartbeat, no guards.
  Breakage is red and visible; the daily Airflow pass + k8s restarts respond.
- **No invented constants.** Every sized value lives in the spec's
  `calibration` part (ingest-pipelines), learned from the ledger. Bounds are
  hardcoded physics; values are data.
- **One board, one owner.** Never double-scrape a board atlas-kt owns.
- **Observability = validation gates**, not dashboards of counts.

## Layout

```
config/                  Django settings (control plane + worker entrypoint)
../ingest-pipelines/     SEPARATE config repo: ONE .json per project + overlays/
src/ingest/
  core/                  the base classes + contracts (pure python, no Django)
  domains/               spec loader + per-domain code (jobs/: gate·sink·boards)
  utils/                 http · normalize · clickhouse (pure functions)
  scraping/              families (the loops) + platforms/ (facts) + registry
  ledger/                records (CH tables as classes) + gates (G1–G7)
  orchestration/         Temporal: config · workflows · activities · worker · trigger
deploy/                  gitops manifests per runnable unit (worker-http/-browser/…)
```

## Dependency law
`core` imports nothing. `utils`/`scraping`/`ledger`/`calibration` import `core`.
`orchestration` imports all. Airflow/streaming read ClickHouse, not the code.
**Arrows point one way. No Temporal import below `orchestration/`.**

## Run a scraper offline (the 2am tool)
```
python -m pytest tests/                 # family + registry-coverage gates
manage.py scrape <platform> --board <id> --dry   # live fetch, prints evidence, no CH/Temporal
```
