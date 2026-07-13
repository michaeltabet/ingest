# ingest — HiredSignal job-board scraper / ingest

New Python system that acquires job postings from ATS platforms and lands them
raw in ClickHouse. Runs in parallel with the legacy `atlas-kt` (Kotlin) until
per-platform cutover. This repo is **acquisition + ingest only** — parsing,
identity, and serving are downstream.

> Design contract: `docs/SCRAPER-REDESIGN-WHITEPAPER-v3.pdf`
> Build constraints: `docs/BUILD-BRIEF.md`
> **Read both before changing anything.**

## The whole system is five base classes

Everything is a subclass of exactly one of these. Add a subclass file — never
touch plumbing.

| Base (`src/ingest/core/`) | Contract | Subclasses in |
|---|---|---|
| `Record` | owns its ClickHouse schema; DDL is generated from the class | `ledger/records.py` |
| `Scraper` | `(board, ctx) -> RawResult` (raw bytes + evidence counts) | families: `scraping/families.py`; platforms: `scraping/platforms/` |
| `Gate` | `check(run) -> GateResult` (validation + a defined failure action) | `ledger/gates.py` |
| `Hypothesis` | `value` + `recalibrate(ledger)` + `clamp` (no invented constants) | `calibration/hypotheses.py` |
| `Step` | Temporal activity wrapper; returns a Record (counts), never data | `orchestration/steps.py` |

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
| every ClickHouse table | `ledger/records.py` (DDL generated → `clickhouse/migrations/`) |
| Temporal config / queues / routing | `orchestration/config.py` |
| one Temporal Schedule per platform | `orchestration/schedules.py` |
| the daily judgment (janitor/calibrate/gates/parse) | `airflow/dags/` |
| Kafka topics + Debezium CDC | `streaming/` |
| per-platform partition (budget, cron, rps, owner) | `boards.Platform` DB row |

## Doctrine (why it's built this way)

- **Extract, don't parse.** Scrapers dump raw bytes + evidence counts. Parsing
  is a later Airflow batch, replayable from raw. Network → scraper; bytes → batch.
- **Fail loud, handle daily.** Temporal: attempts=1, no heartbeat, no guards.
  Breakage is red and visible; the daily Airflow pass + k8s restarts respond.
- **No invented constants.** Every sized value is a `Hypothesis` recalibrated
  from the ledger. Bounds are hardcoded; values are learned.
- **One board, one owner.** Never double-scrape a board atlas-kt owns.
- **Observability = validation gates**, not dashboards of counts.

## Layout

```
config/                  Django settings (control plane + worker entrypoint)
src/ingest/
  core/                  the five base classes + contracts (pure python, no Django)
  utils/                 http · normalize · clickhouse (pure functions)
  scraping/              families (the loops) + platforms/ (facts) + registry
  ledger/                records (CH tables as classes) + gates (G1–G7)
  calibration/           hypotheses (every sized number)
  orchestration/         Temporal: config · schedules · workflows · activities · steps
  boards/                Django app: Platform + Board (per-platform partition)
airflow/dags/            janitor · gates · calibrate · parse · audit
streaming/               debezium (Temporal VISIBILITY tables) · topics · sinks
clickhouse/              migrate.py (DDL from Record classes) + migrations/
fixtures/<platform>/     recorded responses — CI gate
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
