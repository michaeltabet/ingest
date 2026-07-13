# BUILD BRIEF — Job-Board Scraping Platform (new project)

**Read this whole file before touching anything. Then read
`~/Desktop/SCRAPER-REDESIGN-WHITEPAPER-v3.pdf` (the design contract — 14 sections).
This brief constrains HOW you build what that paper specifies.**

---

## 0. Context in four lines

- A working Kotlin+Temporal scraper (atlas-kt) scrapes ~13,269 job boards across 55 ATS
  platforms. It stays running and MUST NOT be touched.
- This is a **NEW project** — Python/Django. Not a port of atlas code; a port of its
  *knowledge* (platform quirks are documented in atlas-kt's code comments).
- Goal: 10–20X board scale next month. Boards are DB rows; platforms are one-file drops.
- Michael can read Python, not Kotlin. Readability is a hard requirement, not a preference.

## 1. HARD PROHIBITIONS — violating any of these is failing the task

1. **DO NOT create the project until Michael gives you the project NAME and its location.**
   Do not name anything "atlas", "atlas-py", or with an "hs-" prefix.
2. **DO NOT touch the cluster.** No MRs, no kubectl, no ArgoCD apps, no namespaces, no
   schedules — nothing deploys without Michael's explicit yes, step by step.
3. **DO NOT touch atlas-kt** (repo, deployment, schedules, boards it owns).
4. **DO NOT run ahead.** One step at a time. Show each file/decision, get a yes, continue.
   Michael interrupts hard when you dump unrequested work — that is a signal you already failed.
5. **DO NOT invent constants.** No magic numbers in code. Every sized value (slots, batch
   size, budgets, intervals) is a `Hypothesis` object with a stated initial guess and a
   `recalibrate()` fed by ledger data. Bounds (clamps) are allowed; values are learned.
6. **DO NOT add defensive tuning.** Doctrine is FAIL LOUD: Temporal retries = attempts 1,
   start_to_close absurd (30d, exists only because the SDK requires one), NO heartbeats,
   NO slot "guards", NO payload workarounds. If it breaks, it breaks red and visible;
   the daily Airflow pass and k8s restarts are the response. OOM-kills are data, not incidents.
7. **DO NOT parse in scrapers.** Extract-and-dump only. Scrapers land raw bytes + evidence
   counts. Field mapping/parsing is a later Airflow batch (bronze→silver). The boundary:
   needs-the-NETWORK → scraper; needs-only-BYTES → batch.
8. **DO NOT pass data through Temporal.** Activities write raw payloads to ClickHouse
   themselves and return only small counts. Temporal carries decisions and numbers.
9. **DO NOT put logic in runtimes or utils into base classes.** Utilities are pure functions
   (`utils/`). Inheritance exists ONLY for the template chain (Scraper → family → platform).
   An `if platform == "x"` inside a family file is a design failure.
10. **DO NOT double-scrape.** Every board has ONE owner (atlas-kt | new). Never scrape a
    board the new system doesn't own, except inside a declared parity window.

## 1b. PORTING ≠ TRANSLATING — the DRY/modular contract (read twice)

The single biggest failure mode: opening an atlas-kt scraper (500+ lines of
Kotlin) and translating it into Python. DO NOT. atlas-kt's structure IS the
disease (37 copies of stripHtml, 42 of sha256, a retry loop per file). You
port the **FACTS**, and only the facts:

- the endpoint URLs and HTTP method
- the pagination shape (offset / page / token / one-shot) → expressed as the
  CURSOR hook, never as a loop
- the quirks (page-size degrade on 400, required headers, host variants)
- where the stubs live in the response and which fields are STABLE for digest

Everything else already exists ONCE in the engine and must not reappear:

| If you find yourself writing… | STOP — it lives in |
|---|---|
| a while/for loop over pages | the family (`families.py`) |
| retry / backoff / sleep | `utils/http.py` |
| sha256 / digest code | `utils/normalize.py` |
| HTML stripping / date parsing | `utils/normalize.py` (batch phase) |
| concurrency (gather/chunks) | the family |
| writing to ClickHouse | the Step (`orchestration`) |
| `if platform == "x"` anywhere | nowhere — it's a platform override |

**Mechanical litmus test for every platform file (review-blocker if violated):**
1. imports: ONLY `core.models`, its family, `utils.normalize.digest_json`, stdlib `json`
2. contains NO `while`, `sleep`, `hashlib`, `try/except` around HTTP, no client construction
3. defines ONLY: `platform`, hook methods (`list_request`, `parse_list`,
   `detail_request?`), and optional constant overrides
4. size ~15–60 lines. Bigger → either the family is missing a hook (fix the
   family ONCE, for everyone) or you are translating Kotlin (start over)
5. has a RECORDED live fixture (see fixtures/PROVENANCE.md) — synthetic = not ported

Worked example — Workday, ~500 lines of Kotlin, becomes ~40 lines of Python:
retry loop → gone (http.py); detail-chunking → gone (family); stripHtml/sha256
→ gone (normalize/batch); page-size degrade 200→20 on HTTP 400 → a small
override of ONE hook; what remains = the cxs URL template, the POST body,
offset-cursor advance by len(postings), the detail URL join. THAT is the port.

## 2. The five base abstractions — everything is a subclass of exactly one

| Base | Contract | Subclasses live in |
|---|---|---|
| `Record` | owns its ClickHouse schema; DDL is GENERATED from the class (no hand SQL) | `apps/ledger/records.py` |
| `Scraper` | `(board, ctx) -> RawResult` (payloads + evidence counts) | families in `apps/scraping/families.py`, platforms in `apps/scraping/platforms/` |
| `Gate` | `check(run) -> GateResult` — validation with a defined failure action | `apps/ledger/gates.py` (G1–G7 per whitepaper §10) |
| `Hypothesis` | value + `recalibrate(ledger)` + clamp | `apps/calibration/hypotheses.py` |
| `Step` | Temporal activity wrapper; returns a Record (counts), never data | `apps/orchestration/` |

Scraping families (the loops, each written ONCE): OneShot, Paged, PagedDetail, Browser.
The pagination hook is a CURSOR: `parse_list(resp) -> (stubs, cursor|None)` — covers
offset/page/token/one-shot with one shape. Platform files are FACTS ONLY (~15–40 lines).
Transport is COMPOSITION: clients injected via context, never inherited, never constructed
by a scraper. 54 of 55 platforms are plain HTTP; browser tier is a bolt-on.

## 3. The complete repo structure (agreed — do not restructure)

```
<name>/
├── manage.py
├── config/                          # Django settings, env-driven
├── utils/                           # http.py · normalize.py · clickhouse.py — pure functions
├── apps/
│   ├── boards/                      # Platform + Board models = per-platform partition
│   │                                #   Platform(name, family, task_queue, schedule_cron,
│   │                                #            nightly_budget, rps_cap, batch_hypothesis,
│   │                                #            enabled/kill-switch, owner)
│   ├── scraping/                    # base.py · families.py · registry.py (auto-discovery)
│   │   └── platforms/               #   one file per platform; CI-gated fixtures
│   ├── ledger/                      # records.py · gates.py
│   ├── calibration/                 # hypotheses.py
│   └── orchestration/               # config.py (TemporalConfig, QueueSpec, queue_for(platform))
│                                    # schedules.py (one Temporal Schedule PER PLATFORM,
│                                    #   sync_schedules() diffs DB rows -> Temporal)
│                                    # workflows.py (PlatformRun parent per platform/night;
│                                    #   batched children for cheap families, per-board heavy)
│                                    # activities.py (plan_run, scrape) + management commands:
│                                    #   run_worker --queue X · sync_schedules · scrape --dry
├── airflow/
│   ├── dags/                        # janitor_daily · gates_daily · calibrate_weekly
│   │                                # parse_bronze · audit_weekly
│   └── plugins/                     # thin hooks; DAGs IMPORT the apps, never re-implement
├── streaming/
│   ├── debezium/temporal-visibility.json   # CDC: Temporal Postgres VISIBILITY tables
│   │                                       # (NOT history tables — those are protobuf blobs)
│   ├── topics.yaml                  # Kafka topics as reviewed config
│   └── sinks/clickhouse-sink.yaml   # Kafka -> CH temporal_meta
├── clickhouse/
│   ├── migrate.py                   # generates DDL FROM the Record classes
│   └── migrations/                  # versioned generated DDL
├── fixtures/<platform>/             # recorded live responses — CI refuses platforms without them
├── tests/
└── deploy/                          # k8s: worker-http · worker-browser (separate image,
                                     # browser baked in) · airflow · debezium · control
                                     # ALL cluster changes via gitops MR + ArgoCD, never by hand
```

## 4. Non-negotiable mechanics (from the whitepaper — read it for the why)

- **Evidence-carrying results:** RawResult = payloads + {list_status, pages_fetched,
  stubs_seen, details_ok, details_failed, bytes_in, errors}. Judgment happens OUTSIDE
  the scraper. Zero-rows ≠ success, at the type level.
- **Incremental extraction:** always fetch lists blind; fetch details only for stubs whose
  digest is NOT in the board's seen-set (passed IN as a parameter — scraper stays a pure
  function; ClickHouse owns state; query per-board/per-platform, dedup-on-read).
  Digest only STABLE fields — a timestamp inside a stub silently kills dedup (known trap).
- **Per-platform partitioning:** one Temporal Schedule per platform (staggered), one parent
  run per platform per night, budgets/rps/queues per Platform row. Nothing shared across
  platforms except engine code.
- **Scheduling brain (Airflow-computed, Temporal-executed):** score = EWMA new-jobs / cost;
  greedy knapsack under the platform's budget; FLOOR RULE: every board at least every N days.
  Chronic failers deprioritize themselves economically. No scheduler work beyond greedy+floor
  until the top-10 platforms are live (explicit gate).
- **Observability = the gate chain G1–G7** (whitepaper §10) + ONE dashboard + ONE drill-down
  query + ONE weekly trend view + FOUR alerts total (adding a fifth requires deleting one).
  Gate evaluations are rows in gate_results — gates are data.
- **20X ramp:** onboard boards in WAVES (20–30K/night) — a board's FIRST scrape is full-fetch;
  250K cold boards in one night is a self-inflicted incident. Per-vendor politeness caps
  (rps per platform) bind always, independent of the planner.

## 5. Build order — each step shown to Michael before the next

1. Skeleton: the five base classes + utils + Django apps wiring. SHOW IT.
2. Records + clickhouse/migrate.py generating DDL. SHOW THE GENERATED DDL.
3. Families (OneShot, PagedDetail first) + FixtureClient + family contract tests.
4. Greenhouse (one-shot, ~15 lines) + fixtures + tests GREEN offline.
5. Workday (paged+detail, ~40 lines — port quirks from atlas-kt comments:
   page-size degrade 200→20 on 400, Accept-Language header, empty-page terminates
   pagination, advance offset by len(postings) not page_size, detail failures
   logged-and-skipped) + fixtures + tests GREEN.
6. `manage.py scrape <platform> --board X --dry` working end-to-end offline.
7. STOP. Everything after (Temporal wiring, Airflow DAGs, streaming, deploy) is
   proposed to Michael step by step, and NOTHING touches the cluster without his yes.

## 6. Working style with Michael (matters as much as the code)

- ADHD: short, scannable replies. ONE step at a time. You carry the structure.
- Never say "flapping". Never claim "out of CPU/resource-starved" as a diagnosis.
- He will interrupt and redirect — that's steering, follow it immediately, don't defend.
- When he asks "explain what you understood", give a faithful compact recap.
- All his standing rules (CLAUDE.md + memory) apply: no cluster mutations without a yes,
  gitops-only changes, tailnet-only internal GUIs, no new resources without approval.
