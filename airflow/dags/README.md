# Airflow DAGs — the judgment layer

Airflow (KubernetesExecutor) owns all daily judgment. DAGs IMPORT the `ingest`
package (same image, `DJANGO_SETTINGS_MODULE` set) — they never re-implement
gates, hypotheses, or scraping. Wired in build-order step 7.

| DAG | Cadence | Does |
|---|---|---|
| `janitor_daily` | daily | read evidence → ghosts + failures → re-fire fresh Temporal runs |
| `gates_daily` | daily | run `ledger.gates.run_chain` (G2–G6) → gate_results + the 4 alerts |
| `calibrate_weekly` | weekly | call each `Hypothesis.recalibrate()` → write values to Platform rows |
| `parse_bronze` | scheduled | bronze → silver via KubernetesPodOperator (replayable) |
| `audit_weekly` | weekly | G7: Temporal history/payload bloat, ownership violations |

Each is a thin DAG file whose tasks call into `ingest.*`. No logic lives here.
