"""Concrete hypotheses — every sized number in the system, each with its own
recalibrate(). The weekly Airflow calibrate DAG calls recalibrate() and writes
results back to the Platform row / hypothesis store. Bounds are hardcoded
(physics); values are learned.

`ledger` is a read-only stats accessor (per-platform aggregates from
scrape_evidence + temporal_meta). Bodies show the CONTRACT + formula; the exact
queries are wired when the ledger is live.
"""
from __future__ import annotations

from ingest.core.hypothesis import Hypothesis


class WorkerSlots(Hypothesis):
    """Concurrent activities per worker pod. NOT a guard — an OOM is a loud,
    recorded event; k8s restarts, this recalibrates from observed memory."""
    name = "worker_slots"
    lo, hi = 1, 200

    def recalibrate(self, ledger) -> "WorkerSlots":
        p95 = ledger.p95_mem_per_scrape(self)          # bytes
        limit = ledger.pod_mem_limit(self)             # bytes
        v = (0.8 * limit / p95) if p95 else self.value
        return WorkerSlots(v, rationale=f"0.8*limit/p95_mem ({p95=})")


class BatchSize(Hypothesis):
    """Boards per child workflow for cheap families. Sized so a child runs
    ~target seconds — fat boards → small batches, automatically."""
    name = "batch_size"
    lo, hi = 1, 500

    def recalibrate(self, ledger) -> "BatchSize":
        per = ledger.ewma_board_seconds(self)
        target = 600.0
        v = (target / per) if per else self.value
        return BatchSize(v, rationale=f"{target}s/{per:.1f}s_per_board")


class DetailConcurrency(Hypothesis):
    """Concurrent detail fetches per board (paged+detail). Trades TCP budget
    vs wall-clock; sized against observed per-vendor 429 rates."""
    name = "detail_concurrency"
    lo, hi = 1, 50

    def recalibrate(self, ledger) -> "DetailConcurrency":
        rate = ledger.rate_429(self)
        v = self.value * (0.5 if rate > 0.02 else 1.2)   # AIMD-ish
        return DetailConcurrency(v, rationale=f"AIMD on 429={rate:.3f}")


class RefetchDays(Hypothesis):
    """Full re-fetch interval per platform — heals silently-edited details.
    Calibrated from measured edit rate on forced re-fetches."""
    name = "refetch_days"
    lo, hi = 1, 90

    def recalibrate(self, ledger) -> "RefetchDays":
        edit_rate = ledger.silent_edit_rate(self)
        v = 30.0 * (0.5 if edit_rate > 0.1 else 1.5)
        return RefetchDays(v, rationale=f"edit_rate={edit_rate:.3f}")


class FloorDays(Hypothesis):
    """Max staleness — every board scraped at least this often regardless of
    score. Calibrated from observed dead-board wake-up rate."""
    name = "floor_days"
    lo, hi = 1, 30

    def recalibrate(self, ledger) -> "FloorDays":
        wake = ledger.wakeup_rate(self)
        v = 7.0 * (0.5 if wake > 0.05 else 1.5)
        return FloorDays(v, rationale=f"wakeup_rate={wake:.3f}")
