"""Concrete gates G1-G7 + the chain. Observability as validation, not counts.

Each gate is a class with a defined failure ACTION. run_chain() evaluates all
of them and returns GateResults (persisted as GateResultRecord rows). The four
alerts and the fail-the-workflow behavior come from these `on_fail` actions —
there is no alerting logic scattered anywhere else.

Signatures take a `run` (evidence view) and, where needed, a `history` accessor
for baselines. Bodies here are the CONTRACT; the baseline queries are wired
when the ledger is live (Airflow gates_daily DAG).
"""
from __future__ import annotations

from ingest.core.gate import Gate, GateResult


class DeliveryGate(Gate):           # G1 — in the Temporal child, per board
    name = "G1_delivery"
    on_fail = "fail"

    def check(self, run) -> GateResult:
        ok = run.list_ok and (run.stubs_seen > 0 or run.had_zero_last_run)
        return self._result(ok, stubs_seen=run.stubs_seen, status=run.list_status)


class PresenceGate(Gate):           # G2 — daily; the ghost / dead-worker detector
    name = "G2_presence"
    on_fail = "page"

    def check(self, run) -> GateResult:
        return self._result(run.has_evidence_today, board_id=run.board_id)


class VolumeGate(Gate):             # G3 — daily; silent-success alarm
    name = "G3_volume"
    on_fail = "page"

    def check(self, run) -> GateResult:
        ok = run.new_jobs_within_band and not run.near_zero_all_boards
        return self._result(ok, new_jobs=run.new_jobs, band=run.band)


class LandingGate(Gate):            # G4 — daily; raw rows reconcile with evidence
    name = "G4_landing"
    on_fail = "alarm"

    def check(self, run) -> GateResult:
        return self._result(run.raw_rows == run.details_ok_claimed,
                            raw=run.raw_rows, claimed=run.details_ok_claimed)


class ParseGate(Gate):              # G5 — per parse; bronze->silver health
    name = "G5_parse"
    on_fail = "alarm"

    def check(self, run) -> GateResult:
        return self._result(run.parse_error_rate <= run.parse_baseline,
                            rate=run.parse_error_rate)


class OwnershipGate(Gate):          # G6 — daily; protects the OLD system
    name = "G6_ownership"
    on_fail = "page"

    def check(self, run) -> GateResult:
        return self._result(not run.double_scraped_outside_parity,
                            board_id=run.board_id)


class TemporalAuditGate(Gate):      # G7 — weekly; CDC-driven
    name = "G7_temporal_audit"
    on_fail = "alarm"

    def check(self, run) -> GateResult:
        ok = (not run.red_without_evidence) and (not run.history_bloat)
        return self._result(ok)


CHAIN = [DeliveryGate, PresenceGate, VolumeGate, LandingGate,
         ParseGate, OwnershipGate, TemporalAuditGate]


def run_chain(run, gates=None) -> list:
    """Evaluate gates against a run's evidence view. Returns GateResults."""
    return [g().check(run) for g in (gates or CHAIN)]
