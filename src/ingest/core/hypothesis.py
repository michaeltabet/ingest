"""Base class #4: Hypothesis (no invented constants).

Every sized value in the system — worker slots, batch size, detail
concurrency, refetch interval, floor days — is a Hypothesis: a stated initial
guess plus a recalibrate() that reads the ledger and proposes the next value.

Bounds (clamp) are hardcoded — bounds are physics. Values are learned. One
weird night must never teach the system a value outside its clamp.

Concrete hypotheses live in calibration/hypotheses.py; the weekly Airflow
calibrate DAG calls recalibrate() and writes results back to the Platform row.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class Hypothesis(ABC):
    name: str = "abstract"
    lo: float = 0.0
    hi: float = float("inf")

    def __init__(self, value: float, *, rationale: str = "initial guess"):
        self.value = self.clamp(value)
        self.rationale = rationale

    def clamp(self, v: float) -> float:
        return max(self.lo, min(self.hi, v))

    @abstractmethod
    def recalibrate(self, ledger) -> "Hypothesis":
        """Read ledger stats, return a NEW Hypothesis with the next value.
        Must clamp. `ledger` is a read-only stats accessor."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.name}={self.value} ({self.rationale})"
