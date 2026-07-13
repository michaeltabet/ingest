"""Base class #3: Gate (observability as validation, not dashboards).

Every check is a Gate with check(run) -> GateResult and a defined failure
ACTION (fail-the-workflow / alarm / page). Every evaluation is persisted as a
row (gates are data). Running "all gates" is iterating a list — no scattered
`if` checks anywhere else in the system.

Concrete gates G1..G7 live in ledger/gates.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class GateResult:
    gate: str
    passed: bool
    action: str                      # "none" | "fail" | "alarm" | "page"
    detail: dict = field(default_factory=dict)


class Gate(ABC):
    name: str = "abstract"
    #: what happens when this gate fails
    on_fail: str = "alarm"

    @abstractmethod
    def check(self, run) -> GateResult:
        """`run` is the evidence view for a run/board. Returns a GateResult."""
        raise NotImplementedError

    def _result(self, passed: bool, **detail) -> GateResult:
        return GateResult(
            gate=self.name,
            passed=passed,
            action="none" if passed else self.on_fail,
            detail=detail,
        )
