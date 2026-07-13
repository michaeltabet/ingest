"""Base class #5: Step (a Temporal activity, wrapped).

Every unit of Temporal work is a Step. The ONE law this base enforces: a Step
returns a Record (counts/decisions), never raw data. Data goes to ClickHouse
from inside the step; Temporal carries only numbers. This makes the 2MB-payload
law structural, not a discipline everyone has to remember.

Concrete steps (PlanStep, ScrapeStep, ParseStep) live in orchestration/steps.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from .record import Record


class Step(ABC):
    name: str = "abstract"

    @abstractmethod
    async def run(self, ctx) -> Record:
        """Do the work; write any bulk data to ClickHouse directly; return a
        Record (evidence/counts) — NEVER the scraped payloads."""
        raise NotImplementedError
