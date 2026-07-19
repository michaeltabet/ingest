"""THE scheduler library — what fires a project's runs on a cadence.

Fifth of the engine's five class families (resolver, scraper, temporal flow,
observability, scheduler). A spec says `scheduler.kind = <preset>` and states
the cadence; the class holds those facts for whatever executes them (the
config repo's dags/ folder renders Airflow DAGs from specs using this).

    airflow   cron lives in the spec (`scheduler.cron`; null = manual only).
"""
from __future__ import annotations


class Scheduler:
    """Contract: the declared cadence facts for one project."""

    ARGS: set = set()              # subclasses declare their JSON args

    def __init__(self, cron: str | None = None):
        self.cron = cron           # None = not scheduled; hand-fired only

    def scheduled(self) -> bool:
        return self.cron is not None


class AirflowScheduler(Scheduler):
    """Airflow executes the cadence: one DAG per project, generated from the
    specs (see the config repo's dags/). This class is the facts' ONE home."""

    ARGS = {"cron"}


_KINDS = {"airflow": AirflowScheduler}


def build(preset: str, part: dict) -> Scheduler:
    """One word picks the class; the class declares its args (ARGS) and the
    JSON supplies them."""
    cls = _KINDS.get(preset)
    if cls is None:
        raise ValueError(f"no scheduler preset {preset!r} in the library "
                         f"(have: {sorted(_KINDS)}) — add the class to "
                         f"ingest/scheduling.py")
    args = {k: v for k, v in part.items() if k != "kind"}
    unknown = set(args) - cls.ARGS
    if unknown:
        raise ValueError(f"scheduler {preset!r}: unknown {sorted(unknown)} "
                         f"(class ARGS = {sorted(cls.ARGS)})")
    return cls(**args)
