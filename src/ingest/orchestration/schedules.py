"""Per-platform Temporal Schedules, generated FROM the boards.Platform rows.

sync_schedules() diffs the DB (one enabled Platform = one Schedule, staggered
by its schedule_cron) against Temporal's registered schedules and applies the
delta. Add a Platform row in the Django admin → its schedule exists tonight;
no deploy. Disable/kill-switch → its schedule is removed.

STATUS: SKELETON (build-order step 7). Shape documented; SDK wiring pending go.
"""
from __future__ import annotations


def desired_schedules(platforms) -> dict:
    """platforms: iterable of Platform rows → {schedule_id: spec}. Pure; unit-
    testable without Temporal."""
    out = {}
    for p in platforms:
        if not getattr(p, "enabled", True):
            continue
        out[f"ingest-{p.name}"] = {
            "cron": p.schedule_cron,
            "workflow": "PlatformRun",
            "arg": p.name,
            "queue": p.task_queue,
        }
    return out


def diff(desired: dict, existing: dict) -> dict:
    return {
        "add": {k: v for k, v in desired.items() if k not in existing},
        "remove": [k for k in existing if k not in desired],
        "update": {k: v for k, v in desired.items()
                   if k in existing and existing[k] != v},
    }
