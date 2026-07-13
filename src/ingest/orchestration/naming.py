"""Workflow ID naming convention — THE one place IDs are built.

You must be able to read any row in the Temporal UI and know exactly what it
is without opening it. Convention (dot-separated, most-general → most-specific):

    run.<platform>.<yyyy-mm-dd>              nightly platform parent
    batch.<platform>.<yyyy-mm-dd>.<seq>      child: batch of cheap boards
    board.<platform>.<slug>.<yyyy-mm-dd>     child: one heavy board
    sweep.<platform>.<yyyy-mm-dd>            janitor re-run parent
    parity.<platform>.<slug>.<yyyy-mm-dd>    parity-window comparison run

Examples as seen in the UI:
    run.greenhouse.2026-07-13
    board.workday.gitlab.2026-07-13
    batch.lever.2026-07-13.004

Rules:
  * lowercase; dots separate levels; no UUIDs in the visible ID (Temporal
    already tracks run_id underneath for uniqueness across re-runs).
  * date = the RUN's date (the nightly window), not "now" — deterministic.
  * every workflow started anywhere in this codebase MUST get its ID from
    these functions. Hand-built IDs are a review-blocker.

Namespace: everything runs in the `ingest` Temporal namespace. Never atlas's.
"""
from __future__ import annotations

NAMESPACE = "ingest"


def _clean(s: str) -> str:
    return s.strip().lower().replace(" ", "-").replace(".", "-")


def platform_run(platform: str, run_date: str) -> str:
    return f"run.{_clean(platform)}.{run_date}"


def board_scrape(platform: str, slug: str, run_date: str) -> str:
    return f"board.{_clean(platform)}.{_clean(slug)}.{run_date}"


def batch_scrape(platform: str, run_date: str, seq: int) -> str:
    return f"batch.{_clean(platform)}.{run_date}.{seq:03d}"


def sweep_run(platform: str, run_date: str) -> str:
    return f"sweep.{_clean(platform)}.{run_date}"


def parity_run(platform: str, slug: str, run_date: str) -> str:
    return f"parity.{_clean(platform)}.{_clean(slug)}.{run_date}"
