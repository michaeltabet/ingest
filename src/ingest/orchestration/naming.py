"""Workflow ID naming convention — THE one place IDs are built.

You must be able to read any row in the Temporal UI and know exactly what it
is without opening it. Convention (dot-separated): date, then platform, then
source — with the kind as a trailing qualifier.

    <yyyy-mm-dd>.<platform>.run              nightly platform parent
    <yyyy-mm-dd>.<platform>.<seq>.batch      child: batch of cheap sources
    <yyyy-mm-dd>.<platform>.<key>.source    child: one source scrape
    <yyyy-mm-dd>.<platform>.sweep            janitor re-run parent
    <yyyy-mm-dd>.<platform>.<key>.parity    parity-window comparison run

Examples as seen in the UI:
    2026-07-13.vendora.run
    2026-07-13.vendorb.acme.source
    2026-07-13.vendora.004.batch

Date leads so the UI's alphabetical ordering is chronological: one run-date is
one contiguous block, and the current day sorts together instead of being
scattered across platform groupings.

Rules:
  * lowercase; dots separate levels; no UUIDs in the visible ID (Temporal
    already tracks run_id underneath for uniqueness across re-runs).
  * date = the RUN's date (the nightly window), not "now" — deterministic.
  * the kind token trails but MUST NOT be dropped: without it
    platform_run/sweep_run collide (both `<date>.<platform>`), as do
    source_scrape/parity_run (both `<date>.<platform>.<key>`). Temporal
    enforces workflow-ID uniqueness, so a collision is a hard start failure,
    not a cosmetic clash.
  * every workflow started anywhere in this codebase MUST get its ID from
    these functions. Hand-built IDs are a review-blocker.

Namespace: declared in the project's spec ([temporal] namespace). Never atlas's.
"""
from __future__ import annotations



def _clean(s: str) -> str:
    return s.strip().lower().replace(" ", "-").replace(".", "-")


def platform_run(platform: str, run_date: str) -> str:
    return f"{run_date}.{_clean(platform)}.run"


def source_scrape(platform: str, key: str, run_date: str) -> str:
    return f"{run_date}.{_clean(platform)}.{_clean(key)}.source"


def batch_scrape(platform: str, run_date: str, seq: int) -> str:
    return f"{run_date}.{_clean(platform)}.{seq:03d}.batch"


def sweep_run(platform: str, run_date: str) -> str:
    return f"{run_date}.{_clean(platform)}.sweep"


def parity_run(platform: str, key: str, run_date: str) -> str:
    return f"{run_date}.{_clean(platform)}.{_clean(key)}.parity"
