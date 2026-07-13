"""Temporal workflows. DETERMINISTIC — decisions only, no IO, no clocks.

    PlatformRun   parent, one per platform per night. Reads a frozen RunPlan
                  (from plan_run activity), fans out children, does not wait on
                  stragglers.
    ScrapeBatch   child for cheap families — N boards (N from BatchSize).
    ScrapeBoard   child for heavy families — one board.

STATUS: SKELETON. The Temporal SDK wiring (@workflow.defn / @workflow.run) is
added in build-order step 7, only after the offline engine is proven and with
Michael's explicit go. This file documents the shape; it does not yet import
temporalio so the package stays importable without the SDK.
"""
from __future__ import annotations

# from temporalio import workflow   # added in step 7
#
# @workflow.defn
# class PlatformRun:
#     @workflow.run
#     async def run(self, platform: str) -> dict:
#         plan = await workflow.execute_activity(plan_run, platform, ...)
#         children = [workflow.start_child_workflow(...) for batch in plan.batches]
#         # fire-and-forget; abandon-on-close; parent finishes with failures RECORDED
#         return {"platform": platform, "batches": len(plan.batches)}
