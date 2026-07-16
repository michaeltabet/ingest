"""Daily janitor — closes the nightly window and hands judgment to an agent.

Doctrine is fail loud, handle daily: boards retry FOREVER inside Temporal
(workflows.py, maximum_attempts=0), so a broken board never fails red on its
own — something must close the window every morning. This is it.

    stop            terminate every run still open from the previous window
    report          JSON of recent PlatformRuns (the triage agent's input)
    refire <p...>   re-start PlatformRun for the named platforms only
    triage          run the Claude triage agent (Sonnet, headless)

stop/report/refire are deterministic; triage is judgment. The agent CALLS the
deterministic commands — it never reimplements them.

Env: TEMPORAL_ADDRESS/NAMESPACE, RUN_DATE (refire), CLAUDE_CODE_OAUTH_TOKEN
(triage), JANITOR_MODEL (default sonnet — this is a cheap-model job).
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys

from temporalio.client import Client

from ingest.orchestration.config import TemporalConfig


async def _client() -> Client:
    cfg = TemporalConfig.from_env()
    return await Client.connect(cfg.target, namespace=cfg.namespace)


async def stop() -> None:
    client = await _client()
    n = 0
    async for wf in client.list_workflows('ExecutionStatus="Running"'):
        await client.get_workflow_handle(wf.id).terminate(
            "janitor: window closed before the run finished")
        print(f"terminated {wf.id}")
        n += 1
    print(f"janitor stop: terminated {n} still-open runs")


async def report() -> None:
    client = await _client()
    runs = []
    async for wf in client.list_workflows('WorkflowType="PlatformRun"'):
        runs.append({
            "id": wf.id,
            "status": wf.status.name,
            "start": wf.start_time.isoformat() if wf.start_time else None,
            "close": wf.close_time.isoformat() if wf.close_time else None,
        })
    runs.sort(key=lambda r: r["start"] or "", reverse=True)
    print(json.dumps({"platform_runs": runs[:100]}, indent=1))


async def refire(platforms: list) -> None:
    if not platforms:
        raise SystemExit("refire needs platform names — it never refires ALL")
    from ingest.orchestration.trigger import trigger
    await trigger(platforms=platforms,
                  run_date=os.environ.get("RUN_DATE") or None)


# The triage agent's contract. Lives here as code (not a loose file) so it is
# versioned, reviewed, and shipped with the package it commands.
PROMPT = """\
You are the nightly janitor for the ingest job-board scraper. Last night's
PlatformRuns have been terminated/closed. Your job: categorize every failure
and act. You are time-boxed to one hour — breadth over depth.

1. Run: python -m ingest.orchestration.janitor report
2. For each non-COMPLETED PlatformRun, find WHY: query Temporal for its failed
   ScrapeBoard children (temporalio is installed; namespace `ingest`), read
   activity failure messages, check evidence counts in ClickHouse if CH_* env
   is set.
3. Bucket each platform: transient (network, rate-limit, upstream 5xx) |
   board-gone | platform-change | code-bug.
4. transient -> python -m ingest.orchestration.janitor refire <platform>
5. code-bug / platform-change -> fix the scraper in this repo, run the
   platform's tests, then push a branch and open an MR with push options:
     git push -o merge_request.create -o merge_request.title="janitor: <fix>"
   NEVER merge. NEVER push to main. Stop at green MR.
6. End with a summary table: platform | category | action taken.

Hard rules (doctrine — violating these is worse than fixing nothing):
- NO defensive tuning: never add retries, timeouts, heartbeats, or guards.
- NO invented constants: sized values are Hypothesis objects, never literals.
- board-gone is a Platform-row/data question — report it, don't code around it.
"""


def triage() -> None:
    model = os.environ.get("JANITOR_MODEL", "sonnet")
    raise SystemExit(subprocess.call(
        ["claude", "-p", PROMPT, "--model", model,
         "--allowedTools", "Bash,Read,Grep,Glob,Edit,Write"]))


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "report"
    if cmd == "stop":
        asyncio.run(stop())
    elif cmd == "report":
        asyncio.run(report())
    elif cmd == "refire":
        asyncio.run(refire(sys.argv[2:]))
    elif cmd == "triage":
        triage()
    else:
        raise SystemExit(f"unknown janitor command: {cmd}")


if __name__ == "__main__":
    main()
