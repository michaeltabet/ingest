"""The ingest worker, hosted as a Knative Function.

Knative needs a process that answers HTTP; the worker needs a process that
polls Temporal forever. This runs both: the worker as a background task
started with the function, and a one-route health handler that reports
whether that task is still alive. If the worker dies the handler says so and
Knative's probes restart the pod.
"""
from __future__ import annotations

import asyncio
import os

from ingest.orchestration.worker import run_worker


def new():
    return Function()


class Function:
    def __init__(self):
        self.task: asyncio.Task | None = None

    async def start(self, cfg):
        queue = os.environ.get("WORKER_QUEUE", "scrape-http")
        self.task = asyncio.create_task(run_worker(queue), name="temporal-worker")

    async def stop(self):
        if self.task:
            self.task.cancel()

    def alive(self) -> bool:
        return self.task is not None and not self.task.done()

    async def handle(self, scope, receive, send):
        status = 200 if self.alive() else 503
        body = b"worker running\n" if status == 200 else b"worker stopped\n"
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": body})
