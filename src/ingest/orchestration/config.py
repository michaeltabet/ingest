"""Temporal configuration — as classes, from env. THE one home for connection,
queues, and platform→queue routing. No YAML sprawl, no settings in workflows.

Worker slot counts are NOT constants here — they reference WorkerSlots
hypotheses (calibrated). This module only decides WHICH queue a platform's
work lands on, by its family.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class QueueSpec:
    name: str
    # slots resolved from a WorkerSlots hypothesis at worker startup
    slots_hypothesis: str = "worker_slots"


QUEUE_HTTP = QueueSpec("scrape-http")
QUEUE_BROWSER = QueueSpec("scrape-browser")

# family -> queue. HTTP families share a wide/cheap pool; browser families a
# narrow/fat one (separate image, browser baked in).
_FAMILY_QUEUE = {
    "one_shot": QUEUE_HTTP,
    "paged": QUEUE_HTTP,
    "paged_detail": QUEUE_HTTP,
    "html": QUEUE_HTTP,
    "browser": QUEUE_BROWSER,
    "browser_session": QUEUE_BROWSER,
}


def queue_for(family: str) -> QueueSpec:
    return _FAMILY_QUEUE.get(family, QUEUE_HTTP)


@dataclass
class TemporalConfig:
    target: str
    namespace: str
    tls: bool

    @classmethod
    def from_env(cls) -> "TemporalConfig":
        return cls(
            target=os.environ.get("TEMPORAL_TARGET", "127.0.0.1:7233"),
            namespace=os.environ.get("TEMPORAL_NAMESPACE", "ingest"),
            tls=os.environ.get("TEMPORAL_TLS", "false").lower() == "true",
        )


# Fail-loud activity options (white paper §7.2). Deliberate non-values:
#   attempts = 1        (Temporal default is INFINITE — must be written down)
#   no heartbeat        (the daily G2 presence gate is the dead-worker detector)
#   start_to_close      absurd; exists only because the SDK requires one
ACTIVITY_OPTIONS = {
    "maximum_attempts": 1,
    "start_to_close_seconds": 30 * 24 * 3600,   # 30d — never meant to fire
    "heartbeat_seconds": None,
}
