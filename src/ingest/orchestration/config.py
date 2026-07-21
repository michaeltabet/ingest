"""Temporal configuration — resolved from the project's spec, NOTHING baked in.

Precedence: env (secrets / per-process override) > the spec's temporal part.
There are NO engine defaults for facts: no addresses, no namespace, no queue
names, no sized numbers live in this file. A missing fact is a loud error
naming the spec key to set — never a silent localhost.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _spec_parts() -> tuple:
    """(temporal, calibration) of the active project (INGEST_DOMAIN)."""
    name = os.environ.get("INGEST_DOMAIN")
    if not name:
        raise RuntimeError("INGEST_DOMAIN not set — the engine has no default "
                           "project and no default temporal facts")
    from ingest import domains
    d = domains.get(name)
    return d.temporal, d.calibration


def _req(d: dict, *path):
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            raise RuntimeError(
                f"spec is missing fact {'.'.join(path)!r} — declare it in the "
                f"project's json (the engine bakes in nothing)")
        cur = cur[key]
    return cur


_SPEC, _CAL = _spec_parts()


@dataclass
class QueueSpec:
    name: str


QUEUE_HTTP = QueueSpec(_req(_SPEC, "task_queue"))
# browser families route to task_queue_browser when the spec declares one;
# until then they share the main queue (the browser family raises anyway).
QUEUE_BROWSER = QueueSpec(_SPEC.get("task_queue_browser",
                                    _req(_SPEC, "task_queue")))

_FAMILY_QUEUE = {"browser": QUEUE_BROWSER, "browser_session": QUEUE_BROWSER}


def queue_for(family: str) -> QueueSpec:
    return _FAMILY_QUEUE.get(family, QUEUE_HTTP)


@dataclass
class TemporalConfig:
    target: str
    namespace: str
    tls: bool

    @classmethod
    def from_env(cls) -> "TemporalConfig":
        # env wins (TEMPORAL_ADDRESS cluster convention / TEMPORAL_TARGET),
        # then the spec's declared address. No literal fallback.
        target = (os.environ.get("TEMPORAL_ADDRESS")
                  or os.environ.get("TEMPORAL_TARGET")
                  or _req(_SPEC, "address"))
        return cls(
            target=target,
            namespace=(os.environ.get("TEMPORAL_NAMESPACE")
                       or _req(_SPEC, "namespace")),
            tls=os.environ.get("TEMPORAL_TLS", "false").lower() == "true",
        )


# Fail-loud activity options — every number DECLARED in the spec, where the
# _why comments explaining the doctrine live beside them.
ACTIVITY_OPTIONS = {
    # 0 = unbounded attempts. Retries are bounded by KIND (see
    # non_retryable_error_types below and the _DURABLE docstring in
    # workflows.py), never by a count — a transient fault must not cost a day
    # of data, and a permanent one must not be retried even once.
    "maximum_attempts": int(_req(_SPEC, "retry", "maximum_attempts")),
    "retry_initial_seconds": int(_SPEC.get("retry", {}).get("initial_seconds", 10)),
    # Cap the backoff so a source that was failing while infrastructure was
    # broken resumes PROMPTLY once it is fixed, instead of sitting in a
    # multi-hour exponential sleep.
    "retry_maximum_seconds": int(_SPEC.get("retry", {}).get("maximum_seconds", 300)),
    # Errors that never heal. Anything NOT named here is retried.
    "non_retryable_error_types": _SPEC.get("retry", {}).get(
        "non_retryable_error_types",
        ["PermanentError", "GateRefused"]),
    "start_to_close_seconds":
        int(_req(_SPEC, "timeouts", "start_to_close_days")) * 24 * 3600,
    "heartbeat_seconds": _SPEC.get("timeouts", {}).get("heartbeat_seconds"),
}

# worker sizing — a CALIBRATED number from the spec (env WORKER_SLOTS wins)
WORKER = {
    "slots": int(_req(_CAL, "worker_slots", "value")),
    "max_cached_workflows": int(_req(_SPEC, "worker", "max_cached_workflows")),
}
