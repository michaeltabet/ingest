"""Base class #3: Domain (L1 contract). The engine/domain seam.

The engine (families, the page-walk, the streaming sink protocol, the Temporal
fan-out) knows HOW to acquire paginated remote data. It does not know WHAT the
data is. A Domain is the WHAT: one vertical, bundled as the
things the engine must be handed to run it:

    registry   which scrapers exist and how to look one up by platform key
    resolver   the key resolver — how (platform, key) becomes a concrete
               Source (id convention, stored-URL lookup)
    gate       this domain's definition of a successful run — completeness
               rules are the project's physics, stated in its own package
    sink       where this domain's rows land (its own tables, its own schema)

Rules:
- The engine NEVER imports from ingest.domains.*. Domain code is resolved by
  name at the orchestration boundary (activities), nowhere deeper.
- A domain NEVER reaches into the engine's loop. If a domain needs the walk to
  behave differently, that is a new family (engine MR), not a domain hack.
- Gate wording is single-sourced: the Verdict's reason IS the raised error AND
  the ledger row's recorded error. They can never disagree because they are
  the same string.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Verdict:
    """A gate's judgment of one run, from its evidence summary alone.

    outcome: success | empty | failure  (the ledger vocabulary)
    reason:  set iff failure — the ONE string that is both raised and recorded.
    """
    outcome: str
    reason: str | None = None

    @property
    def failed(self) -> bool:
        return self.outcome == "failure"


class Gate(ABC):
    """A project's quality gate. Pure: summary dict in, Verdict out. OPTIONAL:
    a project with no gate part gets FetchedGate — "we got the data in and
    there is data", nothing more. Quality rules come later, per project,
    when the project wants them."""

    @abstractmethod
    def evaluate(self, summary: dict) -> Verdict: ...


class FetchedGate(Gate):
    """The default: did the fetch work and did data land. No cleaning, no
    completeness math — landing the page IS the success."""

    def evaluate(self, s: dict) -> Verdict:
        if s["list_ok"]:
            return Verdict("success")
        return Verdict("failure",
                       f"fetch failed for {s['source_id']}: "
                       f"list_status={s['list_status']}")


class Sink(ABC):
    """Where a domain's rows land. Sync methods — the engine calls them via
    asyncio.to_thread, mirroring the blocking ClickHouse clients.

    flush   mid-scrape batches (bronze payloads + landed items). Called
            repeatedly; must be append-safe (a run that dies after a flush
            must be invisible downstream — see commit).
    commit  the END of a run: final flush of whatever the buffers still hold,
            then the evidence row carrying the gate's verdict, written LAST.
            The evidence row is the COMMIT MARKER — downstream reads gate on
            it, so batches from an uncommitted run are never visible.
    """

    @abstractmethod
    def flush(self, source, payloads: list, items: list, run_id: str) -> None:
        """source is passed because landed items deliberately don't carry
        their source identity — it is stamped at persist time, here."""

    @abstractmethod
    def commit(self, result, verdict: Verdict, run_id: str) -> None:
        """result: the RawResult (buffers + counters); its summary() and the
        verdict become the evidence row."""


class SourceResolver:
    """The KEY resolver — a class of its own because what a key MEANS is
    project-specific (a tenant slug, a ticker, a handle); the engine never
    interprets it. resolve() turns (platform, key) into a concrete Source;
    url() is overridden when the address must be looked up from an inventory;
    keys() lists the project's inventory for the nightly fan-out."""

    def resolve(self, platform: str, key: str) -> "Source":
        from ingest.core.models import Source
        return Source(source_id=f"{platform}:{key}", platform=platform,
                      key=key, url=self.url(platform, key))

    def url(self, platform: str, key: str) -> str:
        return ""

    def keys(self, platform: str, limit: int | None = None) -> list:
        """The project's source INVENTORY for one platform — what the
        nightly trigger fans out. Base: none."""
        return []


@dataclass(frozen=True)
class Domain:
    """One vertical, assembled from its <name>.conf spec (in the separate
    ingest-pipelines config repo) by ingest.domains.get(name)."""
    name: str
    registry: object                 # ScraperRegistry (duck-typed: .get, .all_platforms)
    resolver: SourceResolver
    gate: Gate
    make_sink: object                # () -> Sink; a factory because sinks hold
                                     # per-run client state and activities run
                                     # many sources concurrently
    # declared facts from the spec (strings as written; consumers parse):
    temporal: dict = None            # address/namespace/queue/retry/timeouts/...
    database: dict = None            # which store; creds as env-var NAMES only
    trigger: dict = None             # sources_limit (the wave bound)
    observability: object = None     # Observability class (4th family)
    scheduler: object = None         # Scheduler class (5th family)
    calibration: dict = None         # learned sized numbers {name: {value,lo,hi}}
    test: dict = None                # escalation-test config (validator, ladder)
    k8s: dict = None                 # deploy facts consumed by gitops
    platforms: dict = None           # per-platform parts: type/resolver/enabled

    def __post_init__(self):
        for f in ("temporal", "database", "trigger", "calibration",
                  "test", "k8s", "platforms"):
            if getattr(self, f) is None:
                object.__setattr__(self, f, {})

    def enabled_platforms(self) -> list:
        """Registry order, minus spec kill-switched ones. A platform with no
        [platform.*] part is enabled — the switch is opt-out."""
        return [p for p in self.registry.all_platforms()
                if self.platforms.get(p, {}).get("enabled", True)]
