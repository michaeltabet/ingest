"""Domain resolution — assembles a Domain from its JSON spec in the SEPARATE
config repo (ingest-pipelines). The factory contract:

    the JSON is the ORDER FORM        (facts + one-word picks, zero logic)
    the engine is the FACTORY         (families, resolvers, walk, orchestration)

One <project>.json per project. Every pick in the JSON is a NAME into an
engine library — scraper type -> families.SPEC_TYPES, key resolver ->
scraping/resolvers.py presets, gate/sink -> module:Class refs. A strategy the
engine doesn't have yet is added TO THE LIBRARY, never written in a spec.

Spec location: INGEST_PIPELINES env (in-cluster: the ConfigMap mount), else a
sibling checkout `../ingest-pipelines`. Missing = a loud error, not a default.

Overlays: overlays/<name>/<project>.json, selected with INGEST_OVERLAY=<name>,
merged over the base per-key (two levels deep — a platform part can be
overridden without restating the rest).

Parts (unknown parts/keys fail loud; _keys are reader comments, stripped):

    project    {name, description}           name must match the file name
    temporal   WHICH temporal + the knobs: {address, ui, namespace,
               task_queue, retry{maximum_attempts}, timeouts{...},
               payload_max_bytes, worker{slots, max_cached_workflows}}
    trigger    {cron, sources_limit}         the nightly wave
    database   WHICH store: {kind, host, port, database, user_env,
               password_env} — creds only as env-var NAMES
    scrapers   {package}                     registry auto-discovers it
    gate       {ref}                         module:Class
    sink       {ref}                         module:Class
    source     {resolver, inventory}         resolver = library preset name
                                             (module:Class = escape hatch)
    k8s        {worker_replicas, worker_slots, worker_memory}   gitops facts
    overlays   {name: description}           declared, discoverable
    platforms  {<key>: {type, resolver, enabled}}
               type VALIDATED against the class family (drift fails loud);
               resolver: derived | stored_url; enabled: kill-switch (default true)
"""
from __future__ import annotations

import importlib
import importlib.util
import json
import os
import sys
from pathlib import Path

from ingest.core.domain import Domain, Gate, Sink, SourceResolver
from ingest.scraping.registry import ScraperRegistry

_PARTS = {
    "project": {"name", "description"},
    "code": {"path"},
    "temporal": {"address", "ui", "namespace", "task_queue", "retry",
                 "timeouts", "payload_max_bytes", "worker"},
    "trigger": {"sources_limit"},
    "observability": None,  # kind picks the class; THE CLASS validates args
    "scheduler": None,      # same
    "database": {"kind", "host", "port", "database", "user_env",
                 "password_env"},
    "scrapers": {"package"},
    "gate": {"ref"},
    "sink": {"ref"},
    "source": {"resolver", "inventory", "key_column", "keys"},
    "k8s": {"worker_replicas", "worker_slots", "worker_memory"},
    "calibration": None, # the learned numbers: {name: {value, lo, hi}}
    "test": None,        # the escalation test: validator ref, ladder, tables
    "overlays": None,    # free-form: overlay name -> description
    "platforms": None,   # validated per platform below
}
_PLATFORM_KEYS = {"type", "resolver", "enabled"}
_REQUIRED = ("project", "scrapers", "sink")
_RESOLVER_PRESETS = {"derived", "stored_url"}

_CACHE: dict = {}


def pipelines_dir() -> Path:
    override = os.environ.get("INGEST_PIPELINES")
    if override:
        return Path(override)
    sibling = Path(__file__).resolve().parents[3].parent / "ingest-pipelines"
    if sibling.is_dir():
        return sibling
    raise FileNotFoundError(
        "no pipelines config repo: set INGEST_PIPELINES or check out "
        "ingest-pipelines next to the ingest repo")


def _resolve(ref: str, what: str):
    """'module.path:attr' -> the attr. The ONLY indirection a spec allows."""
    try:
        mod_name, attr = ref.split(":", 1)
        return getattr(importlib.import_module(mod_name), attr)
    except (ValueError, ImportError, AttributeError) as e:
        raise ValueError(f"pipelines: bad {what} ref {ref!r}: {e}") from e


def _strip_comments(node):
    """Keys starting with _ are prose FOR THE READER (JSON has no comments) —
    invisible to validation and consumers."""
    if isinstance(node, dict):
        return {k: _strip_comments(v) for k, v in node.items()
                if not k.startswith("_")}
    return node


def _merge(base: dict, overlay: dict) -> dict:
    """Two-level merge: an overlay restates only what it changes."""
    out = dict(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def _validate_platform_parts(fname: str, spec: dict,
                             registry: ScraperRegistry) -> dict:
    """platforms parts, checked against the CODE: the platform must exist in
    the scrapers package and its one-word type must match the class's family
    (families.SPEC_TYPES) — a spec that drifts from code is an error."""
    from ingest.scraping.families import SPEC_TYPES
    known = set(registry.all_platforms())
    out = {}
    for key, part in (spec.get("platforms") or {}).items():
        if not isinstance(part, dict):
            raise ValueError(f"{fname}: platforms.{key} must be an object")
        if key not in known:
            raise ValueError(f"{fname}: platforms.{key} has no scraper class "
                             f"in the package (have {sorted(known)})")
        unknown = set(part) - _PLATFORM_KEYS
        if unknown:
            raise ValueError(f"{fname}: unknown key(s) {sorted(unknown)} "
                             f"in platforms.{key}")
        declared = part.get("type", "")
        family = registry.get(key).family
        if SPEC_TYPES.get(declared) != family:
            raise ValueError(
                f"{fname}: platforms.{key} declares type={declared!r} but the "
                f"class family is {family!r} — spec drifted from code "
                f"(one-word types: {sorted(SPEC_TYPES)})")
        preset = part.get("resolver", "derived")
        if preset not in _RESOLVER_PRESETS:
            raise ValueError(f"{fname}: platforms.{key} resolver={preset!r} "
                             f"(allowed: {sorted(_RESOLVER_PRESETS)})")
        out[key] = {"type": declared, "resolver": preset,
                    "enabled": bool(part.get("enabled", True))}
    return out


def _load(name: str) -> Domain:
    base_path = pipelines_dir() / f"{name}.json"
    if not base_path.is_file():
        raise KeyError(f"no pipeline spec for project {name!r} "
                       f"(looked for {base_path}; have {sorted(all_domains())})")
    fname = base_path.name
    try:
        spec = json.loads(base_path.read_text())
    except json.JSONDecodeError as e:
        raise ValueError(f"{fname}: not valid JSON — {e}") from e

    overlay = os.environ.get("INGEST_OVERLAY")
    if overlay:
        opath = pipelines_dir() / "overlays" / overlay / f"{name}.json"
        if not opath.is_file():
            raise KeyError(f"overlay {overlay!r} has no {name}.json ({opath})")
        spec = _merge(spec, json.loads(opath.read_text()))

    spec = _strip_comments(spec)
    # the project's CODE REPO, declared in the spec ("code": {"path": ...}) —
    # joins sys.path so the spec's refs (<pkg>.gate:Class) import by name.
    # This repo holds only order forms; code lives where the spec says.
    if "code" in spec:
        code_dir = Path(os.path.expanduser(spec["code"]["path"]))
        if not code_dir.is_absolute():
            code_dir = pipelines_dir() / code_dir
        if code_dir.is_dir():
            if str(code_dir) not in sys.path:
                sys.path.insert(0, str(code_dir))
        else:
            # code.path is a DEV convenience (a checkout on someone's laptop).
            # In a container the project code is already on PYTHONPATH, so a
            # missing path is only fatal if the package truly can't be found —
            # otherwise a laptop-shaped path ("~/jobs-scraper") would break
            # every in-cluster run (07-18).
            pkg = spec["scrapers"]["package"].split(".")[0]
            if importlib.util.find_spec(pkg) is None:
                raise ValueError(
                    f"{fname}: code.path {code_dir} does not exist AND "
                    f"package {pkg!r} is not importable")
    for part, val in spec.items():
        if part not in _PARTS:
            raise ValueError(f"{fname}: unknown part {part!r} "
                             f"(allowed: {sorted(_PARTS)})")
        if not isinstance(val, dict):
            raise ValueError(f"{fname}: part {part!r} must be an object")
        if _PARTS[part] is not None:
            unknown = set(val) - _PARTS[part]
            if unknown:
                raise ValueError(f"{fname}: unknown key(s) {sorted(unknown)} "
                                 f"in {part!r}")
    missing = [p for p in _REQUIRED if p not in spec]
    if missing:
        raise ValueError(f"{fname}: missing required part(s) {missing}")
    if spec["project"].get("name") != name:
        raise ValueError(f"{fname}: project.name="
                         f"{spec['project'].get('name')!r} must match the "
                         f"file name {name!r}")

    if "gate" in spec:
        gate = _resolve(spec["gate"]["ref"], "gate")()
        if not isinstance(gate, Gate):
            raise ValueError(f"{fname}: gate ref is not a core.domain.Gate")
    else:
        # no gate part = no judgment: we got the data in and there is data
        from ingest.core.domain import FetchedGate
        gate = FetchedGate()
    sink_cls = _resolve(spec["sink"]["ref"], "sink")
    if not (isinstance(sink_cls, type) and issubclass(sink_cls, Sink)):
        raise ValueError(f"{fname}: sink ref is not a core.domain.Sink class")

    # the key resolver: a preset NAME from the engine's resolver library
    # ("use this kind of resolver"); a module:Class ref is the escape hatch.
    src = spec.get("source", {})
    if "resolver" in src:
        if ":" in src["resolver"]:
            resolver = _resolve(src["resolver"], "source")()
        else:
            from ingest.scraping import resolvers as resolver_library
            resolver = resolver_library.build(src["resolver"], src,
                                              spec.get("database", {}))
    else:
        resolver = SourceResolver()
    if not isinstance(resolver, SourceResolver):
        raise ValueError(f"{fname}: source resolver is not a "
                         f"core.domain.SourceResolver")

    observability = None
    if "observability" in spec:
        from ingest import observability as obs_library
        observability = obs_library.build(spec["observability"]["kind"],
                                          spec["observability"])
    scheduler = None
    if "scheduler" in spec:
        from ingest import scheduling as sched_library
        scheduler = sched_library.build(spec["scheduler"]["kind"],
                                        spec["scheduler"])

    registry = ScraperRegistry(spec["scrapers"]["package"])
    platforms = _validate_platform_parts(fname, spec, registry)

    return Domain(name=name, registry=registry, resolver=resolver, gate=gate,
                  make_sink=sink_cls,
                  temporal=spec.get("temporal", {}),
                  database=spec.get("database", {}),
                  trigger=spec.get("trigger", {}),
                  observability=observability, scheduler=scheduler,
                  test=spec.get("test", {}),
                  calibration=spec.get("calibration", {}),
                  k8s=spec.get("k8s", {}),
                  platforms=platforms)


def get(name: str) -> Domain:
    if name not in _CACHE:
        _CACHE[name] = _load(name)
    return _CACHE[name]


def register(domain: Domain) -> None:
    """Inject a Domain built in code (tests, offline repair). Same fail-loud
    lookup path; spec files remain the ONE home for real domains."""
    _CACHE[domain.name] = domain


def all_domains() -> list:
    try:
        d = pipelines_dir()
    except FileNotFoundError:
        return sorted(_CACHE)
    specs = sorted(p.stem for p in d.glob("*.json")) if d.is_dir() else []
    return sorted(set(specs) | set(_CACHE))
