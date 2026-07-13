"""Platform registry — auto-discovers scraping/platforms/*.py.

Every module in platforms/ that defines a Scraper subclass with a `platform`
attribute is registered by that key. Adding a platform = dropping a file. No
registration boilerplate, no central list to edit.
"""
from __future__ import annotations

import importlib
import pkgutil

from ingest.core.scraper import Scraper

_REGISTRY: dict = {}


def _discover():
    if _REGISTRY:
        return
    from ingest.scraping import platforms
    for mod in pkgutil.iter_modules(platforms.__path__):
        if mod.name.startswith("_"):
            continue
        m = importlib.import_module(f"ingest.scraping.platforms.{mod.name}")
        for obj in vars(m).values():
            if (isinstance(obj, type) and issubclass(obj, Scraper)
                    and getattr(obj, "platform", "abstract") != "abstract"):
                _REGISTRY[obj.platform] = obj


def get(platform: str) -> Scraper:
    _discover()
    cls = _REGISTRY.get(platform)
    if cls is None:
        raise KeyError(f"no scraper registered for platform={platform!r} "
                       f"(have {sorted(_REGISTRY)})")
    return cls()


def all_platforms() -> list:
    _discover()
    return sorted(_REGISTRY)
