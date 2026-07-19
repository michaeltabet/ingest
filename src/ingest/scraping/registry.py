"""Platform registry — auto-discovers Scraper subclasses in a package.

Every module in the target package that defines a Scraper subclass with a
`platform` attribute is registered by that key. Adding a platform = dropping a
file. No registration boilerplate, no central list to edit.

Domain-neutral: each project owns a ScraperRegistry over ITS scrapers
package (declared in its spec). The engine ships NO platforms of its own.
"""
from __future__ import annotations

import importlib
import pkgutil

from ingest.core.scraper import Scraper


class ScraperRegistry:
    def __init__(self, package: str):
        self.package = package
        self._registry: dict = {}

    def _discover(self):
        if self._registry:
            return
        pkg = importlib.import_module(self.package)
        for mod in pkgutil.iter_modules(pkg.__path__):
            if mod.name.startswith("_"):
                continue
            m = importlib.import_module(f"{self.package}.{mod.name}")
            for obj in vars(m).values():
                if (isinstance(obj, type) and issubclass(obj, Scraper)
                        and getattr(obj, "platform", "abstract") != "abstract"):
                    self._registry[obj.platform] = obj

    def get(self, platform: str) -> Scraper:
        self._discover()
        cls = self._registry.get(platform)
        if cls is None:
            raise KeyError(f"no scraper registered for platform={platform!r} "
                           f"in {self.package} (have {sorted(self._registry)})")
        return cls()

    def all_platforms(self) -> list:
        self._discover()
        return sorted(self._registry)

