"""Base class #2: Scraper (L1 contract).

The acquisition abstraction. Families implement the LOOP once; platforms
supply the FACTS. Three levels, no more:

    Scraper (this)  ->  <Family>Scraper (the loop)  ->  <Platform>Scraper (facts)

Rules (enforced by review + the registry-coverage test, not by the compiler):
- Family = mechanism, platform = facts. `if platform == "x"` in a family is a bug.
- Transport is injected via ctx; a scraper never constructs a client.
- The pagination hook is a CURSOR: parse_list(body, cursor) -> ListPage.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from .context import ScrapeContext
from .models import RawResult, Source


class Scraper(ABC):
    #: routing key -> family -> task queue (see orchestration/config.py)
    family: str = "abstract"

    #: set by the platform subclass; the registry uses it as the lookup key
    platform: str = "abstract"

    @abstractmethod
    async def fetch(self, source: Source, ctx: ScrapeContext) -> RawResult:
        """Acquire raw payloads for one source. Implemented by the FAMILY."""
        raise NotImplementedError
