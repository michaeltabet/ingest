"""ScrapeContext (L1 boundary). What a scraper is HANDED, not what it builds.

Clients are injected (composition), never constructed by a scraper. This is
what makes offline repair possible: tests inject a fixture-replaying client
and the seen-set; nothing touches the network or ClickHouse.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging


@dataclass
class ScrapeContext:
    http: object                       # utils.http.HttpClient (duck-typed: .send)
    known_digests: set = field(default_factory=set)   # seen-set for dedup
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("ingest"))
    browser: object | None = None      # only browser-family scrapers use this
