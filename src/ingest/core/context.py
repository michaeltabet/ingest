"""ScrapeContext (L1 boundary). What a scraper is HANDED, not what it builds.

Clients are injected (composition), never constructed by a scraper. This is
what makes offline repair possible: tests inject a fixture-replaying client
and the seen-set; nothing touches the network or ClickHouse.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging


def _no_heartbeat(*_detail) -> None:
    """Default heartbeat: does nothing. A scraper driven outside an activity
    (tests, offline repair, the recorder) must not know or care that Temporal
    exists."""


@dataclass
class ScrapeContext:
    http: object                       # utils.http.HttpClient (duck-typed: .send)
    heartbeat: object = _no_heartbeat  # callable(*detail) -> None. The family
                                       # calls it after each page and each
                                       # detail batch; the activity wires it to
                                       # temporalio activity.heartbeat, which is
                                       # what makes a STALLED scrape detectable.
                                       # Without it an activity that stops
                                       # progressing stays 'Started' until
                                       # start_to_close expires — 30 days.
    known_digests: set = field(default_factory=set)   # seen-set for dedup
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("ingest"))
    browser: object | None = None      # only browser-family scrapers use this
    sink: object | None = None         # async (payloads, items) -> None; the family
                                       # flushes buffered raw/items through this
                                       # mid-scrape so a big source never holds its
                                       # whole catalog in RAM. None = buffer all
                                       # (tests, offline repair).
