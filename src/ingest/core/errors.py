"""Error taxonomy (L0). The distinction that drives retry decisions.

Transient  → worth trying again (the janitor re-runs it on its cadence).
Permanent  → fail now, flag the source; retrying is pointless.

We CLASSIFY; Temporal does NOT retry heroically (attempts=1). Failure lands in
the ledger as data. Cross-run retry belongs to the daily pass, not the code.
"""
from __future__ import annotations


class ScrapeError(Exception):
    """Base for scrape failures. Carries source/url so breakage is LOUD and
    precise ('workday: detail 403') — never a silent swallow."""

    def __init__(self, message: str, *, status: int | None = None,
                 url: str | None = None):
        super().__init__(message)
        self.status = status
        self.url = url


class TransientError(ScrapeError):
    """429, 5xx, timeouts, connection resets — the world blinked."""


class PermanentError(ScrapeError):
    """403 (anti-bot), 404, malformed source — will not heal by retrying."""


def classify(status: int, *, url: str | None = None) -> ScrapeError:
    if status == 429 or 500 <= status <= 599:
        return TransientError(f"transient HTTP {status}", status=status, url=url)
    return PermanentError(f"permanent HTTP {status}", status=status, url=url)
