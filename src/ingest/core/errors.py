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


class GateRefused(PermanentError):
    """The source scraped, but its quality gate refused the result.

    Its own type because it is NOT an infrastructure fault: the scrape
    worked and the data is bad, so retrying it changes nothing. This was
    raised as a bare RuntimeError until 2026-07-21; once retries became
    kind-bounded, an untyped gate refusal would have been treated as
    transient and retried forever — exactly the slot-eating retry
    population of 2026-07-15. Named in non_retryable_error_types.
    """


def classify(status: int, *, url: str | None = None) -> ScrapeError:
    if status == 429 or 500 <= status <= 599:
        return TransientError(f"transient HTTP {status}", status=status, url=url)
    return PermanentError(f"permanent HTTP {status}", status=status, url=url)
