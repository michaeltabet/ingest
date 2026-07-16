"""Layer 1 — the board input gate. Pydantic + per-platform regex.

The question this answers: BEFORE we spend a Temporal workflow on a board,
is the board even shaped right? A slug that can't match its platform's format,
or a url-dependent platform (oracle/workday) with no usable url, is REJECTED
here — it never reaches the worker, so it can't become an infinite-Running
ghost. Fail-loud stays: a rejected board gets an evidence row with
outcome='rejected' and the reason, it is not silently dropped.

Slug patterns are grounded in the REAL boards table (profiled 2026-07-15), not
guessed. Char classes and lengths come from what actually exists per platform:
  - most platforms: lowercase alnum + dash  ^[a-z0-9-]+$
  - ashby: also uppercase + dot (Ashby org tokens are mixed-case)
  - lever/greenhouse: a few dots slip in
  - oracle/workday: slug is NOT enough — the tenant host lives on board.url

Usage:
    from ingest.scraping.validation import validate_board, BoardIn
    ok, reason = validate_board("oracle", "etihadrail-iaalbv", url="")
    # -> (False, "url required: oracle needs a tenant url with /sites/ (got '')")
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


# --- per-platform slug specs (grounded in the real boards table) ------------

@dataclass(frozen=True)
class SlugSpec:
    """One platform's input contract."""
    slug_re: re.Pattern
    requires_url: bool = False
    # if requires_url: a predicate on the parsed url that must hold, + a human hint
    url_ok: "callable | None" = None
    url_hint: str = ""

    def slug_valid(self, slug: str) -> bool:
        return bool(slug) and bool(self.slug_re.match(slug))


# common shapes
_LC_DASH = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")          # bamboohr, icims, personio, ...
_LC_DASH_DOT = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")    # lever, greenhouse (dots slip in)
_MIXED_DOT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")  # ashby (mixed case + dots)
_TALEO = re.compile(r"^[a-z0-9][a-z0-9-]{0,30}$")           # becomes {slug}.taleo.net host


def _oracle_url_ok(u) -> bool:
    # https://{tenant-host}/hcmUI/CandidateExperience/en/sites/{site}
    return bool(u.netloc) and "/sites/" in u.path


def _workday_url_ok(u) -> bool:
    # {tenant}.wd{N}.myworkdayjobs.com
    return "myworkdayjobs.com" in (u.netloc or "")


SPECS: dict[str, SlugSpec] = {
    "greenhouse":      SlugSpec(_LC_DASH_DOT),
    "lever":           SlugSpec(_LC_DASH_DOT),
    "ashby":           SlugSpec(_MIXED_DOT),
    "bamboohr":        SlugSpec(_LC_DASH),
    "icims":           SlugSpec(_LC_DASH),
    "personio":        SlugSpec(_LC_DASH),
    "recruitee":       SlugSpec(_LC_DASH),
    "smartrecruiters": SlugSpec(_LC_DASH),
    "teamtailor":      SlugSpec(re.compile(r"^[a-z0-9][a-z0-9-]{0,79}$")),
    "workable":        SlugSpec(_LC_DASH),
    "taleo":           SlugSpec(_TALEO),   # slug-only: builds {slug}.taleo.net
    "oracle":          SlugSpec(_MIXED_DOT, requires_url=True,
                                url_ok=_oracle_url_ok,
                                url_hint="oracle needs a tenant url with /sites/"),
    "workday":         SlugSpec(_LC_DASH_DOT, requires_url=True,
                                url_ok=_workday_url_ok,
                                url_hint="workday needs a *.myworkdayjobs.com url"),
}


# --- the functional gate ----------------------------------------------------

def validate_board(platform: str, slug: str, url: str = "") -> tuple[bool, str]:
    """(ok, reason). reason == '' when ok. Never raises — this is a gate, not IO."""
    spec = SPECS.get(platform)
    if spec is None:
        return False, f"unknown platform {platform!r}"
    if not spec.slug_valid(slug):
        return False, f"bad slug {slug!r} for {platform} (want {spec.slug_re.pattern})"
    if spec.requires_url:
        if not url:
            return False, f"url required: {spec.url_hint} (got {url!r})"
        if spec.url_ok and not spec.url_ok(urlparse(url)):
            return False, f"bad url for {platform}: {spec.url_hint} (got {url!r})"
    return True, ""


# --- the Pydantic model (same rules, for typed ingestion paths) -------------

class BoardIn(BaseModel):
    """Validated board input. Construction FAILS on a malformed board — that's
    the point: you cannot build one that would hang the worker."""
    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    platform: str
    slug: str
    url: str = ""

    @field_validator("platform")
    @classmethod
    def _known_platform(cls, v: str) -> str:
        if v not in SPECS:
            raise ValueError(f"unknown platform {v!r}")
        return v

    @model_validator(mode="after")
    def _shape_ok(self) -> "BoardIn":
        ok, reason = validate_board(self.platform, self.slug, self.url)
        if not ok:
            raise ValueError(reason)
        return self
