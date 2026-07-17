"""Pure utility functions. Date/HTML/digest helpers — COMPOSITION, not
inheritance. No scraper subclasses these; they call them. One home, so there
is never a second copy (atlas-kt had 37 stripHtmls / 42 sha256s)."""
from __future__ import annotations

import hashlib
import json
import re

_SCRIPT = re.compile(r"(?is)<script[^>]*>.*?</script>")
_STYLE = re.compile(r"(?is)<style[^>]*>.*?</style>")
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_ENTITIES = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">",
             "&quot;": '"', "&#39;": "'", "&apos;": "'"}


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(obj) -> str:
    """Stable content hash of a JSON-able object — the stub dedup key.
    Sorted keys → order-independent. Digest STABLE fields only (caller's job)."""
    return sha256_hex(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode())


def raw_json(obj) -> str:
    """One job's JSON, verbatim, for LANDING into `jobs` (ELT, no parse).
    Compact + unicode-preserving. One home so every platform lands identically."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def strip_html(html: str) -> str:
    """Batch-phase util (job descriptions). One implementation, not 37."""
    clean = _SCRIPT.sub("", html)
    clean = _STYLE.sub("", clean)
    clean = _TAG.sub(" ", clean)
    clean = _WS.sub(" ", clean).strip()
    for ent, ch in _ENTITIES.items():
        clean = clean.replace(ent, ch)
    return clean


# The floor below which a "description" is a placeholder, not a JD. ONE home —
# every platform jd_present() and the JSON-LD helper use this, never a literal.
MIN_JD_CHARS = 30

_LDJSON = re.compile(
    r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', re.I | re.S)


def ldjson_description_nonempty(raw: str) -> bool:
    """True if the page embeds a schema.org JobPosting whose `description` has
    real content. Used by scrapers that land the job PAGE (icims, personio) to
    tell a real JD from an empty shell. Parse failure → True: don't block on a
    detector bug, silver still sees the raw. One home for both platforms."""
    for block in _LDJSON.findall(raw or ""):
        try:
            obj = json.loads(block.strip())
        except Exception:
            return True
        items = obj if isinstance(obj, list) else [obj]
        for it in items:
            if isinstance(it, dict) and len(str(it.get("description", "")).strip()) > MIN_JD_CHARS:
                return True
    return False
