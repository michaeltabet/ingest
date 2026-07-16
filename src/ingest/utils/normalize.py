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
