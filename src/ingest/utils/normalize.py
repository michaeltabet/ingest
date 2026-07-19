"""Pure utility functions — digest helpers. COMPOSITION, not inheritance.
One home, so there is never a second copy."""
from __future__ import annotations

import hashlib
import json


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(obj) -> str:
    """Stable content hash of a JSON-able object — the stub dedup key.
    Sorted keys → order-independent. Digest STABLE fields only (caller's job)."""
    return sha256_hex(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode())


def raw_json(obj) -> str:
    """One item's JSON, verbatim, for LANDING (ELT, no parse). Compact +
    unicode-preserving. One home so every platform lands identically."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
