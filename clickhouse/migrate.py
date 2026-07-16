"""Generate ClickHouse DDL FROM the Record classes. No hand-written SQL.

    python clickhouse/migrate.py            # print DDL for every Record table
    python clickhouse/migrate.py --write    # write clickhouse/migrations/NNN_*.sql

Applying to a live ClickHouse is a separate, explicit step (deploy/), never
automatic. This makes every schema change a reviewable artifact.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.core.record import Record
import ingest.discovery.records  # noqa: F401  (registers the subclasses)
import ingest.ledger.records  # noqa: F401  (registers the subclasses)


def all_ddl() -> str:
    tables = sorted(Record.subclasses(), key=lambda c: c.__table__)
    return "\n\n".join(t.ddl() for t in tables)


if __name__ == "__main__":
    ddl = all_ddl()
    if "--write" in sys.argv:
        out = Path(__file__).parent / "migrations" / "001_initial.sql"
        out.parent.mkdir(exist_ok=True)
        out.write_text(ddl + "\n")
        print(f"wrote {out}")
    else:
        print(ddl)
