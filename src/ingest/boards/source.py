"""Board source — reads the REAL board list.

In-cluster: ClickHouse `ingest.boards` (loaded from runs_board, 5,671 boards).
Local dev: falls back to ~/autoapply/db.sqlite3 (runs_board) when CH is absent.
Same interface either way: `boards_for(platform) -> [slug, ...]`.
"""
from __future__ import annotations

import os
import sqlite3

SQLITE = os.environ.get("BOARDS_DB", os.path.expanduser("~/autoapply/db.sqlite3"))


def _use_clickhouse() -> bool:
    return bool(os.environ.get("CH_HOST"))


def _ch():
    from ingest.utils.clickhouse import ConnectClickHouse
    return ConnectClickHouse.from_env()


def boards_for(platform: str, limit: int | None = None) -> list:
    if _use_clickhouse():
        sql = ("SELECT DISTINCT slug FROM boards "
               f"WHERE platform = '{platform.lower()}' AND enabled = 1")
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [r[0] for r in _ch().query(sql)]
    q = ("SELECT slug FROM runs_board WHERE platform_id=? "
         "AND slug NOT IN ('','None') AND slug IS NOT NULL")
    if limit:
        q += f" LIMIT {int(limit)}"
    with sqlite3.connect(SQLITE) as c:
        return [r[0] for r in c.execute(q, (platform.lower(),))]


def counts() -> dict:
    if _use_clickhouse():
        return {p: n for p, n in _ch().query(
            "SELECT platform, count(*) FROM boards WHERE enabled=1 GROUP BY platform")}
    with sqlite3.connect(SQLITE) as c:
        return {p: n for p, n in c.execute(
            "SELECT platform_id, count(*) FROM runs_board "
            "WHERE slug NOT IN ('','None') GROUP BY platform_id")}
