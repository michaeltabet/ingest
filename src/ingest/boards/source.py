"""Board source — reads the REAL board list.

Canonical source today: ~/autoapply/db.sqlite3 → runs_board (5,671 boards,
board_id, company, slug, platform_id) joined with runs_platform (scrape_enabled).
This is the bridge until boards live in the Django Board table on the cluster;
same interface either way: `boards_for(platform) -> [slug, ...]`.
"""
from __future__ import annotations

import os
import sqlite3

DB = os.environ.get("BOARDS_DB",
                    os.path.expanduser("~/autoapply/db.sqlite3"))


def _con():
    return sqlite3.connect(DB)


def enabled_platforms() -> list:
    with _con() as c:
        return [r[0] for r in c.execute(
            "SELECT board_platform_key FROM runs_platform WHERE scrape_enabled=1")]


def boards_for(platform_key: str, limit: int | None = None) -> list:
    q = ("SELECT slug FROM runs_board WHERE platform_id=? "
         "AND slug NOT IN ('','None') AND slug IS NOT NULL")
    if limit:
        q += f" LIMIT {int(limit)}"
    with _con() as c:
        return [r[0] for r in c.execute(q, (platform_key.lower(),))]


def counts() -> dict:
    with _con() as c:
        return {p: n for p, n in c.execute(
            "SELECT platform_id, count(*) FROM runs_board "
            "WHERE slug NOT IN ('','None') GROUP BY platform_id")}
