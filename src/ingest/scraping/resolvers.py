"""THE resolver library — named key-resolution strategies the spec picks from.

A spec says `source.resolver = <preset>` and the loader hands the engine the
matching class. What a KEY means is the project's business; these classes only
know how to turn one into a URL/inventory. A strategy the engine lacks is a
NEW CLASS HERE — never logic in a spec.

    derived           the URL is derivable from the key alone; the platform's
                      list_request template does the building. No inventory.
    static            the inventory IS the spec: source.keys = {platform:
                      [key, ...]} — for projects whose sources are a fixed,
                      declared list (a page, a feed), no store needed.
    clickhouse-table  keys AND stored URLs live in a ClickHouse table declared
                      by `source.inventory = clickhouse:<table>`, with the key
                      column named by `source.key_column` (a spec fact — the
                      engine bakes in no column names).

`module:Class` refs remain the escape hatch for a strategy that is truly
one-project-only — a second user means it moves here.
"""
from __future__ import annotations

from ingest.core.domain import SourceResolver


class ClickHouseTableResolver(SourceResolver):
    """Inventory + stored-URL lookup over one ClickHouse table. The table and
    key column are spec facts; the client comes from the spec's database part."""

    def __init__(self, table: str, key_column: str, database: dict):
        self.table = table
        self.key_column = key_column
        self.database = database

    def _ch(self):
        from ingest.utils.clickhouse import ConnectClickHouse
        return ConnectClickHouse.from_spec(self.database)

    def keys(self, platform: str, limit: int | None = None) -> list:
        sql = (f"SELECT DISTINCT {self.key_column} FROM {self.table} "
               f"WHERE platform = '{platform.lower()}' AND enabled = 1")
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [r[0] for r in self._ch().query(sql)]

    def url(self, platform: str, key: str) -> str:
        rows = self._ch().query(
            f"SELECT url FROM {self.table} WHERE platform='{platform}' "
            f"AND {self.key_column}='{key}' AND url != '' "
            f"ORDER BY url DESC LIMIT 1")
        return rows[0][0] if rows else ""


class StaticResolver(SourceResolver):
    """Inventory declared in the spec itself — keys per platform, no store."""

    def __init__(self, keys_by_platform: dict):
        self.keys_by_platform = keys_by_platform or {}

    def keys(self, platform: str, limit: int | None = None) -> list:
        ks = list(self.keys_by_platform.get(platform, []))
        return ks[:limit] if limit else ks


def build(preset: str, source_part: dict, database: dict) -> SourceResolver:
    """A preset name + the spec's source part -> a resolver. Unknown preset =
    a loud error naming what exists; the fix is a new class in this library."""
    if preset == "derived":
        return SourceResolver()
    if preset == "static":
        if not isinstance(source_part.get("keys"), dict):
            raise ValueError("resolver 'static' needs source.keys = "
                             "{platform: [key, ...]}")
        return StaticResolver(source_part["keys"])
    if preset == "clickhouse-table":
        inventory = source_part.get("inventory", "")
        kind, _, table = inventory.partition(":")
        if kind != "clickhouse" or not table:
            raise ValueError("resolver 'clickhouse-table' needs "
                             "source.inventory = clickhouse:<table>")
        key_column = source_part.get("key_column")
        if not key_column:
            raise ValueError("resolver 'clickhouse-table' needs "
                             "source.key_column (the engine bakes in no "
                             "column names)")
        return ClickHouseTableResolver(table, key_column, database)
    raise ValueError(
        f"no resolver preset {preset!r} in the library (have: derived, "
        f"static, clickhouse-table) — add the class to ingest/scraping/resolvers.py")
