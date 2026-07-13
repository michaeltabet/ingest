"""Thin ClickHouse door. insert(rows) / query(sql) and nothing else.

Deliberately minimal — this is a transport tool, not a place for logic. The
real client (clickhouse-connect) is imported lazily so core/tests don't need
it. A NullClient is provided for --dry runs (prints instead of writing).
"""
from __future__ import annotations

from typing import Protocol


class ClickHouse(Protocol):
    def insert(self, table: str, rows: list, columns: tuple) -> None: ...
    def query(self, sql: str) -> list: ...


class NullClickHouse:
    """No-op sink for offline --dry runs and tests. Records calls."""

    def __init__(self):
        self.inserted = []

    def insert(self, table: str, rows: list, columns: tuple) -> None:
        self.inserted.append((table, len(rows)))

    def query(self, sql: str) -> list:
        return []


class ConnectClickHouse:
    """Real client over clickhouse-connect (lazy import)."""

    def __init__(self, *, host: str, port: int = 8123, database: str = "ingest",
                 user: str = "default", password: str = ""):
        import clickhouse_connect
        self._c = clickhouse_connect.get_client(
            host=host, port=port, database=database, username=user, password=password)

    @classmethod
    def from_env(cls) -> "ConnectClickHouse":
        import os
        return cls(
            host=os.environ.get("CH_HOST", "127.0.0.1"),
            port=int(os.environ.get("CH_PORT", "8123")),
            database=os.environ.get("CH_DATABASE", "ingest"),
            user=os.environ.get("CH_USER", "default"),
            password=os.environ.get("CH_PASSWORD", ""),
        )

    def insert(self, table: str, rows: list, columns: tuple) -> None:
        self._c.insert(table, rows, column_names=list(columns))

    def query(self, sql: str) -> list:
        return self._c.query(sql).result_rows
