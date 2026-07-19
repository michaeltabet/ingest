"""Thin ClickHouse door. insert(rows) / query(sql) and nothing else.

Deliberately minimal — this is a transport tool, not a place for logic. The
real client (clickhouse-connect) is imported lazily so core/tests don't need
it. WHICH ClickHouse is never known here: every fact comes from the spec's
database part via from_spec (env wins per-key); credentials only as the
env-var NAMES the spec declares. A NullClient is provided for --dry runs.
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

    def __init__(self, *, host: str, port: int, database: str,
                 user: str, password: str):
        import clickhouse_connect
        self._c = clickhouse_connect.get_client(
            host=host, port=port, database=database, username=user, password=password)

    @classmethod
    def from_spec(cls, db: dict) -> "ConnectClickHouse":
        """The spec's database part IS the address book (which store, which
        db, which env vars hold the credentials). Env wins per-key; nothing
        is baked in — a missing fact or unset credential env is a loud error."""
        import os

        def req(key):
            if key not in db:
                raise RuntimeError(f"spec is missing database fact {key!r}")
            return db[key]

        def cred(env_key):
            name = req(env_key)
            if name not in os.environ:
                raise RuntimeError(f"credential env {name!r} (named by spec "
                                   f"{env_key!r}) is not set")
            return os.environ[name]

        return cls(
            host=os.environ.get("CH_HOST") or req("host"),
            port=int(os.environ.get("CH_PORT") or req("port")),
            database=os.environ.get("CH_DATABASE") or req("database"),
            user=cred("user_env"),
            password=cred("password_env"),
        )

    def insert(self, table: str, rows: list, columns: tuple) -> None:
        self._c.insert(table, rows, column_names=list(columns))

    def query(self, sql: str) -> list:
        return self._c.query(sql).result_rows

    def command(self, sql: str) -> None:
        """DDL door (CREATE TABLE IF NOT EXISTS ...) — sinks own their schema."""
        self._c.command(sql)
