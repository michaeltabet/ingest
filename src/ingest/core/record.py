"""Base class #1: Record (persistence as a class — no hand-written SQL).

A ClickHouse table is not a .sql file. It is a Record subclass that knows its
own columns, engine, and ordering, and GENERATES its own DDL. clickhouse/
migrate.py walks the Record subclasses and emits versioned migrations, so the
schema can never drift from the code.

Columns are declared as an ordered mapping of name -> ClickHouse type. Keeping
this dependency-free (no ORM) is deliberate: ClickHouse is analytical, append-
mostly, and the DDL is trivial to generate.
"""
from __future__ import annotations

from abc import ABC


class Record(ABC):
    __table__: str = ""
    __engine__: str = "MergeTree"
    __order_by__: tuple = ()
    __partition_by__: str | None = None
    #: ordered: column name -> ClickHouse type
    __columns__: dict = {}

    @classmethod
    def ddl(cls) -> str:
        if not cls.__table__ or not cls.__columns__:
            raise ValueError(f"{cls.__name__}: __table__ and __columns__ required")
        cols = ",\n  ".join(f"{n} {t}" for n, t in cls.__columns__.items())
        parts = [f"CREATE TABLE IF NOT EXISTS {cls.__table__} (\n  {cols}\n)"]
        parts.append(f"ENGINE = {cls.__engine__}")
        if cls.__partition_by__:
            parts.append(f"PARTITION BY {cls.__partition_by__}")
        if cls.__order_by__:
            parts.append(f"ORDER BY ({', '.join(cls.__order_by__)})")
        return "\n".join(parts) + ";"

    @classmethod
    def columns(cls) -> tuple:
        return tuple(cls.__columns__.keys())

    @classmethod
    def subclasses(cls) -> list:
        """All concrete Record tables — used by clickhouse/migrate.py."""
        out = []
        for sub in cls.__subclasses__():
            if sub.__table__:
                out.append(sub)
            out.extend(sub.subclasses())
        return out
