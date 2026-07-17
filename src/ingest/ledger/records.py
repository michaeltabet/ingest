"""Concrete Record tables. THE only definition of every ClickHouse table.

DDL is generated from these classes by clickhouse/migrate.py — there is no
hand-written SQL anywhere. Change a column here → a new migration is generated
→ reviewed in an MR. Schema cannot drift from code.
"""
from __future__ import annotations

from ingest.core.record import Record


class RawPayloadRecord(Record):
    """BRONZE. Append-only, immutable. Raw responses, verbatim."""
    __table__ = "scrape_raw"
    __engine__ = "MergeTree"
    __partition_by__ = "toYYYYMMDD(fetched_at)"
    __order_by__ = ("platform", "board_id", "digest")
    __columns__ = {
        "platform": "LowCardinality(String)",
        "board_id": "String",
        "run_id": "String",
        "url": "String",
        "kind": "LowCardinality(String)",     # list | detail
        "http_status": "UInt16",
        "digest": "String",                    # sha256 of body
        "stub_digest": "String",               # dedup key (detail rows)
        "body": "String",                      # raw bytes
        "fetched_at": "DateTime64(3)",
    }


class EvidenceRecord(Record):
    """The LEDGER. One row per board per run — cost accounting, gate input,
    and observability source, all at once."""
    __table__ = "scrape_evidence"
    __engine__ = "MergeTree"
    __partition_by__ = "toYYYYMMDD(run_at)"
    __order_by__ = ("platform", "board_id", "run_at")
    __columns__ = {
        "run_id": "String",
        "platform": "LowCardinality(String)",
        "board_id": "String",
        "list_status": "UInt16",
        "pages_fetched": "UInt32",
        "stubs_seen": "UInt32",
        "items_seen": "UInt32",       # every posting the pages contained
        "dupes_seen": "UInt32",       # stubs served twice (pagination shifted)
        "reported_total": "UInt32",   # the board's own claim; gate input
        "jobs_extracted": "UInt32",
        "details_ok": "UInt32",
        "details_failed": "UInt32",
        "details_gone": "UInt32",     # detail 404/410 — churn, not fault
        "jobs_no_jd": "UInt32",       # landed jobs with no description (fake-pass input)
        "payloads": "UInt32",
        "bytes_in": "UInt64",
        "outcome": "LowCardinality(String)",   # success | empty | failure
        "errors": "Array(String)",
        "run_at": "DateTime64(3)",
    }


class JobRecord(Record):
    """LANDED jobs (ELT). One row per job, raw JSON verbatim — NOT parsed.
    Field extraction is a downstream transform over `raw`.

    ReplacingMergeTree keyed on (platform, board_id, external_id) with
    fetched_at as the version → a rerun REPLACES the same job, never dupes.
    Idempotent by construction: run the flow twice, same rows.
    """
    __table__ = "jobs"
    __engine__ = "ReplacingMergeTree(fetched_at)"
    __partition_by__ = "platform"
    __order_by__ = ("platform", "board_id", "external_id")
    __columns__ = {
        "platform": "LowCardinality(String)",
        "board_id": "String",
        "external_id": "String",
        "raw": "String",                       # the single job's JSON, verbatim
        "digest": "String",
        "run_id": "String",
        "fetched_at": "DateTime64(3)",
    }


class JobRunRecord(Record):
    """MEMBERSHIP: which jobs a given run saw. Content-free, append-only.

    Needed because `jobs` is a ReplacingMergeTree: a run that dies mid-scrape
    (batches now stream out via the sink) can overwrite rows from the last
    good run before failing its gate. Silver therefore reads:
        evidence (outcome='success', latest per board)  → the committed run_id
        jobs_runs (that run_id)                          → its job set
        jobs                                             → the content
    The evidence row is the COMMIT MARKER — written last, after all batches.
    TTL keeps this small: membership older than 30 days has no reader (silver
    only ever wants the latest successful run per board)."""
    __table__ = "jobs_runs"
    __engine__ = "MergeTree"
    __partition_by__ = "toYYYYMMDD(fetched_at)"
    __order_by__ = ("platform", "board_id", "run_id", "external_id")
    __columns__ = {
        "platform": "LowCardinality(String)",
        "board_id": "String",
        "external_id": "String",
        "run_id": "String",
        "fetched_at": "DateTime64(3)",
    }

    @classmethod
    def ddl(cls) -> str:
        base = super().ddl()
        return base[:-1] + "\nTTL toDateTime(fetched_at) + INTERVAL 30 DAY;"


class GateResultRecord(Record):
    """Every gate evaluation — gates are data. 'Did the gates run?' is
    itself checkable."""
    __table__ = "gate_results"
    __engine__ = "MergeTree"
    __order_by__ = ("gate", "run_at")
    __columns__ = {
        "run_id": "String",
        "gate": "LowCardinality(String)",
        "platform": "LowCardinality(String)",
        "passed": "UInt8",
        "action": "LowCardinality(String)",    # none | fail | alarm | page
        "detail": "String",                    # json
        "run_at": "DateTime64(3)",
    }


class TemporalMetaRecord(Record):
    """Temporal workflow VISIBILITY, landed here via Debezium CDC. Joinable to
    evidence by run/board. (CDC targets visibility tables, not the protobuf
    history tables.)"""
    __table__ = "temporal_meta"
    __engine__ = "ReplacingMergeTree"
    __order_by__ = ("workflow_id",)
    __columns__ = {
        "workflow_id": "String",
        "workflow_type": "LowCardinality(String)",
        "status": "LowCardinality(String)",
        "start_time": "DateTime64(3)",
        "close_time": "Nullable(DateTime64(3))",
        "history_length": "UInt32",
        "task_queue": "LowCardinality(String)",
    }
