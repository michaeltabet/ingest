"""board_candidates — staging table between cc_scan and promote.

Append-only status ledger: each pass over a candidate INSERTS a new row with
the latest status; readers take argMax(status, discovered_at). ReplacingMergeTree
keyed by (source, crawl, platform, slug, status) keeps re-runs idempotent
without hiding the status history inside a merge.

cc_scan.py cannot import this class (it is stdlib-only for the Spark image),
so it embeds the DDL as a string constant. tests/test_discovery.py asserts the
two stay identical — the schema still has exactly one owner.
"""
from __future__ import annotations

from ingest.core.record import Record


class BoardCandidate(Record):
    __table__ = "board_candidates"
    __engine__ = "ReplacingMergeTree(discovered_at)"
    __order_by__ = ("source", "crawl", "platform", "slug", "status")
    __columns__ = {
        "source": "LowCardinality(String)",       # 'commoncrawl'
        "crawl": "LowCardinality(String)",        # e.g. CC-MAIN-2026-25
        "platform": "LowCardinality(String)",
        "slug": "String",
        "url": "String DEFAULT ''",               # constructed scrape/probe URL
        "n_urls": "UInt32 DEFAULT 0",             # index rows that matched this tenant
        "status": "LowCardinality(String) DEFAULT 'candidate'",
        "reason": "String DEFAULT ''",
        "discovered_at": "DateTime DEFAULT now()",
    }
