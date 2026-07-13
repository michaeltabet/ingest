"""Concrete Steps — the units of Temporal work. Each returns a Record (counts),
never raw data (the 2MB-payload law, enforced by the Step base).

ScrapeStep is the bridge: registry -> scraper.fetch() -> write raw payloads to
ClickHouse -> return the evidence Record. The scraper does the network work;
the step does the persistence; Temporal sees only the evidence.
"""
from __future__ import annotations

from ingest.core.models import Board
from ingest.core.record import Record
from ingest.core.step import Step
from ingest.ledger.records import EvidenceRecord
from ingest.scraping import registry


class ScrapeStep(Step):
    name = "scrape"

    def __init__(self, http, clickhouse):
        self.http = http
        self.clickhouse = clickhouse

    async def run(self, ctx) -> Record:
        """ctx: {board: dict, run_id: str, known_digests: set}."""
        from ingest.core.context import ScrapeContext
        board = Board(**ctx["board"])
        scraper = registry.get(board.platform)
        sctx = ScrapeContext(http=self.http, known_digests=ctx.get("known_digests", set()))

        result = await scraper.fetch(board, sctx)

        # raw bytes → ClickHouse directly (NOT through Temporal)
        if result.payloads:
            rows = [(p.platform, p.board_id, ctx["run_id"], p.url, p.kind,
                     p.http_status, p.digest, p.stub_digest or "", p.body, p.fetched_at)
                    for p in result.payloads]
            self.clickhouse.insert(
                "scrape_raw", rows,
                ("platform", "board_id", "run_id", "url", "kind", "http_status",
                 "digest", "stub_digest", "body", "fetched_at"))

        # return only EVIDENCE (counts) — this is what Temporal carries
        s = result.summary()
        return _Evidence(ctx["run_id"], s)


class _Evidence(EvidenceRecord):
    """Lightweight carrier of one evidence row (subclass keeps DDL identity)."""
    def __init__(self, run_id, summary):
        self.run_id = run_id
        self.data = summary
