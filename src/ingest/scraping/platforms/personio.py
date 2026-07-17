"""Personio. Facts ported from atlas-kt PersonioScraper.kt, CORRECTED 2026-07-17.

one_shot list + per-job detail:
  LIST   GET https://{slug}.jobs.personio.com/search.json
         → [...positions...] — metadata ONLY. `description` is ALWAYS "".
  DETAIL GET https://{slug}.jobs.personio.com/job/{id}
         → the job page, carrying a schema.org JSON-LD JobPosting whose
           `description` IS the JD.

The previous version was a OneShotScraper landing the list stub as the job, on
the claim "the JD is already in the list response". It is not: `description`
came back "" for all 4,509 landed jobs while every board still reported success
— a fake pass that stood for weeks because the gate counts rows, not content.
`/job/{id}.json` DOES 404 (that part of the old note was right); dropping
`.json` returns the page — verified live 2026-07-17 against 1komma5grad/781758
(HTTP 200, JSON-LD JobPosting, description 2,736 chars).
"""
from __future__ import annotations

import json

from ingest.core.models import Board, ListPage, Request, Stub
from ingest.scraping.families import PagedDetailScraper
from ingest.utils.normalize import digest_json, ldjson_description_nonempty


class PersonioScraper(PagedDetailScraper):
    platform = "personio"
    detail_concurrency = 8   # personio 429s hard at the family default
                             # (measured 2026-07-17); AIMD calibration owns
                             # tuning this up if the vendor relaxes

    def list_request(self, board: Board, cursor) -> Request:
        return Request("GET", f"https://{board.slug}.jobs.personio.com/search.json")

    def parse_list(self, body: bytes, cursor) -> ListPage:
        data = json.loads(body or b"[]")
        jobs = data if isinstance(data, list) else data.get("positions", [])
        stubs = [Stub(digest=digest_json({"id": j.get("id")}),
                      external_id=str(j.get("id")),
                      detail_url=str(j.get("id")))   # detail built from id
                 for j in jobs if j.get("id") is not None]
        # one_shot: search.json IS the whole list — there is no page 2.
        return ListPage(stubs=stubs, next_cursor=None, raw_body=body, status=200)

    def detail_request(self, stub, board: Board) -> Request:
        return Request("GET",
            f"https://{board.slug}.jobs.personio.com/job/{stub.external_id}")

    def jd_present(self, raw: str) -> bool:
        # the detail page carries a schema.org JobPosting; its description is
        # the JD. Empty description (the old one-shot bug) = no JD = fail loud.
        return ldjson_description_nonempty(raw)
