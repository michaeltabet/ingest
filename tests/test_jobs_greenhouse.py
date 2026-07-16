"""Real, asserting test for the greenhouse LANDING flow (ELT, no parse).

Landing is factored into the family; the platform only attaches each job's raw
JSON to its Stub. This test asserts that contract and the count invariant.
"""
from __future__ import annotations

import json
from pathlib import Path

import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingest.scraping.platforms.greenhouse import GreenhouseScraper

FIXTURE = ROOT / "fixtures" / "greenhouse" / "list.json"


def test_stub_per_job_carries_raw():
    """one_shot contract: one stub per job, each carrying valid raw JSON."""
    body = FIXTURE.read_bytes()
    source_n = len(json.loads(body)["jobs"])
    stubs = GreenhouseScraper().parse_list(body, None).stubs
    assert len(stubs) == source_n > 0, "one stub per job in the source array"
    for s in stubs:
        assert s.external_id, f"missing id: {s}"
        assert s.raw, f"stub has no raw: {s}"
        parsed = json.loads(s.raw)                 # raw must round-trip
        assert str(parsed.get("id")) == s.external_id


def test_family_lands_one_row_per_stub():
    """The family's landing turns every raw-bearing stub into a Job row."""
    from ingest.core.models import RawResult
    s = GreenhouseScraper()
    stubs = s.parse_list(FIXTURE.read_bytes(), None).stubs
    res = RawResult(board_id="greenhouse:samsara", platform="greenhouse")
    s._land_stubs(res, stubs)
    assert len(res.jobs) == len(stubs) > 0
    assert all(j.external_id and j.raw for j in res.jobs)


if __name__ == "__main__":
    test_stub_per_job_carries_raw()
    test_family_lands_one_row_per_stub()
    print("all greenhouse landing tests passed")
