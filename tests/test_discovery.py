"""Discovery: extractor table-tests, probe-replay tests, DDL pin."""
from __future__ import annotations

import httpx
import pytest

from ingest.discovery import cc_scan, promote
from ingest.discovery.records import BoardCandidate
from ingest.scraping import registry
from ingest.scraping.validation import validate_board


def test_ddl_pinned_to_record():
    # cc_scan is stdlib-only so it embeds the DDL; the Record class owns it.
    assert cc_scan.DDL == BoardCandidate.ddl()


EXPECT = [
    # (host, path) -> (platform, slug) or None
    (("boards.greenhouse.io", "/stripe/jobs/123"), ("greenhouse", "stripe")),
    (("job-boards.greenhouse.io", "/Datadog"), ("greenhouse", "datadog")),
    (("boards.eu.greenhouse.io", "/embed/job_board"), None),
    (("jobs.lever.co", "/netflix/uuid-here"), ("lever", "netflix")),
    (("jobs.eu.lever.co", "/hipo"), ("lever", "hipo")),
    (("jobs.ashbyhq.com", "/OpenAI/some-job"), ("ashby", "OpenAI")),
    (("jobs.ashbyhq.com", "/Docker.Inc"), ("ashby", "Docker.Inc")),
    (("apply.workable.com", "/deliverect/j/ABCD/"), ("workable", "deliverect")),
    (("apply.workable.com", "/api/v3/accounts"), None),
    (("careers.smartrecruiters.com", "/BoschGroup/744000"), ("smartrecruiters", "boschgroup")),
    (("acme.teamtailor.com", "/jobs/1"), ("teamtailor", "acme")),
    (("www.teamtailor.com", "/en/pricing"), None),
    (("career.teamtailor.com", "/"), None),
    (("acme.jobs.personio.de", "/job/42"), ("personio", "acme")),
    (("acme.jobs.personio.com", "/"), ("personio", "acme")),
    (("acme.personio.de", "/"), None),
    (("acme.recruitee.com", "/o/dev"), ("recruitee", "acme")),
    (("api.recruitee.com", "/c/x"), None),
    (("acme.bamboohr.com", "/careers/42"), ("bamboohr", "acme")),
    (("acme.bamboohr.com", "/login.php"), None),
    (("careers-acme.icims.com", "/jobs/1234/login"), ("icims", "acme")),
    (("jobs-acme.icims.com", "/jobs/intro"), None),
    (("acme.taleo.net", "/careersection/2/jobsearch.ftl"), ("taleo", "acme")),
    (("tbe.taleo.net", "/tbe/ats/careers"), None),
    (("acme.wd5.myworkdayjobs.com", "/en-US/AcmeCareers/job/x"), ("workday", "acme")),
    (("acme.wd1.myworkdayjobs.com", "/External"), ("workday", "acme")),
    (("acme.wd1.myworkdayjobs.com", "/wday/cxs/acme/External/jobs"), None),
    (("acme.wd1.myworkdayjobs.com", "/en-US"), None),   # bare locale ≠ a site
    (("acme.wd1.myworkdayjobs.com", "/fr"), None),
    (("efgh.fa.us2.oraclecloud.com",
      "/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions"), ("oracle", "CX_1001")),
    (("efgh.fa.us2.oraclecloud.com", "/fscmUI/faces/deeplink"), None),
    (("www.greenhouse.io", "/customers"), None),
]


def test_extract_table():
    for (host, path), want in EXPECT:
        got = cc_scan.extract(host, path)
        if want is None:
            assert got is None, f"{host}{path} -> {got}, wanted None"
        else:
            assert got is not None, f"{host}{path} -> None, wanted {want}"
            assert got[:2] == want, f"{host}{path} -> {got[:2]}, wanted {want}"


def test_extracted_urls_survive_the_shape_gate():
    # whatever cc_scan emits must be insertable by promote: same gate.
    for (host, path), want in EXPECT:
        got = cc_scan.extract(host, path)
        if got is None:
            continue
        platform, slug, url = got
        ok, reason = validate_board(platform, slug, url)
        assert ok, f"{platform}:{slug} rejected by validate_board: {reason}"


def test_workday_url_is_tenant_site():
    _, _, url = cc_scan.extract("acme.wd5.myworkdayjobs.com", "/en-US/AcmeCareers/job/x")
    assert url == "https://acme.wd5.myworkdayjobs.com/AcmeCareers"


def test_personio_url_is_always_com():
    # The production scraper (scraping/platforms/personio.py) hardcodes
    # .jobs.personio.com and ignores board.url — a .de crawl must not emit a
    # URL the worker would never fetch. The promote probe gates .com liveness.
    for host in ("acme.jobs.personio.de", "acme.jobs.personio.com"):
        _, _, url = cc_scan.extract(host, "/job/42")
        assert url == "https://acme.jobs.personio.com/search.json", host


# ---------------------------------------------------------------------------
# probe: replayed scraper requests + the page-1 shape gate
# ---------------------------------------------------------------------------

class _Res:
    def __init__(self, status_code=200, content=b"{}"):
        self.status_code = status_code
        self.content = content


def test_probe_shape_table_covers_every_platform():
    # a platform missing from _SHAPES would mark every live tenant bad_shape
    assert set(promote._SHAPES) == set(registry.all_platforms())


def test_probe_taleo_replays_the_scrapers_post(monkeypatch):
    seen = {}

    def fake_request(method, url, *, json=None, headers=None, **kw):
        seen.update(method=method, url=url, json=json)
        return _Res(200, b'{"pagingData": {"totalCount": 0}, "requisitionList": []}')

    monkeypatch.setattr(httpx, "request", fake_request)
    ok, reason = promote._probe(
        "taleo", "acme",
        "https://acme.taleo.net/careersection/rest/jobboard/searchjobs?lang=en&portal=1")
    assert (ok, reason) == (True, "200")
    assert seen["method"] == "POST"        # taleo lists via POST, never GET
    assert seen["url"] == ("https://acme.taleo.net/careersection/rest/jobboard/"
                           "searchjobs?lang=en&portal=1")
    assert seen["json"]["pageNo"] == 1     # page 1, same body the worker sends


def test_probe_workday_posts_cxs_jobs(monkeypatch):
    seen = {}

    def fake_request(method, url, *, json=None, headers=None, **kw):
        seen.update(method=method, url=url, json=json)
        return _Res(200, b'{"total": 0, "jobPostings": []}')

    monkeypatch.setattr(httpx, "request", fake_request)
    ok, _ = promote._probe("workday", "acme",
                           "https://acme.wd5.myworkdayjobs.com/AcmeCareers")
    assert ok
    assert seen["method"] == "POST"
    # the candidate URL is the HTML page; the scraper's first request is cxs
    assert seen["url"] == "https://acme.wd5.myworkdayjobs.com/wday/cxs/acme/AcmeCareers/jobs"
    assert seen["json"] == {"limit": 20, "offset": 0, "searchText": ""}


def test_probe_shape_gate_rejects_html_200(monkeypatch):
    # parked/redirected tenant: 200 + marketing HTML must NOT promote
    monkeypatch.setattr(httpx, "request",
                        lambda *a, **kw: _Res(200, b"<html>We're hiring!</html>"))
    ok, reason = promote._probe(
        "greenhouse", "acme",
        "https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true")
    assert (ok, reason) == (False, "bad_shape")


def test_probe_shape_gate_rejects_wrong_envelope(monkeypatch):
    # valid JSON, wrong shape (greenhouse without "jobs") — still a rejection
    monkeypatch.setattr(httpx, "request",
                        lambda *a, **kw: _Res(200, b'{"error": "no such board"}'))
    ok, reason = promote._probe(
        "greenhouse", "acme",
        "https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true")
    assert (ok, reason) == (False, "bad_shape")


def test_probe_never_raises_on_junk():
    # workday's request builder raises on a non-workday url; the run must get
    # an evidence row with the exception class, not die. No network involved.
    ok, reason = promote._probe("workday", "acme", "not-a-url")
    assert (ok, reason) == (False, "ValueError")


def test_promote_classifies_and_writes(monkeypatch):
    class FakeCH:
        def __init__(self):
            self.inserted = {}

        def query(self, sql):
            if "count()" in sql:
                return [(4,)]          # recency gate: recent rows exist
            if "FROM boards" in sql:
                return [("greenhouse", "alreadyknown")]
            return [
                ("commoncrawl", "CC-MAIN-2026-25", "greenhouse", "alreadyknown",
                 "https://boards-api.greenhouse.io/v1/boards/alreadyknown/jobs?content=true", 9, "candidate"),
                ("commoncrawl", "CC-MAIN-2026-25", "greenhouse", "newco",
                 "https://boards-api.greenhouse.io/v1/boards/newco/jobs?content=true", 5, "candidate"),
                ("commoncrawl", "CC-MAIN-2026-25", "greenhouse", "-badslug",
                 "https://boards-api.greenhouse.io/v1/boards/-badslug/jobs?content=true", 1, "candidate"),
                ("commoncrawl", "CC-MAIN-2026-25", "workday", "acme",
                 "https://acme.wd5.myworkdayjobs.com/External", 3, "candidate"),
            ]

        def insert(self, table, rows, columns):
            self.inserted.setdefault(table, []).extend(rows)

    monkeypatch.setattr(promote, "_probe", lambda p, s, u: (True, "200"))
    ch = FakeCH()
    summary = promote.run(ch, probe_limit=250, concurrency=2, dry_run=False)

    assert summary["known"] == 1
    assert summary["rejected"] == 1
    assert summary["promoted"] == 2
    boards = {(r[0], r[1]): r for r in ch.inserted["boards"]}
    assert boards[("greenhouse", "newco")][5] == 1        # enabled
    assert boards[("workday", "acme")][5] == 0            # deliberate ramp
    assert boards[("greenhouse", "newco")][3] == "greenhouse:newco"  # board_id
    statuses = {(r[2], r[3]): r[6] for r in ch.inserted["board_candidates"]}
    assert statuses[("greenhouse", "alreadyknown")] == "known"
    assert statuses[("greenhouse", "-badslug")] == "rejected"
    assert statuses[("greenhouse", "newco")] == "promoted"


def test_promote_defers_beyond_probe_limit(monkeypatch):
    class FakeCH:
        def __init__(self):
            self.inserted = {}

        def query(self, sql):
            if "count()" in sql:
                return [(5,)]
            if "FROM boards" in sql:
                return []
            return [
                ("commoncrawl", "c", "lever", f"co{i}",
                 f"https://api.lever.co/v0/postings/co{i}?mode=json", 10 - i, "candidate")
                for i in range(5)
            ]

        def insert(self, table, rows, columns):
            self.inserted.setdefault(table, []).extend(rows)

    monkeypatch.setattr(promote, "_probe", lambda p, s, u: (True, "200"))
    summary = promote.run(FakeCH(), probe_limit=2, concurrency=2, dry_run=False)
    assert summary["promoted"] == 2
    assert summary["deferred"] == 3


def test_promote_probes_cross_crawl_duplicates_once(monkeypatch):
    class FakeCH:
        def __init__(self):
            self.inserted = {}

        def query(self, sql):
            if "count()" in sql:
                return [(3,)]
            if "FROM boards" in sql:
                return []
            # co1 surfaced by two crawls (rows arrive n_urls DESC, per the
            # ORDER BY); co2 by one — co1 must be probed/inserted ONCE.
            return [
                ("commoncrawl", "CC-MAIN-2026-25", "lever", "co1",
                 "https://api.lever.co/v0/postings/co1?mode=json", 9, "candidate"),
                ("commoncrawl", "CC-MAIN-2026-21", "lever", "co1",
                 "https://api.lever.co/v0/postings/co1?mode=json", 4, "candidate"),
                ("commoncrawl", "CC-MAIN-2026-25", "lever", "co2",
                 "https://api.lever.co/v0/postings/co2?mode=json", 2, "candidate"),
            ]

        def insert(self, table, rows, columns):
            self.inserted.setdefault(table, []).extend(rows)

    probed = []
    monkeypatch.setattr(promote, "_probe",
                        lambda p, s, u: probed.append((p, s)) or (True, "200"))
    ch = FakeCH()
    summary = promote.run(ch, probe_limit=250, concurrency=1, dry_run=False)
    assert summary["dup_crawl"] == 1
    assert summary["promoted"] == 2
    assert probed.count(("lever", "co1")) == 1
    assert len(ch.inserted["boards"]) == 2


def test_promote_bails_on_stale_candidate_pool():
    class StaleCH:
        def query(self, sql):
            assert "count()" in sql    # the gate must fire before anything else
            return [(0,)]

        def insert(self, table, rows, columns):
            raise AssertionError("nothing may be written past a stale gate")

    with pytest.raises(SystemExit, match="no board_candidates rows in the last"):
        promote.run(StaleCH(), probe_limit=1, concurrency=1, dry_run=True)
