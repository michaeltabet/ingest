"""cc_scan — find ATS job boards in the Common Crawl columnar URL index.

SELF-CONTAINED ON PURPOSE: pyspark + stdlib only, no ingest imports, so the
stock apache/spark image runs it with zero pip step (the extractor functions
below get cloudpickled by value to executors). The one shared contract — the
board_candidates DDL — is embedded as DDL below; tests/test_discovery.py pins
it byte-for-byte to ingest.discovery.records.BoardCandidate.ddl().

Reads   s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=<CRAWL>/subset=warc/
        (public bucket, anonymous S3A; ~300 parquet files per crawl. The index
        is SURT-sorted, so the url_host_registered_domain IN (...) predicate
        prunes almost every row group — the scan moves GBs, not the full index.)
Writes  ingest.board_candidates via the ClickHouse HTTP interface (stdlib
        urllib; no clickhouse-connect on the Spark image).

Env: CC_CRAWL (optional — defaults to the newest crawl from collinfo.json),
     CH_HOST, CH_PORT, CH_DATABASE, CH_USER, CH_PASSWORD.
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# schema contract (pinned to records.BoardCandidate by a unit test)
# ---------------------------------------------------------------------------

DDL = """CREATE TABLE IF NOT EXISTS board_candidates (
  source LowCardinality(String),
  crawl LowCardinality(String),
  platform LowCardinality(String),
  slug String,
  url String DEFAULT '',
  n_urls UInt32 DEFAULT 0,
  status LowCardinality(String) DEFAULT 'candidate',
  reason String DEFAULT '',
  discovered_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(discovered_at)
ORDER BY (source, crawl, platform, slug, status);"""

# ---------------------------------------------------------------------------
# extraction: (host, path) -> (platform, slug, constructed scrape URL) | None
# URL templates mirror the live ingest.boards rows exactly (profiled 2026-07-16)
# ---------------------------------------------------------------------------

# registered domains worth reading from the index at all
DOMAINS = (
    "greenhouse.io", "lever.co", "ashbyhq.com", "workable.com",
    "smartrecruiters.com", "teamtailor.com", "personio.com", "personio.de",
    "recruitee.com", "bamboohr.com", "icims.com", "taleo.net",
    "myworkdayjobs.com", "oraclecloud.com",
)

_SEG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_LOCALE = re.compile(r"^[a-z]{2}(-[A-Z]{2})?$")
_WD_HOST = re.compile(r"^([a-z0-9-]+)\.wd\d+\.myworkdayjobs\.com$")
_NON_TENANT_SUBS = frozenset({
    "www", "app", "api", "docs", "blog", "help", "support", "status",
    "careers", "jobs", "career", "resources", "partners", "marketplace",
    "developer", "developers", "go", "get", "mail", "email",
})


def _seg1(path: str) -> str:
    parts = [p for p in path.split("/") if p]
    return parts[0] if parts else ""


def _sub(host: str, suffix: str) -> str:
    """leftmost label(s) before suffix, '' if host IS the suffix or nested."""
    if not host.endswith("." + suffix):
        return ""
    sub = host[: -(len(suffix) + 1)]
    return sub if ("." not in sub and sub not in _NON_TENANT_SUBS) else ""


def extract(host: str, path: str):
    host = host.lower()

    if host in ("boards.greenhouse.io", "job-boards.greenhouse.io",
                "boards.eu.greenhouse.io", "job-boards.eu.greenhouse.io"):
        s = _seg1(path).lower()
        if s and s != "embed" and _SEG.match(s):
            return ("greenhouse", s,
                    f"https://boards-api.greenhouse.io/v1/boards/{s}/jobs?content=true")

    elif host in ("jobs.lever.co", "jobs.eu.lever.co"):
        s = _seg1(path).lower()
        if s and _SEG.match(s):
            return ("lever", s, f"https://api.lever.co/v0/postings/{s}?mode=json")

    elif host == "jobs.ashbyhq.com":
        s = _seg1(path)
        s = urllib.parse.unquote(s)
        if s and _SEG.match(s):
            return ("ashby", s, f"https://api.ashbyhq.com/posting-api/job-board/{s}")

    elif host == "apply.workable.com":
        s = _seg1(path).lower()
        if s and s not in ("api", "j") and _SEG.match(s):
            return ("workable", s,
                    f"https://apply.workable.com/api/v1/widget/accounts/{s}?details=true")

    elif host in ("careers.smartrecruiters.com", "jobs.smartrecruiters.com"):
        s = _seg1(path).lower()
        if s and _SEG.match(s):
            return ("smartrecruiters", s,
                    f"https://api.smartrecruiters.com/v1/companies/{s}/postings")

    elif host.endswith(".teamtailor.com"):
        s = _sub(host, "teamtailor.com")
        if s:
            return ("teamtailor", s, f"https://{s}.teamtailor.com/jobs.json")

    elif host.endswith(".jobs.personio.com") or host.endswith(".jobs.personio.de"):
        tld = "com" if host.endswith(".com") else "de"
        s = _sub(host, f"jobs.personio.{tld}")
        if s:
            # ALWAYS .com, whatever TLD the crawl saw: the production scraper
            # (scraping/platforms/personio.py) hardcodes .jobs.personio.com and
            # ignores board.url — a .de row here would drift from what actually
            # gets scraped. The promote probe gates whether .com answers.
            return ("personio", s, f"https://{s}.jobs.personio.com/search.json")

    elif host.endswith(".recruitee.com"):
        s = _sub(host, "recruitee.com")
        if s:
            return ("recruitee", s, f"https://{s}.recruitee.com/api/offers/")

    elif host.endswith(".bamboohr.com"):
        s = _sub(host, "bamboohr.com")
        if s and (path.startswith("/careers") or path.startswith("/jobs")):
            return ("bamboohr", s, f"https://{s}.bamboohr.com/careers/list")

    elif host.endswith(".icims.com"):
        # live convention is careers-{slug}.icims.com; other subdomain shapes
        # (jobs-*, internal-*, bare tenant) are NOT the scraped surface — skip.
        sub = host[: -len(".icims.com")]
        if sub.startswith("careers-") and "." not in sub:
            s = sub[len("careers-"):]
            if s:
                return ("icims", s, f"https://careers-{s}.icims.com/sitemap.xml")

    elif host.endswith(".taleo.net"):
        s = _sub(host, "taleo.net")
        if s and s != "tbe":  # tbe.taleo.net = Taleo Business Edition, different API
            return ("taleo", s,
                    f"https://{s}.taleo.net/careersection/rest/jobboard/searchjobs?lang=en&portal=1")

    elif host.endswith(".myworkdayjobs.com"):
        m = _WD_HOST.match(host)
        if m:
            parts = [p for p in path.split("/") if p]
            # /{site}/... or /{locale}/{site}/...; wday/* is API plumbing.
            # Strip a locale segment unconditionally: a bare /en-US is the
            # tenant's locale landing page, never a site name.
            if parts and _LOCALE.match(parts[0]):
                parts = parts[1:]
            if parts and parts[0] != "wday" and _SEG.match(parts[0]):
                return ("workday", m.group(1), f"https://{host}/{parts[0]}")

    elif host.endswith(".oraclecloud.com"):
        # /hcmUI/CandidateExperience/{lang}/sites/{site}/...
        m = re.match(r"^/hcmUI/CandidateExperience/[^/]+/sites/([A-Za-z0-9._-]+)", path)
        if m:
            site = m.group(1)
            return ("oracle", site,
                    f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}")

    return None


# ---------------------------------------------------------------------------
# ClickHouse over HTTP (stdlib only)
# ---------------------------------------------------------------------------

def _ch(query: str, body: bytes = b"") -> None:
    url = "http://{}:{}/?{}".format(
        os.environ["CH_HOST"], os.environ.get("CH_PORT", "8123"),
        urllib.parse.urlencode({
            "database": os.environ.get("CH_DATABASE", "ingest"),
            "query": query,
            "wait_end_of_query": "1",
        }))
    req = urllib.request.Request(url, data=body or None, method="POST")
    req.add_header("X-ClickHouse-User", os.environ["CH_USER"])
    req.add_header("X-ClickHouse-Key", os.environ["CH_PASSWORD"])
    with urllib.request.urlopen(req, timeout=120) as res:
        res.read()


def resolve_crawl() -> str:
    if os.environ.get("CC_CRAWL"):
        return os.environ["CC_CRAWL"]
    with urllib.request.urlopen("https://index.commoncrawl.org/collinfo.json",
                                timeout=60) as res:
        return json.load(res)[0]["id"]


def main() -> None:
    from pyspark.sql import SparkSession, functions as F

    crawl = resolve_crawl()
    path = (f"s3a://commoncrawl/cc-index/table/cc-main/warc/"
            f"crawl={crawl}/subset=warc/")
    print(json.dumps({"event": "cc_scan_start", "crawl": crawl, "path": path}),
          flush=True)

    spark = SparkSession.builder.appName(f"board-discovery-{crawl}").getOrCreate()
    df = (spark.read.parquet(path)
          .where(F.col("fetch_status") == 200)
          .where(F.col("url_host_registered_domain").isin(*DOMAINS))
          .select("url_host_name", "url_path"))

    def part(rows):
        for r in rows:
            hit = extract(r[0] or "", r[1] or "")
            if hit:
                yield (hit, 1)

    agg = (df.rdd.mapPartitions(part)
           .reduceByKey(lambda a, b: a + b))
    agg.persist()  # count() then collect() without re-scanning the index

    # size gate BEFORE collect(): count() stays on the executors, so a
    # runaway extractor can never OOM the driver with 10M dicts first.
    n = agg.count()
    if n == 0:
        raise SystemExit(f"cc_scan: 0 candidates in {crawl} — "
                         "index path, domain list, or extractors are wrong")
    if n > 500_000:
        raise SystemExit(f"cc_scan: {n} candidates — extractor is "
                         "matching garbage, refusing to collect")

    found = (agg.map(lambda kv: {"source": "commoncrawl", "crawl": crawl,
                                 "platform": kv[0][0], "slug": kv[0][1],
                                 "url": kv[0][2], "n_urls": kv[1],
                                 "status": "candidate"})
             .collect())

    per = {}
    for row in found:
        per[row["platform"]] = per.get(row["platform"], 0) + 1

    _ch(DDL)
    lines = [json.dumps(r, ensure_ascii=False) for r in found]
    for i in range(0, len(lines), 20_000):
        _ch("INSERT INTO board_candidates FORMAT JSONEachRow",
            ("\n".join(lines[i:i + 20_000])).encode("utf-8"))

    print(json.dumps({"event": "cc_scan_done", "crawl": crawl,
                      "candidates": len(found), "per_platform": per}),
          flush=True)
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
