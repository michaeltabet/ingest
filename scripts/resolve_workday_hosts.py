"""Resolve Workday host per board and store it in ingest.boards.url.

Workday tenants live at {slug}.wd{N}.myworkdayjobs.com where wd{N} varies per
tenant and is NOT in runs_board. We probe wd1..wd17 for each workday slug,
find the datacentre that answers the cxs jobs endpoint, and persist the full
base URL so WorkdayScraper (which reads board.url) works — the proven atlas
approach, just resolved once and cached.

    .venv/bin/python scripts/resolve_workday_hosts.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import urllib.request
import urllib.error

WD_RANGE = range(1, 18)
CONCURRENCY = 20


def _probe_one(slug: str):
    body = json.dumps({"limit": 1, "offset": 0, "searchText": ""}).encode()
    for n in WD_RANGE:
        host = f"{slug}.wd{n}.myworkdayjobs.com"
        url = f"https://{host}/wday/cxs/{slug}/{slug}/jobs"
        try:
            req = urllib.request.Request(url, data=body, method="POST",
                headers={"Content-Type": "application/json",
                         "Accept-Language": "en-US,en;q=0.9",
                         "User-Agent": "ingest/0.1"})
            with urllib.request.urlopen(req, timeout=8) as r:
                if r.status == 200:
                    return slug, f"https://{host}"
        except urllib.error.HTTPError as e:
            if e.code in (400, 422):   # right host, wrong body shape = tenant EXISTS here
                return slug, f"https://{host}"
        except Exception:
            pass
    return slug, None


async def resolve(slugs):
    sem = asyncio.Semaphore(CONCURRENCY)
    async def one(slug):
        async with sem:
            return await asyncio.to_thread(_probe_one, slug)
    return await asyncio.gather(*[one(s) for s in slugs])


def main():
    from ingest.utils.clickhouse import ConnectClickHouse
    ch = ConnectClickHouse.from_env()
    slugs = [r[0] for r in ch.query(
        "SELECT slug FROM boards WHERE platform='workday' AND (url='' OR url IS NULL)")]
    print(f"resolving {len(slugs)} workday hosts (wd1..wd17, {CONCURRENCY} concurrent)...")
    results = asyncio.run(resolve(slugs))
    found = [(s, u) for s, u in results if u]
    print(f"resolved {len(found)}/{len(slugs)}")
    # write resolved urls back (ReplacingMergeTree on platform,slug — re-insert row with url)
    if found:
        rows = [["workday", s, "", "", u, 1] for s, u in found]
        ch.insert("boards", rows, ("platform", "slug", "company", "board_id", "url", "enabled"))
        print(f"stored {len(found)} urls into ingest.boards")
    for s, u in found[:8]:
        print(f"  {s} -> {u}")


if __name__ == "__main__":
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    main()
