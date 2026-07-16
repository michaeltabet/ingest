"""Resolve Workday careers URLs by brute-forcing (host x site) combos.

Workday careers live at {tenant}.wd{N}.myworkdayjobs.com/{site}. Neither the
wd{N} datacentre nor the {site} name is derivable from the tenant slug, and
Workday blocks discovery (406). BUT the /wday/cxs/{tenant}/{site}/jobs endpoint
returns 200+total only for the exact (host, site) pair, and a clean 404 for a
wrong site on the right host — so a guess is *verifiable*.

Strategy: for each tenant, blast common wd{N} x common site-name patterns in
parallel, keep the first pair that returns jobs. ~40% resolve on patterns;
the stubborn ones (custom site names) fall through and are left for a search
pass. Idempotent: re-run only tries the still-unresolved.

    python scripts/resolve_workday.py <tenants.tsv> > resolved.tsv
    # tenants.tsv: one `slug` per line
"""
from __future__ import annotations

import asyncio
import json
import sys
import urllib.request

WD = [1, 2, 3, 5, 6, 10, 12, 101, 102, 103, 104, 105]
POOL = 8          # tenants resolved concurrently
PER_TENANT = 40   # concurrent probes within a tenant


def site_candidates(t: str) -> list:
    T = t.capitalize()
    return [f"{t}careers", f"{T}Careers", f"{t}Careers", f"{T}careers",
            t, T, "External", "Careers", "External_Careers",
            f"{t}careers2", f"{t}_careers", f"{t}jobs", f"{t}-careers"]


def _probe(host: str, tenant: str, site: str) -> int:
    body = json.dumps({"appliedFacets": {}, "limit": 1, "offset": 0,
                       "searchText": ""}).encode()
    req = urllib.request.Request(
        f"https://{host}/wday/cxs/{tenant}/{site}/jobs", data=body, method="POST",
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0",
                 "Accept": "application/json"})
    try:
        d = json.loads(urllib.request.urlopen(req, timeout=4).read())
        return (d.get("total") or 0) if isinstance(d, dict) else 0
    except Exception:
        return -1


async def resolve(t: str):
    combos = [(f"{t}.wd{n}.myworkdayjobs.com", t, s)
              for n in WD for s in site_candidates(t)]
    sem = asyncio.Semaphore(PER_TENANT)

    async def one(h, tn, s):
        async with sem:
            n = await asyncio.to_thread(_probe, h, tn, s)
            return (h, s, n) if n > 0 else None

    tasks = [asyncio.create_task(one(*c)) for c in combos]
    try:
        for fut in asyncio.as_completed(tasks):
            r = await fut
            if r:
                return r
    finally:
        for x in tasks:
            x.cancel()
    return None


async def main(path):
    slugs = [l.strip() for l in open(path) if l.strip()]
    sys.stderr.write(f"resolving {len(slugs)} workday tenants...\n")
    pool = asyncio.Semaphore(POOL)
    hit = [0]

    async def go(t):
        async with pool:
            try:
                r = await asyncio.wait_for(resolve(t), timeout=25)
            except Exception:
                r = None
            if r:
                print(f"{t}\thttps://{r[0]}/{r[1]}\t{r[2]}", flush=True)
                hit[0] += 1

    await asyncio.gather(*[go(t) for t in slugs])
    sys.stderr.write(f"RESOLVED {hit[0]}/{len(slugs)}\n")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
