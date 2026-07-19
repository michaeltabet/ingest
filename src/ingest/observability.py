"""THE observability library — where a project's run data is WATCHED.

Fourth of the engine's five class families (resolver, scraper, temporal flow,
observability, scheduler). A spec says `observability.kind = <preset>` and
declares WHERE that system lives; the class knows how to point at it and how
to provision the project's dashboard. A backend the engine lacks is a NEW
CLASS HERE — never ad-hoc dashboards scattered around.

    superset   Apache Superset — the PLATFORM. Inside it live many KINDS of
               observability, each a view class in VIEW_KINDS (live today;
               gate-failures, churn, ... tomorrow). The spec declares which
               views a project gets: observability.views = ["live", ...].
               ensure() provisions connection + dataset + dashboard + every
               declared view, idempotently, via the REST API.
"""
from __future__ import annotations

import json
import os


class Observability:
    """Contract: where do I LOOK (url) and make it exist (ensure)."""

    def url(self) -> str:
        raise NotImplementedError

    def ensure(self, domain) -> str:
        """Idempotently provision the project's dashboard; returns its URL.
        Fail loud: a missing backend/credential is an error, never a skip."""
        raise NotImplementedError


class SupersetView:
    """One KIND of observability inside Superset. A view owns its SQL and its
    chart; a new kind = a new class here + its word in VIEW_KINDS. Defined as
    CODE so the dashboard is reproducible, never hand-built in the UI."""

    name = ""          # chart/dataset name
    sql = ""           # the virtual dataset's SQL
    columns: list = []  # what the table shows, in order

    def ensure(self, api, domain, db_id: int, dash_id: int) -> None:
        ds_id = api.dataset(self.name, db_id, self.sql)
        api.table_chart(self.name, ds_id, dash_id, self.columns)


class JobTableView(SupersetView):
    """THE table: one row per landed job — url, platform, site, slug, and the
    job description itself, newest first, so a human can page through the
    actual postings the scrapers produced."""

    name = "jobs"
    columns = ["fetched_at", "platform", "site", "slug", "url", "title", "jd"]
    sql = """
SELECT
  fetched_at,
  platform,
  splitByChar(':', board_id)[1] AS site,
  splitByChar(':', board_id)[2] AS slug,
  coalesce(
    nullif(JSONExtractString(raw, 'absolute_url'), ''),
    nullif(JSONExtractString(raw, 'jobUrl'), ''),
    nullif(JSONExtractString(raw, 'applyUrl'), ''),
    nullif(JSONExtractString(raw, 'postingUrl'), ''),
    nullif(JSONExtractString(raw, 'hostedUrl'), ''),
    nullif(JSONExtractString(raw, 'url'), ''),
    nullif(JSONExtract(raw, 'jobPostingInfo', 'externalUrl', 'String'), ''),
    ''
  ) AS url,
  left(coalesce(
    nullif(JSONExtractString(raw, 'title'), ''),
    nullif(JSONExtract(raw, 'jobPostingInfo', 'title', 'String'), ''),
    nullif(JSONExtractString(raw, 'name'), ''),
    nullif(JSONExtract(raw, 'result', 'jobOpening', 'jobOpeningName', 'String'), ''),
    nullif(JSONExtractString(raw, 'text'), ''),
    ''), 120) AS title,
  extractTextFromHTML(decodeHTMLComponent(decodeHTMLComponent(coalesce(
    nullif(JSONExtract(raw, 'jobPostingInfo', 'jobDescription', 'String'), ''),
    nullif(JSONExtract(raw, 'jobAd', 'sections', 'jobDescription', 'text', 'String'), ''),
    nullif(JSONExtractString(raw, 'content'), ''),
    nullif(JSONExtractString(raw, 'description'), ''),
    nullif(JSONExtractString(raw, 'descriptionPlain'), ''),
    nullif(JSONExtractString(raw, 'descriptionHtml'), ''),
    nullif(JSONExtract(raw, 'result', 'jobOpening', 'description', 'String'), ''),
    raw)))) AS jd
FROM ingest.jobs
WHERE match(run_id, '^[0-9]{4}-[0-9]{2}-[0-9]{2}\\.t[0-9]{4}')
  AND extract(run_id, '^([0-9]{4}-[0-9]{2}-[0-9]{2}\\.t[0-9]{4})') = (
        SELECT max(extract(run_id, '^([0-9]{4}-[0-9]{2}-[0-9]{2}\\.t[0-9]{4})'))
        FROM ingest.jobs
        WHERE match(run_id, '^[0-9]{4}-[0-9]{2}-[0-9]{2}\\.t[0-9]{4}')
      )
"""


class TestRunView(SupersetView):
    """THE TEST RUN, live: one row per board as it is judged, newest first.
    Scoped to the CURRENT run only (Temporal dev is wiped between runs, so
    'today' is the run) — no historical noise."""

    name = "test_run"
    columns = ["run_at", "platform", "slug", "outcome", "why",
               "items_seen", "claimed", "landed", "not_real", "http"]
    sql = """
SELECT
  run_at,
  platform,
  splitByChar(':', board_id)[2] AS slug,
  outcome,
  multiIf(outcome = 'success', '',
          list_status IN (403, 404, 410), 'board is GONE (404/403)',
          list_status = 0, 'scraper raised (blocked/dead tenant)',
          arrayStringConcat(errors) LIKE '%complete=False%', 'pagination stopped short',
          arrayStringConcat(errors) LIKE '%not_real_jobs%', 'landed junk, not real jobs',
          substring(arrayStringConcat(errors, ' | '), 1, 90)) AS why,
  items_seen,
  reported_total AS claimed,
  jobs_extracted AS landed,
  jobs_no_jd AS not_real,
  list_status AS http
FROM ingest.scrape_evidence
WHERE match(run_id, '^[0-9]{4}-[0-9]{2}-[0-9]{2}\\.t[0-9]{4}')
  AND extract(run_id, '^([0-9]{4}-[0-9]{2}-[0-9]{2}\\.t[0-9]{4})') = (
        SELECT max(extract(run_id, '^([0-9]{4}-[0-9]{2}-[0-9]{2}\\.t[0-9]{4})'))
        FROM ingest.scrape_evidence
        WHERE match(run_id, '^[0-9]{4}-[0-9]{2}-[0-9]{2}\\.t[0-9]{4}')
      )
"""


VIEW_KINDS = {"jobs": JobTableView, "test_run": TestRunView}


class _SupersetAPI:
    """The few Superset REST calls a view needs, idempotent by name."""

    def __init__(self, session, base_url):
        self.s = session
        self.base = base_url

    def _find(self, endpoint, col, val):
        q = json.dumps({"filters": [{"col": col, "opr": "eq", "value": val}]})
        r = self.s.get(f"/api/v1/{endpoint}/?q={q}")
        r.raise_for_status()
        res = r.json().get("result", [])
        return res[0]["id"] if res else None

    def database(self, name: str, uri: str) -> int:
        found = self._find("database", "database_name", name)
        if found:
            return found
        r = self.s.post("/api/v1/database/", json={
            "database_name": name, "sqlalchemy_uri": uri,
            "expose_in_sqllab": True})
        if r.status_code not in (200, 201):
            raise RuntimeError(f"database create: {r.status_code} {r.text[:200]}")
        return r.json()["id"]

    def dataset(self, name: str, db_id: int, sql: str) -> int:
        found = self._find("dataset", "table_name", name)
        if found:
            self.s.put(f"/api/v1/dataset/{found}", json={"sql": sql})
            return found
        r = self.s.post("/api/v1/dataset/", json={
            "database": db_id, "table_name": name, "sql": sql})
        if r.status_code not in (200, 201):
            raise RuntimeError(f"dataset create: {r.status_code} {r.text[:200]}")
        return r.json()["id"]

    def dashboard(self, title: str, slug: str) -> int:
        found = self._find("dashboard", "dashboard_title", title)
        if found:
            return found
        r = self.s.post("/api/v1/dashboard/", json={
            "dashboard_title": title, "slug": slug, "published": True})
        if r.status_code not in (200, 201):
            raise RuntimeError(f"dashboard create: {r.status_code} {r.text[:200]}")
        return r.json()["id"]

    def table_chart(self, name: str, ds_id: int, dash_id: int, columns: list):
        params = json.dumps({
            "viz_type": "table", "datasource": f"{ds_id}__table",
            "query_mode": "raw", "all_columns": columns,
            "order_by_cols": [json.dumps([columns[0], False])],
            "row_limit": 10000, "server_pagination": True,
            "server_page_length": 25, "time_range": "No filter"})
        found = self._find("chart", "slice_name", name)
        body = {"slice_name": name, "viz_type": "table",
                "datasource_id": ds_id, "datasource_type": "table",
                "params": params, "dashboards": [dash_id]}
        if found:
            self.s.put(f"/api/v1/chart/{found}", json=body)
            return found
        r = self.s.post("/api/v1/chart/", json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"chart create: {r.status_code} {r.text[:200]}")
        return r.json()["id"]


class SupersetObservability(Observability):
    """Apache Superset — the platform. Facts from the spec: url, credential
    env NAMES, dashboard title, and WHICH observability kinds (views) this
    project gets."""

    ARGS = {"url", "user_env", "password_env", "dashboard", "views"}

    def __init__(self, url: str, user_env: str, password_env: str,
                 dashboard: str, views: list):
        self.base_url = url.rstrip("/")
        self.user_env = user_env
        self.password_env = password_env
        self.dashboard = dashboard
        unknown = set(views) - set(VIEW_KINDS)
        if unknown:
            raise ValueError(f"unknown observability views {sorted(unknown)} "
                             f"(have: {sorted(VIEW_KINDS)}) — add the view "
                             f"class to ingest/observability.py")
        self.views = views

    def url(self) -> str:
        return self.base_url

    # --- REST plumbing -------------------------------------------------------
    def _cred(self, env_key: str) -> str:
        name = getattr(self, env_key)
        if name not in os.environ:
            raise RuntimeError(f"credential env {name!r} (named by spec "
                               f"observability.{env_key}) is not set")
        return os.environ[name]

    def _session(self):
        import httpx
        s = httpx.Client(base_url=self.base_url, verify=True, timeout=30.0,
                         follow_redirects=True)
        r = s.post("/api/v1/security/login", json={
            "username": self._cred("user_env"),
            "password": self._cred("password_env"),
            "provider": "db", "refresh": True})
        if r.status_code != 200:
            raise RuntimeError(f"superset login failed: {r.status_code} "
                               f"{r.text[:200]}")
        s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        csrf = s.get("/api/v1/security/csrf_token/")
        if csrf.status_code == 200:
            s.headers["X-CSRFToken"] = csrf.json()["result"]
        return s

    def ensure(self, domain) -> str:
        """Create/refresh this project's dashboard FROM CODE. Idempotent."""
        db = domain.database
        api = _SupersetAPI(self._session(), self.base_url)
        uri = (f"clickhousedb://{os.environ[db['user_env']]}:"
               f"{os.environ[db['password_env']]}@{db['host']}:{db['port']}"
               f"/{db['database']}")
        db_id = api.database(f"{domain.name}-{db.get('kind', 'db')}", uri)
        slug = f"{domain.name}-jobs"
        dash_id = api.dashboard(self.dashboard, slug)
        for view in self.views:
            VIEW_KINDS[view]().ensure(api, domain, db_id, dash_id)
        return f"{self.base_url}/superset/dashboard/{slug}/"


_KINDS = {"superset": SupersetObservability}


def build(preset: str, part: dict) -> Observability:
    """One word picks the backend; THE CLASS declares its args (ARGS) and the
    JSON supplies them — the loader never knows any kind's arg names."""
    cls = _KINDS.get(preset)
    if cls is None:
        raise ValueError(f"no observability preset {preset!r} in the library "
                         f"(have: {sorted(_KINDS)}) — add the class to "
                         f"ingest/observability.py")
    args = {k: v for k, v in part.items() if k != "kind"}
    missing = cls.ARGS - set(args)
    unknown = set(args) - cls.ARGS
    if missing or unknown:
        raise ValueError(f"observability {preset!r}: missing {sorted(missing)} "
                         f"unknown {sorted(unknown)} (class ARGS = "
                         f"{sorted(cls.ARGS)})")
    return cls(**args)
