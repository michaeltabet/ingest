# ingest worker — wraps the factored scraper + Temporal workflow code as a
# client to the EXISTING cluster Temporal. Does NOT run a Temporal server.
# Entrypoint: the Temporal worker joining queue scrape-http.
FROM python:3.12-slim

WORKDIR /app

# deps first (cache layer): httpx, clickhouse-connect, temporalio, django
COPY pyproject.toml README.md ./
COPY src ./src
# pipeline specs live in the SEPARATE ingest-pipelines config repo, mounted
# at runtime (ConfigMap/volume) — INGEST_PIPELINES points at the mount
ENV INGEST_PIPELINES=/etc/ingest-pipelines

# install the whole ingest package intact — every factored module preserved
RUN pip install --no-cache-dir .

# connection comes from env at runtime (TEMPORAL_ADDRESS / CH_* ), never baked
# queue comes from the mounted spec (INGEST_DOMAIN + INGEST_PIPELINES env)
ENTRYPOINT ["python", "-u", "-m", "ingest.orchestration.worker"]
