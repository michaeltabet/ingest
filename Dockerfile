# ingest worker — wraps the factored scraper + Temporal workflow code as a
# client to the EXISTING cluster Temporal. Does NOT run a Temporal server.
# Entrypoint: the Temporal worker joining queue scrape-http.
FROM python:3.12-slim

WORKDIR /app

# deps first (cache layer): httpx, clickhouse-connect, temporalio, django
COPY pyproject.toml README.md ./
COPY src ./src

# install the whole ingest package intact — every factored module preserved
RUN pip install --no-cache-dir .

# connection comes from env at runtime (TEMPORAL_ADDRESS / CH_* ), never baked
ENTRYPOINT ["python", "-u", "-m", "ingest.orchestration.worker"]
CMD ["--queue", "scrape-http"]
