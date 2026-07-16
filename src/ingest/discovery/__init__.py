"""Board discovery — grow ingest.boards from the open web, not by hand.

Two stages, ClickHouse as the handoff (same fail-loud doctrine as scraping):

  cc_scan.py   PySpark over the Common Crawl columnar URL index → every ATS
               tenant URL the crawl saw → distinct (platform, slug, url) into
               ingest.board_candidates. SELF-CONTAINED: pyspark + stdlib only,
               so it runs on the stock apache/spark image with no pip step.
               Runs as a ScheduledSparkApplication (Kubeflow Spark Operator).

  promote.py   candidates → boards. Anti-join against ingest.boards, shape-gate
               with scraping.validation.validate_board, live-probe by replaying
               each platform scraper's first-page request (method + body — some
               platforms POST) and shape-checking the response, insert
               survivors. Runs as a CronJob on the ingest source tgz.

Every candidate gets a status row (known / rejected / probe_failed / promoted)
— rejections are evidence, not log lines.
"""
