CREATE TABLE IF NOT EXISTS jobs_runs (
  platform LowCardinality(String),
  board_id String,
  external_id String,
  run_id String,
  fetched_at DateTime64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(fetched_at)
ORDER BY (platform, board_id, run_id, external_id)
TTL toDateTime(fetched_at) + INTERVAL 30 DAY;
