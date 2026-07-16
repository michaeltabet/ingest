-- 003: gate inputs into the ledger + the committed-jobs reader.
-- ALTERs and VIEWs are not expressible as Record DDL (migrate.py generates
-- CREATE TABLE only) — this file is the reviewable artifact for both.
-- Idempotent: IF NOT EXISTS throughout; safe to re-run.

ALTER TABLE scrape_evidence ADD COLUMN IF NOT EXISTS items_seen UInt32 AFTER stubs_seen;
ALTER TABLE scrape_evidence ADD COLUMN IF NOT EXISTS dupes_seen UInt32 AFTER items_seen;
ALTER TABLE scrape_evidence ADD COLUMN IF NOT EXISTS reported_total UInt32 AFTER dupes_seen;

-- jobs_committed: THE sanctioned way to read landed jobs.
-- jobs is a ReplacingMergeTree (latest writer wins), and scrapes stream
-- batches out mid-run — so rows from a run that later FAILED its gate can sit
-- in jobs. The evidence row (outcome='success') is the commit marker, written
-- last; jobs_runs is the per-run membership. This view = content of exactly
-- the latest successful run per board. Query THIS, not jobs FINAL.
-- (Known limit: if a newer failed run re-fetched the same external_id, FINAL
-- serves that newer row's content — presence is gated, content is
-- latest-fetched. Raw in scrape_raw makes any row reconstructable.)
CREATE VIEW IF NOT EXISTS jobs_committed AS
SELECT j.*
FROM jobs AS j FINAL
WHERE (j.platform, j.board_id, j.external_id) IN (
    SELECT r.platform, r.board_id, r.external_id
    FROM jobs_runs AS r
    WHERE (r.platform, r.board_id, r.run_id) IN (
        SELECT platform, board_id, argMax(run_id, run_at)
        FROM scrape_evidence
        WHERE outcome = 'success'
        GROUP BY platform, board_id));
