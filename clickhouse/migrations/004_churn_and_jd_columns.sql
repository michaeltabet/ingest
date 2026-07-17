-- 004: persist the two gate inputs added 2026-07-17, so evidence rows carry
-- the same facts the gate ruled on (a verdict whose inputs aren't in the
-- ledger can't be audited).
--   details_gone: detail 404/410 — job pulled mid-run; churn, not fault.
--   jobs_no_jd:   landed jobs whose body carries no description; a board where
--                 ALL landed jobs lack a JD fails loud (the 07-17 fake-pass).
ALTER TABLE scrape_evidence
    ADD COLUMN IF NOT EXISTS details_gone UInt32 DEFAULT 0 AFTER details_failed,
    ADD COLUMN IF NOT EXISTS jobs_no_jd   UInt32 DEFAULT 0 AFTER details_gone;
