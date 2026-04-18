-- Add parse_job_id to parsing_sessions for idempotent upsert on acks_late re-delivery.
--
-- Without this column, an acks_late re-delivery (worker killed while job is
-- 'processing') creates a duplicate parsing_sessions row for the same parse_jobs.id.
-- With it, INSERT ... ON CONFLICT (parse_job_id) returns the existing id — safe.
--
-- NULL is allowed so rows inserted before this migration are unaffected.
-- NULLs do not trigger the unique constraint in PostgreSQL, so legacy rows coexist.

ALTER TABLE parsing_sessions
    ADD COLUMN IF NOT EXISTS parse_job_id BIGINT DEFAULT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_parsing_sessions_parse_job_id
    ON parsing_sessions (parse_job_id)
    WHERE parse_job_id IS NOT NULL;
