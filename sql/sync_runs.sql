-- sync_runs: structured telemetry for every sync_account() invocation.
--
-- One row per run. Written from src/runtime/sync_runs.py, called in the
-- finally block of src/sync/scheduler.py::sync_account. Failure to write
-- a row is logged and swallowed — observability must never break sync.
--
-- Status values are the STATUS_* constants in src/sync/scheduler.py:
--   ok | skipped_no_connector | invalid_credentials | no_new_signals | error
-- Treated as free-form TEXT rather than ENUM so scheduler.py remains the
-- source of truth for vocabulary.
--
-- Applied manually via Supabase service-role key or Studio. This file is
-- the canonical schema for recreate/audit.

CREATE TABLE IF NOT EXISTS sync_runs (
    run_id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    integration_account_id TEXT         NOT NULL,
    instance_id            TEXT         NOT NULL,
    integration_id         TEXT         NOT NULL,
    started_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at            TIMESTAMPTZ,
    status                 TEXT         NOT NULL,
    signals_written        INTEGER      NOT NULL DEFAULT 0,
    duration_ms            INTEGER,
    error                  TEXT,
    backfill               BOOLEAN      NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS sync_runs_account_idx  ON sync_runs (integration_account_id, started_at DESC);
CREATE INDEX IF NOT EXISTS sync_runs_instance_idx ON sync_runs (instance_id,            started_at DESC);
CREATE INDEX IF NOT EXISTS sync_runs_status_idx   ON sync_runs (status,                 started_at DESC);

-- Default-deny: RLS on, zero policies. Writes come from scheduler via service
-- role (bypasses RLS); reads come from the ingestion API's /stats/* endpoints,
-- also via service role. Anon / authenticated sessions must never see rows —
-- this table contains integration_account_id, instance_id, and error strings
-- that can leak customer identity or PII. Supabase flagged this as
-- `rls_disabled_in_public` on 2026-04-21; the table was exposed to the public
-- anon key for ~5 days before the advisor noticed. Enabling here so recreates
-- are safe by construction.
ALTER TABLE sync_runs ENABLE ROW LEVEL SECURITY;
