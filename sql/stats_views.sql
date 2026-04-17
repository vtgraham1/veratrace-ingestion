-- Aggregation views over twu_signals + sync_runs, consumed by the /stats/*
-- endpoints added in src/main.py. Apply once via Supabase Studio SQL editor:
--   https://supabase.com/dashboard/project/tumrbdswcoppmyqvoaly/sql/new
--
-- Views (not stored procedures) because:
--   - queryable via PostgREST with standard select/filter params
--   - query plan cached by Postgres; single SELECT from Python
--   - swap underlying tables later without changing the endpoint contract

-- Per-account aggregate stats: counts + last sync.
CREATE OR REPLACE VIEW v_account_stats AS
SELECT
    instance_id,
    source->>'integration_account_id' AS integration_account_id,
    COUNT(*)                          AS twu_count,
    COUNT(DISTINCT actor->>'tenantId') AS instance_count,
    MAX(processed_at)                 AS last_sync
FROM twu_signals
WHERE source->>'integration_account_id' IS NOT NULL
GROUP BY instance_id, source->>'integration_account_id';

-- Per-account-per-tenant breakdown for TWUStackedBarChart.
CREATE OR REPLACE VIEW v_account_instance_breakdown AS
SELECT
    instance_id,
    source->>'integration_account_id' AS integration_account_id,
    actor->>'tenantId'                AS tenant_id,
    COUNT(*)                          AS twu_count
FROM twu_signals
WHERE source->>'integration_account_id' IS NOT NULL
  AND actor->>'tenantId' IS NOT NULL
GROUP BY instance_id, source->>'integration_account_id', actor->>'tenantId';

-- Recent sync_runs, projected as a view so the endpoint doesn't hit the raw
-- table directly. Lets us add/remove columns later without breaking the API.
CREATE OR REPLACE VIEW v_account_recent_runs AS
SELECT
    run_id,
    integration_account_id,
    instance_id,
    status,
    started_at,
    finished_at,
    signals_written,
    duration_ms,
    error,
    backfill
FROM sync_runs;
