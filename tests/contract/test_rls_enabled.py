"""
Contract test — every table in the public schema of the Supabase project
must have Row-Level Security enabled.

Why: 2026-04-21 Supabase Security Advisor flagged `sync_runs` as publicly
accessible because `sql/sync_runs.sql` did not `ALTER TABLE ... ENABLE ROW
LEVEL SECURITY`. The anon key (shipped in the browser bundle) could read
every row. Table was empty at the time; once populated it would leak
integration_account_id, instance_id, and error messages containing PII.

This test catches the same class of drift on every future table — if
someone adds a new table via DDL and forgets `ENABLE ROW LEVEL SECURITY`,
CI fails loudly instead of waiting for Supabase's weekly advisor email.

Requires a Supabase Management API personal access token (SUPABASE_ACCESS_TOKEN
in env, e.g. `sbp_...`) — the regular service role key can't reach
`pg_catalog.pg_tables` via PostgREST. Skipped cleanly without the token so
existing CI setups don't break; add the token when you want this enforced.

Run locally:
  SUPABASE_PROJECT_REF=tumrbdswcoppmyqvoaly \\
  SUPABASE_ACCESS_TOKEN=sbp_... CI_CONTRACT=true \\
    python -m pytest tests/contract/test_rls_enabled.py --contract -v
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

import pytest


PROJECT_REF = os.environ.get("SUPABASE_PROJECT_REF", "")
ACCESS_TOKEN = os.environ.get("SUPABASE_ACCESS_TOKEN", "")


pytestmark = pytest.mark.skipif(
    not (PROJECT_REF and ACCESS_TOKEN),
    reason="SUPABASE_PROJECT_REF and SUPABASE_ACCESS_TOKEN required for RLS contract test",
)


def _run_sql(query: str) -> list[dict]:
    """Execute SQL against Supabase via the Management API and return rows."""
    url = f"https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query"
    body = json.dumps({"query": query}).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/json",
            # Cloudflare in front of api.supabase.com blocks default urllib UA
            # with error 1010; a plausible UA clears it.
            "User-Agent": "veratrace-ingestion-contract-test/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace") if e.fp else ""
        raise AssertionError(
            f"Supabase Management API returned HTTP {e.code}: {body[:300]}"
        ) from e


def test_every_public_table_has_rls_enabled():
    rows = _run_sql(
        "SELECT tablename, rowsecurity FROM pg_tables "
        "WHERE schemaname = 'public' ORDER BY tablename"
    )
    assert rows, "pg_tables query returned no public tables — unexpected"
    exposed = [r["tablename"] for r in rows if not r["rowsecurity"]]
    assert exposed == [], (
        f"These public tables have RLS disabled and are readable with the "
        f"anon key: {exposed}. Add `ALTER TABLE <name> ENABLE ROW LEVEL "
        f"SECURITY;` to their schema file and run it on the project. See "
        f"feedback_new_supabase_tables_need_rls.md for the pattern."
    )
