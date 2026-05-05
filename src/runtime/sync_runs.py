"""
sync_runs writer — persists one row per sync_account() invocation.

Called from src/sync/scheduler.py::sync_account in the finally block.
Observability-only: write failures are logged and swallowed. A Supabase
outage MUST NOT break the sync itself.

Schema canonical in sql/sync_runs.sql.
"""
from __future__ import annotations
import json
import logging
import urllib.error
import urllib.request

from src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from src.runtime.log import http_error_body, logfmt

logger = logging.getLogger(__name__)

SYNC_RUNS_TABLE = "sync_runs"


def _headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def write_sync_run(run: dict) -> None:
    """Insert a single sync_runs row. Never raises.

    Expected keys (snake_case, matching the schema):
      integration_account_id, instance_id, integration_id, status
    Optional:
      finished_at, signals_written, duration_ms, error, backfill
    started_at defaults to now() in the DB.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.warning("sync_runs write skipped: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY unset")
        return

    url = f"{SUPABASE_URL}/rest/v1/{SYNC_RUNS_TABLE}"
    short_aid = run.get("integration_account_id", "?")[:8]
    try:
        req = urllib.request.Request(url, data=json.dumps(run).encode(), headers=_headers(), method="POST")
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as e:
        logger.error(logfmt(
            "sync_run_write_failed",
            status=e.code,
            account_id=short_aid,
            body=http_error_body(e),
        ))
    except (urllib.error.URLError, ValueError) as e:
        logger.error(logfmt(
            "sync_run_write_failed",
            account_id=short_aid,
            error=str(e)[:200],
        ))
