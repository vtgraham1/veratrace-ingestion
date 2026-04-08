"""
Cursor manager — persists sync cursors per (integration_account_id, stream).

Critical rule: checkpoint AFTER writes commit, never before.
If we crash between write and checkpoint, we re-fetch (idempotent via upsert).
If we checkpoint before write, we skip records permanently.
"""
import json
import logging
import urllib.request
import urllib.error
from dataclasses import dataclass

from src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

logger = logging.getLogger(__name__)

CURSOR_TABLE = "sync_cursors"


@dataclass
class CursorState:
    integration_account_id: str
    stream: str
    cursor: str
    last_sync_at: str
    records_synced: int


def _headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def get_cursor(integration_account_id: str, stream: str) -> str | None:
    """Get the last saved cursor for a stream. Returns None if no cursor exists."""
    url = (
        f"{SUPABASE_URL}/rest/v1/{CURSOR_TABLE}"
        f"?integration_account_id=eq.{integration_account_id}"
        f"&stream=eq.{stream}"
        f"&select=cursor"
    )
    req = urllib.request.Request(url, headers=_headers())
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        if data:
            return data[0]["cursor"]
    except Exception as e:
        logger.warning("Failed to read cursor: %s", e)
    return None


def save_cursor(
    integration_account_id: str,
    stream: str,
    cursor: str,
    records_synced: int = 0,
) -> None:
    """
    Upsert the cursor. Call this AFTER signal writes are committed.

    Uses Supabase upsert (ON CONFLICT UPDATE) to handle both insert and update.
    """
    import datetime

    payload = json.dumps({
        "integration_account_id": integration_account_id,
        "stream": stream,
        "cursor": cursor,
        "last_sync_at": datetime.datetime.utcnow().isoformat() + "Z",
        "records_synced": records_synced,
    }).encode()

    url = f"{SUPABASE_URL}/rest/v1/{CURSOR_TABLE}"
    headers = _headers()
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"

    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as e:
        logger.error("Failed to save cursor: %s %s", e.code, e.read().decode()[:200])
        raise
