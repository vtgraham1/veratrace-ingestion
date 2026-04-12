"""
Sync scheduler — orchestrates incremental syncs for all active integration accounts.

Called by cron every 15 minutes during business hours.
For each active account: fetch cursor → sync incremental → write signals → save cursor → trigger compiler.

Usage:
  python3 -m src.sync.scheduler                    # sync all active accounts
  python3 -m src.sync.scheduler --account ACCT_ID  # sync one account
  python3 -m src.sync.scheduler --backfill ACCT_ID # backfill one account
"""
import json
import logging
import os
import sys
import urllib.request

from src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, CONTROL_PLANE_URL
from src.connectors import CONNECTOR_MAP
from src.runtime.cursor_manager import get_cursor, save_cursor
from src.runtime.signal_writer import write_signals
from src.runtime.task_trigger import trigger_compilation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("sync")
logger.info("Registered connectors: %s", list(CONNECTOR_MAP.keys()))


def _supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def fetch_active_accounts(instance_id=None):
    """Fetch all ACTIVE integration accounts from the control plane DB."""
    url = f"{SUPABASE_URL}/rest/v1/integration_accounts?status=eq.ACTIVE&select=*"
    if instance_id:
        url += f"&instance_id=eq.{instance_id}"

    req = urllib.request.Request(url, headers=_supabase_headers())
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.error("Failed to fetch integration accounts: %s", e)
        return []


def sync_account(account, backfill=False):
    """Run a sync for a single integration account."""
    integration_id = account.get("integration_id", "")
    account_id = account.get("integration_account_id", "")
    instance_id = account.get("instance_id", "")

    connector_cls = CONNECTOR_MAP.get(integration_id)
    if not connector_cls:
        logger.warning("No connector for integration_id=%s, skipping", integration_id)
        return

    # Parse credentials and identity from JSONB
    credentials = account.get("auth_credentials", {})
    external_identity = account.get("external_identity", {})
    if isinstance(credentials, str):
        credentials = json.loads(credentials)
    if isinstance(external_identity, str):
        external_identity = json.loads(external_identity)

    logger.info("Instantiating %s connector for account %s (cred_keys=%s, identity_keys=%s)",
                integration_id, account_id[:8], list(credentials.keys()), list(external_identity.keys()))

    connector = connector_cls(
        instance_id=instance_id,
        integration_account_id=account_id,
        credentials=credentials,
        external_identity=external_identity,
    )

    # Validate credentials first
    if not connector.validate_credentials():
        logger.error("Invalid credentials for account %s (keys present: %s), skipping",
                     account_id[:8], list(credentials.keys()))
        return

    stream = f"{integration_id}:contacts"

    if backfill:
        logger.info("Starting backfill for %s (account=%s)", integration_id, account_id[:8])
        result = connector.sync_backfill()
    else:
        cursor = get_cursor(account_id, stream)
        logger.info(
            "Incremental sync for %s (account=%s, cursor=%s)",
            integration_id, account_id[:8], cursor[:20] if cursor else "none",
        )
        result = connector.sync_incremental(cursor)

    if not result.signals:
        logger.info("No new signals (account=%s)", account_id[:8])
        return

    # Write signals to database
    written = write_signals(result.signals)
    logger.info("Wrote %d signals (account=%s)", written, account_id[:8])

    # Save cursor AFTER successful write
    if result.cursor:
        save_cursor(account_id, stream, result.cursor, records_synced=written)

    # Trigger TWU compilation
    task = trigger_compilation(instance_id, [account_id])
    if task:
        logger.info("Triggered compiler task %s", task.get("taskId", "?")[:8])
    else:
        logger.warning("Compiler task trigger failed (non-fatal)")


def run_all(backfill=False):
    """Sync all active integration accounts."""
    accounts = fetch_active_accounts()
    logger.info("Found %d active integration accounts", len(accounts))

    for account in accounts:
        try:
            sync_account(account, backfill=backfill)
        except Exception as e:
            logger.error(
                "Sync failed for account %s: %s",
                account.get("integration_account_id", "?")[:8],
                str(e)[:200],
            )


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--backfill" in args:
        idx = args.index("--backfill")
        if idx + 1 < len(args):
            acct_id = args[idx + 1]
            accounts = fetch_active_accounts()
            acct = next((a for a in accounts if a.get("integration_account_id") == acct_id), None)
            if acct:
                sync_account(acct, backfill=True)
            else:
                logger.error("Account not found: %s", acct_id)
        else:
            logger.error("--backfill requires an account ID")
    elif "--account" in args:
        idx = args.index("--account")
        if idx + 1 < len(args):
            acct_id = args[idx + 1]
            accounts = fetch_active_accounts()
            acct = next((a for a in accounts if a.get("integration_account_id") == acct_id), None)
            if acct:
                sync_account(acct)
            else:
                logger.error("Account not found: %s", acct_id)
        else:
            logger.error("--account requires an account ID")
    else:
        run_all()
