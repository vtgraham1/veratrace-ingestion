"""
Freshdesk connector — pulls tickets via List Tickets API and transforms
them into TwuSignals with AI attribution via agent classification.

Supports:
- Incremental sync via updated_since parameter
- Backfill via date range
- Basic Auth (API key as username, X as password)
- AI attribution by cross-referencing responder against agent cache
- Conversation fetch for per-reply attribution

Rate limits: 200 calls/min (Growth), 700/min (Enterprise). We use 3 rps.
"""
from __future__ import annotations

import base64
import json
import logging
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

from src.connectors.base import (
    BaseConnector, ConnectionTestResult, SyncResult, ConnectorHealth,
)
from src.connectors.freshdesk.signal_mapper import map_ticket_to_signals
from src.connectors.freshdesk.schema import REQUIRED_FIELDS

logger = logging.getLogger(__name__)


class FreshdeskConnector(BaseConnector):
    """
    Freshdesk integration connector.

    Credentials expected:
        credentials["api_key"]  — Freshdesk API key (Profile Settings)
        credentials["domain"]   — Portal subdomain (e.g., "acme" for acme.freshdesk.com)
    External identity:
        external_identity["tenantId"] — Freshdesk domain
    """

    CONFIG = {
        **BaseConnector.CONFIG,
        "rate_limit_rps": 3.0,
        "rate_ceiling_pct": 70,
        "backfill_days_default": 30,
        "max_results_per_page": 100,
        "cursor_format": "iso8601",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._api_key = self.credentials.get("api_key", "")
        self._domain = self.credentials.get("domain", "").strip().replace(".freshdesk.com", "")
        self._base_url = f"https://{self._domain}.freshdesk.com/api/v2"
        self._auth = base64.b64encode(f"{self._api_key}:X".encode()).decode()

        effective_rps = self.CONFIG["rate_limit_rps"] * self.CONFIG["rate_ceiling_pct"] / 100
        self._sync_delay = 1.0 / effective_rps if effective_rps > 0 else 1.0

        # Agent cache — populated on first sync
        self._agent_cache = {}  # agent_id → agent dict
        self._agents_loaded = False

    # ── Setup ──────────────────────────────────────────────────────────────

    def validate_credentials(self):
        return bool(self._api_key and self._domain)

    def test_connection(self):
        try:
            result = self._api_get("/tickets?per_page=1")
            count = len(result) if isinstance(result, list) else 0
            return ConnectionTestResult(
                success=True,
                message=f"Connected to {self._domain}.freshdesk.com",
                region=self.detect_region(),
                details={"domain": self._domain},
            )
        except Exception as e:
            msg = str(e)[:200]
            if "401" in msg:
                msg = "Invalid API key. Go to Profile Settings → Your API Key in Freshdesk."
            elif "getaddrinfo" in msg or "404" in msg:
                msg = f"Could not reach {self._domain}.freshdesk.com. Verify your portal URL."
            return ConnectionTestResult(success=False, message=msg)

    def detect_region(self):
        # Freshdesk is global SaaS — no region routing
        return "global"

    # ── Agent Cache ────────────────────────────────────────────────────────

    def _load_agents(self):
        """Cache all agents for bot/human classification."""
        if self._agents_loaded:
            return
        try:
            page = 1
            while True:
                agents = self._api_get(f"/agents?per_page=100&page={page}")
                if not agents:
                    break
                for agent in agents:
                    aid = agent.get("id")
                    if aid:
                        self._agent_cache[aid] = {
                            "name": agent.get("contact", {}).get("name", "Unknown"),
                            "email": agent.get("contact", {}).get("email", ""),
                            "agent_type": agent.get("type", ""),
                            "active": agent.get("active", True),
                        }
                if len(agents) < 100:
                    break
                page += 1
                time.sleep(self._sync_delay)
            self._agents_loaded = True
            logger.info("Agent cache loaded: %d agents", len(self._agent_cache))
        except Exception as e:
            logger.warning("Failed to load agents (non-fatal): %s", str(e)[:100])
            self._agents_loaded = True

    # ── Sync ───────────────────────────────────────────────────────────────

    def sync_incremental(self, cursor=None):
        self._load_agents()

        if cursor:
            updated_since = cursor
        else:
            updated_since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        all_signals = []
        api_calls = 0
        page = 1
        latest_updated = updated_since

        while True:
            if api_calls > 0:
                time.sleep(self._sync_delay)

            try:
                tickets = self._api_get(
                    f"/tickets?updated_since={updated_since}"
                    f"&per_page={self.CONFIG['max_results_per_page']}"
                    f"&page={page}"
                    f"&order_by=updated_at&order_type=asc"
                    f"&include=description,stats"
                )
                api_calls += 1
            except Exception as e:
                logger.error("Ticket query failed: %s", e)
                break

            if not tickets:
                break

            for ticket in tickets:
                # Fetch conversations for reply attribution
                conversations = []
                ticket_id = ticket.get("id")
                if ticket_id:
                    time.sleep(self._sync_delay)
                    try:
                        conversations = self._api_get(f"/tickets/{ticket_id}/conversations")
                        api_calls += 1
                    except Exception as e:
                        logger.debug("Conversation fetch failed for %s: %s", ticket_id, str(e)[:50])

                signals = map_ticket_to_signals(
                    ticket, conversations, self._agent_cache,
                    self.instance_id, self.integration_account_id,
                )
                all_signals.extend(signals)

                updated = ticket.get("updated_at", "")
                if updated and updated > latest_updated:
                    latest_updated = updated

            if len(tickets) < self.CONFIG["max_results_per_page"]:
                break
            page += 1

        logger.info(
            "Sync complete: %d signals from %d API calls",
            len(all_signals), api_calls,
        )

        return SyncResult(
            signals=all_signals,
            cursor=latest_updated,
            has_more=False,
            records_fetched=len(all_signals),
            api_calls_made=api_calls,
        )

    def sync_backfill(self, start_date=None):
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(
                days=self.CONFIG["backfill_days_default"]
            )
        cursor = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self.sync_incremental(cursor=cursor)

    # ── API Client ─────────────────────────────────────────────────────────

    def _api_get(self, path):
        url = f"{self._base_url}{path}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Basic {self._auth}")
        req.add_header("Accept", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", "60"))
                logger.warning("Rate limited — waiting %ds", retry_after)
                time.sleep(min(retry_after, 120))
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read())
            raise

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_expected_schema(self):
        from src.connectors.freshdesk.schema import EXPECTED_FIELDS
        return {"fields": list(EXPECTED_FIELDS)}

    def get_expected_fields(self):
        from src.connectors.freshdesk.schema import EXPECTED_FIELDS
        return EXPECTED_FIELDS

    def get_health(self):
        return ConnectorHealth(status="HEALTHY")
