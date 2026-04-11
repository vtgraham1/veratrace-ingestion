"""
Intercom connector — pulls conversations with Fin AI attribution
and transforms them into TwuSignals.

Intercom has first-class AI attribution via the ai_agent object:
  ai_agent.did_resolve — whether Fin resolved the conversation
  ai_agent.resolution_state — "resolved" or "escalated"
  ai_agent.resolution_rating — customer CSAT

Rate limits: 1000 req/min (public), 10K req/min (private apps).
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

from src.connectors.base import (
    BaseConnector, ConnectionTestResult, SyncResult, ConnectorHealth,
)
from src.connectors.intercom.signal_mapper import map_conversation_to_signals

logger = logging.getLogger(__name__)

INTERCOM_API_BASE = "https://api.intercom.io"
INTERCOM_API_VERSION = "2.11"


class IntercomConnector(BaseConnector):
    """
    Intercom integration connector.

    Credentials expected:
        credentials["accessToken"] — Intercom API token
    External identity:
        external_identity["tenantId"] — Intercom workspace ID
    """

    CONFIG = {
        **BaseConnector.CONFIG,
        "rate_limit_rps": 16.0,
        "rate_ceiling_pct": 70,
        "max_results_per_page": 150,
        "cursor_format": "token",
        "backfill_days_default": 30,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._access_token = self.credentials.get("accessToken", "")
        effective_rps = self.CONFIG["rate_limit_rps"] * self.CONFIG["rate_ceiling_pct"] / 100
        self._sync_delay = 1.0 / effective_rps if effective_rps > 0 else 1.0

    def validate_credentials(self):
        return bool(self._access_token)

    def test_connection(self):
        try:
            result = self._api_get("/me")
            workspace = result.get("app", {})
            name = workspace.get("name", result.get("name", ""))
            workspace_id = workspace.get("id_code", result.get("id", ""))
            return ConnectionTestResult(
                success=True,
                message=f"Connected to workspace '{name}' ({workspace_id})",
                details={"workspace_name": name, "workspace_id": workspace_id},
            )
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Connection failed: {str(e)[:200]}")

    def detect_region(self):
        return "global"

    def sync_incremental(self, cursor=None):
        if cursor:
            try:
                since_ts = int(cursor)
            except ValueError:
                since_ts = int(datetime.fromisoformat(cursor.replace("Z", "+00:00")).timestamp())
        else:
            since_ts = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())

        all_signals = []
        api_calls = 0
        next_cursor = None

        while True:
            if api_calls > 0:
                time.sleep(self._sync_delay)

            try:
                search_body = {
                    "query": {"field": "updated_at", "operator": ">", "value": since_ts},
                    "pagination": {"per_page": self.CONFIG["max_results_per_page"]},
                }
                if next_cursor:
                    search_body["pagination"]["starting_after"] = next_cursor

                result = self._api_post("/conversations/search", search_body)
                api_calls += 1
            except Exception as e:
                logger.error("Intercom search failed: %s", e)
                break

            conversations = result.get("conversations", result.get("data", []))
            for conv in conversations:
                signals = map_conversation_to_signals(conv, self.instance_id, self.integration_account_id)
                all_signals.extend(signals)

            pages = result.get("pages", {})
            next_obj = pages.get("next")
            next_cursor = next_obj.get("starting_after") if isinstance(next_obj, dict) else next_obj
            if not next_cursor or not conversations:
                break

            logger.info("Fetched page: %d conversations, %d signals", len(conversations), len(all_signals))

        new_cursor = str(int(datetime.now(timezone.utc).timestamp()))
        return SyncResult(signals=all_signals, cursor=new_cursor, api_calls_made=api_calls)

    def sync_backfill(self, start_date=None):
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=self.CONFIG["backfill_days_default"])
        return self.sync_incremental(cursor=str(int(start_date.timestamp())))

    def _api_get(self, path):
        url = f"{INTERCOM_API_BASE}{path}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Accept", "application/json")
        req.add_header("Intercom-Version", INTERCOM_API_VERSION)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def _api_post(self, path, body):
        url = f"{INTERCOM_API_BASE}{path}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        req.add_header("Intercom-Version", INTERCOM_API_VERSION)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def get_expected_schema(self):
        from src.connectors.intercom.schema import EXPECTED_CONVERSATION_FIELDS
        return {"fields": list(EXPECTED_CONVERSATION_FIELDS)}

    def get_expected_fields(self):
        from src.connectors.intercom.schema import EXPECTED_CONVERSATION_FIELDS
        return EXPECTED_CONVERSATION_FIELDS

    def get_health(self):
        return ConnectorHealth(status="HEALTHY")
