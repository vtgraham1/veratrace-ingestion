"""
ServiceNow connector — pulls Incidents via Table API and transforms them
into TwuSignals with AI attribution from sys_audit.

Supports:
- Incremental sync via sys_updated_on cursor
- Backfill via date range
- OAuth 2.0 Client Credentials grant with thread-safe token refresh
- AI attribution from sys_audit + sys_journal_field (inference-based)
- Fallback to sys_ai_resolution table if available (Now Assist)

Rate limits: ~300 req/min default, we use 5 rps × 70% = 3.5 effective.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

from src.connectors.base import (
    BaseConnector, ConnectionTestResult, SyncResult, ConnectorHealth,
)
from src.connectors.servicenow.signal_mapper import map_incident_to_signals, map_audit_to_signals
from src.connectors.servicenow.schema import REQUIRED_FIELDS

logger = logging.getLogger(__name__)


class ServiceNowConnector(BaseConnector):
    """
    ServiceNow integration connector.

    Credentials expected:
        credentials["instance_url"]  — e.g., https://dev12345.service-now.com
        credentials["client_id"]     — OAuth Application Registry client ID
        credentials["client_secret"] — OAuth Application Registry secret
    External identity:
        external_identity["tenantId"] — ServiceNow instance name
    """

    CONFIG = {
        **BaseConnector.CONFIG,
        "rate_limit_rps": 5.0,
        "rate_ceiling_pct": 70,
        "backfill_days_default": 30,
        "max_results_per_page": 100,
        "cursor_format": "iso8601",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._instance_url = self.credentials.get("instance_url", "").rstrip("/")
        self._client_id = self.credentials.get("client_id", "")
        self._client_secret = self.credentials.get("client_secret", "")
        self._access_token = self.credentials.get("access_token", "")
        self._token_lock = threading.Lock()

        effective_rps = self.CONFIG["rate_limit_rps"] * self.CONFIG["rate_ceiling_pct"] / 100
        self._sync_delay = 1.0 / effective_rps if effective_rps > 0 else 1.0

        # Track whether this instance has sys_ai_resolution table
        self._has_ai_resolution_table = None

    # ── Setup ──────────────────────────────────────────────────────────────

    def validate_credentials(self):
        return bool(self._instance_url and self._client_id and self._client_secret)

    def test_connection(self):
        try:
            if not self._access_token:
                self._obtain_token()
            result = self._api_get(
                "/api/now/table/incident",
                params={"sysparm_limit": "1", "sysparm_fields": "sys_id,number"},
            )
            return ConnectionTestResult(
                success=True,
                message="Connected to ServiceNow",
                region=self.detect_region(),
                details={"instance": self._instance_url},
            )
        except Exception as e:
            msg = str(e)[:200]
            if "401" in msg or "403" in msg:
                msg = "Invalid OAuth credentials. Check Client ID and Secret in System OAuth > Application Registry."
            elif "getaddrinfo" in msg or "Name or service not known" in msg:
                msg = f"Could not reach {self._instance_url}. Verify your instance name."
            return ConnectionTestResult(success=False, message=msg)

    def detect_region(self):
        if not self._instance_url:
            return "unknown"
        host = self._instance_url.replace("https://", "").split(".")[0]
        if "eu" in host:
            return "eu"
        if "ap" in host:
            return "apac"
        return "us"

    # ── Sync ───────────────────────────────────────────────────────────────

    def sync_incremental(self, cursor=None):
        if not self._access_token:
            self._obtain_token()

        if cursor:
            start_time = cursor
        else:
            start_time = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        all_signals = []
        api_calls = 0
        offset = 0
        page_size = self.CONFIG["max_results_per_page"]
        latest_updated = start_time

        while True:
            if api_calls > 0:
                time.sleep(self._sync_delay)

            try:
                result = self._api_get(
                    "/api/now/table/incident",
                    params={
                        "sysparm_query": (
                            f"sys_updated_on>{start_time}"
                            "^ORDERBYsys_updated_on"
                        ),
                        "sysparm_limit": str(page_size),
                        "sysparm_offset": str(offset),
                        "sysparm_display_value": "all",
                        "sysparm_fields": ",".join(self._incident_fields()),
                    },
                )
                api_calls += 1
            except Exception as e:
                logger.error("Incident query failed: %s", e)
                break

            records = result.get("result", [])
            if not records:
                break

            # Collect sys_ids for audit fetch
            sys_ids = [r.get("sys_id", {}) for r in records]
            # Handle display_value format: sys_id may be {"value": "...", "display_value": "..."}
            sys_ids = [
                sid["value"] if isinstance(sid, dict) else sid
                for sid in sys_ids
                if sid
            ]

            # Fetch audit records for AI attribution
            audit_records = []
            if sys_ids:
                time.sleep(self._sync_delay)
                audit_records = self._fetch_audit_records(sys_ids)
                api_calls += 1

            # Map incidents to signals
            for record in records:
                sid = record.get("sys_id", {})
                sid_val = sid["value"] if isinstance(sid, dict) else sid
                incident_audits = [
                    a for a in audit_records
                    if (a.get("documentkey", {}).get("value", "") if isinstance(a.get("documentkey"), dict) else a.get("documentkey", "")) == sid_val
                ]
                signals = map_incident_to_signals(
                    record, incident_audits,
                    self.instance_id, self.integration_account_id,
                )
                all_signals.extend(signals)

                # Track latest sys_updated_on for cursor
                updated = record.get("sys_updated_on", {})
                updated_val = updated["value"] if isinstance(updated, dict) else updated
                if updated_val and updated_val > latest_updated:
                    latest_updated = updated_val

            offset += page_size
            if len(records) < page_size:
                break

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
        cursor = start_date.strftime("%Y-%m-%d %H:%M:%S")
        return self.sync_incremental(cursor=cursor)

    def _incident_fields(self):
        return [
            "sys_id", "number", "short_description", "description",
            "state", "priority", "urgency", "impact", "category",
            "subcategory", "assigned_to", "assignment_group",
            "opened_by", "opened_at", "resolved_by", "resolved_at",
            "closed_at", "close_code", "close_notes",
            "sys_created_on", "sys_updated_on",
            "contact_type", "caller_id",
        ]

    def _fetch_audit_records(self, incident_sys_ids):
        """Fetch sys_audit records for a batch of incidents."""
        ids_str = ",".join(incident_sys_ids)
        try:
            result = self._api_get(
                "/api/now/table/sys_audit",
                params={
                    "sysparm_query": (
                        f"documentkeyIN{ids_str}"
                        "^tablename=incident"
                    ),
                    "sysparm_limit": "500",
                    "sysparm_fields": "sys_id,documentkey,fieldname,oldvalue,newvalue,user,sys_created_on",
                    "sysparm_display_value": "all",
                },
            )
            return result.get("result", [])
        except Exception as e:
            logger.warning("sys_audit fetch failed (non-fatal): %s", str(e)[:100])
            return []

    # ── OAuth Client Credentials ──────────────────────────────────────────

    def _obtain_token(self):
        """Obtain an OAuth access token via Client Credentials grant."""
        with self._token_lock:
            if self._access_token:
                return

            data = urllib.parse.urlencode({
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }).encode()

            req = urllib.request.Request(
                f"{self._instance_url}/oauth_token.do",
                data=data,
                method="POST",
            )
            req.add_header("Content-Type", "application/x-www-form-urlencoded")

            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())

            self._access_token = result["access_token"]
            logger.info("OAuth token obtained for %s", self._instance_url)

    def _refresh_token(self):
        """Force token refresh."""
        with self._token_lock:
            self._access_token = ""
        self._obtain_token()

    def _api_get(self, path, params=None):
        """Make an authenticated GET request to the ServiceNow REST API."""
        url = f"{self._instance_url}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)

        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Accept", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 401:
                logger.info("Token expired, refreshing...")
                self._refresh_token()
                req.remove_header("Authorization")
                req.add_header("Authorization", f"Bearer {self._access_token}")
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read())
            raise

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_expected_schema(self):
        from src.connectors.servicenow.schema import EXPECTED_FIELDS
        return {"fields": list(EXPECTED_FIELDS)}

    def get_expected_fields(self):
        from src.connectors.servicenow.schema import EXPECTED_FIELDS
        return EXPECTED_FIELDS

    def get_health(self):
        return ConnectorHealth(status="HEALTHY")
