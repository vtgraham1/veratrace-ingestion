"""
Genesys Cloud connector — pulls conversations via Analytics API and
transforms them into TwuSignals with first-class bot attribution.

Supports:
- Incremental sync via interval-based conversation detail queries
- Backfill via date range
- OAuth 2.0 Client Credentials with region-based token endpoint
- First-class bot attribution via participant.purpose field

Rate limits: varies by endpoint, we use 5 rps × 70% = 3.5 effective.
"""
from __future__ import annotations

import base64
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
from src.connectors.genesys.signal_mapper import map_conversation_to_signals
from src.connectors.genesys.schema import REQUIRED_FIELDS

logger = logging.getLogger(__name__)

# Region → API domain mapping
REGION_DOMAINS = {
    "us-east-1": "mypurecloud.com",
    "us-west-2": "usw2.pure.cloud",
    "eu-west-1": "mypurecloud.ie",
    "eu-central-1": "mypurecloud.de",
    "ap-southeast-2": "mypurecloud.com.au",
    "ap-northeast-1": "mypurecloud.jp",
    "ca-central-1": "cac1.pure.cloud",
    # Aliases for convenience
    "us": "mypurecloud.com",
    "eu": "mypurecloud.de",
    "apac": "mypurecloud.com.au",
}


class GenesysConnector(BaseConnector):
    """
    Genesys Cloud integration connector.

    Credentials expected:
        credentials["client_id"]     — OAuth client ID
        credentials["client_secret"] — OAuth client secret
        credentials["region"]        — e.g., "us-east-1", "mypurecloud.com", or "us"
    External identity:
        external_identity["tenantId"] — Genesys Cloud org ID
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
        self._client_id = self.credentials.get("client_id", "")
        self._client_secret = self.credentials.get("client_secret", "")
        self._region_input = self.credentials.get("region", "us-east-1")
        self._api_domain = self._resolve_domain(self._region_input)
        self._access_token = ""
        self._token_lock = threading.Lock()

        effective_rps = self.CONFIG["rate_limit_rps"] * self.CONFIG["rate_ceiling_pct"] / 100
        self._sync_delay = 1.0 / effective_rps if effective_rps > 0 else 1.0

    def _resolve_domain(self, region):
        """Resolve region input to API domain."""
        if "." in region:
            return region  # Already a domain
        return REGION_DOMAINS.get(region, "mypurecloud.com")

    # ── Setup ──────────────────────────────────────────────────────────────

    def validate_credentials(self):
        return bool(self._client_id and self._client_secret)

    def test_connection(self):
        try:
            self._obtain_token()
            result = self._api_get("/api/v2/organizations/me")
            org_name = result.get("name", "Unknown")
            return ConnectionTestResult(
                success=True,
                message=f"Connected to {org_name}",
                region=self.detect_region(),
                details={"org": org_name, "domain": self._api_domain},
            )
        except Exception as e:
            msg = str(e)[:200]
            if "401" in msg or "403" in msg:
                msg = (
                    "Invalid OAuth credentials. Check Client ID and Secret in "
                    "Genesys Cloud Admin > Integrations > OAuth."
                )
            elif "getaddrinfo" in msg:
                msg = (
                    f"Authentication failed for region '{self._region_input}'. "
                    "Verify your region — US East: mypurecloud.com, EU: mypurecloud.de, "
                    "APAC: mypurecloud.com.au"
                )
            return ConnectionTestResult(success=False, message=msg)

    def detect_region(self):
        domain = self._api_domain
        if "mypurecloud.de" in domain or "eu" in domain:
            return "eu"
        if "mypurecloud.com.au" in domain or "ap" in domain:
            return "apac"
        if "mypurecloud.jp" in domain:
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
                "%Y-%m-%dT%H:%M:%S.000Z"
            )

        end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        interval = f"{start_time}/{end_time}"

        all_signals = []
        api_calls = 0
        paging_cursor = None
        latest_end = start_time

        while True:
            if api_calls > 0:
                time.sleep(self._sync_delay)

            body = {
                "interval": interval,
                "order": "asc",
                "orderBy": "conversationStart",
                "paging": {
                    "pageSize": self.CONFIG["max_results_per_page"],
                },
            }
            if paging_cursor:
                body["paging"]["cursor"] = paging_cursor

            try:
                result = self._api_post(
                    "/api/v2/analytics/conversations/details/query",
                    body,
                )
                api_calls += 1
            except Exception as e:
                logger.error("Conversation query failed: %s", e)
                break

            conversations = result.get("conversations", [])
            if not conversations:
                break

            for conv in conversations:
                signals = map_conversation_to_signals(
                    conv, self.instance_id, self.integration_account_id,
                )
                all_signals.extend(signals)

                conv_end = conv.get("conversationEnd", "")
                if conv_end and conv_end > latest_end:
                    latest_end = conv_end

            # Check for next page
            paging_cursor = result.get("cursor")
            if not paging_cursor:
                break

        logger.info(
            "Sync complete: %d signals from %d API calls",
            len(all_signals), api_calls,
        )

        return SyncResult(
            signals=all_signals,
            cursor=latest_end,
            has_more=False,
            records_fetched=len(all_signals),
            api_calls_made=api_calls,
        )

    def sync_backfill(self, start_date=None):
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(
                days=self.CONFIG["backfill_days_default"]
            )
        cursor = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return self.sync_incremental(cursor=cursor)

    # ── OAuth Client Credentials ──────────────────────────────────────────

    def _obtain_token(self):
        """Obtain OAuth token via Client Credentials grant with Basic auth."""
        with self._token_lock:
            if self._access_token:
                return

            auth_str = base64.b64encode(
                f"{self._client_id}:{self._client_secret}".encode()
            ).decode()

            data = urllib.parse.urlencode({
                "grant_type": "client_credentials",
            }).encode()

            req = urllib.request.Request(
                f"https://login.{self._api_domain}/oauth/token",
                data=data,
                method="POST",
            )
            req.add_header("Authorization", f"Basic {auth_str}")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")

            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())

            self._access_token = result["access_token"]
            logger.info("OAuth token obtained for %s", self._api_domain)

    def _refresh_token(self):
        """Force token refresh."""
        with self._token_lock:
            self._access_token = ""
        self._obtain_token()

    def _api_get(self, path):
        """Make an authenticated GET request to the Genesys Cloud API."""
        url = f"https://api.{self._api_domain}{path}"
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

    def _api_post(self, path, body):
        """Make an authenticated POST request."""
        url = f"https://api.{self._api_domain}{path}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Content-Type", "application/json")
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
        from src.connectors.genesys.schema import EXPECTED_FIELDS
        return {"fields": list(EXPECTED_FIELDS)}

    def get_expected_fields(self):
        from src.connectors.genesys.schema import EXPECTED_FIELDS
        return EXPECTED_FIELDS

    def get_health(self):
        return ConnectorHealth(status="HEALTHY")
