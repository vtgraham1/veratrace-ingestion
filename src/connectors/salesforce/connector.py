"""
Salesforce connector — pulls Cases, Opportunities, and Tasks via REST API
and transforms them into TwuSignals.

Supports:
- Incremental sync via SystemModstamp cursor (SOQL query)
- Backfill via CreatedDate range
- OAuth 2.0 token refresh

Rate limits: 5 req/sec, 10K requests/day (Professional Edition).
"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

from src.connectors.base import (
    BaseConnector, ConnectionTestResult, SyncResult, ConnectorHealth,
)
from src.connectors.salesforce.signal_mapper import map_records_to_signals, map_case_history_to_signals
from src.connectors.salesforce.schema import REQUIRED_FIELDS

logger = logging.getLogger(__name__)

SF_API_VERSION = "v60.0"


class SalesforceConnector(BaseConnector):
    """
    Salesforce integration connector.

    Credentials expected:
        credentials["access_token"]  — OAuth bearer token
        credentials["refresh_token"] — for token renewal
        credentials["instance_url"]  — e.g., https://na1.salesforce.com
        credentials["client_id"]     — OAuth Connected App client ID
        credentials["client_secret"] — OAuth Connected App secret
    External identity:
        external_identity["tenantId"] — Salesforce Org ID
    """

    CONFIG = {
        **BaseConnector.CONFIG,
        "rate_limit_rps": 5.0,
        "rate_ceiling_pct": 70,
        "backfill_days_default": 30,
        "max_results_per_page": 2000,
        "cursor_format": "iso8601",
        "sync_objects": ["Case", "Opportunity"],
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._instance_url = self.credentials.get("instance_url", "").rstrip("/")
        self._access_token = self.credentials.get("access_token", "")
        self._refresh_token = self.credentials.get("refresh_token", "")
        self._client_id = self.credentials.get("client_id", "")
        self._client_secret = self.credentials.get("client_secret", "")
        self._token_lock = threading.Lock()

        effective_rps = self.CONFIG["rate_limit_rps"] * self.CONFIG["rate_ceiling_pct"] / 100
        self._sync_delay = 1.0 / effective_rps if effective_rps > 0 else 1.0

    # ── Setup ──────────────────────────────────────────────────────────────

    def validate_credentials(self):
        return bool(self._instance_url and (self._access_token or self._refresh_token))

    def test_connection(self):
        try:
            result = self._soql_query("SELECT count() FROM Case LIMIT 1")
            count = result.get("totalSize", 0)
            return ConnectionTestResult(
                success=True,
                message=f"Connected — {count} cases accessible",
                region=self.detect_region(),
            )
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"Connection failed: {str(e)[:200]}",
                region=self.detect_region(),
            )

    def detect_region(self):
        # Parse from instance_url: na1 → us, eu5 → eu, ap4 → apac
        if not self._instance_url:
            return "unknown"
        host = self._instance_url.replace("https://", "").split(".")[0]
        if host.startswith("na") or host.startswith("us"):
            return "us"
        if host.startswith("eu"):
            return "eu"
        if host.startswith("ap"):
            return "apac"
        return host

    # ── Sync ───────────────────────────────────────────────────────────────

    def sync_incremental(self, cursor=None):
        if cursor:
            start_time = cursor
        else:
            start_time = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

        all_signals = []
        api_calls = 0

        for obj_type in self.CONFIG["sync_objects"]:
            signals, calls = self._sync_object(obj_type, start_time)
            all_signals.extend(signals)
            api_calls += calls

        new_cursor = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(
            "Sync complete: %d signals from %d API calls across %s",
            len(all_signals), api_calls, self.CONFIG["sync_objects"],
        )

        return SyncResult(
            signals=all_signals,
            cursor=new_cursor,
            has_more=False,
            records_fetched=len(all_signals),
            api_calls_made=api_calls,
        )

    def sync_backfill(self, start_date=None):
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=self.CONFIG["backfill_days_default"])
        cursor = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self.sync_incremental(cursor=cursor)

    def _sync_object(self, obj_type, since_timestamp):
        """Fetch records of a given type modified since timestamp."""
        fields = self._get_fields_for_object(obj_type)
        soql = (
            f"SELECT {', '.join(fields)} FROM {obj_type} "
            f"WHERE SystemModstamp > {since_timestamp} "
            f"ORDER BY SystemModstamp ASC"
        )

        signals = []
        api_calls = 0
        next_url = None

        while True:
            if api_calls > 0:
                time.sleep(self._sync_delay)

            try:
                if next_url:
                    result = self._api_get(next_url)
                else:
                    result = self._soql_query(soql)
                api_calls += 1
            except Exception as e:
                logger.error("SOQL query failed for %s: %s", obj_type, e)
                break

            records = result.get("records", [])
            for record in records:
                mapped = map_records_to_signals(
                    record, obj_type,
                    self.instance_id, self.integration_account_id,
                )
                signals.extend(mapped)

            # For Cases, also fetch CaseHistory for attribution
            if obj_type == "Case" and records:
                case_ids = [r["Id"] for r in records if r.get("Id")]
                history_signals, hist_calls = self._fetch_case_history(case_ids, since_timestamp)
                signals.extend(history_signals)
                api_calls += hist_calls

            next_url = result.get("nextRecordsUrl")
            if not next_url or not records:
                break

            logger.info("Fetched page: %d %s records, %d signals total", len(records), obj_type, len(signals))

        return signals, api_calls

    def _fetch_case_history(self, case_ids, since_timestamp):
        """Fetch CaseHistory with actor attribution for a batch of Cases."""
        if not case_ids:
            return [], 0

        ids_str = ", ".join(f"'{cid}'" for cid in case_ids)
        soql = (
            f"SELECT CaseId, CreatedById, CreatedBy.UserType, CreatedBy.Name, "
            f"CreatedDate, Field, OldValue, NewValue "
            f"FROM CaseHistory "
            f"WHERE CaseId IN ({ids_str}) "
            f"AND CreatedDate > {since_timestamp} "
            f"ORDER BY CreatedDate ASC"
        )

        signals = []
        api_calls = 0

        try:
            time.sleep(self._sync_delay)
            result = self._soql_query(soql)
            api_calls += 1

            history_records = result.get("records", [])
            if history_records:
                signals = map_case_history_to_signals(
                    history_records, self.instance_id, self.integration_account_id,
                )
                logger.info("CaseHistory: %d changes → %d signals", len(history_records), len(signals))
        except Exception as e:
            logger.warning("CaseHistory fetch failed (non-fatal): %s", str(e)[:100])

        return signals, api_calls

    def _get_fields_for_object(self, obj_type):
        """Return SOQL field list for an object type."""
        base = ["Id", "CreatedDate", "SystemModstamp", "OwnerId"]
        if obj_type == "Case":
            return base + [
                "Subject", "Status", "Priority", "Origin", "IsClosed",
                "Description", "ClosedDate", "ContactId",
            ]
        elif obj_type == "Opportunity":
            return base + [
                "Name", "StageName", "Amount", "CloseDate",
                "IsClosed", "IsWon", "Probability",
            ]
        return base

    # ── API Client ─────────────────────────────────────────────────────────

    def _soql_query(self, soql):
        """Execute a SOQL query and return results."""
        import urllib.parse
        encoded = urllib.parse.quote(soql, safe="")
        url = f"{self._instance_url}/services/data/{SF_API_VERSION}/query?q={encoded}"
        return self._api_get(url)

    def _api_get(self, url):
        """Make an authenticated GET request to the Salesforce REST API."""
        if not url.startswith("http"):
            url = f"{self._instance_url}{url}"

        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Accept", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                # Check rate limit headers
                limit_info = resp.headers.get("Sforce-Limit-Info", "")
                if limit_info:
                    logger.debug("SF API usage: %s", limit_info)
                return data
        except urllib.error.HTTPError as e:
            if e.code == 401:
                # Token expired — try refresh
                logger.info("Token expired, attempting refresh...")
                try:
                    self._refresh_access_token()
                except RuntimeError as refresh_err:
                    logger.error("Token refresh unavailable: %s — sync will fail until re-authed", refresh_err)
                    raise e from refresh_err
                # Retry once with new token
                req.remove_header("Authorization")
                req.add_header("Authorization", f"Bearer {self._access_token}")
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read())
            raise

    def _refresh_access_token(self):
        """Refresh the OAuth access token. Thread-safe.

        Requires client_id and client_secret in credentials. If the control
        plane provided tokens without these (OAuth callback flow), refresh
        is not possible — the sync will work with the initial access token
        but fail after it expires (~1-2 hours).
        """
        with self._token_lock:
            if not self._refresh_token:
                raise RuntimeError("No refresh token available")
            if not self._client_id or not self._client_secret:
                raise RuntimeError(
                    "Token refresh requires client_id and client_secret in credentials. "
                    "Ask the control plane to include these in auth_credentials, or add a "
                    "server-side refresh endpoint."
                )

            data = urllib.parse.urlencode({
                "grant_type": "refresh_token",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
            }).encode()

            req = urllib.request.Request(
                f"{self._instance_url}/services/oauth2/token",
                data=data,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())

            self._access_token = result["access_token"]
            if "instance_url" in result:
                self._instance_url = result["instance_url"].rstrip("/")
            logger.info("Token refreshed successfully")

    def _api_post(self, path, body):
        """Make an authenticated POST request."""
        url = f"{self._instance_url}/services/data/{SF_API_VERSION}{path}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_expected_schema(self):
        from src.connectors.salesforce.schema import EXPECTED_CASE_FIELDS
        return {"fields": list(EXPECTED_CASE_FIELDS)}

    def get_expected_fields(self):
        from src.connectors.salesforce.schema import EXPECTED_CASE_FIELDS
        return EXPECTED_CASE_FIELDS

    # ── Health ─────────────────────────────────────────────────────────────

    def get_health(self):
        return ConnectorHealth(status="HEALTHY")
