"""
Amazon Connect connector — pulls Contact Trace Records (CTRs) and transforms
them into TwuSignals.

Supports:
- Tier 2: SearchContacts API polling (2 req/sec, fallback)
- Backfill: SearchContacts with date range (24-month CTR retention)

Rate limits: 2 req/sec default per account per region.
Region: parsed from instance ARN position 4.
Multi-region: separate integration account per region.
"""
import logging
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, NoCredentialsError

from src.connectors.base import (
    BaseConnector, ConnectionTestResult, SyncResult, ConnectorHealth,
)
from src.connectors.amazon_connect.signal_mapper import ctr_to_signals
from src.connectors.amazon_connect.schema import EXPECTED_CTR_FIELDS, EXPECTED_SCHEMA_HASH
from src.runtime.region_router import detect_region_from_arn
from src.runtime.schema_validator import detect_drift, is_breaking
from src.runtime.retry_engine import with_retry, CircuitBreaker

logger = logging.getLogger(__name__)

# Connect API: 2 req/sec per account per region. We use 70% = 1.4 req/sec.
# boto3 has built-in retry, but we add our own rate control between pages.
SECONDS_BETWEEN_API_CALLS = 0.72  # ~1.4 req/sec
BACKFILL_SECONDS_BETWEEN_CALLS = 1.5  # 50% ceiling for backfills
MAX_RESULTS_PER_PAGE = 100


class AmazonConnectConnector(BaseConnector):
    """
    Amazon Connect integration connector.

    Credentials expected:
        credentials["roleArn"] — IAM role ARN to assume for API access
    External identity:
        external_identity["tenantId"] — Connect instance ARN
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._instance_arn = self.external_identity.get("tenantId", "")
        self._region = self.detect_region()
        self._instance_id_from_arn = self._parse_instance_id()
        self._circuit_breaker = CircuitBreaker()
        self._schema_hash = EXPECTED_SCHEMA_HASH
        self._assumed_creds = None
        self._assumed_creds_expiry = 0

    def _parse_instance_id(self):
        """Extract the Connect instance ID from the ARN."""
        parts = self._instance_arn.split("/")
        return parts[-1] if "/" in self._instance_arn else ""

    # ── Setup ──────────────────────────────────────────────────────────────

    def validate_credentials(self):
        role_arn = self.credentials.get("roleArn", "")
        if not role_arn.startswith("arn:aws:iam:"):
            return False
        if not self._instance_arn.startswith("arn:aws:connect:"):
            return False
        return True

    def test_connection(self):
        """Assume the role and call DescribeInstance to verify access."""
        try:
            client = self._get_connect_client()
            resp = client.describe_instance(InstanceId=self._instance_id_from_arn)
            instance = resp.get("Instance", {})
            alias = instance.get("InstanceAlias", "")
            status = instance.get("InstanceStatus", "")
            return ConnectionTestResult(
                success=True,
                message=f"Connected to '{alias}' (status: {status})",
                region=self._region,
                details={
                    "instance_alias": alias,
                    "instance_status": status,
                    "instance_id": instance.get("Id", ""),
                },
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            msg = e.response["Error"]["Message"]
            return ConnectionTestResult(
                success=False,
                message=f"{code}: {msg}",
                region=self._region,
            )
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"Connection failed: {str(e)[:200]}",
                region=self._region,
            )

    def detect_region(self):
        return detect_region_from_arn(self._instance_arn) or "us-east-1"

    # ── AWS Client Management ──────────────────────────────────────────────

    def _assume_role(self):
        """
        Assume the customer's IAM role via STS.
        Caches credentials until 5 minutes before expiry.
        """
        now = time.time()
        if self._assumed_creds and self._assumed_creds_expiry > now + 300:
            return self._assumed_creds

        role_arn = self.credentials.get("roleArn", "")
        external_id = self.credentials.get("externalId", "")
        session_name = f"veratrace-{self.integration_account_id[:8]}"

        logger.info("Assuming role %s for region %s", role_arn[:60], self._region)

        sts = boto3.client("sts", region_name=self._region)
        assume_params = {
            "RoleArn": role_arn,
            "RoleSessionName": session_name,
            "DurationSeconds": 3600,
        }
        # External ID prevents confused deputy attacks — required by our CloudFormation template
        if external_id:
            assume_params["ExternalId"] = external_id

        resp = sts.assume_role(**assume_params)
        creds = resp["Credentials"]
        self._assumed_creds = {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
        self._assumed_creds_expiry = creds["Expiration"].timestamp()
        return self._assumed_creds

    def _get_connect_client(self):
        """Get a boto3 Connect client using assumed role credentials."""
        creds = self._assume_role()
        return boto3.client(
            "connect",
            region_name=self._region,
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            aws_session_token=creds["aws_session_token"],
            config=BotoConfig(
                retries={"max_attempts": 2, "mode": "adaptive"},
                connect_timeout=10,
                read_timeout=30,
            ),
        )

    # ── Sync ───────────────────────────────────────────────────────────────

    def sync_incremental(self, cursor=None):
        """
        Fetch contacts since the last cursor (ISO timestamp).
        Uses SearchContacts API with time range + pagination.
        Rate limited to 70% of 2 req/sec = 1.4 req/sec.
        """
        if cursor:
            start_time = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
        else:
            start_time = datetime.now(timezone.utc) - timedelta(hours=24)

        end_time = datetime.now(timezone.utc)

        logger.info(
            "Syncing Connect contacts: %s → %s (instance=%s, region=%s)",
            start_time.isoformat()[:19], end_time.isoformat()[:19],
            self._instance_id_from_arn[:8], self._region,
        )

        return self._search_contacts(start_time, end_time, SECONDS_BETWEEN_API_CALLS)

    def sync_backfill(self, start_date=None):
        """
        Backfill historical contacts at reduced rate (50% ceiling).
        Connect retains CTRs for 24 months.
        """
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=30)

        end_time = datetime.now(timezone.utc)
        logger.info("Backfilling Connect contacts from %s", start_date.isoformat()[:19])

        return self._search_contacts(start_date, end_time, BACKFILL_SECONDS_BETWEEN_CALLS)

    def _search_contacts(self, start_time, end_time, rate_delay):
        """
        Core search + paginate + map logic.
        Handles pagination, rate limiting, schema drift, and signal mapping.
        """
        client = self._get_connect_client()
        signals = []
        api_calls = 0
        next_token = None
        drift_checked = False

        while True:
            # Rate control — sleep between pages
            if api_calls > 0:
                time.sleep(rate_delay)

            # Build request
            search_params = {
                "InstanceId": self._instance_id_from_arn,
                "TimeRange": {
                    "Type": "INITIATION_TIMESTAMP",
                    "StartTime": start_time,
                    "EndTime": end_time,
                },
                "MaxResults": MAX_RESULTS_PER_PAGE,
                "Sort": {
                    "FieldName": "INITIATION_TIMESTAMP",
                    "Order": "ASCENDING",
                },
            }
            if next_token:
                search_params["NextToken"] = next_token

            try:
                def _do_search():
                    return client.search_contacts(**search_params)

                resp = with_retry(
                    _do_search,
                    max_retries=3,
                    circuit_breaker=self._circuit_breaker,
                )
                api_calls += 1
            except Exception as e:
                logger.error("SearchContacts failed after retries: %s", e)
                break

            contacts = resp.get("Contacts", [])

            # Schema drift check on first page
            if contacts and not drift_checked:
                first_contact = contacts[0]
                current_hash, drifts = detect_drift(
                    first_contact, self._schema_hash, EXPECTED_CTR_FIELDS
                )
                if drifts:
                    severity = "BREAKING" if is_breaking(drifts) else "non-breaking"
                    logger.warning(
                        "Schema drift detected (%s): %d field(s) changed",
                        severity, len(drifts),
                    )
                    self._schema_hash = current_hash
                drift_checked = True

            # Map contacts to signals
            for contact in contacts:
                mapped = ctr_to_signals(
                    contact,
                    instance_id=self.instance_id,
                    integration_account_id=self.integration_account_id,
                )
                signals.extend(mapped)

            # Pagination
            next_token = resp.get("NextToken")
            if not next_token or not contacts:
                break

            logger.info(
                "Fetched page: %d contacts, %d signals total, continuing...",
                len(contacts), len(signals),
            )

        new_cursor = end_time.isoformat()
        logger.info(
            "Sync complete: %d contacts → %d signals (%d API calls)",
            len(signals) // 2, len(signals), api_calls,  # rough: ~2-3 signals per contact
        )

        return SyncResult(
            signals=signals,
            cursor=new_cursor,
            has_more=False,
            records_fetched=api_calls * MAX_RESULTS_PER_PAGE,  # approximate
            api_calls_made=api_calls,
        )

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_expected_schema(self):
        return {"hash": EXPECTED_SCHEMA_HASH, "fields": list(EXPECTED_CTR_FIELDS)}

    def get_expected_fields(self):
        return EXPECTED_CTR_FIELDS

    # ── Health ─────────────────────────────────────────────────────────────

    def get_health(self):
        if self._circuit_breaker.is_open():
            return ConnectorHealth(status="FAILED", last_error="Circuit breaker open")
        return ConnectorHealth(status="HEALTHY")
