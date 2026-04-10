"""
TODO(connector): Your Platform connector.

Reference: src/connectors/amazon_connect/connector.py

Implements:
  - validate_credentials()  — check credential format
  - test_connection()       — make a live API call to verify access
  - detect_region()         — determine API region from credentials
  - sync_incremental()      — fetch records since last cursor
  - sync_backfill()         — fetch historical records
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from src.connectors.base import (
    BaseConnector, ConnectionTestResult, SyncResult, ConnectorHealth,
)
from src.connectors._template.signal_mapper import map_to_signals
from src.connectors._template.schema import EXPECTED_FIELDS, REQUIRED_FIELDS

logger = logging.getLogger(__name__)


class TemplateConnector(BaseConnector):
    """
    TODO(connector): Rename this class to match your platform.

    Credentials expected:
        credentials["apiKey"] or credentials["accessToken"] — TODO: define
    External identity:
        external_identity["tenantId"] — TODO: your platform's tenant identifier
    """

    # TODO(connector): Override with your platform's limits
    CONFIG = {
        **BaseConnector.CONFIG,
        "rate_limit_rps": 10.0,           # TODO: vendor API rate limit
        "backfill_days_default": 30,       # TODO: how far back to backfill
        "max_results_per_page": 100,       # TODO: vendor page size
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # TODO(connector): Extract platform-specific config from credentials/identity
        self._tenant_id = self.external_identity.get("tenantId", "")

    def validate_credentials(self):
        # TODO(connector): Check credential format (not a live call)
        return bool(self.credentials.get("apiKey"))

    def test_connection(self):
        # TODO(connector): Make a lightweight API call to verify credentials
        # Example: GET /api/v1/me or similar health endpoint
        try:
            # client = self._get_client()
            # resp = client.get("/me")
            return ConnectionTestResult(success=True, message="Connected")
        except Exception as e:
            return ConnectionTestResult(success=False, message=str(e)[:200])

    def detect_region(self):
        # TODO(connector): Parse region from credentials/identity if applicable
        return "us"

    def sync_incremental(self, cursor=None):
        """
        TODO(connector): Fetch records modified since the last cursor.

        Pattern:
          1. Build API request with cursor (timestamp, ID, or page token)
          2. Paginate through results
          3. Map each result to TwuSignals via signal_mapper
          4. Return SyncResult with new cursor
        """
        if cursor:
            # TODO: Parse cursor (ISO timestamp, numeric ID, etc.)
            pass
        else:
            # No cursor — default to recent window
            pass

        signals = []
        # TODO: Paginate, rate limit, map to signals
        # for page in self._paginate(start_time, end_time):
        #     for record in page:
        #         signals.extend(map_to_signals(record, self.instance_id, self.integration_account_id))

        new_cursor = datetime.now(timezone.utc).isoformat()
        return SyncResult(signals=signals, cursor=new_cursor)

    def sync_backfill(self, start_date=None):
        # TODO(connector): Fetch all historical records from start_date
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=self.CONFIG["backfill_days_default"])
        return self.sync_incremental(cursor=start_date.isoformat())

    def get_expected_schema(self):
        return {"fields": list(EXPECTED_FIELDS)}

    def get_expected_fields(self):
        return EXPECTED_FIELDS

    def get_health(self):
        return ConnectorHealth(status="HEALTHY")
