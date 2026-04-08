"""
Base connector interface — every integration connector implements this.

Provides a consistent contract for setup, sync, webhooks, schema validation,
and health reporting. The runtime infrastructure (rate limiter, retry engine,
cursor manager) wraps these methods transparently.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from src.runtime.signal_writer import TwuSignal


@dataclass
class ConnectionTestResult:
    success: bool
    message: str
    region: str = ""
    details: dict = field(default_factory=dict)


@dataclass
class SyncResult:
    signals: list[TwuSignal]
    cursor: str | None = None  # next cursor for incremental sync
    has_more: bool = False  # True if more pages available
    records_fetched: int = 0
    records_skipped: int = 0
    api_calls_made: int = 0


@dataclass
class QuotaUsage:
    limit: int  # vendor's stated limit
    consumed: int  # our consumption in current window
    remaining: int
    window: str = "daily"  # "daily", "per_minute", etc.
    ceiling_pct: int = 70


@dataclass
class ConnectorHealth:
    status: str  # HEALTHY, DEGRADED, FAILED, PENDING_SYNC
    last_sync_at: str | None = None
    last_error: str | None = None
    records_synced_last_run: int = 0
    schema_drift_detected: bool = False


class BaseConnector(ABC):
    """
    Abstract base class for all integration connectors.

    Subclasses must implement the abstract methods.
    Optional methods (webhooks) have default no-op implementations.
    """

    def __init__(self, instance_id: str, integration_account_id: str, credentials: dict, external_identity: dict):
        self.instance_id = instance_id
        self.integration_account_id = integration_account_id
        self.credentials = credentials
        self.external_identity = external_identity

    # ── Setup ──────────────────────────────────────────────────────────────

    @abstractmethod
    def validate_credentials(self) -> bool:
        """Validate that stored credentials are syntactically correct."""
        ...

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """Make a live API call to verify credentials work."""
        ...

    @abstractmethod
    def detect_region(self) -> str:
        """Determine the API region from credentials/identity."""
        ...

    # ── Sync ───────────────────────────────────────────────────────────────

    @abstractmethod
    def sync_incremental(self, cursor: str | None = None) -> SyncResult:
        """Fetch records modified since the last cursor."""
        ...

    @abstractmethod
    def sync_backfill(self, start_date: datetime | None = None) -> SyncResult:
        """Fetch all historical records from start_date (or earliest available)."""
        ...

    # ── Webhooks (optional) ────────────────────────────────────────────────

    def validate_webhook_signature(self, payload: bytes, signature: str) -> bool:
        """Verify webhook signature. Override for connectors that support webhooks."""
        return False

    def process_webhook(self, event: dict) -> list[TwuSignal]:
        """Transform a webhook event into signals. Override for webhook connectors."""
        return []

    # ── Schema ─────────────────────────────────────────────────────────────

    @abstractmethod
    def get_expected_schema(self) -> dict:
        """Return the expected API response schema (field names + types)."""
        ...

    @abstractmethod
    def get_expected_fields(self) -> set[str]:
        """Return the set of expected top-level field names."""
        ...

    # ── Health ─────────────────────────────────────────────────────────────

    def get_quota_usage(self) -> QuotaUsage | None:
        """Return current API quota consumption. Override per vendor."""
        return None

    def get_health(self) -> ConnectorHealth:
        """Return current connector health status."""
        return ConnectorHealth(status="PENDING_SYNC")
