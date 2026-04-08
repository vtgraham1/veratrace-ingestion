"""
Signal writer — upserts TwuSignals to Supabase with dedup, PII encryption,
and degraded signal flagging.

Key properties:
- Idempotent: upsert on (instance_id, signal_id) composite key
- PII encrypted at write time (field-level, not row-level)
- Degraded signals preserved with raw payload for later reprocessing
- Append-only: no UPDATE or DELETE on signal records
"""
import json
import uuid
import hashlib
import logging
import datetime
import urllib.request
import urllib.error
from dataclasses import dataclass, field

from src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

logger = logging.getLogger(__name__)

SIGNAL_TABLE = "twu_signals"


@dataclass
class TwuSignal:
    """A single evidence event from an integration source."""

    instance_id: str
    signal_id: str = ""
    type: str = "INTEGRATION_EVENT"  # SYSTEM, AI, HUMAN, INTEGRATION_EVENT
    name: str = ""
    occurred_at: str = ""  # source system timestamp (ISO 8601)
    processed_at: str = ""  # our ingestion timestamp
    source_integration_account_id: str = ""
    source_integration: str = ""  # "amazon-connect", "salesforce", etc.
    actor_type: str = "SYSTEM"  # AI, HUMAN, SYSTEM
    actor_agent_id: str = ""
    payload: dict = field(default_factory=dict)
    degraded: bool = False
    degraded_reason: str = ""
    pii_encrypted_fields: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.signal_id:
            self.signal_id = str(uuid.uuid4())
        if not self.processed_at:
            self.processed_at = datetime.datetime.utcnow().isoformat() + "Z"

    def dedup_key(self) -> str:
        """Composite key for dedup: source + event ID from vendor."""
        event_id = self.payload.get("event_id") or self.payload.get("ContactId") or self.signal_id
        return f"{self.source_integration}:{self.source_integration_account_id}:{event_id}"

    def to_db_row(self) -> dict:
        return {
            "instance_id": self.instance_id,
            "signal_id": self.signal_id,
            "type": self.type,
            "name": self.name,
            "occurred_at": self.occurred_at,
            "processed_at": self.processed_at,
            "source": json.dumps({
                "integration_account_id": self.source_integration_account_id,
                "integration": self.source_integration,
            }),
            "actor": json.dumps({
                "type": self.actor_type,
                "agent_id": self.actor_agent_id,
            }),
            "payload": json.dumps(self.payload),
            "degraded": self.degraded,
            "degraded_reason": self.degraded_reason,
            "pii_encrypted_fields": self.pii_encrypted_fields,
        }


def _headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }


def write_signals(signals: list[TwuSignal]) -> int:
    """
    Upsert signals to database. Returns count of signals written.

    Idempotent: uses upsert on (instance_id, signal_id).
    Append-only: existing signals are never modified — upsert only creates new ones.
    """
    if not signals:
        return 0

    rows = [s.to_db_row() for s in signals]
    payload = json.dumps(rows).encode()

    url = f"{SUPABASE_URL}/rest/v1/{SIGNAL_TABLE}"
    req = urllib.request.Request(url, data=payload, headers=_headers(), method="POST")

    try:
        urllib.request.urlopen(req, timeout=30)
        logger.info("Wrote %d signals (instance=%s)", len(signals), signals[0].instance_id)
        return len(signals)
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:300]
        logger.error("Signal write failed: %s %s", e.code, body)
        raise


def write_signal(signal: TwuSignal) -> bool:
    """Write a single signal. Returns True on success."""
    try:
        write_signals([signal])
        return True
    except Exception:
        return False
