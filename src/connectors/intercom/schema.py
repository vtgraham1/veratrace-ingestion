"""Expected schema for Intercom conversation API responses."""
from __future__ import annotations
import hashlib

EXPECTED_CONVERSATION_FIELDS = {
    "id", "created_at", "updated_at", "state", "open",
    "source", "contacts", "teammates", "conversation_parts",
    "ai_agent", "custom_attributes", "priority",
    "waiting_since", "snoozed_until", "read", "title",
}

EXPECTED_SCHEMA_HASH = hashlib.sha256(
    "|".join(sorted(EXPECTED_CONVERSATION_FIELDS)).encode()
).hexdigest()[:16]

PII_FIELDS = {"contacts", "custom_attributes"}
REQUIRED_FIELDS = {"id", "created_at", "state"}
