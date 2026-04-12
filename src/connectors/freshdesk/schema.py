"""
Expected schema for Freshdesk Ticket API responses.
"""
from __future__ import annotations

import hashlib

EXPECTED_FIELDS = {
    "id", "subject", "description", "status", "priority", "source",
    "type", "responder_id", "group_id", "created_at", "updated_at",
    "due_by", "fr_due_by", "tags", "requester_id",
}

EXPECTED_SCHEMA_HASH = hashlib.sha256(
    "|".join(sorted(EXPECTED_FIELDS)).encode()
).hexdigest()[:16]

PII_FIELDS = {
    "description",
    "requester_id",
}

REQUIRED_FIELDS = {
    "id",
    "subject",
    "created_at",
    "status",
}
