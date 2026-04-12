"""
Expected schema for Genesys Cloud Analytics conversation responses.
"""
from __future__ import annotations

import hashlib

EXPECTED_FIELDS = {
    "conversationId", "conversationStart", "conversationEnd",
    "participants", "divisionIds", "originatingDirection",
}

EXPECTED_SCHEMA_HASH = hashlib.sha256(
    "|".join(sorted(EXPECTED_FIELDS)).encode()
).hexdigest()[:16]

PII_FIELDS = {
    "externalContactId",
    "externalOrganizationId",
}

REQUIRED_FIELDS = {
    "conversationId",
    "conversationStart",
    "participants",
}
