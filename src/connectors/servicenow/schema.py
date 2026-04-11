"""
Expected schema for ServiceNow Incident API responses.
Used for schema drift detection and degraded signal flagging.
"""
from __future__ import annotations

import hashlib

EXPECTED_FIELDS = {
    "sys_id", "number", "short_description", "description",
    "state", "priority", "urgency", "impact",
    "category", "subcategory",
    "assigned_to", "assignment_group",
    "opened_by", "opened_at",
    "resolved_by", "resolved_at",
    "closed_at", "close_code", "close_notes",
    "sys_created_on", "sys_updated_on",
    "contact_type", "caller_id",
}

EXPECTED_SCHEMA_HASH = hashlib.sha256(
    "|".join(sorted(EXPECTED_FIELDS)).encode()
).hexdigest()[:16]

PII_FIELDS = {
    "description",
    "close_notes",
    "caller_id",
}

REQUIRED_FIELDS = {
    "sys_id",
    "number",
    "sys_created_on",
    "sys_updated_on",
}
