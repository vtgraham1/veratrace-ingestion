"""
TODO(connector): Expected schema for your platform's API responses.

Reference: src/connectors/amazon_connect/schema.py

Used for schema drift detection. If the vendor changes their API
response format, the schema validator flags it before signals break.
"""
from __future__ import annotations

import hashlib

# TODO(connector): List all fields in the vendor's API response
EXPECTED_FIELDS = {
    "id",
    "created_at",
    "updated_at",
    # TODO: Add all expected top-level fields from the vendor API
}

# Compute hash from field names — changes when schema changes
EXPECTED_SCHEMA_HASH = hashlib.sha256(
    "|".join(sorted(EXPECTED_FIELDS)).encode()
).hexdigest()[:16]

# TODO(connector): Fields that contain PII — must be encrypted at write time
PII_FIELDS = {
    # "customer_email",
    # "phone_number",
}

# TODO(connector): Fields required for a valid signal (missing any = degraded)
REQUIRED_FIELDS = {
    "id",
    "created_at",
}
