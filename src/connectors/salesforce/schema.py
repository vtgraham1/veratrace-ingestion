"""
Expected schema for Salesforce API responses.
Used for schema drift detection.
"""
from __future__ import annotations

import hashlib

EXPECTED_CASE_FIELDS = {
    "Id", "CreatedDate", "SystemModstamp", "Subject", "Status",
    "Priority", "Origin", "OwnerId", "IsClosed", "Description",
    "ClosedDate", "ContactId", "AccountId",
    # Custom AI fields (may not exist in all orgs)
    "AI_Handled__c", "AI_Agent_Name__c", "AI_Confidence__c",
}

EXPECTED_OPP_FIELDS = {
    "Id", "CreatedDate", "SystemModstamp", "Name", "StageName",
    "Amount", "CloseDate", "IsClosed", "IsWon", "OwnerId",
    "Probability", "AccountId",
}

EXPECTED_SCHEMA_HASH = hashlib.sha256(
    "|".join(sorted(EXPECTED_CASE_FIELDS | EXPECTED_OPP_FIELDS)).encode()
).hexdigest()[:16]

PII_FIELDS = {
    "Description",
    "ContactEmail",
    "ContactPhone",
    "SuppliedEmail",
    "SuppliedPhone",
}

REQUIRED_FIELDS = {
    "Id",
    "CreatedDate",
    "SystemModstamp",
}
