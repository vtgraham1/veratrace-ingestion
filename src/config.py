"""
Service configuration — all from environment variables.
No secrets in code. No defaults for credentials.
"""
import os


# ── Database ───────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# ── Control Plane ──────────────────────────────────────────────────────────────

CONTROL_PLANE_URL = os.environ.get(
    "CONTROL_PLANE_URL", "https://veratrace-control-plane.onrender.com"
)

# ── Vendor API Keys (per-connector, loaded on demand) ──────────────────────────

# AWS credentials for Amazon Connect (AssumeRole uses customer's roleArn)
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# ── Service Settings ───────────────────────────────────────────────────────────

# Rate limit ceiling: never consume more than this % of vendor's stated limit
RATE_LIMIT_CEILING_PCT = int(os.environ.get("RATE_LIMIT_CEILING_PCT", "70"))

# DLQ alert threshold: page on-call after this many failures per hour
DLQ_ALERT_THRESHOLD = int(os.environ.get("DLQ_ALERT_THRESHOLD", "100"))

# Sync schedule: minutes between incremental syncs during business hours
SYNC_INTERVAL_MINUTES = int(os.environ.get("SYNC_INTERVAL_MINUTES", "15"))

# PII encryption key (AES-256-GCM — should come from KMS in production)
PII_ENCRYPTION_KEY = os.environ.get("PII_ENCRYPTION_KEY", "")

# ── Feature Flags ──────────────────────────────────────────────────────────────

ENABLE_WEBHOOK_RECEIVER = os.environ.get("ENABLE_WEBHOOK_RECEIVER", "true") == "true"
ENABLE_SCHEMA_DRIFT_ALERTS = os.environ.get("ENABLE_SCHEMA_DRIFT_ALERTS", "true") == "true"
