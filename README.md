# Veratrace Ingestion Service

Standalone service that connects enterprise systems (Amazon Connect, Salesforce, Zendesk, AI model APIs) and produces TwuSignals for the TWU Compiler.

## Architecture

This service owns **vendor API → signal in database**. Joey's control plane owns **signal in database → sealed TWU**.

The boundary is the `twu_signals` table and the `POST /instances/{id}/tasks` trigger.

## Connectors

| Connector | Auth | Real-Time | Polling | Status |
|-----------|------|-----------|---------|--------|
| Amazon Connect | IAM AssumeRole | Kinesis stream | SearchContacts API | In progress |
| Salesforce | OAuth 2.0 | Change Data Capture | REST / Bulk API 2.0 | Planned |
| Zendesk | OAuth 2.0 / API token | Webhooks | Incremental Export | Planned |
| OpenAI / Anthropic | Proxy mode | Request interception | N/A | Planned |

## Shared Runtime

Every connector uses the same infrastructure:

- **Rate limiter** — token bucket per endpoint, 70% of vendor ceiling
- **Retry engine** — decorrelated jitter, circuit breaker at 50% error rate
- **Cursor manager** — per-stream checkpoint persistence
- **Region router** — parse region from ARN/endpoint, enforce data residency
- **Schema validator** — drift detection, degraded signal flagging
- **Signal writer** — idempotent upsert, PII encryption
- **Task trigger** — fire TWU Compiler via SQS after each sync batch

## Running Locally

```bash
export SUPABASE_URL="https://..."
export SUPABASE_SERVICE_ROLE_KEY="..."
export CONTROL_PLANE_URL="https://veratrace-control-plane.onrender.com"

python -m src.main
```

## Testing

```bash
pip install -e ".[dev]"
pytest
```
