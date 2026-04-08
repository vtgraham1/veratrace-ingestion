# Veratrace Ingestion Service

Standalone service that connects enterprise systems (Amazon Connect, Salesforce, Zendesk, AI model APIs) and produces TwuSignals for the TWU Compiler.

## Architecture

This service owns **vendor API → signal in database**. Joey's control plane owns **signal in database → sealed TWU**.

The boundary is the `twu_signals` table and the `POST /instances/{id}/tasks` trigger.

## Connectors

| Connector | Auth | Real-Time | Polling | Status |
|-----------|------|-----------|---------|--------|
| Amazon Connect | IAM AssumeRole + ExternalId | Kinesis stream | SearchContacts API | **Live** |
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

## HTTP API

Runs on port 8090. Proxied via Caddy at `https://ingestion.veratrace.ai`.

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/health` | GET | None | Liveness check |
| `/sync` | POST | `X-API-Key` | Trigger immediate sync for an integration account |
| `/test-connection` | POST | `X-API-Key` | Test AWS credentials (STS AssumeRole + DescribeInstance) |

## Deployment

Deployed to DigitalOcean (`159.203.133.76`) via the **veraagents** repo's GitHub Actions workflow. This repo's CI runs tests only — deploy is handled by `veraagents/.github/workflows/deploy.yml`.

- Cron: every 15 min weekdays (UTC 14:00–02:00)
- API: background process via nohup, restarted on each deploy
- TLS: Caddy reverse proxy with auto Let's Encrypt cert
- Firewall: UFW allows ports 22 (SSH), 80/443 (Caddy), 8090 (API)

## Synthetic Data

Generate realistic demo data without needing real vendor API access:

```bash
# Generate signals (dry run)
python -m synthetic.generator --scenario bpo_contact_center --signals 200

# Seed directly into RDS (requires AWS credentials for Secrets Manager)
python -m synthetic.seed_rds --instance-id UUID --contacts 150
```

Scenarios: `bpo_contact_center` (40% AI), `enterprise_cx` (70% AI), `hybrid_outsourced` (45% AI).

## Running Locally

```bash
export SUPABASE_URL="https://..."
export SUPABASE_SERVICE_ROLE_KEY="..."
export CONTROL_PLANE_URL="https://veratrace-control-plane.onrender.com"
export INGESTION_API_KEY="your-key"  # optional, skipped if empty

python -m src.main          # start HTTP API on :8090
python -m src.main --port 9090  # custom port
```

## Testing

```bash
pip install boto3 pytest
python -m pytest tests/ -v
```

**39 tests** across 4 files:
- `test_signal_mapper.py` — CTR → TwuSignal transformation
- `test_region_router.py` — ARN region detection
- `test_api_auth.py` — API key enforcement on POST endpoints
- `test_synthetic_shapes.py` — contract tests validating JSONB matches Java entity models
