# Veratrace Ingestion Service

Standalone service that connects enterprise systems (Amazon Connect, Salesforce, Zendesk, AI model APIs) and produces TwuSignals for the TWU Compiler.

## Architecture

This service owns **vendor API → signal in database**. Joey's control plane owns **signal in database → sealed TWU**.

The boundary is the `twu_signals` table and the `POST /instances/{id}/tasks` trigger.

## Connectors

| Connector | Auth | Polling | Status |
|-----------|------|---------|--------|
| Amazon Connect | IAM AssumeRole + ExternalId | SearchContacts API (15 min) | **Live** |
| Salesforce | OAuth 2.0 | SOQL + CaseHistory (15 min) | **Built** (OAuth pending backend PR) |
| Zendesk | OAuth 2.0 / API token | Webhooks + Incremental Export | Planned |
| OpenAI / Anthropic | Proxy / SDK wrapper | Per request | Planned |

New connectors auto-register — see [CONTRIBUTING.md](CONTRIBUTING.md).

## Signal Extraction

### Amazon Connect
Each CTR produces 2-4 signals:

| Signal | Type | When |
|--------|------|------|
| `contact_initiated` | SYSTEM | Always |
| `ai_interaction` | AI | When Lex bot processed the contact |
| `agent_connected` | HUMAN | When a human agent took over |
| `contact_completed` | varies | Always — includes Contact Lens, structured attributes |

### Salesforce
Cases produce 2-4 signals + CaseHistory attribution signals:

| Signal | Type | When |
|--------|------|------|
| `case_created` | SYSTEM | Always — from CreatedDate |
| `ai_interaction` | AI | When custom AI fields exist (AI_Handled__c) |
| `case_field_changed` | varies | Per CaseHistory entry — HUMAN/SYSTEM/AI via CreatedBy.UserType |
| `case_resolved` | HUMAN/AI | When IsClosed=true — includes resolution time |
| `opp_created` | SYSTEM | Opportunity created |
| `opp_closed` | HUMAN | Opportunity won/lost |

**AI attribution inference:** CaseHistory tracks every field change with `CreatedBy.UserType` (Standard=HUMAN, AutomatedProcess=SYSTEM, Einstein/Agentforce=AI). Zero customer setup required.

## HTTP API

`https://ingestion.veratrace.ai` — Caddy reverse proxy with auto Let's Encrypt.

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/health` | GET | None | Liveness check |
| `/health/warming` | GET | None | Warming cron status |
| `/sync` | POST | `X-API-Key` | Trigger sync for an integration account |
| `/test-connection` | POST | `X-API-Key` | Test vendor credentials |

**Security:** API key auth on POST endpoints. Per-IP rate limiting (30 req/min). Structured audit logging (JSON) on every API call. TLS 1.3 via Caddy.

## Deployment

Deployed to DigitalOcean via **veraagents** GitHub Actions. This repo's CI runs unit tests only.

| Service | Schedule | What |
|---------|----------|------|
| Ingestion sync | Every 15 min weekdays | Pull CTRs from vendor APIs |
| Sandbox warming | Every hour weekdays | Create real contacts in Connect sandbox |
| Contract tests | Nightly | Validate against live vendor APIs |
| API server | Always running | HTTP API on port 8090 |

## Sandbox Warming

Creates real contacts with AI/human hybrid scenarios:

```bash
python -m synthetic.warm --platform amazon-connect \
  --instance-arn ARN --role-arn ARN --external-id ID \
  --contacts 5 --sync-after
```

Weighted distribution: 35% AI auto-resolved, 25% AI→human handoff, 15% human-only, 10% SLA-critical, 10% transfers, 5% vendor reconciliation. Chat contacts send messages to trigger Lex bot processing.

See `sandbox/README.md` for full setup.

## Adding a New Connector

See [CONTRIBUTING.md](CONTRIBUTING.md) — 6-step guide. Template at `src/connectors/_template/`. Auto-registers via `__init__.py` exports, no manual wiring.

## Testing

```bash
pip install boto3 pytest
python -m pytest tests/ -v                    # Unit tests (97 tests)
python -m pytest tests/contract/ --contract   # Live API contract tests (6 tests)
```

**144 tests** across 9 files:

| File | Tests | What |
|------|-------|------|
| `test_amazon_connect.py` | 9 | Basic CTR signal mapping |
| `test_signal_mapper_ai.py` | 23 | Lex bot, Contact Lens, structured attributes |
| `test_api_auth.py` | 6 | API key enforcement |
| `test_synthetic_shapes.py` | 16 | JSONB contract against Java entity models |
| `test_salesforce.py` | 36 | Case/Opp mapping, CaseHistory attribution, actor classifier, config |
| `test_warmers.py` | 18 | Connect warmer behavior + scenario distribution |
| `test_warmers_salesforce.py` | 11 | Salesforce warmer scenarios + distribution |
| `test_platform.py` | 17 | Auto-discovery, CONFIG, rate limiter, audit log |
| `contract/test_amazon_connect_live.py` | 6 | Live sandbox API tests (nightly) |

## Security Posture

| Control | Status | Detail |
|---------|--------|--------|
| **TLS** | Implemented | Caddy auto-provisions Let's Encrypt. All API traffic encrypted. |
| **Auth** | Implemented | API key on POST endpoints. Client-side key (visible in browser) — production should proxy through authenticated control plane. |
| **Rate limiting** | Implemented | Per-IP, 30 req/min default. Configurable via `RATE_LIMIT_RPM`. |
| **Audit logging** | Implemented | Structured JSON log per API call (method, path, source IP, timestamp). |
| **Credential storage** | Implemented | No credentials in code. All secrets via env vars or AWS Secrets Manager. |
| **PII handling** | Partial | PII fields flagged on signals. Encryption at write time not yet implemented. |
| **Multi-tenant isolation** | Not implemented | API key is global, not per-tenant. Fix: proxy through control plane with Cognito JWT. |
| **Input validation** | Partial | ARN format validation. No deep payload sanitization on sync endpoint. |
| **Dependency scanning** | Not implemented | No automated CVE scanning on boto3/dependencies. |
| **Secret rotation** | Not implemented | API key is static. No rotation mechanism. |

### SOC 2 Gaps (would be flagged in audit)

1. **Multi-tenant isolation** — API key auth doesn't verify the requestor owns the integration account. Any key holder can trigger sync for any account.
2. **PII encryption at rest** — PII fields are flagged but not encrypted before DB write.
3. **Secret rotation** — Static API key with no expiry or rotation.
4. **Dependency scanning** — No Dependabot or Snyk in CI.
5. **Access logging completeness** — Audit log captures method/path/IP but not the authenticated identity (no JWT).
