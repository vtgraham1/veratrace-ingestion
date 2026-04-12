# Sandbox Environments & Credentials

This document lists all sandbox/dev environments used for integration testing and warming. Treat these as **development-only** credentials — never use production customer data in these systems.

## Amazon Connect

| Field | Value |
|-------|-------|
| **Instance ARN** | `arn:aws:connect:us-east-1:717662748559:instance/47f7baa6-ffb...` |
| **Instance URL** | https://veratrace-sandbox.my.connect.aws |
| **Region** | us-east-1 |
| **IAM Role** | `WARM_ROLE_ARN` (in GH secrets) |
| **External ID** | `WARM_EXTERNAL_ID` (in GH secrets) |
| **AWS Account** | 717662748559 (tommydev IAM user) |
| **Contact Flow** | Veratrace-Lex-Demo (auto-discovered) |
| **Warming** | 5 contacts/hour weekdays via `synthetic.warm --platform amazon-connect` |
| **Lifecycle** | Chat contacts → Lex bot triggered → customer messages → disconnect. Full CTR generated. |

### How to refresh AWS creds
```bash
# Local AWS CLI creds are used. Check:
aws sts get-caller-identity
# Copy to server:
ssh vera@159.203.133.76 # then update AWS_ACCESS_KEY_ID/SECRET in ~/veratrace-ingestion/.env
```

---

## Salesforce

| Field | Value |
|-------|-------|
| **Instance URL** | https://orgfarm-5524ab9aba-dev-ed.develop.my.salesforce.com |
| **Org ID** | 00Dfj00000G0zThEAJ |
| **Username** | tommy962@agentforce.com |
| **Auth method** | SF CLI (`sf org login web --alias sf-sandbox`) |
| **CLI alias** | sf-sandbox |
| **Warming** | 3 cases/hour weekdays via `synthetic.warm --platform salesforce` |
| **Lifecycle** | Create Case (New) → CaseComment with AI agent tag → Status→Working → Status→Closed. Generates CaseHistory entries for attribution. |

### How to refresh SF token
```bash
# Token expires periodically. Refresh:
sf org display --target-org sf-sandbox --json | python3 -c "import json,sys; print(json.load(sys.stdin)['result']['accessToken'])"
# Then update SF_ACCESS_TOKEN in the server .env
# Once Joey's OAuth PR merges, refresh tokens will handle this automatically.
```

**Note:** AI custom fields (`AI_Handled__c`, `AI_Agent_Name__c`, `AI_Confidence__c`) don't exist on this sandbox. The warmer falls back gracefully. AI attribution comes from CaseHistory `CreatedBy.UserType` inference instead.

---

## Intercom

| Field | Value |
|-------|-------|
| **Workspace** | TG Capital Ventures (dev workspace) |
| **API Token** | `INTERCOM_ACCESS_TOKEN` (in GH secrets + server .env) |
| **Auth method** | Bearer token |
| **API Version** | 2.11 |
| **Warming** | 3 conversations/hour weekdays via `synthetic.warm --platform intercom` |
| **Lifecycle** | Create conversation (user message) → Bot/Fin reply → Human reply (if escalated) → Close conversation. Full conversation parts with author types. |

### Trial status
- 7-day trial was extended (check expiry)
- If expired, sign up for a new dev workspace at https://app.intercom.com/admins/sign_up
- Fin attribution requires a paid plan — trial workspace simulates Fin via admin replies

---

## ServiceNow

| Field | Value |
|-------|-------|
| **Instance URL** | https://dev379979.service-now.com |
| **Username** | admin |
| **Password** | `dyRBY^9nMi@4` |
| **Auth method** | Basic Auth (warmer), OAuth planned (connector) |
| **OAuth Client ID** | 9d75967e67e94990916cfaf7e40ac074 |
| **PDI type** | Personal Developer Instance (free) |
| **Warming** | 5 incidents every 2 hours weekdays via `synthetic.seed_servicenow` |
| **Lifecycle** | Create incident (New) → Work notes from [Virtual Agent]/[Now Assist]/[Predictive Intelligence]/human agents → Assign to agent → State→In Progress → State→Resolved. Full sys_audit + sys_journal_field trail. |

### Scenarios seeded
- 35% Virtual Agent auto-resolved (password resets, VPN, account unlocks)
- 25% AI-assisted → human resolved (VA triages, agent completes)
- 15% Human-only (P1 database outages, SSO cert renewals)
- 10% Multi-assignment (reassigned between groups)
- 10% SLA breach (workaround applied, target missed)
- 5% Vendor reconciliation (VA claims resolved, user reopens)

### PDI notes
- PDIs hibernate after 10 days of inactivity — log in periodically
- Business rules block direct state=6 on POST — warmer creates in New, then PATCHes to resolve
- OAuth client credentials grant may need additional plugin activation

---

## Genesys Cloud

| Field | Value |
|-------|-------|
| **Status** | **No sandbox** — Genesys has no self-service developer edition |
| **Connector** | Code complete, 33 tests passing against fixtures |
| **Warmer** | No-op stub (scenario definitions ready) |

### To get access
1. Request trial: https://www.genesys.com/free-trial (sales conversation required)
2. Apply to AppFoundry partner program (free dev org)
3. Or wait for a customer to connect their production Genesys org

---

## Server Environment

All credentials are stored on the DO server at `~/veratrace-ingestion/.env` (chmod 600).

The deploy workflow (`veraagents/.github/workflows/deploy.yml`) writes this file from GitHub secrets on each deploy. To update credentials:

1. **Via GitHub secrets** (preferred): update the secret in the repo settings, then push to trigger deploy
2. **Manually on server**: SSH in and edit `~/veratrace-ingestion/.env`

### Required env vars
```
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
CONTROL_PLANE_URL=https://api.veratrace.app
INGESTION_API_KEY=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
WARM_ROLE_ARN=...
WARM_INSTANCE_ARN=...
WARM_EXTERNAL_ID=...
SF_ACCESS_TOKEN=...
SF_INSTANCE_URL=...
INTERCOM_ACCESS_TOKEN=...
SNOW_INSTANCE_URL=https://dev379979.service-now.com
SNOW_USERNAME=admin
SNOW_PASSWORD=...
```

### Warming schedule (all times UTC, weekdays only)
| Platform | Cron | Count | Method |
|----------|------|-------|--------|
| Amazon Connect | :00, hourly 15-01 | 5/hour | `synthetic.warm --platform amazon-connect` |
| Intercom | :15, hourly 15-01 | 3/hour | `synthetic.warm --platform intercom` |
| Salesforce | :30, hourly 15-01 | 3/hour | `synthetic.warm --platform salesforce` |
| ServiceNow | :45, every 2h (15,17,19,21,23) | 5/run | `synthetic.seed_servicenow --incidents 5` |
