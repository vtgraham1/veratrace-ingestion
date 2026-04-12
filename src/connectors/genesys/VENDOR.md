# Genesys Cloud — Connector Vendor Notes

## API Overview

- **Base URL:** `https://api.{region}/api/v2/...`
- **Auth:** OAuth 2.0 Client Credentials (Basic auth on `login.{region}/oauth/token`)
- **Rate limits:** Varies by endpoint (~300 req/access_token common), 429 with Retry-After header
- **Pagination:** Cursor-based (cursor string in response)
- **Analytics queries:** Interval format `"start/end"` (ISO-8601 with `/` separator)

## Region Routing

| Region | API Domain | Login Domain |
|--------|-----------|-------------|
| US East | mypurecloud.com | login.mypurecloud.com |
| US West | usw2.pure.cloud | login.usw2.pure.cloud |
| EU (Frankfurt) | mypurecloud.de | login.mypurecloud.de |
| EU (Ireland) | mypurecloud.ie | login.mypurecloud.ie |
| APAC (Sydney) | mypurecloud.com.au | login.mypurecloud.com.au |
| APAC (Tokyo) | mypurecloud.jp | login.mypurecloud.jp |
| Canada | cac1.pure.cloud | login.cac1.pure.cloud |

## AI Attribution

First-class via `participant.purpose` field — no inference needed:

| purpose | Actor | Notes |
|---------|-------|-------|
| `bot` | AI | Bot/virtual agent segment |
| `agent` | HUMAN | Human agent segment |
| `customer` | — | Not an actor |
| `acd` | SYSTEM | ACD routing |
| `external` | SYSTEM | External transfer |

**Containment:** Bot-only conversations (no agent participant) = fully contained.
**Handoff:** Both bot + agent participants = bot started, agent resolved.

## Key Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/oauth/token` | POST | Token acquisition (on login.{region}) |
| `/api/v2/organizations/me` | GET | Org info (test_connection) |
| `/api/v2/analytics/conversations/details/query` | POST | Conversation detail queries |
| `/api/v2/analytics/conversations/details/jobs` | POST | Async bulk queries (>100K records) |

## Sandbox Setup

1. Sign up at https://developer.genesys.cloud
2. Admin > Integrations > OAuth > Add Client
3. Grant type: Client Credentials
4. Add role: `analytics:conversationDetail:view`, `routing:queue:view`
5. Store: `GENESYS_REGION`, `GENESYS_CLIENT_ID`, `GENESYS_CLIENT_SECRET`

## Quirks

- **Interval required:** Analytics queries must include an `interval` field. No "since timestamp" — must provide start AND end.
- **Max 100K results:** Synchronous queries cap at 100K conversations. Use async jobs endpoint for larger windows.
- **Cursor expiry:** Pagination cursors may expire after ~5 minutes of inactivity.
- **Token expiry:** Access tokens expire (typically 24h). The connector handles automatic refresh on 401.
- **Participant sessions:** Each participant can have multiple sessions (transfers, reconnects). The connector uses the first session for attribution.
