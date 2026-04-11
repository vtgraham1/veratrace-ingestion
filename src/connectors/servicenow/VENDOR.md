# ServiceNow — Connector Vendor Notes

## API Overview

- **Base URL:** `https://{instance}.service-now.com/api/now/table/{table_name}`
- **Auth:** OAuth 2.0 Client Credentials (`/oauth_token.do`)
- **Rate limits:** ~300 requests/minute (default), varies by instance edition
- **Pagination:** `sysparm_offset` + `sysparm_limit` (max 10,000 per request)
- **Cursor:** `sys_updated_on` (ISO 8601 format: `YYYY-MM-DD HH:MM:SS`)

## Tables Used

| Table | Purpose |
|-------|---------|
| `incident` | Primary sync target — incident lifecycle |
| `sys_audit` | Field-level change tracking for AI attribution |
| `sys_journal_field` | Work notes and comments (future) |
| `sys_user` | User lookup for actor classification (future) |

## AI Attribution

ServiceNow doesn't expose a public `sys_ai_resolution` table. AI attribution
is **inferred** from audit trails:

1. **sys_audit.user** — if the actor is a Virtual Agent or Now Assist service
   account, classify as AI
2. **resolved_by** — if the resolver matches AI actor keywords, classify as AI
3. **contact_type** — `virtual_agent` indicates AI-initiated interaction

### Actor Classification Keywords

| Pattern | Actor Type |
|---------|-----------|
| `virtual agent`, `now assist`, `chatbot` | AI |
| `system`, `workflow`, `flow designer` | SYSTEM |
| All others | HUMAN |

If `sys_ai_resolution` table becomes available on an instance, the connector
will query it as a first-class source and skip inference.

## Sandbox Setup

1. Get a free PDI at https://developer.servicenow.com
2. Go to **System OAuth > Application Registry** → Create new OAuth API
3. Set grant type to **Client Credentials**
4. Note the Client ID and Client Secret
5. Store in GH secrets: `SNOW_INSTANCE_URL`, `SNOW_CLIENT_ID`, `SNOW_CLIENT_SECRET`

## Quirks & Gotchas

- **Display values:** Use `sysparm_display_value=all` to get both raw and human-readable
  values. Fields return as `{"value": "raw", "display_value": "Pretty"}`.
- **Date format:** ServiceNow uses `YYYY-MM-DD HH:MM:SS` (no T, no Z). Different from
  ISO 8601 but parseable with `datetime.strptime("%Y-%m-%d %H:%M:%S")`.
- **Pagination limit:** Default max is 10,000 records. For audit tables, query by day
  ranges to avoid hitting the limit.
- **401 on first request:** Token may expire silently. The connector handles this with
  automatic retry after token refresh.
- **sys_audit volume:** Large instances generate millions of audit records. Filter by
  `tablename=incident` and `documentkey IN (...)` to keep queries focused.
