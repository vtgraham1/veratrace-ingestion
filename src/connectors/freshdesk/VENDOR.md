# Freshdesk — Connector Vendor Notes

## API Overview

- **Base URL:** `https://{domain}.freshdesk.com/api/v2/`
- **Auth:** Basic Auth (API key as username, `X` as password)
- **Rate limits:** 200 calls/min (Growth), 700/min (Enterprise)
- **Pagination:** Page-based (`page=N&per_page=100`, max 100 per page)
- **Incremental sync:** `updated_since=ISO8601` query parameter

## AI Attribution

Freshdesk Freddy AI features (Auto-Triage, Copilot, Self-Service, AI Agent)
**do NOT expose explicit API fields** for attribution. No `freddy_resolved`
or `freddy_confidence` fields exist in the public API.

**Inference approach:**
1. Cache all agents via `/api/v2/agents`
2. Cross-reference `responder_id` on tickets against agent cache
3. Check conversation authors via `/api/v2/tickets/{id}/conversations`
4. Classify by name pattern: "Freddy", "Bot", "AI Agent" → AI

This is similar to the Salesforce CaseHistory approach — reliable for orgs
that name their bot accounts consistently, but requires the agent cache.

## Key Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v2/tickets?updated_since=DATE` | GET | Incremental ticket sync |
| `/api/v2/tickets/{id}/conversations` | GET | Reply thread for attribution |
| `/api/v2/agents` | GET | Agent cache for bot/human classification |
| `/api/v2/satisfaction_ratings` | GET | CSAT data |
| `/api/v2/tickets` | POST | Create ticket (warmer) |
| `/api/v2/tickets/{id}/reply` | POST | Add reply (warmer) |
| `/api/v2/tickets/{id}/notes` | POST | Add private note (warmer) |
| `/api/v2/tickets/{id}` | PUT | Update ticket status (warmer) |

## Ticket Status Values

| Value | Status |
|-------|--------|
| 2 | Open |
| 3 | Pending |
| 4 | Resolved |
| 5 | Closed |

## Sandbox Setup

1. Sign up: https://www.freshworks.com/freshdesk/signup/ (21-day trial)
2. Portal URL: `{company}.freshdesk.com`
3. API key: Profile Settings (top right) → Your API Key
4. Store: `FRESHDESK_DOMAIN`, `FRESHDESK_API_KEY`

## Quirks

- **30-day default:** `updated_since` without a date returns only past 30 days
- **Page limit:** Max 100 results per page, max ~300 pages (30K tickets)
- **Rate limit headers:** `X-RateLimit-Remaining`, `X-RateLimit-Total`, `Retry-After`
- **Null fields:** Blank fields return `null`, not omitted
- **Include param:** `include=description,stats` adds body + timing data to list response
- **Conversation source:** `source` on conversations is reply type (2=reply, 4=note), not channel
