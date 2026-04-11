# Vendor Knowledge: Intercom

## API Details
- **Base URL:** https://api.intercom.io
- **Auth:** Bearer token (API token or OAuth access token)
- **API Version:** 2.11 (set via Intercom-Version header)
- **Rate limits:** 1000 req/min (public apps), 10K req/min (private apps)
- **Pagination:** Cursor-based via starting_after parameter
- **Changelog URL:** https://developers.intercom.com/changelog

## Fin AI Agent
- **First-class attribution:** ai_agent object on every conversation
- **did_resolve:** boolean — whether Fin resolved the conversation
- **resolution_state:** "resolved" or "escalated"
- **resolution_rating:** CSAT score (1-5) from customer
- **content_sources:** KB articles Fin used to resolve
- **author.type == "bot":** identifies Fin messages in conversation parts

## Known Quirks
- Timestamps are unix (seconds), not ISO 8601
- conversation_parts may be nested differently in search vs get responses
- Fin is a paid feature — dev workspaces may not have it
- Webhook signatures use HMAC-SHA1 (not SHA256)
- Rate limits are per 10-second windows, not per second

## Sandbox Setup
- Developer Workspace: https://developers.intercom.com (free, max 20 users)
- Fin may not be available — warmer simulates Fin-style conversation parts

## PII Fields
- contacts[].email, contacts[].name
- conversation_parts[].body (may contain customer data)
- custom_attributes (extensible, may contain anything)

## AI Features
- **Fin:** Built-in AI agent with full attribution (did_resolve, confidence, content_sources)
- **Custom Bots:** Workflow-based bots (pre-Fin), appear as author.type == "bot"
- **Operator:** Intercom's automation engine, appears in conversation_parts
