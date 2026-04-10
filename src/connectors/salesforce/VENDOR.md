# Vendor Knowledge: Salesforce

## API Details
- **Base URL:** `{instance_url}/services/data/v60.0/`
- **Auth:** OAuth 2.0 Web Server Flow (access token + refresh token)
- **Rate limits:** 5 req/sec, 10K requests/day (Professional Edition)
- **Pagination:** SOQL NextRecordsUrl, 2000 rows per batch
- **Cursor field:** `SystemModstamp` (ISO 8601 timestamp, updated on any change)
- **Changelog URL:** https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_versions.htm

## Known Quirks
- Token refresh endpoint differs: `login.salesforce.com` (production) vs `test.salesforce.com` (sandbox)
- `SystemModstamp` updates on ANY field change including formula recalculations — may produce duplicate signals
- Bulk API 2.0 jobs are async — takes 5-30 min to complete, separate rate limit from REST
- Change Data Capture events retained only 3 days — not suitable for backfill alone
- Custom AI fields (AI_Handled__c, etc.) vary per org — may not exist
- Salesforce timestamps use `+0000` format (no colon) — needs regex conversion for Python 3.9
- Password grant auth requires security token appended to password for IP-restricted orgs

## Sandbox Setup
- **Instance:** https://orgfarm-5524ab9aba-dev-ed.develop.lightning.force.com
- **Username:** tommy962@agentforce.com
- **Connected App Client ID:** in GH secrets (SF_CLIENT_ID)
- **Cost:** Free (Developer Edition)

## PII Fields
- Description (case details, may contain customer info)
- ContactEmail, ContactPhone, SuppliedEmail, SuppliedPhone

## AI Features
- **Einstein Activity Capture:** Tracks activities but NOT AI attribution
- **Einstein Case Classification:** Auto-categorizes cases (pilot/limited)
- **Custom fields (most common):** AI_Handled__c, AI_Agent_Name__c, AI_Confidence__c
- **Agentforce:** New agentic AI layer — early adoption, no standardized fields yet
- **Key difference from Connect:** Salesforce requires the org to instrument AI attribution; it's not automatic like Lex
