#!/usr/bin/env bash
# Refresh Salesforce access token via SF CLI and push to DO server
# Run locally on Mac — SF CLI handles token refresh internally
set -euo pipefail

TOKEN=$(sf org display --target-org sf-sandbox --json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['result']['accessToken'])")

if [ -z "$TOKEN" ]; then
  echo "ERROR: SF CLI returned no token" >&2
  exit 1
fi

# Update on server
ssh -o ConnectTimeout=10 vera@159.203.133.76 "sed -i 's|^SF_ACCESS_TOKEN=.*|SF_ACCESS_TOKEN=${TOKEN}|' ~/veratrace-ingestion/.env && echo 'SF token refreshed on server'"

# Update GHA secret (so deploys get the fresh token too)
echo -n "$TOKEN" | gh secret set SF_ACCESS_TOKEN --repo Veratrace-AI/veraagents 2>/dev/null && echo "GHA secret updated" || echo "GHA update skipped"
