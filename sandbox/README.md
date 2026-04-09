# Sandbox Setup Guide

## Overview

This directory contains configuration files for setting up a Veratrace sandbox environment with real AI-attributed contacts flowing through Amazon Connect.

**Target instance:** `arn:aws:connect:us-west-2:291925528464:instance/47f7baa6-ffb0-4afd-a025-9d277271e699`

## Setup Order

### 1. Create Lex Bot (Amazon Lex V2 Console, us-west-2)

See `lex-bot-definition.json` for the full bot spec.

**Quick steps:**
1. Amazon Lex → Create bot → `VeratraceSandboxBot`
2. Create 4 intents: `ResetPassword`, `CheckBalance`, `BillingDispute`, `FallbackIntent`
3. Add sample utterances and slots from the JSON file
4. Build the bot
5. Create alias `LIVE` → deploy

**Result:** Bot that auto-resolves password resets and balance checks, escalates billing disputes and unknown queries.

### 2. Register Bot in Connect

1. Amazon Connect console → your instance → Contact Flows
2. Under "Amazon Lex" section → Add Lex bot
3. Select `VeratraceSandboxBot` / `LIVE`

### 3. Create Queue

1. Amazon Connect → Routing → Queues → Add queue
2. Name: `GeneralSupport`
3. Assign a routing profile (or use default)

### 4. Create Contact Flow

See `contact-flow-definition.json` for the flow logic.

**Quick steps:**
1. Contact Flows → Create contact flow → `Veratrace-Lex-Demo`
2. Build using visual editor (see `_visualEditorSteps` in the JSON)
3. Entry → Set Attributes → Get Customer Input (Lex) → Check Intent → Route
4. Save and Publish
5. Note the Contact Flow ID from the ARN

### 5. Update CloudFormation (if needed)

If the existing sandbox role doesn't have Lex permissions, update the stack or add manually:
```
lex:RecognizeText
lex:RecognizeUtterance
lex:GetSession
```

### 6. Update Warmer Config

Once the Lex flow is created, update the warmer to prefer it:

```bash
python -m synthetic.warm \
  --platform amazon-connect \
  --instance-arn arn:aws:connect:us-west-2:291925528464:instance/47f7baa6-ffb0-4afd-a025-9d277271e699 \
  --role-arn <SANDBOX_ROLE_ARN> \
  --external-id vt-sandbox-warm \
  --contacts 5 \
  --sync-after
```

The warmer will auto-discover the `Veratrace-Lex-Demo` flow by name.

### 7. Enable Contact Lens (Optional)

1. Amazon Connect → Instance settings → Contact Lens
2. Enable recording and analysis
3. Choose English (US) for language
4. Creates an S3 bucket for transcript storage

**Cost:** ~$0.10/minute analyzed. Sandbox volume is minimal.

## What This Produces

After setup, the warming cron creates contacts that flow through the real Lex bot:
- ~35% auto-resolved by bot (ResetPassword, CheckBalance) → CTR has `LexBotInteraction` with high confidence
- ~25% bot triaged then human resolved (BillingDispute) → CTR has `LexBotInteraction` with low confidence + `Agent` data  
- ~40% fallback/direct to human → CTR has `FallbackIntent` in Lex + `Agent` data

The signal mapper extracts real `ai_interaction` signals with genuine bot name, intent, confidence — not mocked attributes.

## Files

| File | Purpose |
|------|---------|
| `lex-bot-definition.json` | Lex V2 bot spec (4 intents, utterances, slots) |
| `contact-flow-definition.json` | Connect flow logic (Lex routing + queue transfer) |
| `README.md` | This guide |
