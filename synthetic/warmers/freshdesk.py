"""
Freshdesk warmer — creates real tickets with full lifecycle
in a Freshdesk portal for ingestion pipeline testing.

Creates tickets, adds replies (simulating Freddy + human agents),
transitions through Open → Pending → Resolved, and adds notes.
"""
from __future__ import annotations

import base64
import json
import logging
import random
import time
import urllib.request
import urllib.error

from synthetic.warmers.base import BaseWarmer

logger = logging.getLogger(__name__)

WARMER_ID = "freshdesk"

CUSTOMER_EMAILS = [
    "alex.rivera@example.com", "jordan.chen@example.com",
    "sam.patel@example.com", "morgan.kim@example.com",
    "taylor.brooks@example.com", "casey.wong@example.com",
]

TICKET_SCENARIOS = [
    # ── Freddy auto-resolved (35%) ─────────────────────────────────────────
    {
        "weight": 20, "type": "freddy_resolved",
        "subject": "How do I reset my password?",
        "description": "I forgot my password and need to reset it.",
        "priority": 2, "source": 7, "ticket_type": "Question",
        "bot_reply": "You can reset your password at Settings → Security → Reset Password. I've sent a reset link to your email.",
        "resolve": True,
    },
    {
        "weight": 15, "type": "freddy_resolved",
        "subject": "What are your pricing plans?",
        "description": "Can you tell me about your pricing options?",
        "priority": 1, "source": 2, "ticket_type": "Question",
        "bot_reply": "We offer Starter ($49/mo), Growth ($99/mo), and Enterprise (custom). See veratrace.ai/pricing for details.",
        "resolve": True,
    },

    # ── Freddy → human handoff (25%) ───────────────────────────────────────
    {
        "weight": 15, "type": "freddy_handoff",
        "subject": "I want to dispute my last invoice",
        "description": "There is an unauthorized charge on my latest bill.",
        "priority": 3, "source": 1, "ticket_type": "Problem",
        "bot_reply": "I understand you'd like to dispute a charge. Let me connect you with our billing team.",
        "human_reply": "I've reviewed your account and applied a $50 credit. Updated invoice will be sent shortly.",
        "resolve": True,
    },
    {
        "weight": 10, "type": "freddy_handoff",
        "subject": "Our integration stopped syncing",
        "description": "Data sync from our CRM stopped working yesterday.",
        "priority": 3, "source": 1, "ticket_type": "Incident",
        "bot_reply": "I'm sorry about the sync issue. Let me escalate to our technical team.",
        "human_reply": "Found it — your API token expired. Refreshed it, sync should resume in 15 minutes.",
        "resolve": True,
    },

    # ── Human only (15%) ───────────────────────────────────────────────────
    {
        "weight": 8, "type": "human_only",
        "subject": "Need custom SLA for enterprise deployment",
        "description": "We require specific uptime guarantees for our contract.",
        "priority": 4, "source": 1, "ticket_type": "Feature Request",
        "human_reply": "I'd be happy to discuss custom SLA terms. Setting up a call with our enterprise team.",
        "resolve": True,
    },
    {
        "weight": 7, "type": "human_only",
        "subject": "SOC 2 compliance documentation request",
        "description": "We need your compliance docs for our annual audit.",
        "priority": 2, "source": 1, "ticket_type": "Question",
        "human_reply": "Our security team will prepare the documentation package within 24 hours.",
        "resolve": True,
    },

    # ── Multi-agent (10%) ──────────────────────────────────────────────────
    {
        "weight": 10, "type": "multi_agent",
        "subject": "Complex technical issue with API integration",
        "description": "Multiple errors when calling the REST API from our backend.",
        "priority": 3, "source": 2, "ticket_type": "Incident",
        "human_reply": "I've identified the issue. Transferring to our API team for the fix.",
        "resolve": False,
    },

    # ── SLA breach (10%) ───────────────────────────────────────────────────
    {
        "weight": 10, "type": "sla_breach",
        "subject": "Shared drive access not working",
        "description": "New employee can't access the shared drive after onboarding.",
        "priority": 2, "source": 1, "ticket_type": "Incident",
        "human_reply": "SLA breached — AD sync delay. Applied direct permissions as workaround.",
        "resolve": True,
    },

    # ── Vendor reconciliation (5%) ─────────────────────────────────────────
    {
        "weight": 5, "type": "vendor_recon",
        "subject": "Checking on outsourced support resolution",
        "description": "Vendor claims this was resolved by AI but customer disagrees.",
        "priority": 2, "source": 1, "ticket_type": "Problem",
        "bot_reply": "I've resolved your issue by providing the requested documentation.",
        "human_reply": "Customer reopened — bot fix was incomplete. Resolved manually.",
        "resolve": True,
    },
]

_SCENARIO_WEIGHTS = [s["weight"] for s in TICKET_SCENARIOS]


class FreshdeskWarmer(BaseWarmer):
    """Creates tickets with full lifecycle in a Freshdesk portal."""

    def __init__(self, credentials, external_identity=None):
        super().__init__(credentials, external_identity or {})
        self._api_key = credentials.get("api_key", "")
        self._domain = credentials.get("domain", "").replace(".freshdesk.com", "")
        self._base_url = f"https://{self._domain}.freshdesk.com/api/v2"
        self._auth = base64.b64encode(f"{self._api_key}:X".encode()).decode()
        self._agent_id = None

    def _api_get(self, path):
        url = f"{self._base_url}{path}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Basic {self._auth}")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def _api_post(self, path, body):
        url = f"{self._base_url}{path}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Basic {self._auth}")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def _api_put(self, path, body):
        url = f"{self._base_url}{path}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="PUT")
        req.add_header("Authorization", f"Basic {self._auth}")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def validate_access(self):
        try:
            tickets = self._api_get("/tickets?per_page=1")
            # Get current agent ID for replies
            me = self._api_get("/agents/me")
            self._agent_id = me.get("id")
            logger.info("Freshdesk access validated — agent_id=%s, domain=%s", self._agent_id, self._domain)
            return True
        except Exception as e:
            logger.error("Freshdesk access failed: %s", e)
            return False

    def create_activity(self, scenario_config=None):
        if not scenario_config or "subject" not in scenario_config:
            scenario_config = random.choices(TICKET_SCENARIOS, weights=_SCENARIO_WEIGHTS, k=1)[0]

        scenario = scenario_config
        email = random.choice(CUSTOMER_EMAILS)

        # Step 1: Create ticket
        ticket_body = {
            "subject": scenario["subject"],
            "description": scenario["description"],
            "email": email,
            "priority": scenario.get("priority", 2),
            "source": scenario.get("source", 1),
            "status": 2,  # Open
            "type": scenario.get("ticket_type", "Question"),
        }

        result = self._api_post("/tickets", ticket_body)
        ticket_id = result.get("id")
        logger.info("Created ticket #%s: %s", ticket_id, scenario["subject"])

        # Step 2: Add bot reply if applicable
        if scenario.get("bot_reply"):
            time.sleep(0.5)
            try:
                self._api_post(f"/tickets/{ticket_id}/notes", {
                    "body": f"<p><strong>[Freddy Bot]</strong> {scenario['bot_reply']}</p>",
                    "private": False,
                })
            except Exception as e:
                logger.debug("Bot reply failed: %s", str(e)[:50])

        # Step 3: Add human reply if applicable
        if scenario.get("human_reply"):
            time.sleep(0.5)
            try:
                self._api_post(f"/tickets/{ticket_id}/notes", {
                    "body": f"<p>{scenario['human_reply']}</p>",
                    "private": False,
                })
            except Exception as e:
                logger.debug("Human reply failed: %s", str(e)[:50])

        # Step 4: Transition states
        time.sleep(0.3)
        try:
            self._api_put(f"/tickets/{ticket_id}", {"status": 3})  # Pending
        except Exception as e:
            logger.debug("Status→Pending failed: %s", str(e)[:50])

        # Step 5: Resolve if applicable
        if scenario.get("resolve"):
            time.sleep(0.3)
            try:
                self._api_put(f"/tickets/{ticket_id}", {"status": 4})  # Resolved
            except Exception as e:
                logger.debug("Status→Resolved failed: %s", str(e)[:50])

        return {
            "id": str(ticket_id),
            "type": "ticket",
            "scenario": scenario["type"],
        }

    def verify_activity(self, activity_id):
        try:
            result = self._api_get(f"/tickets/{activity_id}")
            return bool(result.get("id"))
        except Exception:
            return False


WARMER_CLASS = FreshdeskWarmer
