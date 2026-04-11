"""
ServiceNow warmer — creates real Incidents in a ServiceNow PDI
(Personal Developer Instance) so the ingestion pipeline has data to pull.

Uses the ServiceNow Table API to create incidents with weighted
scenarios matching Veratrace TWU attribution patterns.

Requires: SNOW_INSTANCE_URL, SNOW_CLIENT_ID, SNOW_CLIENT_SECRET in env
or access_token in credentials.
"""
from __future__ import annotations

import json
import logging
import random
import urllib.request
import urllib.error
import urllib.parse
import threading

from synthetic.warmers.base import BaseWarmer

logger = logging.getLogger(__name__)

WARMER_ID = "servicenow"

CALLER_NAMES = [
    "Alex Rivera", "Jordan Chen", "Sam Patel", "Morgan Kim",
    "Taylor Brooks", "Casey Wong", "Jamie Foster", "Drew Martinez",
    "Riley Nguyen", "Quinn O'Brien", "Avery Shah", "Blake Thompson",
]

INCIDENT_SCENARIOS = [
    # ── Virtual Agent auto-resolved (35%) ──────────────────────────────────
    {
        "weight": 20,
        "short_description": "Password reset request",
        "category": "inquiry",
        "subcategory": "password",
        "priority": "3",
        "urgency": "3",
        "impact": "3",
        "contact_type": "virtual_agent",
        "state": "6",  # Resolved
        "close_code": "Solved (Permanently)",
        "close_notes": "Password reset link sent automatically via Virtual Agent",
        "description": "User unable to login after password expiry — resolved automatically.",
    },
    {
        "weight": 15,
        "short_description": "VPN access request",
        "category": "network",
        "subcategory": "vpn",
        "priority": "3",
        "urgency": "3",
        "impact": "3",
        "contact_type": "virtual_agent",
        "state": "6",
        "close_code": "Solved (Permanently)",
        "close_notes": "VPN profile provisioned automatically by Virtual Agent",
        "description": "New employee requesting VPN access — auto-provisioned.",
    },

    # ── AI-assisted → human resolved (25%) ─────────────────────────────────
    {
        "weight": 15,
        "short_description": "Outlook not syncing email",
        "category": "software",
        "subcategory": "email",
        "priority": "2",
        "urgency": "2",
        "impact": "2",
        "contact_type": "virtual_agent",
        "state": "6",
        "close_code": "Solved (Permanently)",
        "close_notes": "Virtual Agent collected diagnostics, agent rebuilt Outlook profile",
        "description": "Email sync stopped — Virtual Agent triaged, escalated to desktop support.",
    },
    {
        "weight": 10,
        "short_description": "Printer not working on floor 3",
        "category": "hardware",
        "subcategory": "printer",
        "priority": "3",
        "urgency": "3",
        "impact": "2",
        "contact_type": "virtual_agent",
        "state": "6",
        "close_code": "Solved (Permanently)",
        "close_notes": "Virtual Agent identified printer model, tech replaced toner cartridge",
        "description": "Printer queue stuck — Virtual Agent diagnosed, hardware team dispatched.",
    },

    # ── Human-only resolution (15%) ────────────────────────────────────────
    {
        "weight": 10,
        "short_description": "Database connection timeout in production",
        "category": "software",
        "subcategory": "database",
        "priority": "1",
        "urgency": "1",
        "impact": "1",
        "contact_type": "phone",
        "state": "6",
        "close_code": "Solved (Permanently)",
        "close_notes": "Connection pool exhaustion. Increased max connections and restarted service.",
        "description": "Production database timeouts affecting all users — P1 incident.",
    },
    {
        "weight": 5,
        "short_description": "SSO login failure for external partners",
        "category": "software",
        "subcategory": "login",
        "priority": "2",
        "urgency": "2",
        "impact": "2",
        "contact_type": "email",
        "state": "6",
        "close_code": "Solved (Permanently)",
        "close_notes": "SAML certificate expired, renewed and updated federation metadata",
        "description": "External partners unable to authenticate via SSO.",
    },

    # ── Multi-assignment / reassigned (10%) ────────────────────────────────
    {
        "weight": 10,
        "short_description": "New laptop setup and data migration",
        "category": "hardware",
        "subcategory": "laptop",
        "priority": "3",
        "urgency": "3",
        "impact": "3",
        "contact_type": "self-service",
        "state": "2",  # In Progress
        "close_code": "",
        "close_notes": "",
        "description": "Executive laptop refresh — requires data migration from old device.",
    },

    # ── SLA breach (10%) ───────────────────────────────────────────────────
    {
        "weight": 10,
        "short_description": "Shared drive permissions not applied",
        "category": "software",
        "subcategory": "file_share",
        "priority": "3",
        "urgency": "3",
        "impact": "3",
        "contact_type": "email",
        "state": "6",
        "close_code": "Solved (Work Around)",
        "close_notes": "AD group membership propagation delay. Added direct ACL as workaround.",
        "description": "New team member can't access shared drive — SLA missed due to AD sync lag.",
    },

    # ── Vendor reconciliation (5%) — Now Assist claims resolved, reopened ──
    {
        "weight": 5,
        "short_description": "Software license activation failing",
        "category": "software",
        "subcategory": "license",
        "priority": "3",
        "urgency": "2",
        "impact": "3",
        "contact_type": "virtual_agent",
        "state": "2",  # Reopened (In Progress)
        "close_code": "",
        "close_notes": "",
        "description": "Virtual Agent sent activation instructions but key was invalid. Reopened by user.",
    },
]


class ServiceNowWarmer(BaseWarmer):
    """Create synthetic incidents in a ServiceNow PDI."""

    def __init__(self, credentials, external_identity=None):
        super().__init__(credentials, external_identity or {})
        self._instance_url = credentials.get("instance_url", "").rstrip("/")
        self._client_id = credentials.get("client_id", "")
        self._client_secret = credentials.get("client_secret", "")
        self._access_token = credentials.get("access_token", "")
        self._token_lock = threading.Lock()

    def _obtain_token(self):
        with self._token_lock:
            if self._access_token:
                return
            data = urllib.parse.urlencode({
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }).encode()
            req = urllib.request.Request(
                f"{self._instance_url}/oauth_token.do",
                data=data,
                method="POST",
            )
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())
            self._access_token = result["access_token"]

    def _api_post(self, table, body):
        if not self._access_token:
            self._obtain_token()
        url = f"{self._instance_url}/api/now/table/{table}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def _api_get(self, table, sys_id):
        if not self._access_token:
            self._obtain_token()
        url = f"{self._instance_url}/api/now/table/{table}/{sys_id}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def validate_access(self):
        try:
            if not self._access_token:
                self._obtain_token()
            url = f"{self._instance_url}/api/now/table/incident?sysparm_limit=1"
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {self._access_token}")
            req.add_header("Accept", "application/json")
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp.read()
            return True
        except Exception as e:
            logger.error("ServiceNow access validation failed: %s", e)
            return False

    def create_activity(self, scenario_config=None):
        if scenario_config is None:
            scenario_config = self._pick_scenario()

        caller = random.choice(CALLER_NAMES)
        body = {
            "short_description": scenario_config["short_description"],
            "description": scenario_config.get("description", ""),
            "category": scenario_config.get("category", "inquiry"),
            "subcategory": scenario_config.get("subcategory", ""),
            "priority": scenario_config.get("priority", "3"),
            "urgency": scenario_config.get("urgency", "3"),
            "impact": scenario_config.get("impact", "3"),
            "contact_type": scenario_config.get("contact_type", "phone"),
        }

        # Set resolved fields if resolved
        state = scenario_config.get("state", "1")
        if state == "6":
            body["state"] = "6"
            body["close_code"] = scenario_config.get("close_code", "")
            body["close_notes"] = scenario_config.get("close_notes", "")

        result = self._api_post("incident", body)
        record = result.get("result", {})
        sys_id = record.get("sys_id", "")
        number = record.get("number", "")

        logger.info("Created incident %s (%s): %s", number, sys_id, body["short_description"])
        return {
            "id": sys_id,
            "number": number,
            "type": "incident",
            "scenario": scenario_config["short_description"],
        }

    def verify_activity(self, activity_id):
        try:
            result = self._api_get("incident", activity_id)
            return bool(result.get("result", {}).get("sys_id"))
        except Exception:
            return False

    def _pick_scenario(self):
        weights = [s["weight"] for s in INCIDENT_SCENARIOS]
        return random.choices(INCIDENT_SCENARIOS, weights=weights, k=1)[0]


WARMER_CLASS = ServiceNowWarmer
