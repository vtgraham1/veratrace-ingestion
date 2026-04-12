"""
Genesys Cloud warmer — placeholder for synthetic conversation creation.

Genesys Cloud developer accounts may not support programmatic conversation
creation via API. This warmer validates access and provides the scenario
definitions, but create_activity is a no-op until a sandbox with Web
Messaging or Chat API access is available.

When available, conversations would be created via the Web Messaging API
to trigger bot flows and generate real analytics data.
"""
from __future__ import annotations

import base64
import json
import logging
import random
import urllib.request
import urllib.error
import urllib.parse
import threading

from synthetic.warmers.base import BaseWarmer

logger = logging.getLogger(__name__)

WARMER_ID = "genesys"

CONVERSATION_SCENARIOS = [
    {"weight": 35, "type": "bot_contained", "description": "Bot fully resolves — password reset, FAQ, account lookup"},
    {"weight": 25, "type": "bot_to_agent", "description": "Bot triages then hands off to agent — billing dispute, technical issue"},
    {"weight": 15, "type": "agent_only", "description": "Direct to agent — VIP customer, complex inquiry"},
    {"weight": 10, "type": "multi_queue", "description": "Transferred between queues — wrong department, escalation"},
    {"weight": 10, "type": "abandoned", "description": "Customer abandons in queue — no resolution"},
    {"weight": 5, "type": "bot_callback", "description": "Bot claims contained, customer calls back — vendor reconciliation"},
]


class GenesysWarmer(BaseWarmer):
    """Genesys Cloud warmer — validates access, scenarios ready for when sandbox supports creation."""

    def __init__(self, credentials, external_identity=None):
        super().__init__(credentials, external_identity or {})
        self._client_id = credentials.get("client_id", "")
        self._client_secret = credentials.get("client_secret", "")
        self._region = credentials.get("region", "mypurecloud.com")
        if "." not in self._region:
            from src.connectors.genesys.connector import REGION_DOMAINS
            self._region = REGION_DOMAINS.get(self._region, "mypurecloud.com")
        self._access_token = ""
        self._token_lock = threading.Lock()

    def _obtain_token(self):
        with self._token_lock:
            if self._access_token:
                return
            auth_str = base64.b64encode(
                f"{self._client_id}:{self._client_secret}".encode()
            ).decode()
            data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
            req = urllib.request.Request(
                f"https://login.{self._region}/oauth/token",
                data=data, method="POST",
            )
            req.add_header("Authorization", f"Basic {auth_str}")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())
            self._access_token = result["access_token"]

    def validate_access(self):
        try:
            self._obtain_token()
            url = f"https://api.{self._region}/api/v2/organizations/me"
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {self._access_token}")
            req.add_header("Accept", "application/json")
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp.read()
            return True
        except Exception as e:
            logger.error("Genesys access validation failed: %s", e)
            return False

    def create_activity(self, scenario_config=None):
        # Genesys Cloud dev accounts don't support programmatic conversation creation
        # This is a placeholder — returns a synthetic ID for testing
        scenario = scenario_config or random.choices(
            CONVERSATION_SCENARIOS,
            weights=[s["weight"] for s in CONVERSATION_SCENARIOS],
            k=1,
        )[0]
        logger.warning(
            "Genesys warmer: create_activity is a no-op (sandbox doesn't support "
            "programmatic conversation creation). Scenario: %s", scenario["type"]
        )
        return {"id": f"synthetic-{scenario['type']}", "type": "conversation", "scenario": scenario["type"]}

    def verify_activity(self, activity_id):
        # No-op — synthetic IDs won't exist in the API
        return activity_id.startswith("synthetic-")


WARMER_CLASS = GenesysWarmer
