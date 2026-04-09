"""
Amazon Connect warmer — creates real chat and task contacts
in a Connect sandbox instance so the ingestion pipeline has
actual CTRs to pull via SearchContacts.

Requires:
- IAM role with connect:StartChatContact, connect:StartTaskContact
- At least one published Contact Flow in the Connect instance
- Use the sandbox CloudFormation template (amazon-connect-sandbox.yaml)
"""
from __future__ import annotations

import logging
import random
import time
import uuid

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from synthetic.warmers.base import BaseWarmer

logger = logging.getLogger(__name__)

# ── Veratrace AI/Human Hybrid Contact Scenarios ──────────────────────────────
#
# These contacts model the core Veratrace use case: enterprises running
# hybrid AI + human operations where attribution, SLA compliance, and
# cost accountability matter.

CUSTOMER_NAMES = [
    "Alex Rivera", "Jordan Chen", "Sam Patel", "Morgan Kim",
    "Taylor Brooks", "Casey Wong", "Jamie Foster", "Drew Martinez",
    "Riley Nguyen", "Quinn O'Brien", "Avery Shah", "Blake Thompson",
    "Reese Nakamura", "Skyler Davis", "Finley Clark", "Parker Lee",
]

CUSTOMER_SEGMENTS = ["enterprise", "mid-market", "smb", "consumer"]
PRIORITIES = ["critical", "high", "medium", "medium", "low"]

# ── Contact scenarios with enterprise-realistic distribution weights ──────────
#
# Weights model a typical enterprise contact center:
#   ~35% AI auto-resolved (password resets, FAQ, balance checks)
#   ~25% AI triages then human resolves (billing, contracts, complaints)
#   ~15% Human-only (compliance, legal, complex issues)
#   ~10% SLA-critical (outages, escalations)
#   ~10% Multi-touch / transfers
#    ~5% Vendor reconciliation edge cases (the 80% vs 42% story)
#
# Over 1,100 contacts/month this produces a consistent, realistic mix.

CONTACT_SCENARIOS = [
    # ── AI fully resolves (35% combined weight) ──────────────────────────
    {
        "weight": 20,
        "reason": "password_reset",
        "ai_handled": "true",
        "ai_agent": "ResolveAI-v3",
        "ai_confidence": "0.95",
        "human_needed": "false",
        "resolution": "ai_auto_resolved",
        "description": "Password reset — AI auto-resolved",
    },
    {
        "weight": 15,
        "reason": "account_balance_inquiry",
        "ai_handled": "true",
        "ai_agent": "ConnectBot-IVR",
        "ai_confidence": "0.92",
        "human_needed": "false",
        "resolution": "ai_auto_resolved",
        "description": "Balance check — AI self-service",
    },

    # ── AI triages, human resolves (25% combined weight) ─────────────────
    {
        "weight": 15,
        "reason": "billing_dispute",
        "ai_handled": "true",
        "ai_agent": "SmartRoute-AI",
        "ai_confidence": "0.38",
        "human_needed": "true",
        "resolution": "human_resolved_after_ai",
        "description": "Billing dispute — AI triaged, human resolved",
    },
    {
        "weight": 10,
        "reason": "contract_negotiation",
        "ai_handled": "true",
        "ai_agent": "SmartRoute-AI",
        "ai_confidence": "0.22",
        "human_needed": "true",
        "resolution": "human_resolved_after_ai",
        "description": "Contract negotiation — AI routed to specialist",
    },

    # ── Human-only (15% combined weight) ─────────────────────────────────
    {
        "weight": 8,
        "reason": "escalation_from_ai",
        "ai_handled": "false",
        "ai_agent": "ResolveAI-v3",
        "ai_confidence": "0.15",
        "human_needed": "true",
        "resolution": "human_escalation",
        "description": "Complex issue — AI escalated to human",
    },
    {
        "weight": 7,
        "reason": "compliance_audit_request",
        "ai_handled": "false",
        "ai_agent": "none",
        "ai_confidence": "0.0",
        "human_needed": "true",
        "resolution": "human_only",
        "description": "Compliance request — requires human judgment",
    },

    # ── SLA-critical (10% combined weight) ───────────────────────────────
    {
        "weight": 6,
        "reason": "outage_report",
        "ai_handled": "true",
        "ai_agent": "ConnectBot-IVR",
        "ai_confidence": "0.88",
        "human_needed": "true",
        "sla_target_seconds": "30",
        "resolution": "sla_at_risk",
        "description": "Outage report — SLA critical, transferred to human",
    },
    {
        "weight": 4,
        "reason": "urgent_callback",
        "ai_handled": "true",
        "ai_agent": "SmartRoute-AI",
        "ai_confidence": "0.65",
        "human_needed": "true",
        "sla_target_seconds": "60",
        "resolution": "sla_met",
        "description": "Urgent callback — AI fast-tracked to available agent",
    },

    # ── Multi-touch / transfers (10% weight) ─────────────────────────────
    {
        "weight": 6,
        "reason": "technical_support_complex",
        "ai_handled": "true",
        "ai_agent": "ResolveAI-v3",
        "ai_confidence": "0.55",
        "human_needed": "true",
        "transferred": "true",
        "transfer_reason": "skill_mismatch",
        "resolution": "transferred_then_resolved",
        "description": "Complex tech issue — AI + 2 human agents",
    },
    {
        "weight": 4,
        "reason": "language_transfer",
        "ai_handled": "false",
        "ai_agent": "none",
        "ai_confidence": "0.0",
        "human_needed": "true",
        "transferred": "true",
        "transfer_reason": "language",
        "resolution": "transferred_then_resolved",
        "description": "Language mismatch — transferred to bilingual agent",
    },

    # ── Vendor reconciliation (5% combined weight) ───────────────────────
    {
        "weight": 3,
        "reason": "vendor_bpo_contact",
        "ai_handled": "true",
        "ai_agent": "VendorBot-External",
        "ai_confidence": "0.71",
        "human_needed": "false",
        "vendor_claimed_ai": "true",
        "resolution": "vendor_ai_claimed",
        "description": "BPO vendor contact — legitimately AI-resolved",
    },
    {
        "weight": 2,
        "reason": "vendor_bpo_contact",
        "ai_handled": "false",
        "ai_agent": "VendorBot-External",
        "ai_confidence": "0.12",
        "human_needed": "true",
        "vendor_claimed_ai": "true",
        "resolution": "vendor_ai_overclaimed",
        "description": "BPO vendor contact — claimed AI but actually human",
    },
]

# Pre-compute weights for random.choices
_SCENARIO_WEIGHTS = [s["weight"] for s in CONTACT_SCENARIOS]


class ConnectWarmer(BaseWarmer):
    """Creates real contacts in an Amazon Connect instance."""

    def __init__(self, credentials: dict, external_identity: dict):
        super().__init__(credentials, external_identity)
        self._instance_arn = external_identity.get("tenantId", "")
        self._instance_id = self._instance_arn.split("/")[-1] if "/" in self._instance_arn else ""
        self._region = self._parse_region()
        self._contact_flow_id = None
        self._assumed_creds = None
        self._assumed_creds_expiry = 0

    def _parse_region(self):
        parts = self._instance_arn.split(":")
        return parts[3] if len(parts) > 3 else "us-east-1"

    def _assume_role(self):
        now = time.time()
        if self._assumed_creds and self._assumed_creds_expiry > now + 300:
            return self._assumed_creds

        role_arn = self.credentials.get("roleArn", "")
        external_id = self.credentials.get("externalId", "")

        sts = boto3.client("sts", region_name=self._region)
        params = {
            "RoleArn": role_arn,
            "RoleSessionName": f"veratrace-warmer-{uuid.uuid4().hex[:8]}",
            "DurationSeconds": 3600,
        }
        if external_id:
            params["ExternalId"] = external_id

        resp = sts.assume_role(**params)
        creds = resp["Credentials"]
        self._assumed_creds = {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
        self._assumed_creds_expiry = creds["Expiration"].timestamp()
        return self._assumed_creds

    def _get_client(self):
        creds = self._assume_role()
        return boto3.client(
            "connect",
            region_name=self._region,
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            aws_session_token=creds["aws_session_token"],
            config=BotoConfig(
                retries={"max_attempts": 2, "mode": "adaptive"},
                connect_timeout=10,
                read_timeout=30,
            ),
        )

    def _discover_contact_flow(self, client):
        """Find a usable contact flow in the instance."""
        if self._contact_flow_id:
            return self._contact_flow_id

        try:
            resp = client.list_contact_flows(
                InstanceId=self._instance_id,
                ContactFlowTypes=["CONTACT_FLOW"],
            )
            flows = resp.get("ContactFlowSummaryList", [])
            # Prefer flows with "default" or "sample" in the name
            for flow in flows:
                name_lower = flow.get("Name", "").lower()
                if any(kw in name_lower for kw in ["default", "sample", "inbound", "basic"]):
                    self._contact_flow_id = flow["Id"]
                    logger.info("Using contact flow: %s (%s)", flow["Name"], flow["Id"][:12])
                    return self._contact_flow_id

            # Fall back to first available flow
            if flows:
                self._contact_flow_id = flows[0]["Id"]
                logger.info("Using first contact flow: %s (%s)", flows[0]["Name"], flows[0]["Id"][:12])
                return self._contact_flow_id

        except ClientError as e:
            logger.error("Failed to list contact flows: %s", e)

        return None

    def validate_access(self) -> bool:
        """Verify write permissions by listing contact flows."""
        try:
            client = self._get_client()
            flow_id = self._discover_contact_flow(client)
            if not flow_id:
                logger.error("No contact flows found — create one in the Connect console first")
                return False
            logger.info("Access validated: instance=%s, flow=%s", self._instance_id[:12], flow_id[:12])
            return True
        except ClientError as e:
            logger.error("Access validation failed: %s", e)
            return False

    def create_activity(self, scenario_config: dict) -> dict:
        """
        Create a chat or task contact in Connect with realistic
        AI/human hybrid metadata matching Veratrace use cases.
        """
        client = self._get_client()
        flow_id = self._discover_contact_flow(client)
        if not flow_id:
            raise RuntimeError("No contact flow available")

        # Weighted selection ensures consistent enterprise-realistic distribution
        scenario = random.choices(CONTACT_SCENARIOS, weights=_SCENARIO_WEIGHTS, k=1)[0]
        use_task = random.random() < scenario_config.get("task_ratio", 0.3)
        customer_name = random.choice(CUSTOMER_NAMES)
        segment = random.choice(CUSTOMER_SEGMENTS)
        priority = random.choice(PRIORITIES)

        # Contact attributes carry the AI/human attribution metadata
        # that the ingestion pipeline will extract into TwuSignals
        attributes = {
            "customerSegment": segment,
            "contactReason": scenario["reason"],
            "priority": priority,
            "source": "veratrace-warmer",
            "caseId": f"VT-{uuid.uuid4().hex[:8].upper()}",
            # AI attribution — core Veratrace data
            "aiHandled": scenario["ai_handled"],
            "aiAgent": scenario["ai_agent"],
            "aiConfidence": scenario["ai_confidence"],
            "humanNeeded": scenario["human_needed"],
            "resolution": scenario["resolution"],
        }

        # Add optional fields from scenario
        if scenario.get("transferred"):
            attributes["transferred"] = "true"
            attributes["transferReason"] = scenario.get("transfer_reason", "unknown")
        if scenario.get("sla_target_seconds"):
            attributes["slaTargetSeconds"] = scenario["sla_target_seconds"]
        if scenario.get("vendor_claimed_ai"):
            attributes["vendorClaimedAI"] = "true"

        if use_task:
            resp = client.start_task_contact(
                InstanceId=self._instance_id,
                ContactFlowId=flow_id,
                Name=f"{scenario['description']} — {customer_name}",
                Description=f"{segment.title()} customer: {scenario['description']}",
                Attributes=attributes,
            )
            contact_id = resp["ContactId"]
            return {"id": contact_id, "type": "TASK", "customer": customer_name, "scenario": scenario["reason"]}
        else:
            resp = client.start_chat_contact(
                InstanceId=self._instance_id,
                ContactFlowId=flow_id,
                ParticipantDetails={"DisplayName": customer_name},
                Attributes=attributes,
            )
            contact_id = resp["ContactId"]
            return {"id": contact_id, "type": "CHAT", "customer": customer_name, "scenario": scenario["reason"]}

    def verify_activity(self, activity_id: str) -> bool:
        """Check if the contact produced a CTR visible in SearchContacts."""
        client = self._get_client()
        try:
            resp = client.describe_contact(
                InstanceId=self._instance_id,
                ContactId=activity_id,
            )
            contact = resp.get("Contact", {})
            has_initiation = bool(contact.get("InitiationTimestamp"))
            return has_initiation
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            raise
