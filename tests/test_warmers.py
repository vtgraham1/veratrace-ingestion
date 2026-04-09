"""
Tests for the warming system — validates base warmer behavior
and Connect warmer API call patterns.
"""
from __future__ import annotations

import os
import sys
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from synthetic.warmers.base import BaseWarmer, WarmResult
from synthetic.warmers.amazon_connect import ConnectWarmer, CUSTOMER_NAMES, CONTACT_SCENARIOS, _SCENARIO_WEIGHTS


# ── Base Warmer Tests ────────────────────────────────────────────────────────

class ConcreteWarmer(BaseWarmer):
    """Test implementation of BaseWarmer."""

    def __init__(self):
        super().__init__(credentials={}, external_identity={})
        self.created_ids = []
        self.verified_ids = []
        self.should_fail_create = False
        self.should_fail_verify = False

    def validate_access(self):
        return True

    def create_activity(self, scenario_config):
        if self.should_fail_create:
            raise RuntimeError("Simulated create failure")
        activity_id = f"test-{len(self.created_ids)}"
        self.created_ids.append(activity_id)
        return {"id": activity_id, "type": "test"}

    def verify_activity(self, activity_id):
        self.verified_ids.append(activity_id)
        return not self.should_fail_verify


class TestBaseWarmer:
    def test_warm_creates_requested_count(self):
        warmer = ConcreteWarmer()
        result = warmer.warm(count=3, delay_between=0, verify_delay=0)
        assert result.created == 3
        assert len(result.activity_ids) == 3

    def test_warm_verifies_all_created(self):
        warmer = ConcreteWarmer()
        result = warmer.warm(count=2, delay_between=0, verify_delay=0)
        assert result.verified == 2
        assert len(warmer.verified_ids) == 2

    def test_warm_handles_create_failure(self):
        warmer = ConcreteWarmer()
        warmer.should_fail_create = True
        result = warmer.warm(count=3, delay_between=0, verify_delay=0)
        assert result.created == 0
        assert result.failed == 3
        assert len(result.errors) == 3

    def test_warm_handles_verify_failure(self):
        warmer = ConcreteWarmer()
        warmer.should_fail_verify = True
        result = warmer.warm(count=2, delay_between=0, verify_delay=0)
        assert result.created == 2
        assert result.verified == 0

    def test_warm_result_dataclass(self):
        result = WarmResult()
        assert result.created == 0
        assert result.verified == 0
        assert result.failed == 0
        assert result.activity_ids == []
        assert result.errors == []


# ── Connect Warmer Tests ─────────────────────────────────────────────────────

class TestConnectWarmer:
    def _make_warmer(self):
        return ConnectWarmer(
            credentials={
                "roleArn": "arn:aws:iam::123456789012:role/TestRole",
                "externalId": "vt-test123",
            },
            external_identity={
                "tenantId": "arn:aws:connect:us-east-1:123456789012:instance/abc-def",
            },
        )

    def test_parses_region_from_arn(self):
        warmer = self._make_warmer()
        assert warmer._region == "us-east-1"

    def test_parses_instance_id_from_arn(self):
        warmer = self._make_warmer()
        assert warmer._instance_id == "abc-def"

    def test_customer_names_not_empty(self):
        assert len(CUSTOMER_NAMES) >= 10

    def test_contact_scenarios_cover_ai_human_patterns(self):
        assert len(CONTACT_SCENARIOS) >= 8
        # Verify we have all key patterns
        resolutions = {s["resolution"] for s in CONTACT_SCENARIOS}
        assert "ai_auto_resolved" in resolutions, "Missing AI-only scenario"
        assert "human_escalation" in resolutions, "Missing human escalation scenario"
        assert "human_resolved_after_ai" in resolutions, "Missing AI→human handoff"
        assert "vendor_ai_overclaimed" in resolutions, "Missing vendor reconciliation scenario"

    def test_all_scenarios_have_required_fields(self):
        for s in CONTACT_SCENARIOS:
            assert "reason" in s, f"Missing reason in {s}"
            assert "ai_handled" in s, f"Missing ai_handled in {s}"
            assert "resolution" in s, f"Missing resolution in {s}"
            assert "description" in s, f"Missing description in {s}"
            assert "weight" in s, f"Missing weight in {s}"

    def test_weights_sum_to_100(self):
        total = sum(_SCENARIO_WEIGHTS)
        assert total == 100, f"Weights sum to {total}, expected 100"

    def test_weight_distribution_matches_enterprise_pattern(self):
        # Group weights by category
        ai_resolved = sum(s["weight"] for s in CONTACT_SCENARIOS if s["resolution"] == "ai_auto_resolved")
        ai_to_human = sum(s["weight"] for s in CONTACT_SCENARIOS if s["resolution"] == "human_resolved_after_ai")
        human_only = sum(s["weight"] for s in CONTACT_SCENARIOS if s["resolution"] in ("human_escalation", "human_only"))
        sla = sum(s["weight"] for s in CONTACT_SCENARIOS if "sla" in s["resolution"])
        transfers = sum(s["weight"] for s in CONTACT_SCENARIOS if s.get("transferred"))
        vendor = sum(s["weight"] for s in CONTACT_SCENARIOS if s.get("vendor_claimed_ai"))

        # Enterprise reality: AI resolves 30-40%, rest needs humans
        assert 30 <= ai_resolved <= 40, f"AI auto-resolved weight {ai_resolved}% outside 30-40% range"
        assert ai_to_human >= 20, f"AI→human handoff weight {ai_to_human}% too low"
        assert human_only >= 10, f"Human-only weight {human_only}% too low"
        assert vendor >= 3, f"Vendor reconciliation weight {vendor}% — need at least some"

    @patch.object(ConnectWarmer, "_get_client")
    def test_validate_access_discovers_flow(self, mock_get_client):
        warmer = self._make_warmer()
        mock_client = MagicMock()
        mock_client.list_contact_flows.return_value = {
            "ContactFlowSummaryList": [
                {"Id": "flow-123", "Name": "Default inbound flow"},
            ]
        }
        mock_get_client.return_value = mock_client

        assert warmer.validate_access() is True
        assert warmer._contact_flow_id == "flow-123"

    @patch.object(ConnectWarmer, "_get_client")
    def test_validate_access_fails_without_flows(self, mock_get_client):
        warmer = self._make_warmer()
        mock_client = MagicMock()
        mock_client.list_contact_flows.return_value = {"ContactFlowSummaryList": []}
        mock_get_client.return_value = mock_client

        assert warmer.validate_access() is False

    @patch.object(ConnectWarmer, "_get_client")
    @patch.object(ConnectWarmer, "_discover_contact_flow", return_value="flow-123")
    def test_create_chat_contact(self, mock_discover, mock_get_client):
        warmer = self._make_warmer()
        mock_client = MagicMock()
        mock_client.start_chat_contact.return_value = {"ContactId": "contact-abc"}
        mock_get_client.return_value = mock_client

        result = warmer.create_activity({"task_ratio": 0.0})  # force chat
        assert result["id"] == "contact-abc"
        assert result["type"] == "CHAT"
        mock_client.start_chat_contact.assert_called_once()

    @patch.object(ConnectWarmer, "_get_client")
    @patch.object(ConnectWarmer, "_discover_contact_flow", return_value="flow-123")
    def test_create_task_contact(self, mock_discover, mock_get_client):
        warmer = self._make_warmer()
        mock_client = MagicMock()
        mock_client.start_task_contact.return_value = {"ContactId": "task-xyz"}
        mock_get_client.return_value = mock_client

        result = warmer.create_activity({"task_ratio": 1.0})  # force task
        assert result["id"] == "task-xyz"
        assert result["type"] == "TASK"
        mock_client.start_task_contact.assert_called_once()

    @patch.object(ConnectWarmer, "_get_client")
    def test_verify_activity_returns_true_when_found(self, mock_get_client):
        warmer = self._make_warmer()
        mock_client = MagicMock()
        mock_client.describe_contact.return_value = {
            "Contact": {"InitiationTimestamp": "2026-04-09T10:00:00Z"}
        }
        mock_get_client.return_value = mock_client

        assert warmer.verify_activity("contact-abc") is True

    @patch.object(ConnectWarmer, "_get_client")
    def test_verify_activity_returns_false_when_not_found(self, mock_get_client):
        from botocore.exceptions import ClientError
        warmer = self._make_warmer()
        mock_client = MagicMock()
        mock_client.describe_contact.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "DescribeContact",
        )
        mock_get_client.return_value = mock_client

        assert warmer.verify_activity("contact-abc") is False
