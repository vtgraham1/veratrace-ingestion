"""
Tests for ServiceNow connector: AI attribution, incident signals, config.
"""
from __future__ import annotations

import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.connectors.servicenow.signal_mapper import (
    map_incident_to_signals, _classify_actor, _extract_value,
)
from src.connectors.servicenow.connector import ServiceNowConnector
from src.connectors.servicenow.schema import REQUIRED_FIELDS, EXPECTED_FIELDS

FIXTURES = os.path.join(
    os.path.dirname(__file__), "../../src/connectors/servicenow/test_fixtures"
)


def _load(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return json.load(f)


# ── AI-Resolved Incident ──────────────────────────────────────────────────────


class TestAIResolved:
    """Virtual Agent fully resolved the incident."""

    @pytest.fixture
    def incident(self):
        return _load("incident_ai_resolved.json")

    @pytest.fixture
    def audit_records(self):
        return [
            {
                "sys_id": {"value": "aud001"},
                "documentkey": {"value": "a1b2c3d4e5f6"},
                "fieldname": {"value": "state", "display_value": "State"},
                "oldvalue": {"value": "1", "display_value": "New"},
                "newvalue": {"value": "6", "display_value": "Resolved"},
                "user": {"value": "va001", "display_value": "Virtual Agent"},
                "sys_created_on": {"value": "2026-04-10 09:03:00", "display_value": "2026-04-10 09:03:00"},
            },
        ]

    def test_produces_incident_created(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        created = [s for s in signals if s.name == "incident_created"]
        assert len(created) == 1

    def test_produces_ai_interaction(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"]
        assert len(ai) == 1
        assert ai[0].actor_type == "AI"
        assert ai[0].payload["ai_actor"] == "Virtual Agent"

    def test_resolved_by_ai(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "incident_resolved"]
        assert len(resolved) == 1
        assert resolved[0].payload["resolved_by_type"] == "AI"

    def test_resolution_time_calculated(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "incident_resolved"][0]
        assert resolved.payload["resolution_seconds"] == 180  # 3 minutes

    def test_signal_count(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        # incident_created + ai_interaction + incident_resolved (no agent_assigned since VA)
        assert len(signals) == 3

    def test_raw_preserved(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        assert "_raw" in signals[-1].payload


# ── Human-Resolved Incident ──────────────────────────────────────────────────


class TestHumanResolved:
    """Human agent resolved the incident — no AI involvement."""

    @pytest.fixture
    def incident(self):
        return _load("incident_human_resolved.json")

    @pytest.fixture
    def audit_records(self):
        return [
            {
                "sys_id": {"value": "aud002"},
                "documentkey": {"value": "b2c3d4e5f6a1"},
                "fieldname": {"value": "state", "display_value": "State"},
                "oldvalue": {"value": "2", "display_value": "In Progress"},
                "newvalue": {"value": "6", "display_value": "Resolved"},
                "user": {"value": "usr002", "display_value": "Mike Chen"},
                "sys_created_on": {"value": "2026-04-10 10:15:00", "display_value": "2026-04-10 10:15:00"},
            },
        ]

    def test_no_ai_interaction(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"]
        assert len(ai) == 0

    def test_resolved_by_human(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "incident_resolved"]
        assert len(resolved) == 1
        assert resolved[0].payload["resolved_by_type"] == "HUMAN"
        assert resolved[0].payload["resolved_by"] == "Mike Chen"

    def test_agent_assigned(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        assigned = [s for s in signals if s.name == "agent_assigned"]
        assert len(assigned) == 1
        assert assigned[0].payload["assigned_to"] == "Mike Chen"
        assert assigned[0].actor_type == "HUMAN"

    def test_signal_count(self, incident, audit_records):
        signals = map_incident_to_signals(incident, audit_records, "inst-1", "acc-1")
        # incident_created + agent_assigned + incident_resolved
        assert len(signals) == 3


# ── In-Progress Incident (Virtual Agent contact) ─────────────────────────────


class TestVirtualAgentContact:
    """Incident opened via Virtual Agent, still in progress."""

    @pytest.fixture
    def incident(self):
        return _load("incident_virtual_agent.json")

    def test_no_resolved_signal(self, incident):
        signals = map_incident_to_signals(incident, [], "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "incident_resolved"]
        assert len(resolved) == 0

    def test_has_incident_created(self, incident):
        signals = map_incident_to_signals(incident, [], "inst-1", "acc-1")
        created = [s for s in signals if s.name == "incident_created"]
        assert len(created) == 1
        assert created[0].payload["contact_type"] == "Virtual Agent"

    def test_agent_assigned_human(self, incident):
        signals = map_incident_to_signals(incident, [], "inst-1", "acc-1")
        assigned = [s for s in signals if s.name == "agent_assigned"]
        assert len(assigned) == 1
        assert assigned[0].payload["assigned_to"] == "Sarah Williams"


# ── Actor Classification ─────────────────────────────────────────────────────


class TestClassifyActor:

    def test_virtual_agent_is_ai(self):
        actor_type, actor_id = _classify_actor("Virtual Agent")
        assert actor_type == "AI"

    def test_now_assist_is_ai(self):
        actor_type, _ = _classify_actor("Now Assist Bot")
        assert actor_type == "AI"

    def test_regular_user_is_human(self):
        actor_type, actor_id = _classify_actor("Jane Smith")
        assert actor_type == "HUMAN"
        assert actor_id == "Jane Smith"

    def test_system_account_is_system(self):
        actor_type, _ = _classify_actor("System Administrator")
        assert actor_type == "SYSTEM"

    def test_workflow_is_system(self):
        actor_type, _ = _classify_actor("Flow Designer Automation")
        assert actor_type == "SYSTEM"

    def test_empty_is_system(self):
        actor_type, _ = _classify_actor("", "")
        assert actor_type == "SYSTEM"

    def test_chatbot_is_ai(self):
        actor_type, _ = _classify_actor("IT Support Chatbot")
        assert actor_type == "AI"

    def test_predictive_intelligence_is_ai(self):
        actor_type, _ = _classify_actor("Predictive Intelligence Service")
        assert actor_type == "AI"


# ── Display Value Extraction ─────────────────────────────────────────────────


class TestExtractValue:

    def test_dict_format(self):
        assert _extract_value({"value": "raw", "display_value": "Pretty"}) == "Pretty"

    def test_plain_string(self):
        assert _extract_value("hello") == "hello"

    def test_none(self):
        assert _extract_value(None) == ""

    def test_empty_display_falls_back(self):
        assert _extract_value({"value": "raw", "display_value": ""}) == "raw"


# ── Connector Config ─────────────────────────────────────────────────────────


class TestConnectorConfig:

    def test_servicenow_config(self):
        assert ServiceNowConnector.CONFIG["rate_limit_rps"] == 5.0
        assert ServiceNowConnector.CONFIG["cursor_format"] == "iso8601"
        assert ServiceNowConnector.CONFIG["max_results_per_page"] == 100

    def test_validate_credentials_requires_all(self):
        c = ServiceNowConnector(
            instance_id="test", integration_account_id="test",
            credentials={
                "instance_url": "https://dev123.service-now.com",
                "client_id": "abc",
                "client_secret": "xyz",
            },
            external_identity={"tenantId": "dev123"},
        )
        assert c.validate_credentials() is True

    def test_validate_credentials_missing_url(self):
        c = ServiceNowConnector(
            instance_id="test", integration_account_id="test",
            credentials={"client_id": "abc", "client_secret": "xyz"},
            external_identity={},
        )
        assert c.validate_credentials() is False

    def test_detect_region_eu(self):
        c = ServiceNowConnector(
            instance_id="test", integration_account_id="test",
            credentials={"instance_url": "https://eu-prod.service-now.com", "client_id": "a", "client_secret": "b"},
            external_identity={},
        )
        assert c.detect_region() == "eu"

    def test_detect_region_us(self):
        c = ServiceNowConnector(
            instance_id="test", integration_account_id="test",
            credentials={"instance_url": "https://dev12345.service-now.com", "client_id": "a", "client_secret": "b"},
            external_identity={},
        )
        assert c.detect_region() == "us"


# ── Auto-Discovery ───────────────────────────────────────────────────────────


class TestAutoDiscovery:

    def test_servicenow_auto_discovered(self):
        from src.connectors import CONNECTOR_MAP
        assert "servicenow" in CONNECTOR_MAP
        assert CONNECTOR_MAP["servicenow"] is ServiceNowConnector


# ── Source Integration ────────────────────────────────────────────────────────


class TestSourceIntegration:

    def test_all_signals_source_is_servicenow(self):
        incident = _load("incident_ai_resolved.json")
        audit = [
            {
                "sys_id": {"value": "aud001"},
                "documentkey": {"value": "a1b2c3d4e5f6"},
                "fieldname": {"value": "state"},
                "oldvalue": {"value": "1"},
                "newvalue": {"value": "6"},
                "user": {"value": "va001", "display_value": "Virtual Agent"},
                "sys_created_on": {"value": "2026-04-10 09:03:00"},
            },
        ]
        signals = map_incident_to_signals(incident, audit, "inst-1", "acc-1")
        for s in signals:
            assert s.source_integration == "servicenow"


# ── Degraded Signals ─────────────────────────────────────────────────────────


class TestDegradedSignals:

    def test_missing_required_field_is_degraded(self):
        incident = _load("incident_ai_resolved.json")
        # Remove a required field
        del incident["number"]
        signals = map_incident_to_signals(incident, [], "inst-1", "acc-1")
        created = [s for s in signals if s.name == "incident_created"][0]
        assert created.degraded is True
        assert "number" in created.degraded_reason

    def test_complete_record_not_degraded(self):
        incident = _load("incident_ai_resolved.json")
        signals = map_incident_to_signals(incident, [], "inst-1", "acc-1")
        created = [s for s in signals if s.name == "incident_created"][0]
        assert created.degraded is False


# ── Schema ────────────────────────────────────────────────────────────────────


class TestSchema:

    def test_required_fields_subset_of_expected(self):
        assert REQUIRED_FIELDS.issubset(EXPECTED_FIELDS)

    def test_fixture_has_expected_fields(self):
        incident = _load("incident_ai_resolved.json")
        for field in REQUIRED_FIELDS:
            assert field in incident, f"Fixture missing required field: {field}"
