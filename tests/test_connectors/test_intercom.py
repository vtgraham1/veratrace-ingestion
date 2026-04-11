"""
Tests for Intercom connector: Fin attribution, conversation signals, config.
"""
from __future__ import annotations

import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.connectors.intercom.signal_mapper import map_conversation_to_signals
from src.connectors.intercom.connector import IntercomConnector
from src.connectors.intercom.schema import REQUIRED_FIELDS, EXPECTED_CONVERSATION_FIELDS

FIXTURES = os.path.join(os.path.dirname(__file__), "../../src/connectors/intercom/test_fixtures")


def _load(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return json.load(f)


class TestFinResolved:
    """Fin fully resolved the conversation — no human involvement."""

    @pytest.fixture
    def conv(self):
        return _load("sample_conversation_fin_resolved.json")

    def test_produces_fin_interaction_signal(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        fin = [s for s in signals if s.name == "fin_interaction"]
        assert len(fin) == 1

    def test_fin_did_resolve_is_true(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        fin = [s for s in signals if s.name == "fin_interaction"][0]
        assert fin.payload["did_resolve"] is True
        assert fin.payload["resolution_state"] == "resolved"
        assert fin.payload["confidence"] == 1.0

    def test_fin_has_content_sources(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        fin = [s for s in signals if s.name == "fin_interaction"][0]
        assert "KB Article #123" in fin.payload["content_sources"]

    def test_fin_has_csat(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        fin = [s for s in signals if s.name == "fin_interaction"][0]
        assert fin.payload["csat_rating"] == 5

    def test_resolved_by_ai(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "conversation_resolved"][0]
        assert resolved.payload["resolved_by"] == "AI"
        assert resolved.payload["fin_resolved"] is True

    def test_no_agent_replied_signal(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        human = [s for s in signals if s.name == "agent_replied"]
        assert len(human) == 0

    def test_signal_type_is_ai(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        fin = [s for s in signals if s.name == "fin_interaction"][0]
        assert fin.type == "AI"
        assert fin.actor_type == "AI"


class TestFinEscalated:
    """Fin started but escalated to human agent."""

    @pytest.fixture
    def conv(self):
        return _load("sample_conversation_fin_escalated.json")

    def test_produces_both_fin_and_agent_signals(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert "fin_interaction" in names
        assert "agent_replied" in names

    def test_fin_did_not_resolve(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        fin = [s for s in signals if s.name == "fin_interaction"][0]
        assert fin.payload["did_resolve"] is False
        assert fin.payload["resolution_state"] == "escalated"
        assert fin.payload["confidence"] == 0.3

    def test_resolved_by_human(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "conversation_resolved"][0]
        assert resolved.payload["resolved_by"] == "HUMAN"

    def test_human_agent_identified(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        agent = [s for s in signals if s.name == "agent_replied"][0]
        assert agent.payload["admin_name"] == "Sarah Smith"
        assert agent.actor_type == "HUMAN"


class TestHumanOnly:
    """No Fin involvement — pure human conversation."""

    @pytest.fixture
    def conv(self):
        return _load("sample_conversation_human_only.json")

    def test_no_fin_interaction_signal(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        fin = [s for s in signals if s.name == "fin_interaction"]
        assert len(fin) == 0

    def test_has_agent_replied(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        agent = [s for s in signals if s.name == "agent_replied"]
        assert len(agent) == 1
        assert agent[0].payload["admin_name"] == "Marcus Johnson"

    def test_resolved_by_human(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "conversation_resolved"][0]
        assert resolved.payload["resolved_by"] == "HUMAN"
        assert resolved.payload["fin_involved"] is False

    def test_channel_is_email(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        created = [s for s in signals if s.name == "conversation_created"][0]
        assert created.payload["channel"] == "email"


class TestConversationMetrics:
    """Verify message counts and duration."""

    def test_message_counts(self):
        conv = _load("sample_conversation_fin_escalated.json")
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "conversation_resolved"][0]
        assert resolved.payload["bot_messages"] == 1
        assert resolved.payload["human_messages"] == 1

    def test_duration_calculated(self):
        conv = _load("sample_conversation_fin_resolved.json")
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "conversation_resolved"][0]
        assert resolved.payload["duration_seconds"] == 300  # 1712600300 - 1712600000

    def test_raw_conversation_preserved(self):
        conv = _load("sample_conversation_fin_resolved.json")
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        assert "_raw" in signals[-1].payload


class TestConnectorConfig:

    def test_intercom_config(self):
        assert IntercomConnector.CONFIG["rate_limit_rps"] == 16.0
        assert IntercomConnector.CONFIG["cursor_format"] == "token"
        assert IntercomConnector.CONFIG["max_results_per_page"] == 150

    def test_region_is_global(self):
        c = IntercomConnector(
            instance_id="test", integration_account_id="test",
            credentials={"accessToken": "test-token"},
            external_identity={"tenantId": "workspace-123"},
        )
        assert c.detect_region() == "global"


class TestAutoDiscovery:

    def test_intercom_auto_discovered(self):
        from src.connectors import CONNECTOR_MAP
        assert "intercom" in CONNECTOR_MAP
        assert CONNECTOR_MAP["intercom"] is IntercomConnector


class TestSourceIntegration:

    def test_all_signals_source_is_intercom(self):
        conv = _load("sample_conversation_fin_resolved.json")
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        for s in signals:
            assert s.source_integration == "intercom"
