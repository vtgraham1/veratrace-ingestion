"""
Tests for Genesys Cloud connector: bot attribution, conversation signals, config.
"""
from __future__ import annotations

import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.connectors.genesys.signal_mapper import map_conversation_to_signals
from src.connectors.genesys.connector import GenesysConnector, REGION_DOMAINS
from src.connectors.genesys.schema import REQUIRED_FIELDS, EXPECTED_FIELDS

FIXTURES = os.path.join(
    os.path.dirname(__file__), "../../src/connectors/genesys/test_fixtures"
)


def _load(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return json.load(f)


# ── Bot-Resolved Conversation ─────────────────────────────────────────────────


class TestBotResolved:
    """Bot fully contained the conversation — no human agent."""

    @pytest.fixture
    def conv(self):
        return _load("conversation_bot_resolved.json")

    def test_produces_bot_interaction(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        bot = [s for s in signals if s.name == "bot_interaction"]
        assert len(bot) == 1
        assert bot[0].actor_type == "AI"

    def test_bot_contained_is_true(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        bot = [s for s in signals if s.name == "bot_interaction"][0]
        assert bot.payload["contained"] is True
        assert bot.payload["bot_name"] == "Support Bot"

    def test_no_agent_interaction(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        agent = [s for s in signals if s.name == "agent_interaction"]
        assert len(agent) == 0

    def test_resolved_by_ai(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        ended = [s for s in signals if s.name == "conversation_ended"][0]
        assert ended.payload["resolved_by_type"] == "AI"
        assert ended.payload["bot_contained"] is True
        assert ended.payload["bot_to_agent"] is False

    def test_duration_calculated(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        ended = [s for s in signals if s.name == "conversation_ended"][0]
        assert ended.payload["duration_seconds"] == 180  # 3 minutes

    def test_signal_count(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        # conversation_started + bot_interaction + conversation_ended
        assert len(signals) == 3

    def test_raw_preserved(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        assert "_raw" in signals[-1].payload


# ── Agent-Resolved Conversation ───────────────────────────────────────────────


class TestAgentResolved:
    """Human agent resolved — no bot involvement."""

    @pytest.fixture
    def conv(self):
        return _load("conversation_agent_resolved.json")

    def test_no_bot_interaction(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        bot = [s for s in signals if s.name == "bot_interaction"]
        assert len(bot) == 0

    def test_has_agent_interaction(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        agent = [s for s in signals if s.name == "agent_interaction"]
        assert len(agent) == 1
        assert agent[0].actor_type == "HUMAN"
        assert agent[0].payload["agent_name"] == "Mike Chen"

    def test_resolved_by_human(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        ended = [s for s in signals if s.name == "conversation_ended"][0]
        assert ended.payload["resolved_by_type"] == "HUMAN"
        assert ended.payload["bot_contained"] is False

    def test_media_type_voice(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        started = [s for s in signals if s.name == "conversation_started"][0]
        assert started.payload["media_type"] == "voice"

    def test_signal_count(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        # conversation_started + agent_interaction + conversation_ended
        assert len(signals) == 3


# ── Bot → Agent Handoff ───────────────────────────────────────────────────────


class TestBotHandoff:
    """Bot started, then handed off to human agent."""

    @pytest.fixture
    def conv(self):
        return _load("conversation_bot_handoff.json")

    def test_has_both_bot_and_agent(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert "bot_interaction" in names
        assert "agent_interaction" in names

    def test_bot_not_contained(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        bot = [s for s in signals if s.name == "bot_interaction"][0]
        assert bot.payload["contained"] is False

    def test_agent_after_bot(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        agent = [s for s in signals if s.name == "agent_interaction"][0]
        assert agent.payload["after_bot"] is True

    def test_resolved_by_human(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        ended = [s for s in signals if s.name == "conversation_ended"][0]
        assert ended.payload["resolved_by_type"] == "HUMAN"
        assert ended.payload["bot_to_agent"] is True

    def test_signal_count(self, conv):
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        # conversation_started + bot_interaction + agent_interaction + conversation_ended
        assert len(signals) == 4


# ── Connector Config ─────────────────────────────────────────────────────────


class TestConnectorConfig:

    def test_genesys_config(self):
        assert GenesysConnector.CONFIG["rate_limit_rps"] == 5.0
        assert GenesysConnector.CONFIG["cursor_format"] == "iso8601"

    def test_validate_credentials(self):
        c = GenesysConnector(
            instance_id="test", integration_account_id="test",
            credentials={"client_id": "abc", "client_secret": "xyz", "region": "us-east-1"},
            external_identity={},
        )
        assert c.validate_credentials() is True

    def test_validate_credentials_missing(self):
        c = GenesysConnector(
            instance_id="test", integration_account_id="test",
            credentials={"client_id": "", "client_secret": "xyz"},
            external_identity={},
        )
        assert c.validate_credentials() is False

    def test_detect_region_us(self):
        c = GenesysConnector(
            instance_id="test", integration_account_id="test",
            credentials={"client_id": "a", "client_secret": "b", "region": "us-east-1"},
            external_identity={},
        )
        assert c.detect_region() == "us"

    def test_detect_region_eu(self):
        c = GenesysConnector(
            instance_id="test", integration_account_id="test",
            credentials={"client_id": "a", "client_secret": "b", "region": "eu-central-1"},
            external_identity={},
        )
        assert c.detect_region() == "eu"

    def test_detect_region_apac(self):
        c = GenesysConnector(
            instance_id="test", integration_account_id="test",
            credentials={"client_id": "a", "client_secret": "b", "region": "ap-southeast-2"},
            external_identity={},
        )
        assert c.detect_region() == "apac"

    def test_resolve_domain_from_alias(self):
        c = GenesysConnector(
            instance_id="test", integration_account_id="test",
            credentials={"client_id": "a", "client_secret": "b", "region": "eu"},
            external_identity={},
        )
        assert c._api_domain == "mypurecloud.de"

    def test_resolve_domain_passthrough(self):
        c = GenesysConnector(
            instance_id="test", integration_account_id="test",
            credentials={"client_id": "a", "client_secret": "b", "region": "custom.pure.cloud"},
            external_identity={},
        )
        assert c._api_domain == "custom.pure.cloud"


# ── Region Domains ────────────────────────────────────────────────────────────


class TestRegionDomains:

    def test_all_regions_have_domains(self):
        for region, domain in REGION_DOMAINS.items():
            assert "." in domain, f"Region {region} has invalid domain: {domain}"

    def test_us_east_default(self):
        assert REGION_DOMAINS["us-east-1"] == "mypurecloud.com"


# ── Auto-Discovery ───────────────────────────────────────────────────────────


class TestAutoDiscovery:

    def test_genesys_auto_discovered(self):
        from src.connectors import CONNECTOR_MAP
        assert "genesys" in CONNECTOR_MAP
        assert CONNECTOR_MAP["genesys"] is GenesysConnector


# ── Source Integration ────────────────────────────────────────────────────────


class TestSourceIntegration:

    def test_all_signals_source_is_genesys(self):
        conv = _load("conversation_bot_resolved.json")
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        for s in signals:
            assert s.source_integration == "genesys"


# ── Degraded Signals ─────────────────────────────────────────────────────────


class TestDegradedSignals:

    def test_missing_required_field(self):
        conv = _load("conversation_bot_resolved.json")
        del conv["conversationId"]
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        started = [s for s in signals if s.name == "conversation_started"][0]
        assert started.degraded is True
        assert "conversationId" in started.degraded_reason

    def test_complete_not_degraded(self):
        conv = _load("conversation_bot_resolved.json")
        signals = map_conversation_to_signals(conv, "inst-1", "acc-1")
        started = [s for s in signals if s.name == "conversation_started"][0]
        assert started.degraded is False


# ── Schema ────────────────────────────────────────────────────────────────────


class TestSchema:

    def test_required_subset_of_expected(self):
        assert REQUIRED_FIELDS.issubset(EXPECTED_FIELDS)

    def test_fixture_has_required(self):
        conv = _load("conversation_bot_resolved.json")
        for f in REQUIRED_FIELDS:
            assert f in conv, f"Fixture missing: {f}"
