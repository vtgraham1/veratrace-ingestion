"""
Tests for Freshdesk connector: Freddy attribution, ticket signals, config.
"""
from __future__ import annotations

import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.connectors.freshdesk.signal_mapper import (
    map_ticket_to_signals, _classify_agent,
)
from src.connectors.freshdesk.connector import FreshdeskConnector
from src.connectors.freshdesk.schema import REQUIRED_FIELDS, EXPECTED_FIELDS

FIXTURES = os.path.join(
    os.path.dirname(__file__), "../../src/connectors/freshdesk/test_fixtures"
)

# Mock agent cache — bot agent (9001) and human agent (9002)
AGENT_CACHE = {
    9001: {"name": "Freddy Bot", "email": "freddy@freshdesk.com", "agent_type": "", "active": True},
    9002: {"name": "Mike Chen", "email": "mike@company.com", "agent_type": "", "active": True},
}

# Mock conversations
FREDDY_CONVERSATIONS = [
    {"user_id": 9001, "body": "You can reset your password at Settings → Security.", "created_at": "2026-04-10T14:00:30Z", "source": 2},
]

AGENT_CONVERSATIONS = [
    {"user_id": 9002, "body": "Increased connection pool to 200. Restarting app servers.", "created_at": "2026-04-09T14:35:00Z", "source": 2},
    {"user_id": 9002, "body": "Root cause identified. Deploying fix.", "created_at": "2026-04-10T09:00:00Z", "source": 2},
]

HANDOFF_CONVERSATIONS = [
    {"user_id": 9001, "body": "I understand you'd like to dispute a charge. Let me connect you with billing.", "created_at": "2026-04-10T16:01:00Z", "source": 2},
    {"user_id": 9002, "body": "I've reviewed the charge and issued a refund.", "created_at": "2026-04-10T16:10:00Z", "source": 2},
]


def _load(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return json.load(f)


# ── Freddy-Resolved Ticket ────────────────────────────────────────────────────


class TestFreddyResolved:
    """Freddy Bot fully resolved the ticket — no human involvement."""

    @pytest.fixture
    def ticket(self):
        return _load("ticket_freddy_resolved.json")

    def test_produces_freddy_interaction(self, ticket):
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        freddy = [s for s in signals if s.name == "freddy_interaction"]
        assert len(freddy) == 1
        assert freddy[0].actor_type == "AI"

    def test_freddy_contained(self, ticket):
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        freddy = [s for s in signals if s.name == "freddy_interaction"][0]
        assert freddy.payload["contained"] is True
        assert freddy.payload["ai_agent"] == "Freddy Bot"

    def test_no_agent_replied(self, ticket):
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        human = [s for s in signals if s.name == "agent_replied"]
        assert len(human) == 0

    def test_resolved_by_ai(self, ticket):
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "ticket_resolved"]
        assert len(resolved) == 1
        assert resolved[0].payload["resolved_by_type"] == "AI"
        assert resolved[0].payload["freddy_contained"] is True

    def test_resolution_time(self, ticket):
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "ticket_resolved"][0]
        assert resolved.payload["resolution_seconds"] == 120  # 2 minutes

    def test_signal_count(self, ticket):
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        # ticket_created + freddy_interaction + ticket_resolved
        assert len(signals) == 3


# ── Agent-Resolved Ticket ─────────────────────────────────────────────────────


class TestAgentResolved:
    """Human agent resolved — no Freddy involvement."""

    @pytest.fixture
    def ticket(self):
        return _load("ticket_agent_resolved.json")

    def test_no_freddy_interaction(self, ticket):
        signals = map_ticket_to_signals(ticket, AGENT_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        freddy = [s for s in signals if s.name == "freddy_interaction"]
        assert len(freddy) == 0

    def test_has_agent_replied(self, ticket):
        signals = map_ticket_to_signals(ticket, AGENT_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        agent = [s for s in signals if s.name == "agent_replied"]
        assert len(agent) == 1
        assert agent[0].payload["agent_name"] == "Mike Chen"

    def test_resolved_by_human(self, ticket):
        signals = map_ticket_to_signals(ticket, AGENT_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "ticket_resolved"][0]
        assert resolved.payload["resolved_by_type"] == "HUMAN"
        assert resolved.payload["freddy_contained"] is False

    def test_priority_urgent(self, ticket):
        signals = map_ticket_to_signals(ticket, AGENT_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        created = [s for s in signals if s.name == "ticket_created"][0]
        assert created.payload["priority"] == "Urgent"

    def test_signal_count(self, ticket):
        signals = map_ticket_to_signals(ticket, AGENT_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        # ticket_created + agent_replied + ticket_resolved
        assert len(signals) == 3


# ── Freddy → Agent Handoff ────────────────────────────────────────────────────


class TestFreddyHandoff:
    """Freddy triaged, then handed off to human agent."""

    @pytest.fixture
    def ticket(self):
        return _load("ticket_freddy_handoff.json")

    def test_has_both(self, ticket):
        signals = map_ticket_to_signals(ticket, HANDOFF_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert "freddy_interaction" in names
        assert "agent_replied" in names

    def test_freddy_not_contained(self, ticket):
        signals = map_ticket_to_signals(ticket, HANDOFF_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        freddy = [s for s in signals if s.name == "freddy_interaction"][0]
        assert freddy.payload["contained"] is False

    def test_agent_after_freddy(self, ticket):
        signals = map_ticket_to_signals(ticket, HANDOFF_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        agent = [s for s in signals if s.name == "agent_replied"][0]
        assert agent.payload["after_ai"] is True

    def test_resolved_by_human(self, ticket):
        signals = map_ticket_to_signals(ticket, HANDOFF_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "ticket_resolved"][0]
        assert resolved.payload["resolved_by_type"] == "HUMAN"
        assert resolved.payload["freddy_to_agent"] is True

    def test_signal_count(self, ticket):
        signals = map_ticket_to_signals(ticket, HANDOFF_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        # ticket_created + freddy_interaction + agent_replied + ticket_resolved
        assert len(signals) == 4


# ── Agent Classification ──────────────────────────────────────────────────────


class TestClassifyAgent:

    def test_freddy_is_ai(self):
        actor_type, name = _classify_agent(9001, AGENT_CACHE)
        assert actor_type == "AI"
        assert name == "Freddy Bot"

    def test_human_agent(self):
        actor_type, name = _classify_agent(9002, AGENT_CACHE)
        assert actor_type == "HUMAN"
        assert name == "Mike Chen"

    def test_unknown_agent(self):
        actor_type, name = _classify_agent(9999, AGENT_CACHE)
        assert actor_type == "HUMAN"  # Default to human

    def test_none_is_system(self):
        actor_type, _ = _classify_agent(None, AGENT_CACHE)
        assert actor_type == "SYSTEM"

    def test_bot_keyword(self):
        cache = {100: {"name": "Support Bot v2", "email": "", "agent_type": "", "active": True}}
        actor_type, _ = _classify_agent(100, cache)
        assert actor_type == "AI"

    def test_auto_triage_keyword(self):
        cache = {200: {"name": "Auto-Triage Service", "email": "", "agent_type": "", "active": True}}
        actor_type, _ = _classify_agent(200, cache)
        assert actor_type == "AI"


# ── Connector Config ─────────────────────────────────────────────────────────


class TestConnectorConfig:

    def test_freshdesk_config(self):
        assert FreshdeskConnector.CONFIG["rate_limit_rps"] == 3.0
        assert FreshdeskConnector.CONFIG["cursor_format"] == "iso8601"
        assert FreshdeskConnector.CONFIG["max_results_per_page"] == 100

    def test_validate_credentials(self):
        c = FreshdeskConnector(
            instance_id="test", integration_account_id="test",
            credentials={"api_key": "abc123", "domain": "acme"},
            external_identity={},
        )
        assert c.validate_credentials() is True

    def test_validate_credentials_missing(self):
        c = FreshdeskConnector(
            instance_id="test", integration_account_id="test",
            credentials={"api_key": "", "domain": "acme"},
            external_identity={},
        )
        assert c.validate_credentials() is False

    def test_domain_cleanup(self):
        c = FreshdeskConnector(
            instance_id="test", integration_account_id="test",
            credentials={"api_key": "abc", "domain": "acme.freshdesk.com"},
            external_identity={},
        )
        assert c._domain == "acme"
        assert "acme.freshdesk.com" in c._base_url

    def test_region_is_global(self):
        c = FreshdeskConnector(
            instance_id="test", integration_account_id="test",
            credentials={"api_key": "abc", "domain": "acme"},
            external_identity={},
        )
        assert c.detect_region() == "global"


# ── Auto-Discovery ───────────────────────────────────────────────────────────


class TestAutoDiscovery:

    def test_freshdesk_auto_discovered(self):
        from src.connectors import CONNECTOR_MAP
        assert "freshdesk" in CONNECTOR_MAP
        assert CONNECTOR_MAP["freshdesk"] is FreshdeskConnector


# ── Source Integration ────────────────────────────────────────────────────────


class TestSourceIntegration:

    def test_all_signals_source_is_freshdesk(self):
        ticket = _load("ticket_freddy_resolved.json")
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        for s in signals:
            assert s.source_integration == "freshdesk"


# ── Degraded Signals ─────────────────────────────────────────────────────────


class TestDegradedSignals:

    def test_missing_required(self):
        ticket = _load("ticket_freddy_resolved.json")
        del ticket["subject"]
        signals = map_ticket_to_signals(ticket, [], AGENT_CACHE, "inst-1", "acc-1")
        created = [s for s in signals if s.name == "ticket_created"][0]
        assert created.degraded is True
        assert "subject" in created.degraded_reason

    def test_complete_not_degraded(self):
        ticket = _load("ticket_freddy_resolved.json")
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        created = [s for s in signals if s.name == "ticket_created"][0]
        assert created.degraded is False


# ── Schema ────────────────────────────────────────────────────────────────────


class TestSchema:

    def test_required_subset(self):
        assert REQUIRED_FIELDS.issubset(EXPECTED_FIELDS)

    def test_fixture_has_required(self):
        ticket = _load("ticket_freddy_resolved.json")
        for f in REQUIRED_FIELDS:
            assert f in ticket, f"Fixture missing: {f}"

    def test_raw_preserved(self):
        ticket = _load("ticket_freddy_resolved.json")
        signals = map_ticket_to_signals(ticket, FREDDY_CONVERSATIONS, AGENT_CACHE, "inst-1", "acc-1")
        assert "_raw" in signals[-1].payload
