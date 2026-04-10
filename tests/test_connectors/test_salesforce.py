"""
Tests for Salesforce connector: signal mapping, schema, region detection.
"""
from __future__ import annotations

import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.connectors.salesforce.signal_mapper import map_records_to_signals
from src.connectors.salesforce.connector import SalesforceConnector
from src.connectors.salesforce.schema import REQUIRED_FIELDS, EXPECTED_CASE_FIELDS, EXPECTED_OPP_FIELDS

FIXTURES = os.path.join(os.path.dirname(__file__), "../../src/connectors/salesforce/test_fixtures")


def _load(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return json.load(f)


class TestCaseSignalMapper:
    """Case records → TwuSignals."""

    def test_ai_handled_case_produces_3_signals(self):
        case = _load("sample_case.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert names == ["case_created", "ai_interaction", "case_resolved"]

    def test_human_only_case_produces_2_signals(self):
        case = _load("sample_case_human.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert names == ["case_created", "case_resolved"]

    def test_ai_signal_has_bot_name_and_confidence(self):
        case = _load("sample_case.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["ai_agent"] == "CaseBot-v3"
        assert ai.payload["ai_confidence"] == 0.92
        assert ai.actor_type == "AI"

    def test_resolved_by_ai_when_confidence_high(self):
        case = _load("sample_case.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "case_resolved"][0]
        assert resolved.payload["resolved_by"] == "AI"

    def test_resolved_by_human_when_no_ai(self):
        case = _load("sample_case_human.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "case_resolved"][0]
        assert resolved.payload["resolved_by"] == "HUMAN"

    def test_case_created_has_subject_and_priority(self):
        case = _load("sample_case.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        created = signals[0]
        assert created.payload["subject"] == "Cannot access account — password reset needed"
        assert created.payload["priority"] == "High"

    def test_resolution_time_calculated(self):
        case = _load("sample_case.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        resolved = [s for s in signals if s.name == "case_resolved"][0]
        assert resolved.payload["resolution_seconds"] == 9000  # 2.5 hours

    def test_source_integration_is_salesforce(self):
        case = _load("sample_case.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        for s in signals:
            assert s.source_integration == "salesforce"

    def test_raw_record_preserved(self):
        case = _load("sample_case.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        assert "_raw" in signals[-1].payload

    def test_pii_fields_flagged(self):
        case = _load("sample_case.json")
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        created = signals[0]
        assert "Description" in created.pii_encrypted_fields

    def test_open_case_no_resolved_signal(self):
        case = _load("sample_case.json")
        case["IsClosed"] = False
        case.pop("ClosedDate", None)
        signals = map_records_to_signals(case, "Case", "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert "case_resolved" not in names


class TestOpportunitySignalMapper:

    def test_closed_won_produces_2_signals(self):
        opp = _load("sample_opportunity.json")
        signals = map_records_to_signals(opp, "Opportunity", "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert names == ["opp_created", "opp_closed"]

    def test_opp_closed_shows_won_and_amount(self):
        opp = _load("sample_opportunity.json")
        signals = map_records_to_signals(opp, "Opportunity", "inst-1", "acc-1")
        closed = [s for s in signals if s.name == "opp_closed"][0]
        assert closed.payload["won"] is True
        assert closed.payload["amount"] == 125000.0

    def test_open_opp_produces_1_signal(self):
        opp = _load("sample_opportunity.json")
        opp["IsClosed"] = False
        opp["IsWon"] = False
        signals = map_records_to_signals(opp, "Opportunity", "inst-1", "acc-1")
        assert len(signals) == 1
        assert signals[0].name == "opp_created"


class TestConnectorConfig:

    def test_salesforce_config_overrides(self):
        assert SalesforceConnector.CONFIG["rate_limit_rps"] == 5.0
        assert SalesforceConnector.CONFIG["max_results_per_page"] == 2000
        assert SalesforceConnector.CONFIG["cursor_format"] == "iso8601"

    def test_region_detection_from_instance_url(self):
        c = SalesforceConnector(
            instance_id="test", integration_account_id="test",
            credentials={"instance_url": "https://na1.salesforce.com", "access_token": "x"},
            external_identity={"tenantId": "orgid"},
        )
        assert c.detect_region() == "us"

    def test_region_eu(self):
        c = SalesforceConnector(
            instance_id="test", integration_account_id="test",
            credentials={"instance_url": "https://eu5.salesforce.com", "access_token": "x"},
            external_identity={"tenantId": "orgid"},
        )
        assert c.detect_region() == "eu"

    def test_region_apac(self):
        c = SalesforceConnector(
            instance_id="test", integration_account_id="test",
            credentials={"instance_url": "https://ap4.salesforce.com", "access_token": "x"},
            external_identity={"tenantId": "orgid"},
        )
        assert c.detect_region() == "apac"


class TestSchema:

    def test_required_fields_subset_of_case_fields(self):
        assert REQUIRED_FIELDS.issubset(EXPECTED_CASE_FIELDS)

    def test_required_fields_subset_of_opp_fields(self):
        assert REQUIRED_FIELDS.issubset(EXPECTED_OPP_FIELDS)

    def test_degraded_when_missing_required(self):
        incomplete = {"Id": "test", "Status": "Open"}  # missing CreatedDate, SystemModstamp
        signals = map_records_to_signals(incomplete, "Case", "inst-1", "acc-1")
        # No signals produced since CreatedDate is missing
        assert len(signals) == 0


class TestAutoDiscovery:

    def test_salesforce_auto_discovered(self):
        from src.connectors import CONNECTOR_MAP
        assert "salesforce" in CONNECTOR_MAP
        assert CONNECTOR_MAP["salesforce"] is SalesforceConnector
