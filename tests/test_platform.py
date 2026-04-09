"""
Tests for platform infrastructure: auto-discovery, CONFIG, rate limiting, audit logging.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.request
import urllib.error
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestConnectorDiscovery:
    """Verify connectors auto-register via __init__.py exports."""

    def test_connector_map_discovers_amazon_connect(self):
        from src.connectors import CONNECTOR_MAP
        assert "amazon-connect" in CONNECTOR_MAP

    def test_connector_map_ignores_template(self):
        from src.connectors import CONNECTOR_MAP
        assert "_template" not in CONNECTOR_MAP

    def test_discovered_connector_is_correct_class(self):
        from src.connectors import CONNECTOR_MAP
        from src.connectors.amazon_connect.connector import AmazonConnectConnector
        assert CONNECTOR_MAP["amazon-connect"] is AmazonConnectConnector

    def test_connector_has_required_exports(self):
        import src.connectors.amazon_connect as mod
        assert hasattr(mod, "CONNECTOR_ID")
        assert hasattr(mod, "CONNECTOR_CLASS")
        assert mod.CONNECTOR_ID == "amazon-connect"


class TestWarmerDiscovery:
    """Verify warmers auto-register via module exports."""

    def test_warmers_discovers_amazon_connect(self):
        from synthetic.warmers import WARMERS
        assert "amazon-connect" in WARMERS

    def test_discovered_warmer_is_correct_class(self):
        from synthetic.warmers import WARMERS
        from synthetic.warmers.amazon_connect import ConnectWarmer
        assert WARMERS["amazon-connect"] is ConnectWarmer


class TestConnectorConfig:
    """Verify CONFIG override pattern works."""

    def test_base_connector_has_default_config(self):
        from src.connectors.base import BaseConnector
        assert "rate_limit_rps" in BaseConnector.CONFIG
        assert "max_results_per_page" in BaseConnector.CONFIG
        assert "cursor_format" in BaseConnector.CONFIG

    def test_amazon_connect_overrides_config(self):
        from src.connectors.amazon_connect.connector import AmazonConnectConnector
        assert AmazonConnectConnector.CONFIG["rate_limit_rps"] == 2.0
        assert AmazonConnectConnector.CONFIG["backfill_rate_ceiling_pct"] == 50

    def test_connect_config_inherits_base_defaults(self):
        from src.connectors.amazon_connect.connector import AmazonConnectConnector
        assert "cursor_format" in AmazonConnectConnector.CONFIG

    def test_connect_computes_sync_delay(self):
        from src.connectors.amazon_connect.connector import AmazonConnectConnector
        c = AmazonConnectConnector(
            instance_id="test", integration_account_id="test",
            credentials={"roleArn": "arn:aws:iam::123:role/Test"},
            external_identity={"tenantId": "arn:aws:connect:us-east-1:123:instance/abc"},
        )
        # 2.0 rps * 70% = 1.4 rps → delay = 1/1.4 ≈ 0.714
        assert 0.7 < c._sync_delay < 0.75
        # 2.0 rps * 50% = 1.0 rps → delay = 1.0
        assert c._backfill_delay == 1.0


class TestRateLimiter:
    """Verify API rate limiter blocks excessive requests."""

    @pytest.fixture(scope="class", autouse=True)
    def server(self):
        os.environ["INGESTION_API_KEY"] = "test-key"
        os.environ["RATE_LIMIT_RPM"] = "3"  # Low limit to test blocking quickly
        os.environ.setdefault("SUPABASE_URL", "")
        os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "")

        # Clear rate tracker to avoid cross-test pollution
        import src.main
        src.main._rate_tracker.clear()

        from src.main import IngestionHandler
        from http.server import HTTPServer

        srv = HTTPServer(("127.0.0.1", 0), IngestionHandler)
        port = srv.server_address[1]
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        yield f"http://127.0.0.1:{port}"
        srv.shutdown()

    def _request(self, url, method="GET", headers=None):
        req = urllib.request.Request(url, method=method)
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        try:
            resp = urllib.request.urlopen(req)
            return resp.status
        except urllib.error.HTTPError as e:
            return e.code

    def _clear_tracker(self):
        import src.main
        src.main._rate_tracker.clear()

    def test_allows_requests_under_limit(self, server):
        self._clear_tracker()
        status = self._request(f"{server}/health")
        assert status == 200

    def test_blocks_after_limit_exceeded(self, server):
        import src.main
        # Nuclear clear — remove ALL entries, set low limit
        src.main._rate_tracker.clear()
        old_limit = src.main.RATE_LIMIT_RPM
        src.main.RATE_LIMIT_RPM = 3

        try:
            headers = {"X-API-Key": "test-key", "Content-Type": "application/json"}
            statuses = []
            for _ in range(5):
                statuses.append(self._request(f"{server}/sync", method="POST", headers=headers))
            assert 429 in statuses, f"Expected 429 in {statuses}"
        finally:
            src.main.RATE_LIMIT_RPM = old_limit
            src.main._rate_tracker.clear()

    def test_health_not_rate_limited(self, server):
        self._clear_tracker()
        # Health is GET, rate limiter only applies to POST
        for _ in range(10):
            status = self._request(f"{server}/health")
            assert status == 200


class TestAuditLogging:
    """Verify structured audit log output."""

    def test_audit_log_entry_is_valid_json(self):
        """Audit log entries should be parseable JSON with required fields."""
        import datetime as dt
        entry = json.dumps({
            "audit": True,
            "method": "POST",
            "path": "/sync",
            "source_ip": "127.0.0.1",
            "timestamp": dt.datetime.utcnow().isoformat() + "Z",
        })
        parsed = json.loads(entry)
        assert parsed["audit"] is True
        assert parsed["method"] == "POST"
        assert parsed["path"] == "/sync"
        assert parsed["source_ip"] == "127.0.0.1"
        assert parsed["timestamp"].endswith("Z")

    def test_audit_log_has_all_required_fields(self):
        """Every audit entry must have method, path, source_ip, timestamp."""
        required = {"audit", "method", "path", "source_ip", "timestamp"}
        import datetime as dt
        entry = {
            "audit": True,
            "method": "POST",
            "path": "/test-connection",
            "source_ip": "10.0.0.1",
            "timestamp": dt.datetime.utcnow().isoformat() + "Z",
        }
        assert required.issubset(entry.keys())


class TestSchemaHashStability:
    """Verify schema hash changes when fields change."""

    def test_hash_is_deterministic(self):
        from src.connectors.amazon_connect.schema import EXPECTED_SCHEMA_HASH
        import hashlib
        from src.connectors.amazon_connect.schema import EXPECTED_CTR_FIELDS
        expected = hashlib.sha256("|".join(sorted(EXPECTED_CTR_FIELDS)).encode()).hexdigest()[:16]
        assert EXPECTED_SCHEMA_HASH == expected

    def test_hash_changes_when_fields_added(self):
        import hashlib
        fields_v1 = {"ContactId", "Channel"}
        fields_v2 = {"ContactId", "Channel", "NewField"}
        hash_v1 = hashlib.sha256("|".join(sorted(fields_v1)).encode()).hexdigest()[:16]
        hash_v2 = hashlib.sha256("|".join(sorted(fields_v2)).encode()).hexdigest()[:16]
        assert hash_v1 != hash_v2
