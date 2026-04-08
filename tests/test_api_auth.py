"""
Tests for ingestion API authentication.

Validates that POST endpoints require X-API-Key header
and GET /health is open (liveness check).
"""
import json
import os
import sys
import threading
import time
import urllib.request
import urllib.error
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Set a test API key before importing main
os.environ["INGESTION_API_KEY"] = "test-key-abc123"
os.environ.setdefault("SUPABASE_URL", "")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "")

from src.main import IngestionHandler, main
from http.server import HTTPServer


@pytest.fixture(scope="module")
def server():
    """Start the ingestion API on a random port for testing."""
    srv = HTTPServer(("127.0.0.1", 0), IngestionHandler)
    port = srv.server_address[1]
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    srv.shutdown()


def _request(url, method="GET", headers=None, body=None):
    """Make an HTTP request and return (status, body_dict)."""
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw) if raw else {}
        except (json.JSONDecodeError, ValueError):
            return e.code, {"raw": raw.decode("utf-8", errors="replace")}


class TestHealthEndpoint:
    def test_health_no_auth_required(self, server):
        status, body = _request(f"{server}/health")
        assert status == 200
        assert body["status"] == "ok"


class TestSyncEndpoint:
    def test_sync_rejects_without_api_key(self, server):
        status, body = _request(f"{server}/sync", method="POST", body={"integrationAccountId": "test"})
        assert status == 401
        assert "API key" in body.get("error", "")

    def test_sync_rejects_wrong_api_key(self, server):
        status, body = _request(
            f"{server}/sync", method="POST",
            headers={"X-API-Key": "wrong-key"},
            body={"integrationAccountId": "test"},
        )
        assert status == 401

    def test_sync_accepts_valid_api_key(self, server):
        status, body = _request(
            f"{server}/sync", method="POST",
            headers={"X-API-Key": "test-key-abc123"},
            body={"integrationAccountId": "test"},
        )
        # Should pass auth — may 500 on missing Supabase in test env, but NOT 401
        assert status != 401


class TestTestConnectionEndpoint:
    def test_rejects_without_api_key(self, server):
        status, body = _request(
            f"{server}/test-connection", method="POST",
            body={"roleArn": "test", "instanceArn": "test"},
        )
        assert status == 401

    def test_accepts_valid_api_key(self, server):
        status, body = _request(
            f"{server}/test-connection", method="POST",
            headers={"X-API-Key": "test-key-abc123"},
            body={"roleArn": "arn:aws:iam::123456789012:role/Test", "instanceArn": "arn:aws:connect:us-east-1:123456789012:instance/abc"},
        )
        # Should pass auth (will fail on AWS call, but that's expected)
        assert status in (200, 400, 500)
        assert status != 401
