"""
Unit tests for GET /stats/instances/{id}/* endpoints.

Mocks both the control plane forward call (auth/membership check) and
Supabase view queries so tests never hit real infra.
"""
import io
import json
import os
import sys
import threading
import urllib.error
import urllib.request
from http.server import HTTPServer
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("INGESTION_API_KEY", "test-key-abc123")
os.environ["SUPABASE_URL"] = "https://fake.supabase.test"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "test-service-key"

from src.main import IngestionHandler  # noqa: E402
from src import main as main_module  # noqa: E402


@pytest.fixture(scope="module")
def server():
    main_module._rate_tracker.clear()
    srv = HTTPServer(("127.0.0.1", 0), IngestionHandler)
    port = srv.server_address[1]
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    srv.shutdown()


@pytest.fixture(autouse=True)
def clear_rate_limit():
    main_module._rate_tracker.clear()
    yield


@pytest.fixture(autouse=True)
def fake_supabase_url(monkeypatch):
    """Override src.main.SUPABASE_URL for the duration of each test.

    Module-level import captured the value at load time (""); our test env
    needs a real-shaped URL so _supabase_get builds valid URLs that urlopen
    can parse (even though urlopen is mocked, urllib.request.Request still
    parses the URL at construction time).
    """
    monkeypatch.setattr(main_module, "SUPABASE_URL", "https://fake.supabase.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
    yield


def _get(url, headers=None):
    req = urllib.request.Request(url, method="GET")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw) if raw else {}
        except Exception:
            return e.code, {"raw": raw.decode("utf-8", "replace")}


class _FakeResp:
    """Minimal context-manager response for mocked urlopen."""
    def __init__(self, body, status=200):
        self._body = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode()
        self.status = status
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def read(self): return self._body


_REAL_URLOPEN = urllib.request.urlopen


def _make_dispatcher(fixtures):
    """Return a urlopen-compatible callable that dispatches by URL substring.

    fixtures is a list of (url_substring, response_or_exception).
    The first matching fixture wins. Non-match raises AssertionError.
    Localhost URLs pass through to the real urlopen — the test client
    uses urlopen too and must reach the test server without interception.
    """
    def fake_urlopen(req, timeout=None, *args, **kwargs):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if "127.0.0.1" in url or "localhost" in url:
            return _REAL_URLOPEN(req, timeout=timeout, *args, **kwargs)
        for substr, resp in fixtures:
            if substr in url:
                if isinstance(resp, Exception):
                    raise resp
                return resp
        raise AssertionError(f"No fixture matched URL: {url}")
    return fake_urlopen


INSTANCE_UUID = "8af0894e-fd93-4f53-82b8-67fef4b7dc73"
ACCOUNT_UUID  = "15134f42-7bd7-4ec0-9abd-7e9a1ca10f91"

JOEYS_ACCOUNTS_RESPONSE = [
    {
        "integrationAccountId": ACCOUNT_UUID,
        "integrationId": "amazon-connect",
        "name": "Test AC",
        "status": "ACTIVE",
        "health": "HEALTHY",
        "externalIdentity": {"tenantId": "arn:aws:connect:us-west-2:111:instance/aaa"},
        "authCredentials": {"roleArn": "arn:aws:iam::111:role/x", "externalId": "e"},
    }
]

SUPABASE_STATS_RESPONSE = [
    {"instance_id": INSTANCE_UUID, "integration_account_id": ACCOUNT_UUID,
     "twu_count": 12450, "instance_count": 3, "last_sync": "2026-04-17T14:23:00Z"}
]

SUPABASE_BREAKDOWN_RESPONSE = [
    {"instance_id": INSTANCE_UUID, "integration_account_id": ACCOUNT_UUID,
     "tenant_id": "arn:aws:connect:...aaa", "twu_count": 4500},
    {"instance_id": INSTANCE_UUID, "integration_account_id": ACCOUNT_UUID,
     "tenant_id": "arn:aws:connect:...bbb", "twu_count": 5200},
]


# ── /stats/instances/{id}/accounts ────────────────────────────────────────────

class TestStatsAccounts:
    def test_rejects_missing_auth_header(self, server):
        # No urlopen patch needed — the 401 path returns before any outbound call.
        status, body = _get(f"{server}/stats/instances/{INSTANCE_UUID}/accounts")
        assert status == 401
        assert "Authorization" in body.get("error", "")

    def test_forwards_to_control_plane_and_merges_stats(self, server):
        fixtures = [
            ("api.veratrace.app", _FakeResp(JOEYS_ACCOUNTS_RESPONSE)),
            ("v_account_stats",    _FakeResp(SUPABASE_STATS_RESPONSE)),
            ("v_account_instance_breakdown", _FakeResp(SUPABASE_BREAKDOWN_RESPONSE)),
        ]
        with patch("src.main.urllib.request.urlopen", side_effect=_make_dispatcher(fixtures)):
            status, body = _get(
                f"{server}/stats/instances/{INSTANCE_UUID}/accounts",
                headers={"Authorization": "Bearer fake-jwt"},
            )
        assert status == 200
        assert isinstance(body, list) and len(body) == 1
        acc = body[0]
        assert acc["integrationAccountId"] == ACCOUNT_UUID
        assert acc["integrationId"] == "amazon-connect"
        assert acc["name"] == "Test AC"
        assert acc["status"] == "ACTIVE"
        assert acc["twuCount"] == 12450
        assert acc["instances"] == 3
        assert acc["lastSync"] == "2026-04-17T14:23:00Z"
        assert len(acc["instanceTWUs"]) == 2
        assert acc["instanceTWUs"][0]["twuCount"] in (4500, 5200)

    def test_returns_account_with_zeros_when_no_signals_yet(self, server):
        """Account exists in Joey's API but has no rows in v_account_stats (Phase 1 pre-cutover state)."""
        fixtures = [
            ("api.veratrace.app", _FakeResp(JOEYS_ACCOUNTS_RESPONSE)),
            ("v_account_stats",    _FakeResp([])),
            ("v_account_instance_breakdown", _FakeResp([])),
        ]
        with patch("src.main.urllib.request.urlopen", side_effect=_make_dispatcher(fixtures)):
            status, body = _get(
                f"{server}/stats/instances/{INSTANCE_UUID}/accounts",
                headers={"Authorization": "Bearer fake-jwt"},
            )
        assert status == 200
        assert body[0]["twuCount"] == 0
        assert body[0]["instances"] == 0
        assert body[0]["lastSync"] is None
        assert body[0]["instanceTWUs"] == []

    def test_propagates_control_plane_401(self, server):
        err = urllib.error.HTTPError(
            url="https://api.veratrace.app/...", code=401, msg="Unauthorized",
            hdrs=None, fp=io.BytesIO(b"bad jwt"),
        )
        with patch("src.main.urllib.request.urlopen", side_effect=_make_dispatcher([
            ("api.veratrace.app", err),
        ])):
            status, body = _get(
                f"{server}/stats/instances/{INSTANCE_UUID}/accounts",
                headers={"Authorization": "Bearer bad-jwt"},
            )
        assert status == 401

    def test_propagates_control_plane_403(self, server):
        err = urllib.error.HTTPError(
            url="https://api.veratrace.app/...", code=403, msg="Forbidden",
            hdrs=None, fp=io.BytesIO(b"not a member"),
        )
        with patch("src.main.urllib.request.urlopen", side_effect=_make_dispatcher([
            ("api.veratrace.app", err),
        ])):
            status, body = _get(
                f"{server}/stats/instances/{INSTANCE_UUID}/accounts",
                headers={"Authorization": "Bearer other-user-jwt"},
            )
        assert status == 403

    def test_returns_502_on_supabase_failure(self, server):
        sb_err = urllib.error.HTTPError(
            url="https://fake.supabase.test/rest/v1/v_account_stats",
            code=500, msg="Internal",
            hdrs=None, fp=io.BytesIO(b"supabase down"),
        )
        with patch("src.main.urllib.request.urlopen", side_effect=_make_dispatcher([
            ("api.veratrace.app", _FakeResp(JOEYS_ACCOUNTS_RESPONSE)),
            ("v_account_stats", sb_err),
        ])):
            status, body = _get(
                f"{server}/stats/instances/{INSTANCE_UUID}/accounts",
                headers={"Authorization": "Bearer fake-jwt"},
            )
        assert status == 502
        # Error body should NOT leak mock data. Just a clean error message.
        assert "error" in body


# ── /stats/instances/{id}/accounts/{aid}/operations ───────────────────────────

class TestStatsOperations:
    def test_rejects_missing_auth(self, server):
        status, body = _get(f"{server}/stats/instances/{INSTANCE_UUID}/accounts/{ACCOUNT_UUID}/operations")
        assert status == 401

    def test_returns_runs_ordered_desc(self, server):
        runs = [
            {"run_id": "r1", "integration_account_id": ACCOUNT_UUID, "instance_id": INSTANCE_UUID,
             "status": "ok", "started_at": "2026-04-17T14:00:00Z", "finished_at": "2026-04-17T14:00:02Z",
             "signals_written": 42, "duration_ms": 1337, "error": None, "backfill": False},
            {"run_id": "r2", "integration_account_id": ACCOUNT_UUID, "instance_id": INSTANCE_UUID,
             "status": "error", "started_at": "2026-04-17T13:45:00Z", "finished_at": "2026-04-17T13:45:01Z",
             "signals_written": 0, "duration_ms": 200, "error": "HTTP 401", "backfill": False},
        ]
        fixtures = [
            ("api.veratrace.app", _FakeResp(JOEYS_ACCOUNTS_RESPONSE)),
            ("v_account_recent_runs", _FakeResp(runs)),
        ]
        with patch("src.main.urllib.request.urlopen", side_effect=_make_dispatcher(fixtures)):
            status, body = _get(
                f"{server}/stats/instances/{INSTANCE_UUID}/accounts/{ACCOUNT_UUID}/operations",
                headers={"Authorization": "Bearer fake-jwt"},
            )
        assert status == 200
        assert len(body) == 2
        assert body[0]["runId"] == "r1"
        assert body[0]["signalsWritten"] == 42
        assert body[0]["durationMs"] == 1337
        assert body[0]["status"] == "ok"
        assert body[0]["error"] is None
        assert body[1]["status"] == "error"
        assert body[1]["error"] == "HTTP 401"

    def test_rejects_account_not_in_instance(self, server):
        """If the account_id in the URL isn't in Joey's returned list, 404 — not 200 with empty."""
        fixtures = [
            ("api.veratrace.app", _FakeResp(JOEYS_ACCOUNTS_RESPONSE)),
        ]
        with patch("src.main.urllib.request.urlopen", side_effect=_make_dispatcher(fixtures)):
            status, body = _get(
                f"{server}/stats/instances/{INSTANCE_UUID}/accounts/some-other-account-uuid/operations",
                headers={"Authorization": "Bearer fake-jwt"},
            )
        assert status == 404

    def test_empty_runs_when_no_syncs_yet(self, server):
        fixtures = [
            ("api.veratrace.app", _FakeResp(JOEYS_ACCOUNTS_RESPONSE)),
            ("v_account_recent_runs", _FakeResp([])),
        ]
        with patch("src.main.urllib.request.urlopen", side_effect=_make_dispatcher(fixtures)):
            status, body = _get(
                f"{server}/stats/instances/{INSTANCE_UUID}/accounts/{ACCOUNT_UUID}/operations",
                headers={"Authorization": "Bearer fake-jwt"},
            )
        assert status == 200
        assert body == []


# ── Routing ──────────────────────────────────────────────────────────────────

class TestStatsRouting:
    def test_unknown_stats_path_returns_404(self, server):
        status, body = _get(
            f"{server}/stats/instances/{INSTANCE_UUID}/not-a-real-path",
            headers={"Authorization": "Bearer fake-jwt"},
        )
        assert status == 404

    def test_cors_options_includes_authorization(self, server):
        # Verify the Authorization header is allowed in CORS preflight.
        req = urllib.request.Request(
            f"{server}/stats/instances/{INSTANCE_UUID}/accounts",
            method="OPTIONS",
        )
        resp = urllib.request.urlopen(req, timeout=5)
        allowed = resp.headers.get("Access-Control-Allow-Headers", "")
        assert "Authorization" in allowed
