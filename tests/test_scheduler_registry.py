"""
Unit tests for sync scheduler — fetch_active_accounts behavior, structured logging,
and the --diagnose code path.

The live counterpart lives in tests/contract/test_integration_accounts_registry.py
(env-gated, hits real infra). This file mocks urlopen and tests scheduler logic
in isolation so it always runs in CI.
"""
from __future__ import annotations

import io
import json
import logging
import os
import sys
import time
import urllib.error
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── logfmt helper ─────────────────────────────────────────────────────────────

class TestLogfmt:
    def test_basic_kv(self):
        from src.runtime.log import logfmt
        assert logfmt("foo", a=1, b="bar") == "event=foo a=1 b=bar"

    def test_quotes_values_with_spaces(self):
        from src.runtime.log import logfmt
        assert logfmt("e", msg="hello world") == 'event=e msg="hello world"'

    def test_escapes_embedded_quotes(self):
        from src.runtime.log import logfmt
        assert 'err="HTTP \\"401\\""' in logfmt("e", err='HTTP "401"')

    def test_omits_none_values(self):
        from src.runtime.log import logfmt
        out = logfmt("e", a=1, b=None, c=2)
        assert "b=" not in out
        assert "a=1" in out and "c=2" in out

    def test_escapes_newlines_so_multiline_traceback_stays_one_line(self):
        """A multiline error string must collapse into one logfmt line — otherwise
        downstream log parsers see a partial event with no closing quote."""
        from src.runtime.log import logfmt
        out = logfmt("e", error="line1\nline2\tcol\rend")
        assert "\n" not in out and "\r" not in out and "\t" not in out
        assert 'error="line1\\nline2\\tcol\\rend"' in out


# ── fetch_active_accounts ─────────────────────────────────────────────────────

@pytest.fixture
def fake_supabase_url(monkeypatch):
    """SUPABASE_URL is module-level in scheduler; patch so tests don't depend on .env."""
    from src.sync import scheduler
    monkeypatch.setattr(scheduler, "SUPABASE_URL", "https://fake.supabase.test")
    return "https://fake.supabase.test"


class TestFetchActiveAccounts:
    def test_returns_parsed_list_on_success(self, fake_supabase_url, mock_urlopen_response):
        from src.sync import scheduler
        fixture = [{"integration_account_id": "abc", "status": "ACTIVE"}]
        with patch("src.sync.scheduler.urllib.request.urlopen", return_value=mock_urlopen_response(fixture)):
            assert scheduler.fetch_active_accounts() == fixture

    def test_returns_empty_list_on_404(self, caplog, fake_supabase_url):
        from src.sync import scheduler
        err = urllib.error.HTTPError(url="http://test", code=404, msg="Not Found", hdrs=None, fp=None)
        with caplog.at_level(logging.ERROR, logger="sync"):
            with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=err):
                assert scheduler.fetch_active_accounts() == []
        # The 404 must be logged — silent failure is exactly the regression to avoid here.
        assert any("Failed to fetch integration accounts" in r.message for r in caplog.records)

    def test_appends_instance_id_filter_when_provided(self, fake_supabase_url, mock_urlopen_response):
        from src.sync import scheduler
        captured = {}

        def fake_urlopen(req, timeout=None):
            captured["url"] = req.full_url
            return mock_urlopen_response([])

        with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=fake_urlopen):
            scheduler.fetch_active_accounts(instance_id="inst-123")
        assert "instance_id=eq.inst-123" in captured["url"]

    def test_omits_instance_filter_when_none(self, fake_supabase_url, mock_urlopen_response):
        from src.sync import scheduler
        captured = {}

        def fake_urlopen(req, timeout=None):
            captured["url"] = req.full_url
            return mock_urlopen_response([])

        with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=fake_urlopen):
            scheduler.fetch_active_accounts()
        assert "instance_id=" not in captured["url"]


# ── sync_account structured logging ───────────────────────────────────────────

class TestSyncAccountLogging:
    def test_emits_start_and_end_events(self, caplog):
        """Every sync_account run produces exactly one start and one end event."""
        from src.sync import scheduler

        # Account with unknown integration → exits early as skipped_no_connector
        account = {
            "integration_id": "nonexistent",
            "integration_account_id": "abc12345xxxxx",
            "instance_id": "inst9876xxxxx",
        }
        with caplog.at_level(logging.INFO, logger="sync"):
            scheduler.sync_account(account)

        starts = [r.message for r in caplog.records if "event=sync_account_start" in r.message]
        ends = [r.message for r in caplog.records if "event=sync_account_end" in r.message]
        assert len(starts) == 1
        assert len(ends) == 1
        # Slicing length is a single source of truth — track scheduler.ID_LOG_PREFIX_LEN.
        prefix = "abc12345xxxxx"[:scheduler.ID_LOG_PREFIX_LEN]
        assert f"account_id={prefix}" in starts[0]
        assert "integration_id=nonexistent" in starts[0]
        assert f"status={scheduler.STATUS_SKIPPED_NO_CONNECTOR}" in ends[0]
        assert "duration_ms=" in ends[0]

    def test_end_event_emitted_even_on_exception(self, caplog):
        """The finally block must guarantee an end event so we never lose accounting."""
        from src.sync import scheduler

        boom = MagicMock()
        boom.return_value.validate_credentials.side_effect = RuntimeError("connector blew up")
        with patch.dict(scheduler.CONNECTOR_MAP, {"boom": boom}):
            account = {
                "integration_id": "boom",
                "integration_account_id": "xyz99999",
                "instance_id": "inst11111",
                "auth_credentials": {},
                "external_identity": {},
            }
            with caplog.at_level(logging.INFO, logger="sync"):
                with pytest.raises(RuntimeError):
                    scheduler.sync_account(account)

        ends = [r.message for r in caplog.records if "event=sync_account_end" in r.message]
        assert len(ends) == 1
        assert f"status={scheduler.STATUS_ERROR}" in ends[0]
        assert "connector blew up" in ends[0]


# ── M2M token + control plane fetch (Phase 1 Option B prep) ─────────────────

@pytest.fixture
def reset_m2m_cache():
    """Cache reset for tests that exercise _get_m2m_token; deterministic across runs."""
    from src.sync import scheduler
    scheduler._m2m_token_cache["token"] = None
    scheduler._m2m_token_cache["expires_at"] = 0.0
    yield
    scheduler._m2m_token_cache["token"] = None
    scheduler._m2m_token_cache["expires_at"] = 0.0


@pytest.fixture
def m2m_env(monkeypatch):
    """Sets the four M2M env vars to reasonable test values."""
    monkeypatch.setenv("M2M_CLIENT_ID", "cid")
    monkeypatch.setenv("M2M_CLIENT_SECRET", "csec")
    monkeypatch.setenv("M2M_TOKEN_ENDPOINT", "https://fake/oauth2/token")
    monkeypatch.setenv("M2M_SCOPE", "api/read")


def _fake_token_response(token="tok-abc", expires_in=3600):
    resp = MagicMock()
    resp.read.return_value = json.dumps({
        "access_token": token, "expires_in": expires_in, "token_type": "Bearer",
    }).encode()
    resp.__enter__ = lambda self: resp
    resp.__exit__ = lambda *a: False
    return resp


class TestM2MToken:
    def test_returns_none_when_env_missing(self, reset_m2m_cache, monkeypatch, caplog):
        from src.sync import scheduler
        for k in ("M2M_CLIENT_ID", "M2M_CLIENT_SECRET", "M2M_TOKEN_ENDPOINT", "M2M_SCOPE"):
            monkeypatch.delenv(k, raising=False)
        with caplog.at_level(logging.ERROR, logger="sync"):
            assert scheduler._get_m2m_token() is None
        assert any("event=m2m_token_unavailable" in r.message for r in caplog.records)

    def test_mints_and_caches_token(self, reset_m2m_cache, m2m_env):
        from src.sync import scheduler
        with patch("src.sync.scheduler.urllib.request.urlopen", return_value=_fake_token_response("first")) as up:
            t1 = scheduler._get_m2m_token()
        assert t1 == "first"
        assert up.call_count == 1

        with patch("src.sync.scheduler.urllib.request.urlopen", return_value=_fake_token_response("second")) as up2:
            t2 = scheduler._get_m2m_token()
        assert t2 == "first"
        assert up2.call_count == 0

    def test_refreshes_when_token_near_expiry(self, reset_m2m_cache, m2m_env):
        from src.sync import scheduler
        # Cached token expiring in 5s — under the 60s skew, must refresh.
        # expires_at uses monotonic clock (NTP-jump-safe), so set it accordingly.
        scheduler._m2m_token_cache["token"] = "stale"
        scheduler._m2m_token_cache["expires_at"] = time.monotonic() + 5

        with patch("src.sync.scheduler.urllib.request.urlopen", return_value=_fake_token_response("fresh")):
            assert scheduler._get_m2m_token() == "fresh"

    def test_returns_none_on_socket_timeout(self, reset_m2m_cache, m2m_env, caplog):
        """socket.timeout is NOT a urllib URLError subclass on Python 3.9 (deploy target).
        Without the explicit catch, a slow Cognito hit would propagate uncaught."""
        import socket as _socket
        from src.sync import scheduler
        with caplog.at_level(logging.ERROR, logger="sync"):
            with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=_socket.timeout("timed out")):
                assert scheduler._get_m2m_token() is None
        assert any("event=m2m_token_fetch_failed" in r.message for r in caplog.records)

    def test_sends_basic_auth_and_form_body(self, reset_m2m_cache, m2m_env):
        from src.sync import scheduler
        captured = {}

        def fake(req, timeout=None):
            captured["url"] = req.full_url
            captured["headers"] = dict(req.header_items())
            captured["body"] = req.data
            return _fake_token_response()

        with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=fake):
            scheduler._get_m2m_token()

        assert captured["url"] == "https://fake/oauth2/token"
        # urllib title-cases header names
        assert captured["headers"].get("Authorization", "").startswith("Basic ")
        assert b"grant_type=client_credentials" in captured["body"]
        assert b"scope=api%2Fread" in captured["body"]


class TestFetchViaControlPlane:
    def test_raises_when_token_unavailable(self, reset_m2m_cache, monkeypatch):
        """Must NOT swallow into [] — raising forces the call site to handle the failure."""
        from src.sync import scheduler
        for k in ("M2M_CLIENT_ID", "M2M_CLIENT_SECRET", "M2M_TOKEN_ENDPOINT", "M2M_SCOPE"):
            monkeypatch.delenv(k, raising=False)
        with pytest.raises(scheduler.ControlPlaneFetchError):
            scheduler.fetch_active_accounts_via_control_plane("inst-xxx")

    def test_uses_bearer_token_against_control_plane(self, reset_m2m_cache, monkeypatch, mock_urlopen_response):
        from src.sync import scheduler
        scheduler._m2m_token_cache["token"] = "test-token"
        scheduler._m2m_token_cache["expires_at"] = time.monotonic() + 600
        monkeypatch.setattr(scheduler, "CONTROL_PLANE_URL", "https://fake.api")

        captured = {}
        resp = mock_urlopen_response([{"integrationAccountId": "abc"}])

        def fake(req, timeout=None):
            captured["url"] = req.full_url
            captured["headers"] = dict(req.header_items())
            return resp

        with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=fake):
            result = scheduler.fetch_active_accounts_via_control_plane("inst-xxx")
        assert result == [{"integrationAccountId": "abc"}]
        assert captured["url"] == "https://fake.api/instances/inst-xxx/integration-accounts"
        assert captured["headers"].get("Authorization") == "Bearer test-token"

    def test_raises_with_response_body_on_http_error(self, reset_m2m_cache, monkeypatch):
        """When Joey's API rejects the M2M token, surface his error body so we can debug."""
        from src.sync import scheduler
        scheduler._m2m_token_cache["token"] = "test-token"
        scheduler._m2m_token_cache["expires_at"] = time.monotonic() + 600
        monkeypatch.setattr(scheduler, "CONTROL_PLANE_URL", "https://fake.api")

        err = urllib.error.HTTPError(
            url="https://fake.api/instances/x/integration-accounts",
            code=401, msg="Unauthorized", hdrs=None,
            fp=io.BytesIO(b'{"error":"invalid_token","detail":"missing username claim"}'),
        )
        with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=err):
            with pytest.raises(scheduler.ControlPlaneFetchError) as ei:
                scheduler.fetch_active_accounts_via_control_plane("inst-xxx")
        assert "401" in str(ei.value)
        assert "invalid_token" in str(ei.value)

    def test_handles_none_instance_id_in_log(self, reset_m2m_cache):
        """Slice safety: instance_id is a parameter with no default; None must not crash logging."""
        from src.sync import scheduler
        scheduler._m2m_token_cache["token"] = "test-token"
        scheduler._m2m_token_cache["expires_at"] = time.monotonic() + 600
        with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=urllib.error.URLError("boom")):
            with pytest.raises(scheduler.ControlPlaneFetchError):
                scheduler.fetch_active_accounts_via_control_plane(None)


class TestM2MTokenHttpError:
    def test_logs_body_on_cognito_400(self, reset_m2m_cache, m2m_env, caplog):
        from src.sync import scheduler

        err = urllib.error.HTTPError(
            url="https://fake/oauth2/token", code=400, msg="Bad Request", hdrs=None,
            fp=io.BytesIO(b'{"error":"invalid_client"}'),
        )
        with caplog.at_level(logging.ERROR, logger="sync"):
            with patch("src.sync.scheduler.urllib.request.urlopen", side_effect=err):
                assert scheduler._get_m2m_token() is None
        # Caller must be able to distinguish misconfig (invalid_client) from outage from the log.
        msgs = [r.message for r in caplog.records]
        assert any("status=400" in m and "invalid_client" in m for m in msgs)


# ── diagnose_account ──────────────────────────────────────────────────────────

class TestDiagnoseAccount:
    def test_returns_false_for_unknown_connector(self, caplog):
        from src.sync import scheduler
        with caplog.at_level(logging.ERROR, logger="sync"):
            assert scheduler.diagnose_account({"integration_id": "nope", "integration_account_id": "x"}) is False
        assert any("event=diagnose_result" in r.message and "valid=False" in r.message for r in caplog.records)

    def test_returns_connector_validate_result(self, caplog):
        from src.sync import scheduler
        good = MagicMock()
        good.return_value.validate_credentials.return_value = True
        with patch.dict(scheduler.CONNECTOR_MAP, {"good": good}):
            account = {
                "integration_id": "good",
                "integration_account_id": "abc",
                "auth_credentials": {"k": "v"},
                "external_identity": {},
            }
            with caplog.at_level(logging.INFO, logger="sync"):
                assert scheduler.diagnose_account(account) is True
        assert any("event=diagnose_result" in r.message and "valid=True" in r.message for r in caplog.records)

    def test_parses_credentials_from_string_jsonb(self):
        """auth_credentials may arrive as a JSON string from the DB. Must parse."""
        from src.sync import scheduler
        seen = {}

        def capture(**kwargs):
            seen.update(kwargs)
            m = MagicMock()
            m.validate_credentials.return_value = True
            return m

        with patch.dict(scheduler.CONNECTOR_MAP, {"x": capture}):
            account = {
                "integration_id": "x",
                "integration_account_id": "a",
                "auth_credentials": '{"roleArn": "arn:..."}',
                "external_identity": '{"tenantId": "t"}',
            }
            scheduler.diagnose_account(account)
        assert seen["credentials"] == {"roleArn": "arn:..."}
        assert seen["external_identity"] == {"tenantId": "t"}


# ── _parse_account NamedTuple shape ───────────────────────────────────────────

class TestParseAccount:
    def test_returns_named_tuple_with_typed_fields(self):
        from src.sync import scheduler
        parsed = scheduler._parse_account({
            "integration_id": "amazon-connect",
            "integration_account_id": "aid-1",
            "instance_id": "inst-1",
            "auth_credentials": {"role": "arn"},
            "external_identity": {"tenant": "t"},
        })
        assert isinstance(parsed, scheduler.ParsedAccount)
        assert parsed.integration_id == "amazon-connect"
        assert parsed.account_id == "aid-1"
        assert parsed.instance_id == "inst-1"
        assert parsed.credentials == {"role": "arn"}
        assert parsed.external_identity == {"tenant": "t"}

    def test_handles_missing_optional_fields(self):
        from src.sync import scheduler
        parsed = scheduler._parse_account({"integration_id": "x"})
        assert parsed.account_id == ""
        assert parsed.credentials == {}
        assert parsed.external_identity == {}
