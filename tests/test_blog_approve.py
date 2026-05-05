"""
Pins the two bugs that left email-approved blog posts stuck in draft for weeks:

  1. _handle_blog_approve hard-coded the wrong pipeline log path
     (/opt/veraagents/blog/memory/...) so events never reached the publish cron's
     read_log(), which honors $OPENCLAW_LOGS or ~/.openclaw/logs/... per pipeline.py.

  2. _handle_blog_approve wrote metadata.post_id = slug, but publish.py::publish_post
     queries Supabase by `id=eq.{post_id}` (UUID). A slug-keyed value never resolves,
     so even if the path bug were fixed, the publish step would error out.

Discovered 2026-05-05 when two manually-approved drafts (managing-hybrid-human-ai-workflows,
colorado-ai-act-enterprise-compliance-guide) sat in draft despite both 👍 email clicks.
"""
from __future__ import annotations

import hashlib
import hmac
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
os.environ.setdefault("SUPABASE_URL", "https://fake.supabase.test")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")

from src.main import IngestionHandler  # noqa: E402
from src import main as main_module  # noqa: E402


@pytest.fixture(scope="module")
def server():
    srv = HTTPServer(("127.0.0.1", 0), IngestionHandler)
    port = srv.server_address[1]
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    srv.shutdown()


@pytest.fixture(autouse=True)
def fake_supabase_url(monkeypatch):
    monkeypatch.setattr(main_module, "SUPABASE_URL", "https://fake.supabase.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
    monkeypatch.setattr(main_module, "INGESTION_API_KEY", "test-key-abc123")
    yield


@pytest.fixture
def isolated_log_dir(tmp_path, monkeypatch):
    """Point OPENCLAW_LOGS at a temp dir so the test never writes to ~/.openclaw/."""
    monkeypatch.setenv("OPENCLAW_LOGS", str(tmp_path))
    return tmp_path


_REAL_URLOPEN = urllib.request.urlopen


class _FakeSupabaseLookup:
    """Context-manager response returning one Supabase row for the lookup."""
    def __init__(self, post_id, slug, title="T", status="draft"):
        self._body = json.dumps([{"id": post_id, "slug": slug, "title": title, "status": status}]).encode()
        self.status = 200
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def read(self): return self._body


def _make_dispatcher(supabase_resp):
    """urlopen replacement: localhost passes through, Supabase gets the fake."""
    def fake_urlopen(req, timeout=None, *args, **kwargs):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if "127.0.0.1" in url or "localhost" in url:
            return _REAL_URLOPEN(req, timeout=timeout, *args, **kwargs)
        return supabase_resp
    return fake_urlopen


def _approve_url(server_url: str, slug: str, secret: str = "test-key-abc123") -> str:
    token = hmac.new(secret.encode(), slug.encode(), hashlib.sha256).hexdigest()[:16]
    return f"{server_url}/blog/approve?slug={slug}&token={token}"


class TestBlogApproveLogPath:
    def test_writes_to_openclaw_logs_dir_not_legacy_memory_dir(self, server, isolated_log_dir):
        """The publish cron only reads from $OPENCLAW_LOGS/blog_pipeline_log.jsonl;
        any other path is invisible to it and the post stays in draft."""
        slug = "test-slug"
        post_id = "11111111-2222-3333-4444-555555555555"
        with patch("src.main.urllib.request.urlopen",
                   side_effect=_make_dispatcher(_FakeSupabaseLookup(post_id, slug))):
            r = urllib.request.urlopen(_approve_url(server, slug), timeout=5)
            assert r.status == 200
        assert (isolated_log_dir / "blog_pipeline_log.jsonl").exists()

    def test_metadata_post_id_is_uuid_not_slug(self, server, isolated_log_dir):
        """publish.py looks up the post by `id=eq.{post_id}` — a slug-keyed
        post_id never resolves and the publish call sys.exits."""
        slug = "test-slug"
        post_id = "abcdef12-3456-7890-abcd-ef1234567890"
        with patch("src.main.urllib.request.urlopen",
                   side_effect=_make_dispatcher(_FakeSupabaseLookup(post_id, slug))):
            urllib.request.urlopen(_approve_url(server, slug), timeout=5)

        record = json.loads((isolated_log_dir / "blog_pipeline_log.jsonl").read_text().strip())
        assert record["event"] == "approved_publish"
        assert record["slug"] == slug
        assert record["metadata"]["post_id"] == post_id, (
            "metadata.post_id must be the Supabase UUID — publish.py queries by id, not slug"
        )
        assert record["metadata"]["approved_via"] == "email_link"

    def test_honors_OPENCLAW_LOGS_env_var(self, server, tmp_path, monkeypatch):
        """Production sets OPENCLAW_LOGS to redirect logs; this fixture proves it works
        end-to-end (not just the home-dir default)."""
        custom = tmp_path / "custom_logs"
        monkeypatch.setenv("OPENCLAW_LOGS", str(custom))

        slug = "x"
        with patch("src.main.urllib.request.urlopen",
                   side_effect=_make_dispatcher(_FakeSupabaseLookup("uuid-x", slug))):
            urllib.request.urlopen(_approve_url(server, slug), timeout=5)

        assert (custom / "blog_pipeline_log.jsonl").exists()


class TestBlogApproveAuth:
    def test_rejects_invalid_token(self, server, isolated_log_dir):
        url = f"{server}/blog/approve?slug=x&token=wrongtoken"
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(url, timeout=5)
        assert ei.value.code == 403

    def test_rejects_missing_slug(self, server, isolated_log_dir):
        url = f"{server}/blog/approve?token=abc"
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(url, timeout=5)
        assert ei.value.code == 400

    def test_no_op_on_already_published_post(self, server, isolated_log_dir):
        """If the post was already published (e.g., manual BlogAdmin toggle), the
        approve link should be idempotent — show 'already published' not crash,
        and crucially not write a duplicate approved_publish event."""
        slug = "already-out"
        with patch("src.main.urllib.request.urlopen",
                   side_effect=_make_dispatcher(_FakeSupabaseLookup("uid", slug, status="published"))):
            r = urllib.request.urlopen(_approve_url(server, slug), timeout=5)
            assert r.status == 200
        assert not (isolated_log_dir / "blog_pipeline_log.jsonl").exists()
