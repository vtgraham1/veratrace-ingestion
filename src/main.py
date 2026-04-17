"""
Veratrace Ingestion Service — HTTP API for on-demand sync triggers.

The cron handles scheduled syncs every 15 minutes. This endpoint
handles immediate syncs triggered by the UI (e.g., after a new
integration is connected).

Usage:
  python3 -m src.main                    # start HTTP server on port 8090
  python3 -m src.main --port 8091        # custom port
"""
import datetime
import hashlib
import hmac
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.sync.scheduler import sync_account, fetch_active_accounts
from src.connectors import CONNECTOR_MAP
from src.config import SUPABASE_URL, CONTROL_PLANE_URL

INGESTION_API_KEY = os.environ.get("INGESTION_API_KEY", "")

# Simple per-IP rate limiter: max requests per minute
RATE_LIMIT_RPM = int(os.environ.get("RATE_LIMIT_RPM", "30"))
_rate_tracker = {}  # ip → [timestamps]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("ingestion-api")


class IngestionHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for sync triggers."""

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    def do_POST(self):
        if not self._check_rate_limit():
            return
        if not self._check_api_key():
            return
        self._audit_log("POST", self.path)
        if self.path == "/sync":
            self._handle_sync()
        elif self.path == "/test-connection":
            self._handle_test_connection()
        else:
            self._json_response(404, {"error": "Not found"})

    def _check_rate_limit(self):
        """Per-IP rate limiter. Returns True if under limit."""
        import time as _time
        ip = self.client_address[0]
        now = _time.time()
        window = _rate_tracker.get(ip, [])
        # Remove entries older than 60 seconds
        window = [t for t in window if now - t < 60]
        if len(window) >= RATE_LIMIT_RPM:
            self._json_response(429, {"error": "Rate limit exceeded. Max {} requests/minute.".format(RATE_LIMIT_RPM)})
            logger.warning("Rate limit hit: %s (%d req/min)", ip, len(window))
            return False
        window.append(now)
        _rate_tracker[ip] = window
        return True

    def _check_api_key(self):
        """Validate X-API-Key header. Returns True if authorized."""
        if not INGESTION_API_KEY:
            self._json_response(503, {"error": "API key not configured — server misconfigured"})
            return False
        key = self.headers.get("X-API-Key", "")
        if key != INGESTION_API_KEY:
            self._json_response(401, {"error": "Invalid or missing API key"})
            return False
        return True

    def _audit_log(self, method, path):
        """Structured audit log for every API call."""
        logger.info(json.dumps({
            "audit": True,
            "method": method,
            "path": path,
            "source_ip": self.client_address[0],
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }))

    def do_GET(self):
        if not self._check_rate_limit():
            return
        if self.path == "/health":
            self._json_response(200, {"status": "ok", "supabase": bool(SUPABASE_URL)})
        elif self.path == "/health/warming":
            if not self._check_api_key():
                return
            self._handle_warming_health()
        elif self.path.startswith("/blog/approve"):
            # /blog/approve has its own HMAC auth — no API key needed
            self._handle_blog_approve()
        elif self.path.startswith("/stats/instances/"):
            self._audit_log("GET", self.path)
            self._route_stats()
        else:
            self._json_response(404, {"error": "Not found"})

    def _route_stats(self):
        """Route /stats/instances/{id}/... to the right handler.

        Paths:
          /stats/instances/{INST}/accounts
          /stats/instances/{INST}/accounts/{AID}/operations
        """
        path = self.path.split("?", 1)[0]  # strip query string
        parts = path.strip("/").split("/")
        # ["stats", "instances", INST, "accounts"]                   (len 4)
        # ["stats", "instances", INST, "accounts", AID, "operations"] (len 6)
        if len(parts) == 4 and parts[3] == "accounts":
            self._handle_stats_accounts(parts[2])
        elif len(parts) == 6 and parts[3] == "accounts" and parts[5] == "operations":
            self._handle_stats_operations(parts[2], parts[4])
        else:
            self._json_response(404, {"error": "Not found"})

    # ── Stats auth + helpers ──────────────────────────────────────────────────

    def _auth_via_control_plane(self, instance_id):
        """Validate JWT AND verify instance membership by forwarding to Joey's API.

        Returns (status_code, accounts_list_or_error_msg).
        - (200, [accounts]) → user authenticated + is a member of instance
        - (401, msg)        → no/invalid JWT
        - (403, msg)        → valid JWT but not a member
        - (5xx, msg)        → control plane unreachable or erroring
        """
        auth_header = self.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return 401, "Missing Authorization: Bearer header"

        url = f"{CONTROL_PLANE_URL}/instances/{instance_id}/integration-accounts"
        req = urllib.request.Request(url, headers={"Authorization": auth_header})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return 200, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read()[:300].decode("utf-8", "replace") if e.fp else ""
            if e.code in (401, 403):
                return e.code, body or "access denied"
            logger.error("Control plane returned %d for instance %s: %s", e.code, instance_id[:8], body)
            return 502, f"Control plane error: HTTP {e.code}"
        except urllib.error.URLError as e:
            logger.error("Control plane unreachable: %s", e)
            return 503, f"Control plane unreachable: {str(e)[:100]}"

    def _supabase_get(self, path_and_query):
        """Query Supabase REST with service-role key. Returns parsed JSON or raises."""
        url = f"{SUPABASE_URL}/rest/v1/{path_and_query}"
        req = urllib.request.Request(url, headers={
            "apikey": os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
            "Authorization": f"Bearer {os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')}",
            "Content-Type": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())

    # ── Stats handlers ────────────────────────────────────────────────────────

    def _handle_stats_accounts(self, instance_id):
        """GET /stats/instances/{id}/accounts — per-account aggregates for the UI."""
        status, accounts_or_err = self._auth_via_control_plane(instance_id)
        if status != 200:
            self._json_response(status, {"error": accounts_or_err})
            return

        accounts = accounts_or_err  # camelCase from Joey: integrationAccountId, integrationId, name, status, health, ...

        # Pull Supabase aggregates for this instance (one round-trip each).
        try:
            stats_rows = self._supabase_get(f"v_account_stats?instance_id=eq.{instance_id}&select=*")
            breakdown_rows = self._supabase_get(f"v_account_instance_breakdown?instance_id=eq.{instance_id}&select=*")
        except urllib.error.HTTPError as e:
            body = e.read()[:300].decode("utf-8", "replace") if e.fp else ""
            logger.error("Supabase view fetch failed for instance %s: %d %s", instance_id[:8], e.code, body)
            self._json_response(502, {"error": f"Stats backend error: HTTP {e.code}", "detail": body[:200]})
            return
        except Exception as e:
            logger.error("Supabase view fetch error: %s", e)
            self._json_response(502, {"error": f"Stats backend unreachable: {str(e)[:100]}"})
            return

        # Index by integration_account_id for O(1) enrichment.
        stats_by_aid = {r["integration_account_id"]: r for r in stats_rows}
        breakdown_by_aid = {}
        for r in breakdown_rows:
            breakdown_by_aid.setdefault(r["integration_account_id"], []).append({
                "tenantId": r["tenant_id"],
                "twuCount": r["twu_count"],
            })

        out = []
        for acc in accounts:
            aid = acc.get("integrationAccountId", "")
            stats = stats_by_aid.get(aid, {})
            out.append({
                "integrationAccountId": aid,
                "integrationId":        acc.get("integrationId", ""),
                "name":                 acc.get("name", ""),
                "status":               acc.get("status", ""),
                "health":               acc.get("health", ""),
                "twuCount":             stats.get("twu_count", 0),
                "instances":            stats.get("instance_count", 0),
                "lastSync":             stats.get("last_sync"),
                "instanceTWUs":         breakdown_by_aid.get(aid, []),
            })

        self._json_response(200, out)

    def _handle_stats_operations(self, instance_id, account_id):
        """GET /stats/instances/{id}/accounts/{aid}/operations — recent sync_runs for one account."""
        status, accounts_or_err = self._auth_via_control_plane(instance_id)
        if status != 200:
            self._json_response(status, {"error": accounts_or_err})
            return

        accounts = accounts_or_err
        # Enforce that the requested account_id belongs to this instance.
        account_ids = {a.get("integrationAccountId") for a in accounts}
        if account_id not in account_ids:
            self._json_response(404, {"error": "Account not found for this instance"})
            return

        try:
            rows = self._supabase_get(
                f"v_account_recent_runs?integration_account_id=eq.{account_id}"
                f"&order=started_at.desc&limit=50&select=*"
            )
        except urllib.error.HTTPError as e:
            body = e.read()[:300].decode("utf-8", "replace") if e.fp else ""
            logger.error("Supabase runs fetch failed for account %s: %d %s", account_id[:8], e.code, body)
            self._json_response(502, {"error": f"Stats backend error: HTTP {e.code}"})
            return
        except Exception as e:
            logger.error("Supabase runs fetch error: %s", e)
            self._json_response(502, {"error": f"Stats backend unreachable: {str(e)[:100]}"})
            return

        out = [
            {
                "runId":          r["run_id"],
                "status":         r["status"],
                "startedAt":      r["started_at"],
                "finishedAt":     r.get("finished_at"),
                "signalsWritten": r.get("signals_written", 0),
                "durationMs":     r.get("duration_ms"),
                "error":          r.get("error"),
                "backfill":       r.get("backfill", False),
            }
            for r in rows
        ]
        self._json_response(200, out)

    def _handle_warming_health(self):
        """Check if warming is running and producing contacts."""
        log_path = "/opt/veraagents/logs/warming.log"
        try:
            if not os.path.exists(log_path):
                self._json_response(200, {"status": "no_log", "message": "Warming log not found — cron may not have run yet"})
                return
            stat = os.stat(log_path)
            modified = datetime.datetime.fromtimestamp(stat.st_mtime)
            age_hours = (datetime.datetime.now() - modified).total_seconds() / 3600
            # Read last few lines for status
            with open(log_path, "r") as f:
                lines = f.readlines()
                last_lines = lines[-5:] if len(lines) >= 5 else lines
            last_result = ""
            for line in reversed(last_lines):
                if "Created:" in line or "Failed:" in line or "Warming complete" in line:
                    last_result = line.strip()
                    break
            status = "healthy" if age_hours < 2 else "stale"
            self._json_response(200, {
                "status": status,
                "last_run_hours_ago": round(age_hours, 1),
                "last_result": last_result,
                "log_lines": len(lines),
            })
        except Exception as e:
            self._json_response(500, {"status": "error", "message": str(e)[:200]})

    def _handle_test_connection(self):
        """Test credentials for any registered connector type."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length > 0 else {}

            integration_id = body.get("integrationId", "")
            credentials = body.get("credentials", {})
            external_identity = body.get("externalIdentity", {})

            # Backwards compat: if no integrationId, assume amazon-connect (legacy format)
            if not integration_id:
                integration_id = "amazon-connect"
                credentials = {
                    "roleArn": body.get("roleArn", ""),
                    "externalId": body.get("externalId", ""),
                }
                external_identity = {"tenantId": body.get("instanceArn", "")}

            connector_cls = CONNECTOR_MAP.get(integration_id)
            if not connector_cls:
                self._json_response(400, {
                    "error": f"Unknown integration: {integration_id}",
                    "available": list(CONNECTOR_MAP.keys()),
                })
                return

            if not credentials:
                self._json_response(400, {"error": "credentials required"})
                return

            logger.info("Test connection: %s (keys: %s)", integration_id, list(credentials.keys()))

            connector = connector_cls(
                integration_account_id="test",
                instance_id="test",
                credentials=credentials,
                external_identity=external_identity,
            )
            result = connector.test_connection()
            self._json_response(200, {
                "success": result.success,
                "message": result.message,
                "region": getattr(result, "region", ""),
                "details": getattr(result, "details", None),
            })

        except Exception as e:
            logger.error("Test connection failed (%s): %s", body.get("integrationId", "?"), str(e)[:200])
            self._json_response(500, {"success": False, "message": str(e)[:200]})

    def _handle_sync(self):
        """Trigger an immediate sync for a specific integration account."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length > 0 else {}

            integration_account_id = body.get("integrationAccountId", "")
            instance_id = body.get("instanceId", "")

            if not integration_account_id:
                self._json_response(400, {"error": "integrationAccountId required"})
                return

            logger.info("Immediate sync requested: account=%s", integration_account_id[:8])

            # Find the account and sync it
            accounts = fetch_active_accounts()
            account = next(
                (a for a in accounts
                 if a.get("integration_account_id") == integration_account_id),
                None
            )

            if not account:
                # Account might not be in our Supabase — create a minimal sync
                # using the provided info
                self._json_response(202, {
                    "status": "accepted",
                    "message": "Sync queued. Account not yet in sync registry — will pick up on next cron cycle.",
                })
                return

            sync_account(account)
            self._json_response(200, {
                "status": "synced",
                "message": "First sync completed successfully.",
            })

        except Exception as e:
            logger.error("Sync request failed: %s", str(e)[:200])
            self._json_response(500, {"error": str(e)[:200]})

    def _handle_blog_approve(self):
        """One-click blog post approval via HMAC-signed URL."""
        from urllib.parse import urlparse, parse_qs
        params = parse_qs(urlparse(self.path).query)
        slug = params.get("slug", [""])[0]
        token = params.get("token", [""])[0]

        if not slug or not token:
            self._html_response(400, "Missing slug or token.")
            return

        # Verify HMAC token (signed with INGESTION_API_KEY as secret)
        expected = hmac.new(INGESTION_API_KEY.encode(), slug.encode(), hashlib.sha256).hexdigest()[:16]
        if not hmac.compare_digest(token, expected):
            self._html_response(403, "Invalid or expired approval link.")
            return

        # Update post status in Supabase
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
        if not SUPABASE_URL or not supabase_key:
            self._html_response(500, "Supabase not configured.")
            return

        try:
            # Look up post title from Supabase
            lookup_url = f"{SUPABASE_URL}/rest/v1/blog_posts?slug=eq.{slug}&select=id,title,status"
            req = urllib.request.Request(lookup_url, method="GET")
            req.add_header("apikey", supabase_key)
            req.add_header("Authorization", f"Bearer {supabase_key}")
            with urllib.request.urlopen(req, timeout=10) as r:
                posts = json.loads(r.read())
            if not posts:
                self._html_response(404, f"<h2>Post not found: {slug}</h2>")
                return
            post = posts[0]
            if post.get("status") != "draft":
                self._html_response(200,
                    f"<h2>Already {post.get('status', 'processed')}: {slug}</h2>"
                    f'<p><a href="https://veratrace.ai/blog/{slug}">View →</a></p>'
                )
                return

            # Write approved_publish event to pipeline log (same format as pipeline.py)
            log_file = "/opt/veraagents/blog/memory/blog_pipeline_log.jsonl"
            record = {
                "ts": datetime.datetime.utcnow().isoformat() + "Z",
                "event": "approved_publish",
                "slug": slug,
                "title": post.get("title", ""),
                "metadata": {"post_id": slug, "approved_via": "email_link"},
            }
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            with open(log_file, "a") as f:
                f.write(json.dumps(record) + "\n")

            logger.info("Blog post approved via email link: %s", slug)
            self._html_response(200,
                f"<h2>Approved: {post.get('title', slug)}</h2>"
                f"<p>This post will be published on the next publish cycle (weekdays 1pm EDT).</p>"
                f'<p><a href="https://veratrace.ai/blog/{slug}">Preview →</a></p>'
            )
        except Exception as e:
            logger.error("Blog approve failed (%s): %s", slug, str(e)[:200])
            self._html_response(500, f"Failed to approve: {str(e)[:100]}")

    def _html_response(self, status, body_html):
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Veratrace Blog</title>
        <style>body{{font-family:-apple-system,sans-serif;max-width:500px;margin:60px auto;padding:20px;color:#1a1a1a;}}</style>
        </head><body>{body_html}</body></html>"""
        self.wfile.write(html.encode())

    def _json_response(self, status, body):
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Max-Age", "86400")

    def log_message(self, format, *args):
        # Suppress default access logs — we use structured logging
        pass


def main():
    port = 8090
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        port = int(sys.argv[idx + 1])

    server = HTTPServer(("0.0.0.0", port), IngestionHandler)
    logger.info("Ingestion API listening on :%d", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        server.server_close()


if __name__ == "__main__":
    main()
