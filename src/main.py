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
import json
import logging
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.sync.scheduler import sync_account, fetch_active_accounts
from src.connectors import CONNECTOR_MAP
from src.config import SUPABASE_URL

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
            return True
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
        if self.path == "/health":
            self._json_response(200, {"status": "ok", "supabase": bool(SUPABASE_URL)})
        elif self.path == "/health/warming":
            self._handle_warming_health()
        else:
            self._json_response(404, {"error": "Not found"})

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

    def _json_response(self, status, body):
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-API-Key")
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
