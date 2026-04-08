"""
Veratrace Ingestion Service — HTTP API for on-demand sync triggers.

The cron handles scheduled syncs every 15 minutes. This endpoint
handles immediate syncs triggered by the UI (e.g., after a new
integration is connected).

Usage:
  python3 -m src.main                    # start HTTP server on port 8090
  python3 -m src.main --port 8091        # custom port
"""
import json
import logging
import sys
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.sync.scheduler import sync_account, fetch_active_accounts
from src.config import SUPABASE_URL

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
        if self.path == "/sync":
            self._handle_sync()
        else:
            self._json_response(404, {"error": "Not found"})

    def do_GET(self):
        if self.path == "/health":
            self._json_response(200, {"status": "ok", "supabase": bool(SUPABASE_URL)})
        else:
            self._json_response(404, {"error": "Not found"})

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
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

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
