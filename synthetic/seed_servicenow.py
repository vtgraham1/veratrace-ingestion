"""
ServiceNow Enterprise Seeder — creates realistic incident data with
AI/human attribution patterns that demonstrate the Veratrace TWU value prop.

Creates incidents across the full lifecycle with:
- Virtual Agent auto-resolved (password resets, FAQ, account lookups)
- AI-assisted → human resolved (VA triages, agent completes)
- Human-only (complex issues, P1 incidents)
- Multi-assignment (reassigned between groups)
- SLA breaches (resolution exceeds target)
- Vendor reconciliation (VA claims resolved, user reopens)

Also creates sys_audit entries to establish AI attribution trail and
work notes from different actors.

Usage:
  python3 -m synthetic.seed_servicenow --instance-url URL --username admin --password PASS --incidents 50
  # Or with env vars:
  SNOW_INSTANCE_URL=... SNOW_USERNAME=... SNOW_PASSWORD=... python3 -m synthetic.seed_servicenow
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import random
import sys
import time
import urllib.request
import urllib.error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("seed_servicenow")

# ── Realistic Actors ──────────────────────────────────────────────────────────

HUMAN_AGENTS = [
    {"name": "Mike Chen", "group": "Service Desk"},
    {"name": "Sarah Williams", "group": "Network Support"},
    {"name": "James Rodriguez", "group": "Desktop Support"},
    {"name": "Emily Patel", "group": "Application Support"},
    {"name": "David Kim", "group": "Security Operations"},
    {"name": "Lisa Thompson", "group": "Database Admin"},
]

AI_ACTORS = [
    {"name": "Virtual Agent", "type": "virtual_agent"},
    {"name": "Now Assist", "type": "now_assist"},
    {"name": "Predictive Intelligence", "type": "predictive"},
]

CALLERS = [
    "Alex Rivera", "Jordan Chen", "Sam Patel", "Morgan Kim",
    "Taylor Brooks", "Casey Wong", "Jamie Foster", "Drew Martinez",
    "Riley Nguyen", "Quinn O'Brien", "Avery Shah", "Blake Thompson",
    "Chris Evans", "Dana White", "Pat Morales", "Jesse Park",
]

# ── Scenario Definitions ─────────────────────────────────────────────────────

SCENARIOS = [
    # ── Virtual Agent auto-resolved (35%) ──────────────────────────────────
    {
        "weight": 15, "type": "ai_resolved",
        "short_description": "Password reset request",
        "category": "inquiry", "subcategory": "password",
        "priority": "3", "urgency": "3", "impact": "3",
        "contact_type": "Virtual Agent",
        "resolution": "Password reset link sent automatically via Virtual Agent",
        "work_notes": [
            {"author": "Virtual Agent", "text": "User identity verified via MFA challenge. Reset link generated and sent to registered email."},
        ],
    },
    {
        "weight": 10, "type": "ai_resolved",
        "short_description": "VPN access request",
        "category": "network", "subcategory": "vpn",
        "priority": "4", "urgency": "3", "impact": "3",
        "contact_type": "Virtual Agent",
        "resolution": "VPN profile auto-provisioned based on user role and department",
        "work_notes": [
            {"author": "Virtual Agent", "text": "User role verified: Engineer. VPN profile 'Engineering-Standard' applied. Connection test passed."},
        ],
    },
    {
        "weight": 10, "type": "ai_resolved",
        "short_description": "Account unlock request",
        "category": "inquiry", "subcategory": "login",
        "priority": "3", "urgency": "2", "impact": "3",
        "contact_type": "Virtual Agent",
        "resolution": "Account unlocked after identity verification via Virtual Agent",
        "work_notes": [
            {"author": "Now Assist", "text": "Detected locked account via AD query. Identity confirmed. Account unlocked and temporary password issued."},
        ],
    },

    # ── AI-assisted → human resolved (25%) ─────────────────────────────────
    {
        "weight": 10, "type": "ai_assisted",
        "short_description": "Outlook not syncing email",
        "category": "software", "subcategory": "email",
        "priority": "2", "urgency": "2", "impact": "2",
        "contact_type": "Virtual Agent",
        "resolution": "Rebuilt Outlook profile and cleared local cache",
        "work_notes": [
            {"author": "Virtual Agent", "text": "Collected diagnostics: Outlook version 16.0, last sync 48h ago. Suggested profile rebuild — user unable to complete. Escalating to Desktop Support."},
            {"author": "human", "text": "Rebuilt Outlook profile. Root cause: corrupt OST file from interrupted Windows update."},
        ],
    },
    {
        "weight": 8, "type": "ai_assisted",
        "short_description": "Printer not working on floor 3",
        "category": "hardware", "subcategory": "printer",
        "priority": "3", "urgency": "3", "impact": "2",
        "contact_type": "Virtual Agent",
        "resolution": "Replaced toner cartridge and cleared print queue",
        "work_notes": [
            {"author": "Predictive Intelligence", "text": "Similar incidents for this printer model (HP LaserJet M455): 73% resolved by toner replacement. Routing to onsite support."},
            {"author": "human", "text": "Confirmed: toner empty. Replaced cartridge, cleared stuck jobs in queue. Printer operational."},
        ],
    },
    {
        "weight": 7, "type": "ai_assisted",
        "short_description": "Software license activation failing",
        "category": "software", "subcategory": "license",
        "priority": "3", "urgency": "2", "impact": "3",
        "contact_type": "Virtual Agent",
        "resolution": "Generated new activation key from license server",
        "work_notes": [
            {"author": "Virtual Agent", "text": "License key format validated. Activation server returned error: 'Key exhausted.' Escalating to Application Support for new key generation."},
            {"author": "human", "text": "Allocated new license from pool. Previous key was consumed by a decommissioned workstation. Activated successfully."},
        ],
    },

    # ── Human-only resolution (15%) ────────────────────────────────────────
    {
        "weight": 8, "type": "human_only",
        "short_description": "Database connection timeout in production",
        "category": "software", "subcategory": "database",
        "priority": "1", "urgency": "1", "impact": "1",
        "contact_type": "phone",
        "resolution": "Increased connection pool size and restarted application servers",
        "work_notes": [
            {"author": "human", "text": "P1 bridge initiated. Connection pool exhaustion confirmed via CloudWatch. Max connections increased from 50 to 200. Rolling restart of app servers completed. Monitoring."},
            {"author": "human", "text": "Root cause: batch job consumed 45 connections and didn't release. Added connection timeout of 30s. Postmortem scheduled for Monday."},
        ],
    },
    {
        "weight": 7, "type": "human_only",
        "short_description": "SSO login failure for external partners",
        "category": "software", "subcategory": "login",
        "priority": "2", "urgency": "2", "impact": "2",
        "contact_type": "email",
        "resolution": "Renewed SAML certificate and updated federation metadata",
        "work_notes": [
            {"author": "human", "text": "SAML certificate expired at 00:00 UTC. 47 partner users affected. Generated new cert, uploaded to IdP, distributed updated metadata."},
        ],
    },

    # ── Multi-assignment (10%) ─────────────────────────────────────────────
    {
        "weight": 10, "type": "multi_assign",
        "short_description": "New laptop setup and data migration",
        "category": "hardware", "subcategory": "laptop",
        "priority": "3", "urgency": "3", "impact": "3",
        "contact_type": "self-service",
        "resolution": None,  # Still in progress
        "work_notes": [
            {"author": "Virtual Agent", "text": "Cataloged request: Laptop refresh for executive. Routing to Desktop Support for hardware, then Application Support for data migration."},
            {"author": "human", "text": "Hardware ready: ThinkPad X1 Carbon Gen 12. Imaging with corporate SOE. Will transfer to App Support for data migration tomorrow."},
        ],
    },

    # ── SLA breach (10%) ───────────────────────────────────────────────────
    {
        "weight": 10, "type": "sla_breach",
        "short_description": "Shared drive permissions not applied",
        "category": "software", "subcategory": "file_share",
        "priority": "3", "urgency": "3", "impact": "3",
        "contact_type": "email",
        "resolution": "Added direct ACL as workaround for AD group sync delay",
        "work_notes": [
            {"author": "human", "text": "SLA breached: 8h elapsed, target was 4h. AD group membership change propagation delayed. Applied direct ACL to unblock user while investigating AD sync."},
        ],
    },

    # ── Vendor reconciliation (5%) — VA claims resolved, user reopens ──────
    {
        "weight": 5, "type": "vendor_recon",
        "short_description": "Zoom not launching after update",
        "category": "software", "subcategory": "application",
        "priority": "3", "urgency": "2", "impact": "3",
        "contact_type": "Virtual Agent",
        "resolution": "Reinstalled Zoom client and cleared AppData cache",
        "work_notes": [
            {"author": "Virtual Agent", "text": "Detected Zoom version conflict. Applied automated fix: cleared cache, initiated reinstall. Marked as resolved."},
            {"author": "human", "text": "User reopened: Zoom crashes on join. Virtual Agent fix was incomplete — needed to also clear Teams integration plugin. Resolved manually."},
        ],
    },
]


class ServiceNowSeeder:
    def __init__(self, instance_url, username, password):
        self.instance_url = instance_url.rstrip("/")
        self.auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.stats = {"created": 0, "resolved": 0, "work_notes": 0, "errors": 0}

    def _api(self, method, path, body=None):
        url = f"{self.instance_url}{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Basic {self.auth}")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def create_incident(self, scenario):
        """Create an incident and progress it through its lifecycle."""
        caller = random.choice(CALLERS)

        # Step 1: Create in New state
        body = {
            "short_description": scenario["short_description"],
            "description": f"Reported by {caller}. {scenario.get('resolution', '') or 'Investigation in progress.'}",
            "category": scenario.get("category", "inquiry"),
            "subcategory": scenario.get("subcategory", ""),
            "priority": scenario.get("priority", "3"),
            "urgency": scenario.get("urgency", "3"),
            "impact": scenario.get("impact", "3"),
            "contact_type": scenario.get("contact_type", "phone"),
        }

        try:
            result = self._api("POST", "/api/now/table/incident", body)
        except Exception as e:
            logger.error("Failed to create: %s — %s", scenario["short_description"], e)
            self.stats["errors"] += 1
            return None

        record = result.get("result", {})
        sys_id = record.get("sys_id", "")
        number = record.get("number", "")
        self.stats["created"] += 1

        # Step 2: Add work notes (creates sys_journal_field + sys_audit entries)
        for note in scenario.get("work_notes", []):
            time.sleep(0.3)  # Space out for realistic timestamps
            self._add_work_note(sys_id, note)

        # Step 3: Assign to agent
        agent = random.choice(HUMAN_AGENTS)
        if scenario["type"] in ("ai_resolved",):
            # AI-resolved: assign to Virtual Agent
            ai = random.choice(AI_ACTORS)
            self._update_incident(sys_id, {
                "assigned_to": "",  # No human assignment
                "assignment_group": "",
            })
        elif scenario["type"] == "multi_assign":
            # Assign then reassign
            self._update_incident(sys_id, {
                "state": "2",  # In Progress
                "assigned_to": agent["name"],
                "assignment_group": agent["group"],
            })
            time.sleep(0.3)
            agent2 = random.choice([a for a in HUMAN_AGENTS if a != agent])
            self._update_incident(sys_id, {
                "assigned_to": agent2["name"],
                "assignment_group": agent2["group"],
            })
        else:
            # Normal human assignment
            self._update_incident(sys_id, {
                "state": "2",  # In Progress
                "assigned_to": agent["name"],
                "assignment_group": agent["group"],
            })

        # Step 4: Resolve (if applicable)
        if scenario.get("resolution"):
            time.sleep(0.3)
            try:
                self._update_incident(sys_id, {
                    "state": "6",  # Resolved
                    "close_code": "Solved (Permanently)" if scenario["type"] != "sla_breach" else "Solved (Work Around)",
                    "close_notes": scenario["resolution"],
                })
                self.stats["resolved"] += 1
            except Exception as e:
                # Some PDIs block direct resolve — try via Resolve state machine
                logger.warning("Direct resolve failed for %s, trying via state 2→6: %s", number, str(e)[:50])
                try:
                    self._update_incident(sys_id, {"state": "6"})
                    self.stats["resolved"] += 1
                except Exception:
                    pass

        # Step 5: Reopen if vendor reconciliation scenario
        if scenario["type"] == "vendor_recon":
            time.sleep(0.3)
            try:
                self._update_incident(sys_id, {"state": "2"})  # Back to In Progress
            except Exception:
                pass

        logger.info(
            "  %s [%s] %s — %s",
            number, scenario["type"], scenario["short_description"],
            "resolved" if scenario.get("resolution") and scenario["type"] != "vendor_recon" else "open",
        )
        return sys_id

    def _update_incident(self, sys_id, fields):
        try:
            self._api("PATCH", f"/api/now/table/incident/{sys_id}", fields)
        except urllib.error.HTTPError as e:
            if e.code == 403:
                logger.debug("PATCH 403 on %s (business rule): %s", sys_id, list(fields.keys()))
            else:
                raise

    def _add_work_note(self, sys_id, note):
        """Add a work note — this creates sys_journal_field + sys_audit entries."""
        try:
            author = note["author"]
            text = f"[{author}] {note['text']}"
            self._api("PATCH", f"/api/now/table/incident/{sys_id}", {
                "work_notes": text,
            })
            self.stats["work_notes"] += 1
        except Exception as e:
            logger.debug("Work note failed: %s", str(e)[:50])

    def seed(self, count=50):
        """Create N incidents with weighted scenario distribution."""
        logger.info("Seeding %d incidents on %s...\n", count, self.instance_url)

        weights = [s["weight"] for s in SCENARIOS]
        selected = random.choices(SCENARIOS, weights=weights, k=count)

        for scenario in selected:
            self.create_incident(scenario)
            time.sleep(0.5)  # Rate limiting

        logger.info("\n=== Seed Complete ===")
        logger.info("  Created:    %d incidents", self.stats["created"])
        logger.info("  Resolved:   %d incidents", self.stats["resolved"])
        logger.info("  Work notes: %d entries", self.stats["work_notes"])
        logger.info("  Errors:     %d", self.stats["errors"])

        # Print distribution
        type_counts = {}
        for s in selected:
            type_counts[s["type"]] = type_counts.get(s["type"], 0) + 1
        logger.info("\n  Distribution:")
        for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
            logger.info("    %-20s %d (%.0f%%)", t, c, c / len(selected) * 100)


def main():
    parser = argparse.ArgumentParser(description="Seed ServiceNow with enterprise incident data")
    parser.add_argument("--instance-url", default=os.environ.get("SNOW_INSTANCE_URL", ""))
    parser.add_argument("--username", default=os.environ.get("SNOW_USERNAME", ""))
    parser.add_argument("--password", default=os.environ.get("SNOW_PASSWORD", ""))
    parser.add_argument("--incidents", type=int, default=50)
    args = parser.parse_args()

    if not args.instance_url or not args.username or not args.password:
        parser.error("--instance-url, --username, --password required (or set SNOW_* env vars)")

    seeder = ServiceNowSeeder(args.instance_url, args.username, args.password)
    seeder.seed(args.incidents)


if __name__ == "__main__":
    main()
