"""
Live registry test — proves the scheduler can actually see the integration accounts
that exist in production. Pairs with the unit tests in tests/test_scheduler_registry.py.

Today (2026-04-16) this test FAILS because scheduler.fetch_active_accounts() hits
Supabase for an `integration_accounts` table that doesn't exist (404). It will pass
once Phase 1 is resolved (either dual-write to Supabase, or scheduler reads control
plane API directly). When green, it becomes the cutover gate.

Per `feedback_external_ids.md`: the known-good UUIDs live in env vars, not constants.
Set in deploy workflow + locally for dev.

Required env:
  KNOWN_GOOD_INSTANCE_ID  — instance UUID with at least one connected integration
  KNOWN_GOOD_AC_ACCOUNT_ID — Amazon Connect integration account UUID under that instance

Run: python -m pytest tests/contract/test_integration_accounts_registry.py -v
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


KNOWN_GOOD_INSTANCE_ID = os.environ.get("KNOWN_GOOD_INSTANCE_ID", "")
KNOWN_GOOD_AC_ACCOUNT_ID = os.environ.get("KNOWN_GOOD_AC_ACCOUNT_ID", "")


pytestmark = [
    pytest.mark.skipif(
        not (KNOWN_GOOD_INSTANCE_ID and KNOWN_GOOD_AC_ACCOUNT_ID),
        reason="Registry env vars not set (KNOWN_GOOD_INSTANCE_ID, KNOWN_GOOD_AC_ACCOUNT_ID)",
    ),
    # Cutover gate — expected to fail until Phase 1 resolves (Joey's M2M filter
    # update OR some other path that populates the scheduler's data source).
    # strict=True: when the test starts passing, CI fails with XPASS → signal
    # to remove this marker and deal with whatever newly activates. Silences
    # nightly failure emails in the meantime. See project_phase1_eval_...md.
    pytest.mark.xfail(strict=True, reason="Phase 1 cutover gate — passes when data plane flips"),
]


class TestSchedulerSeesKnownAccount:
    def test_known_ac_account_visible_to_scheduler(self):
        """The scheduler MUST see the known-good AC account in instancetest1.

        This is the canonical 'integrations are wired end-to-end' test. If this is red,
        we'll never sync data no matter how healthy individual connectors look.

        Accepts both ACTIVE and DRAFT — the UI mapper treats both as active, and the
        scheduler should too. If the scheduler is filtering ACTIVE-only when this passes
        for DRAFT, that's a separate bug worth surfacing here.
        """
        from src.sync.scheduler import fetch_active_accounts

        accounts = fetch_active_accounts(instance_id=KNOWN_GOOD_INSTANCE_ID)

        ids = [a.get("integration_account_id") for a in accounts]
        assert KNOWN_GOOD_AC_ACCOUNT_ID in ids, (
            f"Scheduler can't see known-good AC account {KNOWN_GOOD_AC_ACCOUNT_ID} "
            f"under instance {KNOWN_GOOD_INSTANCE_ID}. Got accounts: {ids}. "
            "Phase 1 data-plane fix may be incomplete, OR account status is filtered out."
        )

    def test_known_account_has_required_fields(self):
        """If we see the account, it must have the fields the connector needs."""
        from src.sync.scheduler import fetch_active_accounts

        accounts = fetch_active_accounts(instance_id=KNOWN_GOOD_INSTANCE_ID)
        target = next(
            (a for a in accounts if a.get("integration_account_id") == KNOWN_GOOD_AC_ACCOUNT_ID),
            None,
        )
        if target is None:
            pytest.skip("Account not visible — first test will report the underlying issue")

        # Snake_case (Supabase shape). If Phase 1 lands as Option B (control plane API),
        # this assertion will fail and force us to add a normalization layer — which is
        # the correct response to the camelCase/snake_case mismatch already flagged.
        for field in ("integration_id", "integration_account_id", "instance_id",
                      "auth_credentials", "external_identity"):
            assert field in target, f"Missing required field: {field}. Account shape: {list(target.keys())}"
