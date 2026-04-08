"""
Synthetic data generator — produces realistic TwuSignals matching
Amazon Connect CTR patterns for various enterprise scenarios.

Usage:
  python3 -m synthetic.generator --scenario bpo_contact_center --signals 500
  python3 -m synthetic.generator --scenario bpo_contact_center --signals 500 --load
  python3 -m synthetic.generator --list
"""
import json
import uuid
import random
import sys
import os
from datetime import datetime, timedelta, timezone

# Add parent dir to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.runtime.signal_writer import TwuSignal, write_signals


# ── Agent profiles ─────────────────────────────────────────────────────────

HUMAN_AGENTS = [
    {"id": "agent-001", "name": "Sarah Chen", "role": "Senior Agent", "avg_handle_time": 320, "csat": 4.5},
    {"id": "agent-002", "name": "Marcus Johnson", "role": "Agent", "avg_handle_time": 420, "csat": 4.1},
    {"id": "agent-003", "name": "Priya Patel", "role": "Team Lead", "avg_handle_time": 280, "csat": 4.7},
    {"id": "agent-004", "name": "Alex Torres", "role": "Agent", "avg_handle_time": 380, "csat": 3.9},
    {"id": "agent-005", "name": "Emily Foster", "role": "Senior Agent", "avg_handle_time": 300, "csat": 4.6},
    {"id": "agent-006", "name": "Dev Sharma", "role": "Agent", "avg_handle_time": 450, "csat": 3.8},
    {"id": "agent-007", "name": "Rachel Kim", "role": "Agent", "avg_handle_time": 350, "csat": 4.3},
    {"id": "agent-008", "name": "James Wright", "role": "Team Lead", "avg_handle_time": 260, "csat": 4.8},
]

AI_AGENTS = [
    {"id": "ai-bot-001", "name": "ConnectBot v3.2", "type": "IVR", "resolution_rate": 0.35},
    {"id": "ai-bot-002", "name": "SmartRoute AI", "type": "Routing", "resolution_rate": 0.0},
    {"id": "ai-bot-003", "name": "ResolveAI Pro", "type": "AutoResolve", "resolution_rate": 0.72},
]

QUEUES = ["GeneralSupport", "BillingQueue", "TechSupport", "Escalations", "VIPQueue"]
CHANNELS = ["VOICE", "VOICE", "VOICE", "CHAT", "CHAT", "TASK"]  # weighted toward voice
DISCONNECT_REASONS = [
    "CUSTOMER_DISCONNECT", "CUSTOMER_DISCONNECT", "CUSTOMER_DISCONNECT",
    "AGENT_DISCONNECT", "AGENT_DISCONNECT",
    "THIRD_PARTY_DISCONNECT", "TELECOM_PROBLEM", "CONTACT_FLOW_DISCONNECT",
]

CUSTOMER_SEGMENTS = ["enterprise", "mid-market", "smb", "consumer"]


def _random_timestamp(base, spread_hours=720):
    """Random timestamp within spread_hours of base, biased toward business hours."""
    offset = random.randint(0, spread_hours * 3600)
    ts = base - timedelta(seconds=offset)
    # Bias toward business hours (8am-6pm)
    hour = ts.hour
    if hour < 8 or hour > 18:
        if random.random() < 0.7:  # 70% chance to shift to business hours
            ts = ts.replace(hour=random.randint(8, 17))
    return ts


def generate_contact(instance_id, integration_account_id, base_time, scenario_config):
    """Generate a realistic set of signals for one contact interaction."""
    contact_id = str(uuid.uuid4())
    channel = random.choice(CHANNELS)
    queue = random.choice(QUEUES)
    segment = random.choice(CUSTOMER_SEGMENTS)

    # Determine if AI or human handles this
    ai_ratio = scenario_config.get("ai_ratio", 0.4)
    is_ai_resolved = random.random() < ai_ratio
    ai_agent = random.choice(AI_AGENTS) if is_ai_resolved or random.random() < 0.8 else None
    human_agent = None if is_ai_resolved else random.choice(HUMAN_AGENTS)

    # Timestamps
    initiated_at = _random_timestamp(base_time, scenario_config.get("spread_hours", 720))
    queue_wait = random.randint(5, 120) if not is_ai_resolved else random.randint(1, 10)
    connected_at = initiated_at + timedelta(seconds=queue_wait)

    if human_agent:
        handle_time = max(60, int(random.gauss(human_agent["avg_handle_time"], 90)))
    else:
        handle_time = random.randint(15, 180)  # AI handles faster

    disconnected_at = connected_at + timedelta(seconds=handle_time)
    disconnect_reason = random.choice(DISCONNECT_REASONS)

    # Edge cases
    is_transfer = random.random() < scenario_config.get("transfer_rate", 0.12)
    is_sla_breach = queue_wait > scenario_config.get("sla_threshold_seconds", 60)
    is_escalation = random.random() < scenario_config.get("escalation_rate", 0.08)

    signals = []

    # Signal 1: Contact initiated
    signals.append(TwuSignal(
        instance_id=instance_id,
        type="INTEGRATION_EVENT",
        name="contact_initiated",
        occurred_at=initiated_at.isoformat(),
        source_integration_account_id=integration_account_id,
        source_integration="amazon-connect",
        actor_type="SYSTEM",
        actor_agent_id="connect-routing",
        payload={
            "event": "contact_initiated",
            "contact_id": contact_id,
            "channel": channel,
            "initiation_method": random.choice(["INBOUND", "INBOUND", "INBOUND", "CALLBACK"]),
            "queue": queue,
            "customer_segment": segment,
        },
    ))

    # Signal 2: AI processing (if applicable)
    if ai_agent:
        ai_process_time = random.randint(2, 15)
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="AI",
            name="ai_processing",
            occurred_at=(initiated_at + timedelta(seconds=ai_process_time)).isoformat(),
            source_integration_account_id=integration_account_id,
            source_integration="amazon-connect",
            actor_type="AI",
            actor_agent_id=ai_agent["id"],
            payload={
                "event": "ai_processing",
                "contact_id": contact_id,
                "ai_agent": ai_agent["name"],
                "ai_type": ai_agent["type"],
                "resolved_by_ai": is_ai_resolved,
                "confidence": round(random.uniform(0.45, 0.98), 2) if is_ai_resolved else round(random.uniform(0.2, 0.55), 2),
            },
        ))

    # Signal 3: Agent connected (if human handled)
    if human_agent:
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="agent_connected",
            occurred_at=connected_at.isoformat(),
            source_integration_account_id=integration_account_id,
            source_integration="amazon-connect",
            actor_type="HUMAN",
            actor_agent_id=human_agent["id"],
            payload={
                "event": "agent_connected",
                "contact_id": contact_id,
                "channel": channel,
                "agent_name": human_agent["name"],
                "agent_role": human_agent["role"],
                "queue": queue,
                "queue_wait_seconds": queue_wait,
                "sla_breached": is_sla_breach,
            },
        ))

    # Signal 4: Transfer (edge case)
    if is_transfer and human_agent:
        transfer_agent = random.choice([a for a in HUMAN_AGENTS if a["id"] != human_agent["id"]])
        transfer_at = connected_at + timedelta(seconds=random.randint(60, 180))
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="contact_transferred",
            occurred_at=transfer_at.isoformat(),
            source_integration_account_id=integration_account_id,
            source_integration="amazon-connect",
            actor_type="HUMAN",
            actor_agent_id=transfer_agent["id"],
            payload={
                "event": "contact_transferred",
                "contact_id": contact_id,
                "from_agent": human_agent["name"],
                "to_agent": transfer_agent["name"],
                "transfer_reason": random.choice(["escalation", "skill_mismatch", "shift_end", "language"]),
            },
        ))

    # Signal 5: Contact completed
    signals.append(TwuSignal(
        instance_id=instance_id,
        type="INTEGRATION_EVENT",
        name="contact_completed",
        occurred_at=disconnected_at.isoformat(),
        source_integration_account_id=integration_account_id,
        source_integration="amazon-connect",
        actor_type="AI" if is_ai_resolved else "HUMAN",
        actor_agent_id=(ai_agent["id"] if is_ai_resolved else human_agent["id"]) if (ai_agent or human_agent) else "system",
        payload={
            "event": "contact_completed",
            "contact_id": contact_id,
            "channel": channel,
            "disconnect_reason": disconnect_reason,
            "duration_seconds": handle_time + queue_wait,
            "handle_time_seconds": handle_time,
            "queue_wait_seconds": queue_wait,
            "resolved_by": "AI" if is_ai_resolved else "HUMAN",
            "sla_breached": is_sla_breach,
            "was_transferred": is_transfer,
            "was_escalated": is_escalation,
            "customer_segment": segment,
            "_raw_ctr": {
                "ContactId": contact_id,
                "Channel": channel,
                "Queue": {"Name": queue},
                "DisconnectReason": disconnect_reason,
            },
        },
    ))

    return signals


# ── Scenario configs ───────────────────────────────────────────────────────

SCENARIOS = {
    "bpo_contact_center": {
        "description": "500-agent BPO, 3 shifts, 40% AI routing, SLA disputes",
        "ai_ratio": 0.40,
        "transfer_rate": 0.15,
        "escalation_rate": 0.10,
        "sla_threshold_seconds": 45,
        "spread_hours": 720,  # 30 days
    },
    "enterprise_cx": {
        "description": "50-agent CX team, 70% AI auto-resolve, high CSAT focus",
        "ai_ratio": 0.70,
        "transfer_rate": 0.05,
        "escalation_rate": 0.03,
        "sla_threshold_seconds": 30,
        "spread_hours": 720,
    },
    "hybrid_outsourced": {
        "description": "Vendor claims 80% AI, reality is 45%. Billing reconciliation scenario.",
        "ai_ratio": 0.45,
        "transfer_rate": 0.12,
        "escalation_rate": 0.08,
        "sla_threshold_seconds": 60,
        "spread_hours": 720,
    },
}


def generate_scenario(instance_id, integration_account_id, scenario_name, num_contacts):
    """Generate all signals for a scenario."""
    config = SCENARIOS.get(scenario_name)
    if not config:
        print(f"Unknown scenario: {scenario_name}")
        print(f"Available: {', '.join(SCENARIOS.keys())}")
        return []

    base_time = datetime.now(timezone.utc)
    all_signals = []

    for i in range(num_contacts):
        contact_signals = generate_contact(instance_id, integration_account_id, base_time, config)
        all_signals.extend(contact_signals)
        if (i + 1) % 100 == 0:
            print(f"  Generated {i + 1}/{num_contacts} contacts ({len(all_signals)} signals)")

    print(f"Generated {num_contacts} contacts → {len(all_signals)} signals")
    print(f"  AI resolved: ~{int(config['ai_ratio'] * 100)}%")
    print(f"  Scenario: {config['description']}")
    return all_signals


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--list" in args:
        print("Available scenarios:")
        for name, config in SCENARIOS.items():
            print(f"  {name}: {config['description']}")
        sys.exit(0)

    scenario = "bpo_contact_center"
    num_contacts = 200
    load = "--load" in args

    if "--scenario" in args:
        idx = args.index("--scenario")
        scenario = args[idx + 1]

    if "--signals" in args:
        idx = args.index("--signals")
        num_contacts = int(args[idx + 1])

    # Use the vt-test-oauth instance for testing
    instance_id = os.environ.get("INSTANCE_ID", "549ffaef-158a-4d68-8672-2fa79a91edb9")
    # Use the integration account that was just created
    integration_account_id = os.environ.get("INTEGRATION_ACCOUNT_ID", "test-connect-account")

    signals = generate_scenario(instance_id, integration_account_id, scenario, num_contacts)

    if load and signals:
        print(f"\nLoading {len(signals)} signals to database...")
        # Write in batches of 100
        batch_size = 100
        total_written = 0
        for i in range(0, len(signals), batch_size):
            batch = signals[i:i + batch_size]
            written = write_signals(batch)
            total_written += written
            print(f"  Wrote batch {i // batch_size + 1}: {written} signals ({total_written} total)")
        print(f"Done: {total_written} signals loaded")
    elif not load:
        print(f"\nDry run — {len(signals)} signals generated but not loaded.")
        print("Add --load to write to database.")
