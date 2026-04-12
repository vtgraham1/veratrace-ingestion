"""
Freshdesk ticket → TwuSignal mapper.

AI attribution via agent classification — cross-reference responder_id
and conversation authors against the agent cache to identify bot accounts.

Freshdesk Freddy AI features (Auto-Triage, Copilot, Self-Service) don't
expose explicit API fields. Attribution is inferred from:
1. Agent name patterns ("Freddy", "Bot", "AI Agent", "Auto")
2. Ticket source indicating automation
3. Conversation author cross-referenced against agent cache

Status values: 2=Open, 3=Pending, 4=Resolved, 5=Closed
Priority values: 1=Low, 2=Medium, 3=High, 4=Urgent
Source values: 1=Email, 2=Portal, 3=Phone, 7=Chat, 9=Widget, 10=Outbound
"""
from __future__ import annotations

import logging
from src.runtime.signal_writer import TwuSignal
from src.connectors.freshdesk.schema import REQUIRED_FIELDS, PII_FIELDS

logger = logging.getLogger(__name__)

TICKET_STATUSES = {2: "Open", 3: "Pending", 4: "Resolved", 5: "Closed"}
TICKET_PRIORITIES = {1: "Low", 2: "Medium", 3: "High", 4: "Urgent"}
TICKET_SOURCES = {
    1: "Email", 2: "Portal", 3: "Phone", 7: "Chat",
    9: "Feedback Widget", 10: "Outbound Email",
}

AI_AGENT_KEYWORDS = [
    "freddy", "bot", "ai agent", "auto-triage", "auto triage",
    "copilot", "self-service", "automation", "virtual agent",
]


def _classify_agent(agent_id, agent_cache):
    """
    Classify a Freshdesk agent as AI, HUMAN, or SYSTEM.

    Args:
        agent_id: Freshdesk agent ID (int or str)
        agent_cache: Dict of agent_id → agent info from /api/v2/agents

    Returns:
        Tuple of (actor_type, actor_name)
    """
    if not agent_id:
        return "SYSTEM", "freshdesk-system"

    agent = agent_cache.get(agent_id) or agent_cache.get(str(agent_id))
    if not agent:
        return "HUMAN", f"agent-{agent_id}"

    name = agent.get("name", "")
    name_lower = name.lower()

    # Check for AI/bot keywords in agent name
    if any(kw in name_lower for kw in AI_AGENT_KEYWORDS):
        return "AI", name

    return "HUMAN", name


def _get_pii_fields(ticket):
    return [f for f in PII_FIELDS if ticket.get(f)]


def _calc_resolution_seconds(ticket):
    """Calculate resolution time from ticket stats or timestamps."""
    stats = ticket.get("stats", {})
    if stats:
        # Freshdesk provides first_responded_at, resolved_at, etc.
        resolved = stats.get("resolved_at") or stats.get("closed_at")
        created = ticket.get("created_at")
        if resolved and created:
            try:
                from datetime import datetime as dt
                fmt = "%Y-%m-%dT%H:%M:%SZ"
                t_created = dt.strptime(created[:20].rstrip("Z") + "Z", fmt)
                t_resolved = dt.strptime(resolved[:20].rstrip("Z") + "Z", fmt)
                return int((t_resolved - t_created).total_seconds())
            except (ValueError, TypeError):
                pass
    return None


def map_ticket_to_signals(
    ticket: dict,
    conversations: list,
    agent_cache: dict,
    instance_id: str,
    integration_account_id: str,
) -> list:
    """
    Transform a Freshdesk ticket + conversations into TwuSignals.

    Produces 2-4 signals:
    1. ticket_created (always)
    2. freddy_interaction (if responder or conversation author is AI)
    3. agent_replied (if human agent replied)
    4. ticket_resolved (if status is Resolved or Closed)
    """
    signals = []
    ticket_id = ticket.get("id", "")
    created_at = ticket.get("created_at", "")
    updated_at = ticket.get("updated_at", "")
    subject = ticket.get("subject", "")

    # Check required fields
    missing = set()
    for f in REQUIRED_FIELDS:
        if not ticket.get(f):
            missing.add(f)
    is_degraded = bool(missing)
    degraded_reason = f"Missing: {', '.join(missing)}" if missing else ""

    status = ticket.get("status", 2)
    priority = ticket.get("priority", 1)
    source = ticket.get("source", 1)
    responder_id = ticket.get("responder_id")

    # Classify the assigned responder
    responder_type, responder_name = _classify_agent(responder_id, agent_cache)

    # Analyze conversations for AI/human reply attribution
    ai_replies = []
    human_replies = []
    for conv in (conversations or []):
        author_id = conv.get("user_id")
        author_type, author_name = _classify_agent(author_id, agent_cache)
        if author_type == "AI":
            ai_replies.append({"name": author_name, "created_at": conv.get("created_at", "")})
        elif author_type == "HUMAN":
            human_replies.append({"name": author_name, "created_at": conv.get("created_at", "")})

    has_ai = responder_type == "AI" or len(ai_replies) > 0
    has_human = responder_type == "HUMAN" or len(human_replies) > 0

    # ── Signal 1: ticket_created ──────────────────────────────────────────
    signals.append(TwuSignal(
        instance_id=instance_id,
        type="INTEGRATION_EVENT",
        name="ticket_created",
        occurred_at=created_at,
        source_integration_account_id=integration_account_id,
        source_integration="freshdesk",
        actor_type="SYSTEM",
        actor_agent_id="freshdesk-system",
        payload={
            "event": "ticket_created",
            "ticket_id": ticket_id,
            "subject": subject,
            "priority": TICKET_PRIORITIES.get(priority, str(priority)),
            "source": TICKET_SOURCES.get(source, str(source)),
            "status": TICKET_STATUSES.get(status, str(status)),
            "group_id": ticket.get("group_id"),
            "type": ticket.get("type"),
            "tags": ticket.get("tags", []),
        },
        degraded=is_degraded,
        degraded_reason=degraded_reason,
        pii_encrypted_fields=_get_pii_fields(ticket),
    ))

    # ── Signal 2: freddy_interaction (if AI involvement) ──────────────────
    if has_ai:
        ai_actor = ai_replies[0]["name"] if ai_replies else responder_name
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="AI",
            name="freddy_interaction",
            occurred_at=ai_replies[0]["created_at"] if ai_replies else created_at,
            source_integration_account_id=integration_account_id,
            source_integration="freshdesk",
            actor_type="AI",
            actor_agent_id=ai_actor,
            payload={
                "event": "freddy_interaction",
                "ticket_id": ticket_id,
                "ai_agent": ai_actor,
                "ai_replies": len(ai_replies),
                "contained": has_ai and not has_human,
            },
        ))

    # ── Signal 3: agent_replied (if human involvement) ────────────────────
    if has_human:
        human_actor = human_replies[0]["name"] if human_replies else responder_name
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="agent_replied",
            occurred_at=human_replies[0]["created_at"] if human_replies else updated_at,
            source_integration_account_id=integration_account_id,
            source_integration="freshdesk",
            actor_type="HUMAN",
            actor_agent_id=human_actor,
            payload={
                "event": "agent_replied",
                "ticket_id": ticket_id,
                "agent_name": human_actor,
                "human_replies": len(human_replies),
                "after_ai": has_ai,
            },
        ))

    # ── Signal 4: ticket_resolved (if resolved or closed) ────────────────
    if status in (4, 5):
        resolution_seconds = _calc_resolution_seconds(ticket)

        if has_ai and not has_human:
            resolved_by_type = "AI"
            resolved_by = ai_replies[0]["name"] if ai_replies else responder_name
        elif has_human:
            resolved_by_type = "HUMAN"
            resolved_by = human_replies[-1]["name"] if human_replies else responder_name
        else:
            resolved_by_type = "SYSTEM"
            resolved_by = "freshdesk-system"

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="ticket_resolved",
            occurred_at=updated_at,
            source_integration_account_id=integration_account_id,
            source_integration="freshdesk",
            actor_type=resolved_by_type,
            actor_agent_id=resolved_by,
            payload={
                "event": "ticket_resolved",
                "ticket_id": ticket_id,
                "status": TICKET_STATUSES.get(status, str(status)),
                "resolved_by": resolved_by,
                "resolved_by_type": resolved_by_type,
                "resolution_seconds": resolution_seconds,
                "freddy_contained": has_ai and not has_human,
                "freddy_to_agent": has_ai and has_human,
            },
        ))

    # Preserve raw on last signal
    if signals:
        signals[-1].payload["_raw"] = ticket

    return signals
