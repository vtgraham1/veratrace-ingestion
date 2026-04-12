"""
Genesys Cloud conversation → TwuSignal mapper.

First-class bot attribution via participant.purpose field:
  "bot"      → AI actor
  "agent"    → HUMAN actor
  "customer" → not an actor (the customer)
  "acd"      → SYSTEM (routing)
  "external" → SYSTEM (transfer)

Each conversation produces 2-4 signals based on participant mix.
"""
from __future__ import annotations

import logging
from src.runtime.signal_writer import TwuSignal
from src.connectors.genesys.schema import REQUIRED_FIELDS, PII_FIELDS

logger = logging.getLogger(__name__)

PURPOSE_TO_ACTOR = {
    "bot": "AI",
    "agent": "HUMAN",
    "acd": "SYSTEM",
    "external": "SYSTEM",
    "workflow": "SYSTEM",
}


def _get_participants_by_purpose(conversation):
    """Group participants by purpose, excluding customers."""
    by_purpose = {}
    for p in conversation.get("participants", []):
        purpose = p.get("purpose", "")
        if purpose and purpose != "customer":
            by_purpose.setdefault(purpose, []).append(p)
    return by_purpose


def _get_participant_name(participant):
    """Extract participant name from sessions or attributes."""
    for session in participant.get("sessions", []):
        for seg in session.get("segments", []):
            if seg.get("segmentType") == "interact":
                return seg.get("sourceConversationId", "")
    return participant.get("participantId", "unknown")


def _calc_duration_seconds(conversation):
    """Calculate conversation duration in seconds."""
    start = conversation.get("conversationStart", "")
    end = conversation.get("conversationEnd", "")
    if not start or not end:
        return None
    try:
        from datetime import datetime as dt
        fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
        t_start = dt.strptime(start.rstrip("Z")[:26] + "Z", fmt)
        t_end = dt.strptime(end.rstrip("Z")[:26] + "Z", fmt)
        return int((t_end - t_start).total_seconds())
    except (ValueError, TypeError):
        return None


def _get_pii_fields(conversation):
    """Return list of PII field names present."""
    return [f for f in PII_FIELDS if conversation.get(f)]


def map_conversation_to_signals(
    conversation: dict,
    instance_id: str,
    integration_account_id: str,
) -> list:
    """
    Transform a Genesys Cloud conversation into TwuSignals.

    Produces 2-4 signals:
    1. conversation_started (always)
    2. bot_interaction (if bot participant exists)
    3. agent_interaction (if agent participant exists)
    4. conversation_ended (always)
    """
    signals = []
    conv_id = conversation.get("conversationId", "")
    start_time = conversation.get("conversationStart", "")
    end_time = conversation.get("conversationEnd", "")

    # Check required fields
    missing = set()
    for f in REQUIRED_FIELDS:
        if not conversation.get(f):
            missing.add(f)
    is_degraded = bool(missing)
    degraded_reason = f"Missing: {', '.join(missing)}" if missing else ""

    by_purpose = _get_participants_by_purpose(conversation)
    has_bot = "bot" in by_purpose
    has_agent = "agent" in by_purpose

    # Determine media type from first participant's sessions
    media_type = "unknown"
    for p in conversation.get("participants", []):
        for session in p.get("sessions", []):
            mt = session.get("mediaType", "")
            if mt:
                media_type = mt
                break
        if media_type != "unknown":
            break

    # ── Signal 1: conversation_started ────────────────────────────────────
    signals.append(TwuSignal(
        instance_id=instance_id,
        type="INTEGRATION_EVENT",
        name="conversation_started",
        occurred_at=start_time,
        source_integration_account_id=integration_account_id,
        source_integration="genesys",
        actor_type="SYSTEM",
        actor_agent_id="genesys-system",
        payload={
            "event": "conversation_started",
            "conversation_id": conv_id,
            "media_type": media_type,
            "participant_count": len(conversation.get("participants", [])),
            "has_bot": has_bot,
            "has_agent": has_agent,
        },
        degraded=is_degraded,
        degraded_reason=degraded_reason,
        pii_encrypted_fields=_get_pii_fields(conversation),
    ))

    # ── Signal 2: bot_interaction (if bot participant) ────────────────────
    if has_bot:
        bot_participants = by_purpose["bot"]
        bot = bot_participants[0]
        bot_contained = has_bot and not has_agent

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="AI",
            name="bot_interaction",
            occurred_at=start_time,
            source_integration_account_id=integration_account_id,
            source_integration="genesys",
            actor_type="AI",
            actor_agent_id=bot.get("participantName", bot.get("participantId", "genesys-bot")),
            payload={
                "event": "bot_interaction",
                "conversation_id": conv_id,
                "bot_id": bot.get("participantId", ""),
                "bot_name": bot.get("participantName", ""),
                "contained": bot_contained,
                "sessions": len(bot.get("sessions", [])),
            },
        ))

    # ── Signal 3: agent_interaction (if agent participant) ────────────────
    if has_agent:
        agent_participants = by_purpose["agent"]
        agent = agent_participants[0]

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="agent_interaction",
            occurred_at=end_time or start_time,
            source_integration_account_id=integration_account_id,
            source_integration="genesys",
            actor_type="HUMAN",
            actor_agent_id=agent.get("participantName", agent.get("participantId", "unknown-agent")),
            payload={
                "event": "agent_interaction",
                "conversation_id": conv_id,
                "agent_id": agent.get("participantId", ""),
                "agent_name": agent.get("participantName", ""),
                "sessions": len(agent.get("sessions", [])),
                "after_bot": has_bot,
            },
        ))

    # ── Signal 4: conversation_ended ──────────────────────────────────────
    duration = _calc_duration_seconds(conversation)

    if has_bot and not has_agent:
        resolved_by_type = "AI"
        resolved_by = by_purpose["bot"][0].get("participantName", "genesys-bot")
    elif has_agent:
        resolved_by_type = "HUMAN"
        resolved_by = by_purpose["agent"][0].get("participantName", "unknown-agent")
    else:
        resolved_by_type = "SYSTEM"
        resolved_by = "genesys-system"

    signals.append(TwuSignal(
        instance_id=instance_id,
        type="INTEGRATION_EVENT",
        name="conversation_ended",
        occurred_at=end_time or start_time,
        source_integration_account_id=integration_account_id,
        source_integration="genesys",
        actor_type=resolved_by_type,
        actor_agent_id=resolved_by,
        payload={
            "event": "conversation_ended",
            "conversation_id": conv_id,
            "duration_seconds": duration,
            "resolved_by": resolved_by,
            "resolved_by_type": resolved_by_type,
            "bot_contained": has_bot and not has_agent,
            "bot_to_agent": has_bot and has_agent,
            "media_type": media_type,
        },
    ))

    # Preserve raw on last signal
    if signals:
        signals[-1].payload["_raw"] = conversation

    return signals
