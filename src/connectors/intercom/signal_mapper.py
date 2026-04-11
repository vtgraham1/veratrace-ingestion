"""
Intercom conversation → TwuSignal mapper.

Intercom has first-class AI attribution — no inference needed:
  - ai_agent.did_resolve → Fin resolved it
  - ai_agent.resolution_state → "resolved" or "escalated"
  - conversation_parts[].author.type → "user", "admin", "bot"

Each conversation produces 2-5 signals depending on what happened.
"""
from __future__ import annotations

import logging
from src.runtime.signal_writer import TwuSignal
from src.connectors.intercom.schema import REQUIRED_FIELDS, PII_FIELDS

logger = logging.getLogger(__name__)


def map_conversation_to_signals(
    conversation: dict,
    instance_id: str,
    integration_account_id: str,
) -> list:
    signals = []
    conv_id = str(conversation.get("id", ""))
    created_at = conversation.get("created_at", 0)
    updated_at = conversation.get("updated_at", 0)
    state = conversation.get("state", "open")

    missing = REQUIRED_FIELDS - set(conversation.keys())
    is_degraded = bool(missing)
    degraded_reason = f"Missing: {', '.join(missing)}" if missing else ""

    # Determine channel
    source = conversation.get("source", {}) or {}
    channel = source.get("type", "unknown")

    # Customer info
    contacts = conversation.get("contacts", {})
    contact_list = contacts.get("contacts", contacts) if isinstance(contacts, dict) else contacts
    customer = contact_list[0] if isinstance(contact_list, list) and contact_list else {}
    customer_name = customer.get("name", "")
    customer_email = customer.get("email", "")

    # AI agent info
    ai_agent = conversation.get("ai_agent", {}) or {}
    fin_involved = bool(ai_agent.get("bot"))
    fin_resolved = ai_agent.get("did_resolve", False)
    resolution_state = ai_agent.get("resolution_state", "")

    # Signal 1: conversation_created
    if created_at:
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="conversation_created",
            occurred_at=_unix_to_iso(created_at),
            source_integration_account_id=integration_account_id,
            source_integration="intercom",
            actor_type="SYSTEM",
            actor_agent_id="intercom-system",
            payload={
                "event": "conversation_created",
                "conversation_id": conv_id,
                "channel": channel,
                "customer_name": customer_name,
                "state": state,
            },
            degraded=is_degraded,
            degraded_reason=degraded_reason,
            pii_encrypted_fields=["customer_email"] if customer_email else [],
        ))

    # Signal 2: fin_interaction (when Fin was involved)
    if fin_involved:
        rating = ai_agent.get("resolution_rating", {}) or {}
        content_sources = ai_agent.get("content_sources", [])

        confidence = 1.0 if fin_resolved else 0.5
        if resolution_state == "escalated":
            confidence = 0.3

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="AI",
            name="fin_interaction",
            occurred_at=_unix_to_iso(updated_at or created_at),
            source_integration_account_id=integration_account_id,
            source_integration="intercom",
            actor_type="AI",
            actor_agent_id=ai_agent.get("bot", "Fin"),
            payload={
                "event": "fin_interaction",
                "conversation_id": conv_id,
                "bot_name": ai_agent.get("bot", "Fin"),
                "did_resolve": fin_resolved,
                "resolution_state": resolution_state,
                "confidence": confidence,
                "content_sources": content_sources,
                "csat_rating": rating.get("rating"),
                "csat_remark": rating.get("remark", ""),
            },
        ))

    # Signal 3: agent_replied (when human admin participated)
    parts = conversation.get("conversation_parts", {})
    parts_list = parts.get("conversation_parts", parts.get("data", parts)) if isinstance(parts, dict) else parts
    if not isinstance(parts_list, list):
        parts_list = []

    human_admins = set()
    bot_messages = 0
    human_messages = 0

    for part in parts_list:
        author = part.get("author", {}) or {}
        author_type = author.get("type", "")

        if author_type == "admin":
            admin_id = author.get("id", "")
            admin_name = author.get("name", "")
            if admin_id and admin_id not in human_admins:
                human_admins.add(admin_id)
                signals.append(TwuSignal(
                    instance_id=instance_id,
                    type="INTEGRATION_EVENT",
                    name="agent_replied",
                    occurred_at=_unix_to_iso(part.get("created_at", updated_at)),
                    source_integration_account_id=integration_account_id,
                    source_integration="intercom",
                    actor_type="HUMAN",
                    actor_agent_id=admin_id,
                    payload={
                        "event": "agent_replied",
                        "conversation_id": conv_id,
                        "admin_name": admin_name,
                    },
                ))
            human_messages += 1
        elif author_type == "bot":
            bot_messages += 1

    # Signal 4: conversation_resolved (when closed)
    if state == "closed":
        # Determine who resolved
        if fin_resolved:
            resolved_by = "AI"
            resolver_id = ai_agent.get("bot", "Fin")
        elif human_admins:
            resolved_by = "HUMAN"
            resolver_id = list(human_admins)[-1]  # last human to participate
        else:
            resolved_by = "SYSTEM"
            resolver_id = "system"

        duration_seconds = (updated_at - created_at) if updated_at and created_at else None
        total_messages = bot_messages + human_messages
        rating = ai_agent.get("resolution_rating", {}) or {}

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="conversation_resolved",
            occurred_at=_unix_to_iso(updated_at),
            source_integration_account_id=integration_account_id,
            source_integration="intercom",
            actor_type=resolved_by if resolved_by != "SYSTEM" else "SYSTEM",
            actor_agent_id=resolver_id,
            payload={
                "event": "conversation_resolved",
                "conversation_id": conv_id,
                "resolved_by": resolved_by,
                "duration_seconds": duration_seconds,
                "message_count": total_messages,
                "bot_messages": bot_messages,
                "human_messages": human_messages,
                "csat_rating": rating.get("rating"),
                "fin_involved": fin_involved,
                "fin_resolved": fin_resolved,
            },
        ))

    # Preserve raw conversation in last signal
    if signals:
        signals[-1].payload["_raw"] = conversation

    return signals


def _unix_to_iso(ts):
    """Convert unix timestamp to ISO 8601."""
    if not ts:
        return ""
    try:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return str(ts)
