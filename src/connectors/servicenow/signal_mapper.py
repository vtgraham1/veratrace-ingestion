"""
ServiceNow Incident → TwuSignal mapper.

Transforms ServiceNow Incident records + sys_audit history into TwuSignals.

AI attribution strategy (inference-based):
  1. sys_audit: check who made field changes — service accounts,
     virtual agents, and Now Assist users are classified as AI/SYSTEM
  2. resolved_by: if the resolver is a known AI actor → AI attribution
  3. Fallback: if sys_ai_resolution table exists → first-class attribution

Actor classification:
  - Standard user → HUMAN
  - Service account → SYSTEM
  - Virtual Agent / Now Assist / AI in name → AI
  - web_service_definition → SYSTEM
"""
from __future__ import annotations

import logging
from src.runtime.signal_writer import TwuSignal
from src.connectors.servicenow.schema import REQUIRED_FIELDS, PII_FIELDS

logger = logging.getLogger(__name__)

# ServiceNow incident state values
INCIDENT_STATES = {
    "1": "New",
    "2": "In Progress",
    "3": "On Hold",
    "6": "Resolved",
    "7": "Closed",
    "8": "Canceled",
}

# Keywords indicating AI actor
AI_ACTOR_KEYWORDS = [
    "virtual agent", "now assist", "virtual_agent",
    "nowassist", "ai_agent", "chatbot", "auto-resolve",
    "predictive intelligence", "agent intelligence",
]


def _extract_value(field_data):
    """Extract value from ServiceNow display_value format.
    Fields may be plain strings or {"value": "...", "display_value": "..."}"""
    if isinstance(field_data, dict):
        return field_data.get("display_value", "") or field_data.get("value", "")
    return field_data or ""


def _extract_raw_value(field_data):
    """Extract the raw sys_id / internal value."""
    if isinstance(field_data, dict):
        return field_data.get("value", "")
    return field_data or ""


def _classify_actor(user_display_name, user_value=""):
    """
    Classify a ServiceNow user as AI, HUMAN, or SYSTEM.

    Args:
        user_display_name: Display name of the user
        user_value: sys_id or raw value of the user

    Returns:
        Tuple of (actor_type, actor_id)
    """
    if not user_display_name and not user_value:
        return "SYSTEM", "servicenow-system"

    name_lower = (user_display_name or "").lower()

    # Check for AI actor keywords
    if any(kw in name_lower for kw in AI_ACTOR_KEYWORDS):
        return "AI", user_display_name or user_value

    # Check for system/service account patterns
    if any(pattern in name_lower for pattern in [
        "system", "service account", "integration", "api",
        "workflow", "flow designer", "process automation",
    ]):
        return "SYSTEM", user_display_name or user_value

    # Default: human user
    return "HUMAN", user_display_name or user_value


def _get_pii_fields(record):
    """Return list of PII field names present in the record."""
    return [f for f in PII_FIELDS if record.get(f)]


def map_incident_to_signals(
    record: dict,
    audit_records: list,
    instance_id: str,
    integration_account_id: str,
) -> list:
    """
    Transform a ServiceNow Incident + its audit trail into TwuSignals.

    Produces 2-4 signals per incident:
    1. incident_created (always)
    2. ai_interaction (if AI involvement detected in audit)
    3. agent_assigned (if assigned_to is a human)
    4. incident_resolved (if state is Resolved/Closed)
    """
    signals = []
    sys_id = _extract_raw_value(record.get("sys_id"))
    number = _extract_value(record.get("number"))
    created_at = _extract_value(record.get("sys_created_on")) or _extract_value(record.get("opened_at"))
    updated_at = _extract_value(record.get("sys_updated_on"))

    # Check for required fields
    missing = set()
    for f in REQUIRED_FIELDS:
        val = record.get(f)
        if not val or (isinstance(val, dict) and not val.get("value")):
            missing.add(f)
    is_degraded = bool(missing)
    degraded_reason = f"Missing: {', '.join(missing)}" if missing else ""

    # ── Signal 1: incident_created ────────────────────────────────────────
    signals.append(TwuSignal(
        instance_id=instance_id,
        type="INTEGRATION_EVENT",
        name="incident_created",
        occurred_at=created_at,
        source_integration_account_id=integration_account_id,
        source_integration="servicenow",
        actor_type="SYSTEM",
        actor_agent_id="servicenow-system",
        payload={
            "event": "incident_created",
            "incident_id": sys_id,
            "number": number,
            "short_description": _extract_value(record.get("short_description")),
            "priority": _extract_value(record.get("priority")),
            "urgency": _extract_value(record.get("urgency")),
            "impact": _extract_value(record.get("impact")),
            "category": _extract_value(record.get("category")),
            "subcategory": _extract_value(record.get("subcategory")),
            "contact_type": _extract_value(record.get("contact_type")),
            "opened_by": _extract_value(record.get("opened_by")),
        },
        degraded=is_degraded,
        degraded_reason=degraded_reason,
        pii_encrypted_fields=_get_pii_fields(record),
    ))

    # ── Analyze audit records for AI involvement ──────────────────────────
    ai_actors = []
    for audit in audit_records:
        user_display = _extract_value(audit.get("user"))
        user_raw = _extract_raw_value(audit.get("user"))
        actor_type, actor_id = _classify_actor(user_display, user_raw)
        if actor_type == "AI":
            ai_actors.append({
                "actor_id": actor_id,
                "field": _extract_value(audit.get("fieldname")),
                "occurred_at": _extract_value(audit.get("sys_created_on")),
            })

    # ── Signal 2: ai_interaction (if AI activity detected) ───────────────
    if ai_actors:
        first_ai = ai_actors[0]
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="AI",
            name="ai_interaction",
            occurred_at=first_ai["occurred_at"] or updated_at,
            source_integration_account_id=integration_account_id,
            source_integration="servicenow",
            actor_type="AI",
            actor_agent_id=first_ai["actor_id"],
            payload={
                "event": "ai_interaction",
                "incident_id": sys_id,
                "number": number,
                "ai_actor": first_ai["actor_id"],
                "ai_actions": len(ai_actors),
                "fields_modified": list({a["field"] for a in ai_actors}),
            },
        ))

    # ── Signal 3: agent_assigned (if human assigned) ──────────────────────
    assigned_to = _extract_value(record.get("assigned_to"))
    if assigned_to:
        actor_type, actor_id = _classify_actor(assigned_to, _extract_raw_value(record.get("assigned_to")))
        if actor_type == "HUMAN":
            signals.append(TwuSignal(
                instance_id=instance_id,
                type="INTEGRATION_EVENT",
                name="agent_assigned",
                occurred_at=updated_at,
                source_integration_account_id=integration_account_id,
                source_integration="servicenow",
                actor_type="HUMAN",
                actor_agent_id=actor_id,
                payload={
                    "event": "agent_assigned",
                    "incident_id": sys_id,
                    "number": number,
                    "assigned_to": actor_id,
                    "assignment_group": _extract_value(record.get("assignment_group")),
                },
            ))

    # ── Signal 4: incident_resolved (if resolved or closed) ──────────────
    state_val = _extract_raw_value(record.get("state"))
    if state_val in ("6", "7"):  # Resolved or Closed
        resolved_by_display = _extract_value(record.get("resolved_by"))
        resolved_by_raw = _extract_raw_value(record.get("resolved_by"))
        resolved_at = _extract_value(record.get("resolved_at")) or _extract_value(record.get("closed_at"))

        # Determine resolver actor type
        if resolved_by_display:
            resolver_type, resolver_id = _classify_actor(resolved_by_display, resolved_by_raw)
        elif ai_actors:
            resolver_type, resolver_id = "AI", ai_actors[0]["actor_id"]
        else:
            resolver_type, resolver_id = "HUMAN", "unknown"

        # Calculate resolution time if we have opened_at and resolved_at
        resolution_seconds = None
        opened_at = _extract_value(record.get("opened_at"))
        if opened_at and resolved_at:
            try:
                from datetime import datetime as dt
                # ServiceNow timestamps: "2026-04-10 14:30:00"
                fmt = "%Y-%m-%d %H:%M:%S"
                t_open = dt.strptime(opened_at[:19], fmt)
                t_resolve = dt.strptime(resolved_at[:19], fmt)
                resolution_seconds = int((t_resolve - t_open).total_seconds())
            except (ValueError, TypeError):
                pass

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="incident_resolved",
            occurred_at=resolved_at or updated_at,
            source_integration_account_id=integration_account_id,
            source_integration="servicenow",
            actor_type=resolver_type,
            actor_agent_id=resolver_id,
            payload={
                "event": "incident_resolved",
                "incident_id": sys_id,
                "number": number,
                "state": INCIDENT_STATES.get(state_val, state_val),
                "resolved_by": resolver_id,
                "resolved_by_type": resolver_type,
                "close_code": _extract_value(record.get("close_code")),
                "close_notes": _extract_value(record.get("close_notes")),
                "resolution_seconds": resolution_seconds,
            },
        ))

    # Preserve raw record on the last signal
    if signals:
        signals[-1].payload["_raw"] = record

    return signals


def map_audit_to_signals(
    audit_records: list,
    instance_id: str,
    integration_account_id: str,
) -> list:
    """Map standalone sys_audit records to signals (for direct audit sync)."""
    signals = []
    for audit in audit_records:
        user_display = _extract_value(audit.get("user"))
        actor_type, actor_id = _classify_actor(user_display)

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="incident_field_changed",
            occurred_at=_extract_value(audit.get("sys_created_on")),
            source_integration_account_id=integration_account_id,
            source_integration="servicenow",
            actor_type=actor_type,
            actor_agent_id=actor_id,
            payload={
                "event": "incident_field_changed",
                "incident_id": _extract_raw_value(audit.get("documentkey")),
                "field": _extract_value(audit.get("fieldname")),
                "old_value": _extract_value(audit.get("oldvalue")),
                "new_value": _extract_value(audit.get("newvalue")),
            },
        ))

    return signals
