"""
Salesforce record → TwuSignal mapper.

Transforms Salesforce Case, Opportunity, and Task records into TwuSignals.

Key difference from Connect: a single Case may produce multiple signals
over days/weeks (created, assigned, AI interaction, resolved). Connect
contacts produce 2-4 signals in minutes.

AI attribution: looks for custom fields (AI_Handled__c, AI_Agent_Name__c,
AI_Confidence__c). If not present, defaults to HUMAN/SYSTEM. Unlike Connect
where Lex data is built-in, Salesforce requires the org to instrument AI fields.
"""
from __future__ import annotations

import logging
from src.runtime.signal_writer import TwuSignal
from src.connectors.salesforce.schema import REQUIRED_FIELDS, PII_FIELDS

logger = logging.getLogger(__name__)

# Custom AI attribution fields — org-specific, may not exist
AI_FIELDS = {"AI_Handled__c", "AI_Agent_Name__c", "AI_Confidence__c"}


def map_records_to_signals(
    record: dict,
    record_type: str,
    instance_id: str,
    integration_account_id: str,
) -> list:
    """
    Transform a Salesforce record into TwuSignals.

    Args:
        record: Salesforce API record (Case, Opportunity, or Task)
        record_type: "Case", "Opportunity", or "Task"
        instance_id: Veratrace instance ID
        integration_account_id: Integration account ID

    Returns:
        List of TwuSignal objects.
    """
    if record_type == "Case":
        return _map_case(record, instance_id, integration_account_id)
    elif record_type == "Opportunity":
        return _map_opportunity(record, instance_id, integration_account_id)
    return []


def _map_case(record, instance_id, integration_account_id):
    """Map a Salesforce Case to TwuSignals."""
    signals = []
    record_id = record.get("Id", "")
    created_at = record.get("CreatedDate", "")
    modified_at = record.get("SystemModstamp", "")

    missing = REQUIRED_FIELDS - set(record.keys())
    is_degraded = bool(missing)
    degraded_reason = f"Missing: {', '.join(missing)}" if missing else ""

    # Determine AI involvement
    ai_handled = record.get("AI_Handled__c")
    ai_agent = record.get("AI_Agent_Name__c", "")
    ai_confidence = _safe_float(record.get("AI_Confidence__c"))
    has_ai = ai_handled in ("true", "True", True, "Yes", "Partial")

    # Signal 1: case_created
    if created_at:
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="case_created",
            occurred_at=created_at,
            source_integration_account_id=integration_account_id,
            source_integration="salesforce",
            actor_type="SYSTEM",
            actor_agent_id="salesforce-system",
            payload={
                "event": "case_created",
                "case_id": record_id,
                "subject": record.get("Subject", ""),
                "priority": record.get("Priority", ""),
                "origin": record.get("Origin", ""),
                "status": record.get("Status", ""),
                "owner_id": record.get("OwnerId", ""),
            },
            degraded=is_degraded,
            degraded_reason=degraded_reason,
            pii_encrypted_fields=_get_pii_fields(record),
        ))

    # Signal 2: ai_interaction (if AI was involved)
    if has_ai:
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="AI",
            name="ai_interaction",
            occurred_at=modified_at or created_at,
            source_integration_account_id=integration_account_id,
            source_integration="salesforce",
            actor_type="AI",
            actor_agent_id=ai_agent or "salesforce-ai",
            payload={
                "event": "ai_interaction",
                "case_id": record_id,
                "ai_handled": str(ai_handled),
                "ai_agent": ai_agent,
                "ai_confidence": ai_confidence,
                "resolved_by_ai": ai_handled in ("true", "True", True, "Yes") and record.get("IsClosed"),
            },
        ))

    # Signal 3: case_resolved (if closed)
    is_closed = record.get("IsClosed")
    if is_closed:
        closed_at = record.get("ClosedDate") or modified_at
        resolved_by = "AI" if has_ai and ai_confidence and ai_confidence > 0.8 else "HUMAN"

        # Calculate resolution time if we have both dates
        resolution_seconds = None
        if created_at and closed_at:
            try:
                start = _parse_sf_timestamp(created_at)
                end = _parse_sf_timestamp(closed_at)
                if start and end:
                    resolution_seconds = int((end - start).total_seconds())
            except (ValueError, TypeError):
                pass

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="case_resolved",
            occurred_at=closed_at,
            source_integration_account_id=integration_account_id,
            source_integration="salesforce",
            actor_type=resolved_by if resolved_by == "AI" else "HUMAN",
            actor_agent_id=ai_agent if resolved_by == "AI" else record.get("OwnerId", "unknown"),
            payload={
                "event": "case_resolved",
                "case_id": record_id,
                "status": record.get("Status", ""),
                "resolved_by": resolved_by,
                "resolution_seconds": resolution_seconds,
                "priority": record.get("Priority", ""),
            },
        ))

    # Preserve raw record in last signal
    if signals:
        signals[-1].payload["_raw"] = record

    return signals


def _map_opportunity(record, instance_id, integration_account_id):
    """Map a Salesforce Opportunity to TwuSignals."""
    signals = []
    record_id = record.get("Id", "")
    created_at = record.get("CreatedDate", "")
    modified_at = record.get("SystemModstamp", "")

    missing = REQUIRED_FIELDS - set(record.keys())
    is_degraded = bool(missing)
    degraded_reason = f"Missing: {', '.join(missing)}" if missing else ""

    # Signal 1: opp_created
    if created_at:
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="opp_created",
            occurred_at=created_at,
            source_integration_account_id=integration_account_id,
            source_integration="salesforce",
            actor_type="SYSTEM",
            actor_agent_id="salesforce-system",
            payload={
                "event": "opp_created",
                "opp_id": record_id,
                "name": record.get("Name", ""),
                "stage": record.get("StageName", ""),
                "amount": record.get("Amount"),
                "probability": record.get("Probability"),
                "owner_id": record.get("OwnerId", ""),
            },
            degraded=is_degraded,
            degraded_reason=degraded_reason,
        ))

    # Signal 2: opp_closed (if closed)
    is_closed = record.get("IsClosed")
    if is_closed:
        close_date = record.get("CloseDate") or modified_at
        is_won = record.get("IsWon", False)

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="opp_closed",
            occurred_at=close_date,
            source_integration_account_id=integration_account_id,
            source_integration="salesforce",
            actor_type="HUMAN",
            actor_agent_id=record.get("OwnerId", "unknown"),
            payload={
                "event": "opp_closed",
                "opp_id": record_id,
                "won": is_won,
                "amount": record.get("Amount"),
                "stage": record.get("StageName", ""),
            },
        ))

    if signals:
        signals[-1].payload["_raw"] = record

    return signals


def _safe_float(value):
    """Safely convert a value to float, returning None if not possible."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_sf_timestamp(ts):
    """Parse Salesforce timestamp formats (handles +0000 and Z and +00:00)."""
    if not ts:
        return None
    from datetime import datetime as dt
    import re
    # Salesforce uses +0000 (no colon) — Python 3.9 fromisoformat needs +00:00
    ts = re.sub(r'(\+\d{2})(\d{2})$', r'\1:\2', ts)
    ts = ts.replace("Z", "+00:00")
    return dt.fromisoformat(ts)


def _get_pii_fields(record):
    """Identify PII fields present in this record."""
    return [f for f in PII_FIELDS if record.get(f)]
