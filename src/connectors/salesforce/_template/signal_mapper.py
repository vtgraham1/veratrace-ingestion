"""
TODO(connector): Signal mapper for your platform.

Reference: src/connectors/amazon_connect/signal_mapper.py

Transforms vendor API responses into TwuSignals. Each API record
typically produces 1-4 signals representing the lifecycle of a
work interaction.

Signal naming convention:
  - {entity}_initiated   (e.g., ticket_created, case_opened)
  - ai_interaction       (when AI processed/routed the work)
  - agent_connected      (when a human took over)
  - {entity}_completed   (e.g., ticket_resolved, case_closed)
"""
from __future__ import annotations

import logging
from src.runtime.signal_writer import TwuSignal
from src.connectors._template.schema import REQUIRED_FIELDS, PII_FIELDS

logger = logging.getLogger(__name__)


def map_to_signals(
    record: dict,
    instance_id: str,
    integration_account_id: str,
) -> list:
    """
    TODO(connector): Transform a vendor API record into TwuSignals.

    Args:
        record: Raw API response object (e.g., a ticket, case, contact)
        instance_id: Veratrace instance ID
        integration_account_id: Integration account ID

    Returns:
        List of TwuSignal objects. Typically 1-4 per record.

    Guidelines:
        - Always include a "completed" signal with duration and resolution
        - Set actor_type to "AI", "HUMAN", or "SYSTEM"
        - Set degraded=True if REQUIRED_FIELDS are missing
        - Preserve the raw record in the last signal's payload["_raw"]
    """
    signals = []
    record_id = record.get("id", "")

    # TODO(connector): Check for required fields
    missing = REQUIRED_FIELDS - set(record.keys())
    is_degraded = bool(missing)

    # TODO(connector): Signal 1 — entity initiated
    # signals.append(TwuSignal(
    #     instance_id=instance_id,
    #     type="INTEGRATION_EVENT",
    #     name="entity_initiated",
    #     occurred_at=record.get("created_at", ""),
    #     source_integration_account_id=integration_account_id,
    #     source_integration="your-platform",  # must match CONNECTOR_ID
    #     actor_type="SYSTEM",
    #     actor_agent_id="routing",
    #     payload={"event": "entity_initiated", "record_id": record_id},
    #     degraded=is_degraded,
    # ))

    # TODO(connector): Signal 2 — AI interaction (if AI was involved)

    # TODO(connector): Signal 3 — agent connected (if human handled)

    # TODO(connector): Signal 4 — entity completed
    # Include: duration, resolution, structured attributes, Contact Lens if available

    # Preserve raw record in last signal
    if signals:
        signals[-1].payload["_raw"] = record

    return signals
