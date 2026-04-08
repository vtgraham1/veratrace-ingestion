"""
Schema validator — detects drift in vendor API responses.

Hashes the response shape (field names + types) and compares to expected.
On drift: log, flag signals as degraded, alert, and optionally auto-patch.
"""
import json
import hashlib
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SchemaDrift:
    field: str
    expected: str
    actual: str
    severity: str  # "added", "removed", "type_changed", "renamed"


def compute_schema_hash(data: dict | list) -> str:
    """
    Compute a deterministic hash of the response shape.
    Only considers field names and types, not values.
    """
    def extract_shape(obj, prefix=""):
        shapes = []
        if isinstance(obj, dict):
            for key in sorted(obj.keys()):
                full_key = f"{prefix}.{key}" if prefix else key
                val_type = type(obj[key]).__name__
                shapes.append(f"{full_key}:{val_type}")
                if isinstance(obj[key], (dict, list)):
                    shapes.extend(extract_shape(obj[key], full_key))
        elif isinstance(obj, list) and obj:
            shapes.extend(extract_shape(obj[0], f"{prefix}[]"))
        return shapes

    shape = sorted(extract_shape(data))
    shape_str = "|".join(shape)
    return hashlib.sha256(shape_str.encode()).hexdigest()[:16]


def detect_drift(
    response: dict | list,
    expected_hash: str,
    expected_fields: set[str] | None = None,
) -> tuple[str, list[SchemaDrift]]:
    """
    Compare response schema against expected.

    Returns:
        (current_hash, list of drift events)
    """
    current_hash = compute_schema_hash(response)
    drifts: list[SchemaDrift] = []

    if current_hash == expected_hash:
        return current_hash, drifts

    # Hash differs — analyze what changed
    if expected_fields and isinstance(response, dict):
        current_fields = set(response.keys())
        added = current_fields - expected_fields
        removed = expected_fields - current_fields

        for f in added:
            drifts.append(SchemaDrift(f, "absent", "present", "added"))
        for f in removed:
            drifts.append(SchemaDrift(f, "present", "absent", "removed"))

    if not drifts:
        # Hash changed but no obvious field-level diff — likely type change
        drifts.append(SchemaDrift(
            "__schema__", expected_hash, current_hash, "type_changed"
        ))

    for drift in drifts:
        level = logging.WARNING if drift.severity in ("added",) else logging.ERROR
        logger.log(
            level,
            "Schema drift: field=%s expected=%s actual=%s severity=%s",
            drift.field, drift.expected, drift.actual, drift.severity,
        )

    return current_hash, drifts


def is_breaking(drifts: list[SchemaDrift]) -> bool:
    """Returns True if any drift is breaking (removed or type_changed)."""
    return any(d.severity in ("removed", "type_changed") for d in drifts)
