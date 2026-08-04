"""Versioned common DTO for STOCK/ETF analysis results.

Persistence remains separate from the historical crypto domain; service and API
consumers can nevertheless converge on this stable result representation.
"""
from __future__ import annotations

from typing import Any

ANALYSIS_PAYLOAD_VERSION = 2
COMPLETE_RESULT_FIELDS = (
    "stage", "bias", "state", "hierarchy_gate", "wyckoff_requirement",
    "one_hour_decision", "confirmation_model", "execution_trigger",
    "liquidity_context", "macro_liquidity_context", "entry_liquidity_context",
    "projected_target", "execution_target", "target", "score", "final_score",
    "blocking_reasons", "block_reasons", "reasons",
)


def analysis_result_payload(result: dict[str, Any]) -> dict[str, Any]:
    """Build a lossless v2 DTO while retaining the engine's original output."""
    nested = result.get("state_payload") or result.get("payload") or {}
    complete = dict(nested) if isinstance(nested, dict) else {"engine_payload": nested}
    for key, value in result.items():
        if key not in {"payload", "state_payload"}:
            complete.setdefault(key, value)
    for field in COMPLETE_RESULT_FIELDS:
        complete.setdefault(field, result.get(field))
    complete["schema_version"] = ANALYSIS_PAYLOAD_VERSION
    complete["raw_result"] = result
    return complete


def legacy_analysis_result_payload(payload: Any) -> dict[str, Any]:
    """Expose old rows without falsely labelling them as complete results."""
    value = payload if isinstance(payload, dict) else {}
    return value if "schema_version" in value else {"schema_version": 1, **value}
