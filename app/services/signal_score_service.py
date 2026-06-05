from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models.momentum_current import MomentumCurrent


class SignalScoreService:
    """Harmonize the public signal score after all pipeline gates ran.

    The signal engine can keep its legacy/internal score, while this service owns
    the final score exposed to asset state, candidates and UI payloads.
    """

    ZONE_SCORE_MIN = 0.0
    ZONE_SCORE_MAX = 6.0
    MOMENTUM_SCORE_MIN = -6.0
    MOMENTUM_SCORE_MAX = 8.0

    def __init__(self, db: Session) -> None:
        self.db = db

    def apply(self, signal: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(signal, dict):
            return signal

        legacy_score = self._legacy_score(signal)
        zone_score = self._zone_validity_score(signal)
        momentum_payload = self._momentum_payload(signal)
        final_score = legacy_score + zone_score + momentum_payload["score"]

        breakdown = {
            "legacy": legacy_score,
            "zone_validity": zone_score,
            "momentum": momentum_payload["score"],
        }

        signal["legacy_score"] = legacy_score
        signal.setdefault("legacy_score_breakdown", dict(signal.get("score_breakdown") or {}))
        signal["final_score"] = round(final_score, 4)
        signal["final_score_breakdown"] = breakdown
        signal["score"] = signal["final_score"]
        signal["score_breakdown"] = breakdown
        signal["score_model"] = "legacy_plus_zone_validity_plus_directional_momentum_v1"
        signal["score_debug"] = {
            "zone_validity_range": [self.ZONE_SCORE_MIN, self.ZONE_SCORE_MAX],
            "momentum_range": [self.MOMENTUM_SCORE_MIN, self.MOMENTUM_SCORE_MAX],
            "momentum_raw_score": momentum_payload["raw_score"],
            "momentum_directional_score": momentum_payload["directional_score"],
            "momentum_side": momentum_payload["side"],
            "momentum_found": momentum_payload["found"],
        }
        return signal

    def _legacy_score(self, signal: dict[str, Any]) -> float:
        if signal.get("legacy_score") is not None:
            return self._round_float(signal.get("legacy_score"))
        return self._round_float(signal.get("score") or 0.0)

    def _zone_validity_score(self, signal: dict[str, Any]) -> float:
        zone_validity = signal.get("zone_validity") or {}
        if isinstance(zone_validity, dict) and zone_validity.get("score") is not None:
            return self._clamp(zone_validity.get("score"), self.ZONE_SCORE_MIN, self.ZONE_SCORE_MAX)

        if not isinstance(zone_validity, dict):
            zone_validity = {}

        score = 0.0
        if zone_validity.get("valid"):
            score += 2.0
        if zone_validity.get("wyckoff_ok"):
            score += 2.0
        if zone_validity.get("target_ok"):
            score += 2.0
        return self._clamp(score, self.ZONE_SCORE_MIN, self.ZONE_SCORE_MAX)

    def _momentum_payload(self, signal: dict[str, Any]) -> dict[str, Any]:
        row = self._momentum_row(signal)
        raw_score = self._round_float(row.momentum_score) if row is not None else 0.0
        side = self._signal_side(signal)
        if side == "bear":
            directional_score = -raw_score
        elif side == "bull":
            directional_score = raw_score
        else:
            directional_score = 0.0

        return {
            "found": row is not None,
            "side": side,
            "raw_score": raw_score,
            "directional_score": self._round_float(directional_score),
            "score": self._clamp(directional_score, self.MOMENTUM_SCORE_MIN, self.MOMENTUM_SCORE_MAX),
        }

    def _momentum_row(self, signal: dict[str, Any]) -> MomentumCurrent | None:
        symbol = str(signal.get("symbol") or "").upper()
        if not symbol:
            return None
        return self.db.get(MomentumCurrent, symbol)

    def _signal_side(self, signal: dict[str, Any]) -> str:
        gate_side = str((signal.get("hierarchy_gate") or {}).get("side") or "").lower()
        if gate_side in {"bull", "bear"}:
            return gate_side

        trade_side = str((signal.get("trade") or {}).get("side") or "").lower()
        if trade_side in {"long", "buy", "bull"}:
            return "bull"
        if trade_side in {"short", "sell", "bear"}:
            return "bear"

        bias = str(signal.get("bias") or "").lower()
        if bias.startswith("bull"):
            return "bull"
        if bias.startswith("bear"):
            return "bear"
        return "neutral"

    def _clamp(self, value: Any, minimum: float, maximum: float) -> float:
        return round(min(max(self._round_float(value), minimum), maximum), 4)

    def _round_float(self, value: Any) -> float:
        try:
            return round(float(value), 4)
        except (TypeError, ValueError):
            return 0.0
