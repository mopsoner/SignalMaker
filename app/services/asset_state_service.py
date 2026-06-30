from datetime import datetime, timezone
from typing import Literal

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.models.asset_state import AssetStateCurrent
from app.schemas.asset_state import AssetStateUpsert

AssetSortBy = Literal["score", "updated_at"]


class AssetStateService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self._ensure_15m_columns()

    def _ensure_15m_columns(self) -> None:
        self.db.execute(text("ALTER TABLE asset_state_current ADD COLUMN IF NOT EXISTS rsi_15m DOUBLE PRECISION"))
        self.db.execute(text("UPDATE asset_state_current SET rsi_15m = rsi_5m WHERE rsi_15m IS NULL AND rsi_5m IS NOT NULL"))
        self.db.commit()

    def list_assets(self, *, limit: int, min_score: float | None, stage: str | None, sort_by: AssetSortBy = "score") -> list[AssetStateCurrent]:
        stmt = select(AssetStateCurrent)
        if min_score is not None:
            stmt = stmt.where(AssetStateCurrent.score >= min_score)
        if stage:
            stmt = stmt.where(AssetStateCurrent.stage == stage)
        if sort_by == "updated_at":
            stmt = stmt.order_by(AssetStateCurrent.updated_at.desc(), AssetStateCurrent.score.desc())
        else:
            stmt = stmt.order_by(AssetStateCurrent.score.desc(), AssetStateCurrent.updated_at.desc())
        stmt = stmt.limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_by_symbol(self, symbol: str) -> AssetStateCurrent | None:
        return self.db.get(AssetStateCurrent, symbol.upper())

    def upsert(self, *, symbol: str, payload: AssetStateUpsert) -> AssetStateCurrent:
        symbol = symbol.upper()
        row = self.db.get(AssetStateCurrent, symbol)
        if row is None:
            row = AssetStateCurrent(symbol=symbol)
            self.db.add(row)

        row.stage = payload.stage
        row.bias = payload.bias
        row.session = payload.session
        row.score = payload.score
        row.price = payload.price
        row.rsi_1h = payload.rsi_1h
        row.rsi_15m = payload.rsi_15m
        row.liquidity_context = payload.liquidity_context
        row.execution_target = payload.execution_target
        row.planner_notes = payload.planner_notes
        row.state_payload = payload.state_payload
        row.updated_at = datetime.now(timezone.utc)

        self.db.commit()
        self.db.refresh(row)
        return row

    def _normalize_public_score(self, signal: dict) -> dict:
        payload = dict(signal)
        if payload.get("final_score") is not None:
            payload.setdefault("legacy_score", payload.get("score", 0.0))
            payload.setdefault("legacy_score_breakdown", payload.get("score_breakdown", {}) or {})
            payload["score"] = payload.get("final_score", 0.0)
            payload["score_breakdown"] = payload.get("final_score_breakdown", {}) or {}
        return payload

    def _public_signal_payload(self, signal: dict) -> dict:
        payload = self._normalize_public_score(signal)
        execution_trigger = payload.get("execution_trigger") or payload.get("execution_trigger_5m")
        if execution_trigger:
            payload["execution_trigger"] = execution_trigger
        payload.pop("execution_trigger_5m", None)
        payload.pop("rsi_5m", None)
        payload["rsi_15m"] = payload.get("rsi_main")
        payload["rsi_main_timeframe"] = "15m"
        payload["signal_interval"] = "15m"
        payload["execution_timeframe"] = "15m"
        return payload

    def _stage_from_signal(self, signal: dict) -> str:
        # The hierarchical gate owns the user-facing stage. Fall back to the old
        # pipeline-derived stage only for legacy payloads that do not expose it.
        explicit_stage = signal.get("stage") or (signal.get("hierarchy_gate") or {}).get("stage")
        if explicit_stage:
            return explicit_stage
        pipeline = signal.get("pipeline", {}) or {}
        if pipeline.get("trade"):
            return "trade"
        if pipeline.get("confirm"):
            return "confirm"
        if pipeline.get("zone"):
            return "zone"
        if pipeline.get("liquidity"):
            return "liquidity"
        return "collect"

    def upsert_from_signal(self, signal: dict) -> AssetStateCurrent:
        public_payload = self._public_signal_payload(signal)
        payload = AssetStateUpsert(
            stage=self._stage_from_signal(public_payload),
            bias=public_payload.get("bias"),
            session=public_payload.get("session"),
            score=float(public_payload.get("score", 0.0)),
            price=public_payload.get("price"),
            rsi_1h=public_payload.get("rsi_htf"),
            rsi_15m=public_payload.get("rsi_main"),
            liquidity_context=public_payload.get("liquidity_context"),
            execution_target=public_payload.get("execution_target"),
            planner_notes=public_payload.get("confirm_source"),
            state_payload=public_payload,
        )
        return self.upsert(symbol=public_payload["symbol"], payload=payload)
