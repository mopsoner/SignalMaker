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

    def _public_signal_payload(self, signal: dict) -> dict:
        payload = dict(signal)
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

    def upsert_from_signal(self, signal: dict) -> AssetStateCurrent:
        payload = AssetStateUpsert(
            stage=("trade" if signal.get("pipeline", {}).get("trade") else "confirm" if signal.get("pipeline", {}).get("confirm") else "zone" if signal.get("pipeline", {}).get("zone") else "liquidity" if signal.get("pipeline", {}).get("liquidity") else "collect"),
            bias=signal.get("bias"),
            session=signal.get("session"),
            score=float(signal.get("score", 0.0)),
            price=signal.get("price"),
            rsi_1h=signal.get("rsi_htf"),
            rsi_15m=signal.get("rsi_main"),
            liquidity_context=signal.get("liquidity_context"),
            execution_target=signal.get("execution_target"),
            planner_notes=signal.get("confirm_source"),
            state_payload=self._public_signal_payload(signal),
        )
        return self.upsert(symbol=signal["symbol"], payload=payload)
