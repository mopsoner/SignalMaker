from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.asset_state import AssetStateCurrent
from app.schemas.asset_state import AssetStateUpsert


class AssetStateService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_assets(self, *, limit: int, min_score: float | None, stage: str | None) -> list[AssetStateCurrent]:
        stmt = select(AssetStateCurrent)
        if min_score is not None:
            stmt = stmt.where(AssetStateCurrent.score >= min_score)
        if stage:
            stmt = stmt.where(AssetStateCurrent.stage == stage)
        stmt = stmt.order_by(AssetStateCurrent.score.desc(), AssetStateCurrent.updated_at.desc()).limit(limit)
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
        row.rsi_5m = payload.rsi_5m
        row.liquidity_context = payload.liquidity_context
        row.execution_target = payload.execution_target
        row.planner_notes = payload.planner_notes
        row.state_payload = payload.state_payload
        row.updated_at = datetime.now(timezone.utc)

        self.db.commit()
        self.db.refresh(row)
        return row
