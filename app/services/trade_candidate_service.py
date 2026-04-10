from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.trade_candidate import TradeCandidate


class TradeCandidateService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_candidates(self, limit: int = 100, status: str | None = None) -> list[TradeCandidate]:
        stmt = select(TradeCandidate)
        if status:
            stmt = stmt.where(TradeCandidate.status == status)
        stmt = stmt.order_by(TradeCandidate.score.desc(), TradeCandidate.created_at.desc()).limit(limit)
        return list(self.db.scalars(stmt).all())
