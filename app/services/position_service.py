from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.position import Position


class PositionService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_positions(self, limit: int = 100, status: str | None = None) -> list[Position]:
        stmt = select(Position)
        if status:
            stmt = stmt.where(Position.status == status)
        stmt = stmt.order_by(Position.opened_at.desc()).limit(limit)
        return list(self.db.scalars(stmt).all())
