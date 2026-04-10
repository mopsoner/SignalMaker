from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.live_run import LiveRun


class LiveRunService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_runs(self, limit: int = 50) -> list[LiveRun]:
        stmt = select(LiveRun).order_by(LiveRun.started_at.desc()).limit(limit)
        return list(self.db.scalars(stmt).all())
