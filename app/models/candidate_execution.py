from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class CandidateExecution(Base):
    """The lifecycle of one candidate in one execution environment."""

    __tablename__ = "candidate_executions"
    __table_args__ = (
        UniqueConstraint("candidate_id", "execution_mode", name="uq_candidate_execution_mode"),
        Index("ix_candidate_executions_status", "status"),
    )

    execution_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    candidate_id: Mapped[str] = mapped_column(
        ForeignKey("trade_candidates.candidate_id", ondelete="CASCADE"), nullable=False
    )
    execution_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="claimed", nullable=False)
    claimed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)
    entry_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    entry_order_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    take_profit_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    take_profit_order_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
