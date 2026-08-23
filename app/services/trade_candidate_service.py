from datetime import datetime, timezone

from sqlalchemy import delete, literal, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.models.candidate_execution import CandidateExecution
from app.models.trade_candidate import TradeCandidate


class TradeCandidateService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_candidates(self, limit: int = 100, status: str | None = None) -> list[TradeCandidate]:
        stmt = select(TradeCandidate)
        if status:
            stmt = stmt.where(TradeCandidate.status == status)
        stmt = stmt.order_by(TradeCandidate.created_at.desc(), TradeCandidate.score.desc()).limit(limit)
        return list(self.db.scalars(stmt).all())

    def clear_candidates(self, status: str | None = None) -> int:
        candidate_ids = select(TradeCandidate.candidate_id)
        if status:
            candidate_ids = candidate_ids.where(TradeCandidate.status == status)
        self.db.execute(
            delete(CandidateExecution).where(CandidateExecution.candidate_id.in_(candidate_ids))
        )
        stmt = delete(TradeCandidate)
        if status:
            stmt = stmt.where(TradeCandidate.status == status)
        result = self.db.execute(stmt)
        self.db.commit()
        return int(result.rowcount or 0)

    def get_open_candidates(self, limit: int = 100) -> list[TradeCandidate]:
        # Executor backlog mode: play older unexecuted candidates first instead
        # of always preferring the newest pipeline refresh. This prevents valid
        # open candidates from being starved when the executor limit is reached.
        stmt = select(TradeCandidate).where(TradeCandidate.status == "open").order_by(TradeCandidate.created_at.asc(), TradeCandidate.score.desc()).limit(limit)
        return list(self.db.scalars(stmt).all())

    def claim_open_candidates(self, *, execution_mode: str, limit: int = 100) -> list[TradeCandidate]:
        """Atomically reserve open candidates which this environment has not consumed.

        The unique candidate/mode key is the concurrency primitive: concurrent
        workers may select the same rows, but only one can insert each claim.
        """
        mode = execution_mode.lower()
        if mode not in {"paper", "live"}:
            raise ValueError("execution mode must be paper or live")
        candidate_ids = (
            select(TradeCandidate.candidate_id)
            .where(TradeCandidate.status == "open")
            .order_by(TradeCandidate.created_at.asc(), TradeCandidate.score.desc())
            .limit(limit)
        )
        values = select(
            TradeCandidate.candidate_id + "-" + mode,
            TradeCandidate.candidate_id,
        ).where(TradeCandidate.candidate_id.in_(candidate_ids))
        values = values.add_columns(
            # Literals are supplied by INSERT defaults only for single-row inserts.
            literal(mode),
            literal("claimed"),
            literal(datetime.now(timezone.utc)),
        )
        columns = ["execution_id", "candidate_id", "execution_mode", "status", "claimed_at"]
        dialect = self.db.get_bind().dialect.name
        insert = sqlite_insert(CandidateExecution) if dialect == "sqlite" else postgresql_insert(CandidateExecution)
        statement = insert.from_select(columns, values).on_conflict_do_nothing(
            index_elements=["candidate_id", "execution_mode"]
        ).returning(CandidateExecution.candidate_id)
        claimed_ids = list(self.db.scalars(statement).all())
        self.db.commit()
        if not claimed_ids:
            return []
        stmt = select(TradeCandidate).where(TradeCandidate.candidate_id.in_(claimed_ids)).order_by(
            TradeCandidate.created_at.asc(), TradeCandidate.score.desc()
        )
        return list(self.db.scalars(stmt).all())

    def get_pending_candidates(self, *, execution_mode: str, limit: int = 100) -> list[TradeCandidate]:
        """Return previously claimed work so exchange-pending orders can be reconciled."""
        stmt = (
            select(TradeCandidate)
            .join(CandidateExecution, CandidateExecution.candidate_id == TradeCandidate.candidate_id)
            .where(
                CandidateExecution.execution_mode == execution_mode,
                CandidateExecution.status == "claimed",
                TradeCandidate.status == "open",
            )
            .order_by(CandidateExecution.claimed_at.asc())
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def finish_execution(self, candidate_id: str, *, execution_mode: str, error: str | None = None) -> None:
        self.db.execute(
            update(CandidateExecution)
            .where(
                CandidateExecution.candidate_id == candidate_id,
                CandidateExecution.execution_mode == execution_mode,
                CandidateExecution.status == "claimed",
            )
            .values(
                status="failed" if error else "executed",
                completed_at=datetime.now(timezone.utc),
                error=error,
            )
        )
        self.db.commit()

    def record_pending_error(self, candidate_id: str, *, execution_mode: str, error: str) -> None:
        self.db.execute(
            update(CandidateExecution)
            .where(
                CandidateExecution.candidate_id == candidate_id,
                CandidateExecution.execution_mode == execution_mode,
                CandidateExecution.status == "claimed",
            )
            .values(error=error)
        )
        self.db.commit()

    def release_claim(self, candidate_id: str, *, execution_mode: str) -> None:
        self.db.execute(
            delete(CandidateExecution).where(
                CandidateExecution.candidate_id == candidate_id,
                CandidateExecution.execution_mode == execution_mode,
                CandidateExecution.status == "claimed",
            )
        )
        self.db.commit()

    def upsert_open_candidate(self, *, symbol: str, side: str, stage: str, score: float, entry_price: float | None, stop_price: float | None, target_price: float | None, rr_ratio: float | None, execution_target: dict | None, liquidity_context: dict | None, notes: str | None, payload: dict | None) -> TradeCandidate:
        candidate_id = f"{symbol.upper()}-open"
        now = datetime.now(timezone.utc)
        row = self.db.get(TradeCandidate, candidate_id)
        if row is None:
            row = TradeCandidate(candidate_id=candidate_id, symbol=symbol.upper(), created_at=now)
            self.db.add(row)
        else:
            # Existing open rows are refreshed by every pipeline pass. Keep the
            # page focused on the latest live candidates instead of the first time
            # a symbol ever became a candidate.
            row.created_at = now
        row.side = side
        row.stage = stage
        row.status = "open"
        row.score = score
        row.entry_price = entry_price
        row.stop_price = stop_price
        row.target_price = target_price
        row.rr_ratio = rr_ratio
        row.execution_target = execution_target
        row.liquidity_context = liquidity_context
        row.notes = notes
        row.payload = payload
        self.db.commit()
        self.db.refresh(row)
        return row
