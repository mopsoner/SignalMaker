from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.trade_candidate import TradeCandidateRead
from app.services.momentum_service import MomentumService
from app.services.trade_candidate_service import TradeCandidateService

router = APIRouter()


@router.get("", response_model=list[TradeCandidateRead])
def list_momentum(
    limit: int = Query(default=100, ge=1, le=1000),
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[TradeCandidateRead]:
    """Return the business Momentum candidate backlog used by the Momentum page."""
    return TradeCandidateService(db).list_candidates(limit=limit, status=status, stage="momentum", exclude_test_data=True)


@router.get("/ranking")
def list_momentum_ranking(limit: int = Query(default=200, ge=1, le=1000), db: Session = Depends(get_db)):
    """Read-only candle-feed diagnostic ranking for smoke tests and TUIs."""
    return MomentumService(db).list_rankings(limit=limit)
