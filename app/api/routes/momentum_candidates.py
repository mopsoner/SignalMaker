from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.trade_candidate import TradeCandidateRead
from app.services.momentum_candidate_service import MomentumCandidateService

router = APIRouter()


@router.get("", response_model=list[TradeCandidateRead])
def list_momentum_candidates(
    limit: int = Query(default=100, ge=1, le=1000),
    min_momentum_score: float = Query(default=0.0),
    min_rr: float | None = Query(default=None, ge=0.0),
    require_wyckoff_context: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> list[TradeCandidateRead]:
    return MomentumCandidateService(db).list_candidates(
        limit=limit,
        min_momentum_score=min_momentum_score,
        min_rr=min_rr,
        require_wyckoff_context=require_wyckoff_context,
    )
