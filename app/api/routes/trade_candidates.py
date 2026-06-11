from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.trade_candidate import TradeCandidateRead
from app.services.trade_candidate_service import TradeCandidateService

router = APIRouter()


@router.get("", response_model=list[TradeCandidateRead])
def list_trade_candidates(
    limit: int = Query(default=100, ge=1, le=1000),
    status: str | None = Query(default=None),
    stage: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[TradeCandidateRead]:
    return TradeCandidateService(db).list_candidates(limit=limit, status=status, stage=stage)


@router.post("/{candidate_id}/executed", response_model=TradeCandidateRead)
def mark_trade_candidate_executed(
    candidate_id: str,
    db: Session = Depends(get_db),
) -> TradeCandidateRead:
    row = TradeCandidateService(db).mark_executed(candidate_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Trade candidate not found: {candidate_id}")
    return row
