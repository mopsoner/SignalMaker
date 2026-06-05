from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.momentum import MomentumCandidateRead, MomentumRead
from app.services.momentum_service import MomentumService

router = APIRouter()


@router.get("", response_model=list[MomentumRead])
def list_momentum(
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> list[MomentumRead]:
    return MomentumService(db).list_rankings(limit=limit)


@router.get("/candidates", response_model=list[MomentumCandidateRead])
def list_momentum_candidates(
    limit: int = Query(default=50, ge=1, le=300),
    include_waiting: bool = Query(default=False),
    min_momentum_score: float = Query(default=0.0),
    db: Session = Depends(get_db),
) -> list[MomentumCandidateRead]:
    return MomentumService(db).list_candidates(
        limit=limit,
        include_waiting=include_waiting,
        min_momentum_score=min_momentum_score,
    )
