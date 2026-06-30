from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.trade_candidate import TradeCandidateRead
from app.services.trade_candidate_service import TradeCandidateService

router = APIRouter()


@router.get("", response_model=list[TradeCandidateRead])
def list_trade_candidates(
    limit: int = Query(default=100, ge=1, le=1000),
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[TradeCandidateRead]:
    return TradeCandidateService(db).list_candidates(limit=limit, status=status)


@router.delete("")
def clear_trade_candidates(
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict:
    deleted = TradeCandidateService(db).clear_candidates(status=status)
    return {"deleted": deleted, "status": status or "all"}


@router.delete("/open")
def clear_open_trade_candidates(db: Session = Depends(get_db)) -> dict:
    deleted = TradeCandidateService(db).clear_candidates(status="open")
    return {"deleted": deleted, "status": "open"}
