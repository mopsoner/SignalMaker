from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.ibkr_momentum_service import IBKRMomentumService

router = APIRouter()


@router.get("")
def list_ibkr_momentum(
    limit: int = Query(default=300, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    return IBKRMomentumService(db).list_rankings(limit=limit)
