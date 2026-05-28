from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.momentum import MomentumRead
from app.services.momentum_service import MomentumService

router = APIRouter()


@router.get("", response_model=list[MomentumRead])
def list_momentum(
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> list[MomentumRead]:
    return MomentumService(db).list_rankings(limit=limit)
