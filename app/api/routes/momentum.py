from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.momentum_service import MomentumService

router = APIRouter()


@router.get("")
def list_momentum(limit: int = Query(default=200, ge=1, le=1000), db: Session = Depends(get_db)):
    return MomentumService(db).list_rankings(limit=limit)
