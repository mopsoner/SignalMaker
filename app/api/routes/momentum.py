from fastapi import APIRouter, Depends, Query
from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.momentum import MomentumRead
from app.services.momentum_service import MomentumService
from app.models.momentum_current import MomentumCurrent
from app.models.momentum_structure_current import MomentumStructureCurrent

router = APIRouter()


@router.get("", response_model=list[MomentumRead])
def list_momentum(
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> list[MomentumRead]:
    return MomentumService(db).list_rankings(limit=limit)


@router.delete("/cleanup")
def clear_momentum_analysis(db: Session = Depends(get_db)) -> dict:
    """Clear generated momentum scanner analysis rows.

    This removes persisted momentum ranking and 15m structure-analysis data used
    by the Momentum Ranking page. It does not delete source market candles.
    """
    deleted_structure = db.execute(delete(MomentumStructureCurrent)).rowcount or 0
    deleted_current = db.execute(delete(MomentumCurrent)).rowcount or 0
    db.commit()
    return {
        "deleted": deleted_structure + deleted_current,
        "details": {
            "momentum_structure_current": deleted_structure,
            "momentum_current": deleted_current,
        },
    }
