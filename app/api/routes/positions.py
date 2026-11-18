from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.position import PositionRead
from app.services.position_service import PositionService

router = APIRouter()


@router.get("/goal")
def positions_goal(db: Session = Depends(get_db)) -> dict:
    from app.services.wyckoff_goal_service import WyckoffGoalService
    return WyckoffGoalService(db).summary()


@router.get("/profit-history")
def positions_profit_history(
    days: int = Query(default=30, ge=1, le=90),
    db: Session = Depends(get_db),
) -> dict:
    from app.services.kraken_profit_history import daily_profit
    return daily_profit(db, days=days)


@router.get("/summary")
def positions_summary(
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict:
    return PositionService(db).pnl_summary(status=status)


@router.get("", response_model=list[PositionRead])
def list_positions(
    limit: int = Query(default=100, ge=1, le=1000),
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[PositionRead]:
    return PositionService(db).list_positions(limit=limit, status=status)


@router.delete("")
def clear_positions(
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict:
    deleted = PositionService(db).clear_positions(status=status)
    return {"deleted": deleted, "status": status or "all"}


@router.delete("/open")
def clear_open_positions(db: Session = Depends(get_db)) -> dict:
    deleted = PositionService(db).clear_positions(status="open")
    return {"deleted": deleted, "status": "open"}
