from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.momentum_engine import MomentumEngineRunRequest, MomentumEngineStatus
from app.services.momentum_engine_service import MomentumEngineService
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade

router = APIRouter()


@router.get("/status", response_model=MomentumEngineStatus)
def momentum_engine_status(
    cadence_hours: int = 4,
    starting_capital: float = 1000.0,
    min_momentum_score: float = 0.0,
    db: Session = Depends(get_db),
) -> MomentumEngineStatus:
    return MomentumEngineService(db).status(
        cadence_hours=cadence_hours,
        starting_capital=starting_capital,
        min_momentum_score=min_momentum_score,
    )


@router.post("/run-once", response_model=MomentumEngineStatus)
def momentum_engine_run_once(payload: MomentumEngineRunRequest, db: Session = Depends(get_db)) -> MomentumEngineStatus:
    return MomentumEngineService(db).run_once(
        force=payload.force,
        cadence_hours=payload.cadence_hours,
        starting_capital=payload.starting_capital,
        min_momentum_score=payload.min_momentum_score,
    )


@router.delete("/cleanup")
def clear_momentum_engine(db: Session = Depends(get_db)) -> dict:
    """Clear momentum paper-engine logs, chart events and positions."""
    deleted_trades = db.execute(delete(MomentumEngineTrade)).rowcount or 0
    deleted_positions = db.execute(delete(MomentumEnginePosition)).rowcount or 0
    db.commit()
    return {
        "deleted": deleted_trades + deleted_positions,
        "details": {
            "momentum_engine_trades": deleted_trades,
            "momentum_engine_positions": deleted_positions,
        },
    }
