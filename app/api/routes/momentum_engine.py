from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.momentum_engine import (
    MomentumEngineDecision,
    MomentumEnginePositionPage,
    MomentumEngineRunRequest,
    MomentumEngineStatus,
    MomentumEngineTradePage,
)
from app.services.momentum_engine_service import MomentumEngineService
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.models.momentum_engine_current_decision import MomentumEngineDecisionHistory

router = APIRouter()
MOMENTUM_MARKET_SCOPE = "crypto"
MOMENTUM_STRATEGY = "momentum_rotation_v1"


@router.get("/positions", response_model=MomentumEnginePositionPage)
def momentum_engine_positions(
    status: Literal["open", "closed"] | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> MomentumEnginePositionPage:
    filters = [
        MomentumEnginePosition.market_scope == MOMENTUM_MARKET_SCOPE,
        MomentumEnginePosition.strategy == MOMENTUM_STRATEGY,
    ]
    if status is not None:
        filters.append(MomentumEnginePosition.status == status)
    total = db.scalar(select(func.count()).select_from(MomentumEnginePosition).where(*filters)) or 0
    items = list(db.scalars(
        select(MomentumEnginePosition)
        .where(*filters)
        .order_by(MomentumEnginePosition.opened_at.desc())
        .offset(offset)
        .limit(limit)
    ).all())
    return MomentumEnginePositionPage(items=items, total=total, limit=limit, offset=offset)


@router.get("/trades", response_model=MomentumEngineTradePage)
def momentum_engine_trades(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> MomentumEngineTradePage:
    filters = [
        MomentumEngineTrade.market_scope == MOMENTUM_MARKET_SCOPE,
        MomentumEngineTrade.strategy == MOMENTUM_STRATEGY,
    ]
    total = db.scalar(select(func.count()).select_from(MomentumEngineTrade).where(*filters)) or 0
    items = list(db.scalars(
        select(MomentumEngineTrade)
        .where(*filters)
        .order_by(MomentumEngineTrade.created_at.desc())
        .offset(offset)
        .limit(limit)
    ).all())
    return MomentumEngineTradePage(items=items, total=total, limit=limit, offset=offset)


@router.get("/status", response_model=MomentumEngineStatus)
def momentum_engine_status(
    cadence_hours: int = 1,
    starting_capital: float = 1000.0,
    min_momentum_score: float = 0.0,
    db: Session = Depends(get_db),
) -> MomentumEngineStatus:
    return MomentumEngineService(db).status(
        cadence_hours=cadence_hours,
        starting_capital=starting_capital,
        min_momentum_score=min_momentum_score,
    )


@router.get("/decision", response_model=MomentumEngineDecision)
def momentum_engine_decision(
    cadence_hours: int = 1,
    starting_capital: float = 1000.0,
    min_momentum_score: float = 0.0,
    db: Session = Depends(get_db),
) -> MomentumEngineDecision:
    _ = (cadence_hours, starting_capital, min_momentum_score)
    return MomentumEngineService(db).current_decision()


@router.get("/decisions", response_model=list[MomentumEngineDecision])
def momentum_engine_decisions(
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
) -> list[MomentumEngineDecision]:
    return MomentumEngineService(db).decision_history(limit=limit)


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
    deleted_trades = db.execute(delete(MomentumEngineTrade).where(MomentumEngineTrade.market_scope == "crypto")).rowcount or 0
    deleted_positions = db.execute(delete(MomentumEnginePosition).where(MomentumEnginePosition.market_scope == "crypto")).rowcount or 0
    deleted_decisions = db.execute(delete(MomentumEngineDecisionHistory).where(MomentumEngineDecisionHistory.market_scope == "crypto")).rowcount or 0
    db.commit()
    return {
        "deleted": deleted_trades + deleted_positions + deleted_decisions,
        "details": {
            "momentum_engine_trades": deleted_trades,
            "momentum_engine_positions": deleted_positions,
            "momentum_engine_decisions": deleted_decisions,
        },
    }
