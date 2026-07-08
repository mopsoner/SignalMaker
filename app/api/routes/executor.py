from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db

router = APIRouter()


@router.post("/executor/run-once")
def execute_candidates(
    limit: int = Query(default=10, ge=1, le=100),
    quantity: float = Query(default=1.0, gt=0),
    mode: str = Query(default='paper'),
    db: Session = Depends(get_db),
) -> dict:
    from app.services.executor_service import ExecutorService

    return ExecutorService(db).execute_open_candidates(limit=limit, quantity=quantity, mode=mode)


@router.get("/momentum-engine/decision")
def momentum_decision(db: Session = Depends(get_db)) -> dict:
    from app.services.momentum_decision_service import MomentumDecisionService

    return MomentumDecisionService(db).decision()


@router.post("/executor/momentum/run-once")
def execute_momentum_once(
    quantity: float = Query(default=1.0, gt=0),
    mode: str = Query(default="paper"),
    db: Session = Depends(get_db),
) -> dict:
    from app.services.momentum_decision_service import MomentumDecisionService

    return MomentumDecisionService(db).run_once(quantity=quantity, mode=mode)


@router.post('/executor/reconcile')
def reconcile_executor(db: Session = Depends(get_db)) -> dict:
    from app.services.executor_service import ExecutorService

    return ExecutorService(db).reconcile_live_positions()
