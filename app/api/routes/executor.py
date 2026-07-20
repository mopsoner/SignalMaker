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


@router.get("/executor/momentum/latest")
def latest_momentum_decision() -> dict:
    """Return the last decision recorded by the Raspberry momentum worker.

    Unlike ``run-once``, this read-only endpoint never builds or executes a new
    decision merely because a dashboard was refreshed.
    """
    from raspberry_executor.state import StateStore

    for event in reversed(StateStore().events(limit=1000)):
        if event.get("event_type") == "momentum_decision":
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            return {"decision": payload, "timestamp": event.get("timestamp")}
    return {"decision": None}


@router.post('/executor/reconcile')
def reconcile_executor(db: Session = Depends(get_db)) -> dict:
    from app.services.executor_service import ExecutorService

    return ExecutorService(db).reconcile_live_positions()
