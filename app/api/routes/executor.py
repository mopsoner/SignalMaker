from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from app.services.momentum_decision_service import MomentumDecisionService

from app.api.deps import get_db

router = APIRouter()
public_router = APIRouter()


def _momentum_decision_service(db: Session) -> "MomentumDecisionService":
    from app.services.momentum_decision_service import MomentumDecisionService

    return MomentumDecisionService(db)


@router.get("/momentum-engine/decision")
@public_router.get("/momentum-engine/decision", include_in_schema=False)
def momentum_engine_decision(db: Session = Depends(get_db)) -> dict:
    return _momentum_decision_service(db).decision()


@router.post("/executor/momentum/run-once")
@public_router.post("/executor/momentum/run-once", include_in_schema=False)
def execute_momentum_once(
    quantity: float = Query(default=1.0, gt=0),
    mode: str = Query(default='paper'),
    db: Session = Depends(get_db),
) -> dict:
    return _momentum_decision_service(db).run_once(quantity=quantity, mode=mode)


@router.post("/executor/run-once")
def execute_candidates(
    limit: int = Query(default=10, ge=1, le=100),
    quantity: float = Query(default=1.0, gt=0),
    mode: str = Query(default='paper'),
    sync_momentum_first: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    from app.services.executor_service import ExecutorService

    return ExecutorService(db).execute_open_candidates(limit=limit, quantity=quantity, mode=mode, sync_momentum_first=sync_momentum_first)


@router.post('/executor/sync-momentum-candidates')
def sync_momentum_candidates(
    limit: int | None = Query(default=None, ge=1, le=500),
    min_momentum_score: float | None = Query(default=None),
    min_rr: float | None = Query(default=None),
    require_wyckoff_context: bool | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict:
    from app.services.momentum_candidate_sync_service import MomentumCandidateSyncService

    return MomentumCandidateSyncService(db).sync(
        limit=limit,
        min_momentum_score=min_momentum_score,
        min_rr=min_rr,
        require_wyckoff_context=require_wyckoff_context,
    )


@router.post('/executor/reconcile')
def reconcile_executor(db: Session = Depends(get_db)) -> dict:
    from app.services.executor_service import ExecutorService

    return ExecutorService(db).reconcile_live_positions()
