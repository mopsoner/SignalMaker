from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db

router = APIRouter()


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
