from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db, require_operator
from app.core.config import settings
from app.services.execution.live_configuration import assert_wyckoff_live_configuration
from app.services.executor_service import ExecutorService

router = APIRouter()


@router.post("/executor/run-once")
def execute_candidates(
    limit: int = Query(default=10, ge=1, le=100),
    quantity: float = Query(default=1.0, gt=0),
    db: Session = Depends(get_db),
) -> dict:
    return ExecutorService(db).execute_open_candidates(limit=limit, quantity=quantity, mode="paper")


@router.post("/executor/live/run-once", dependencies=[Depends(require_operator)])
def execute_live_candidates(
    limit: int = Query(default=10, ge=1, le=100),
    quantity: float = Query(default=1.0, gt=0),
    confirmation: str | None = Header(default=None, alias="X-Confirm-Live-Execution"),
    db: Session = Depends(get_db),
) -> dict:
    if confirmation != "EXECUTE-WYCKOFF-LIVE":
        raise HTTPException(status_code=400, detail="explicit live execution confirmation required")
    try:
        assert_wyckoff_live_configuration(settings)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return ExecutorService(db).execute_open_candidates(limit=limit, quantity=quantity, mode="live")


@router.post("/executor/live/candidates/{candidate_id}", dependencies=[Depends(require_operator)])
def execute_live_candidate(
    candidate_id: str,
    quantity: float = Query(default=1.0, gt=0),
    confirmation: str | None = Header(default=None, alias="X-Confirm-Live-Execution"),
    db: Session = Depends(get_db),
) -> dict:
    if confirmation != "EXECUTE-WYCKOFF-LIVE":
        raise HTTPException(status_code=400, detail="explicit live execution confirmation required")
    try:
        assert_wyckoff_live_configuration(settings)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return ExecutorService(db).execute_open_candidates(
        limit=1, quantity=quantity, mode="live", candidate_id=candidate_id
    )


@router.post('/executor/reconcile')
def reconcile_executor(db: Session = Depends(get_db)) -> dict:
    return ExecutorService(db).reconcile_live_positions()
