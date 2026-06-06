from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.momentum_executor_service import MomentumExecutorService

router = APIRouter()


@router.get('/status')
def momentum_executor_status(db: Session = Depends(get_db)) -> dict:
    return MomentumExecutorService(db).status()


@router.post('/run-once')
def momentum_executor_run_once(force: bool = True, db: Session = Depends(get_db)) -> dict:
    return MomentumExecutorService(db).run_once(force=force)
