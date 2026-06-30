from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db

router = APIRouter()


@router.post("/pipeline/run-once")
def run_once(limit: int = Query(default=5, ge=1, le=100), db: Session = Depends(get_db)) -> dict:
    from app.services.pipeline_service import PipelineService

    return PipelineService(db).run_once(limit=limit)
