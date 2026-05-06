from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.gateway import GatewayExecutionEvent, GatewayExecutionReport, GatewayHeartbeat
from app.services.gateway_execution_service import GatewayExecutionService

router = APIRouter()


@router.post('/executions')
def record_gateway_execution(payload: GatewayExecutionReport, db: Session = Depends(get_db)) -> dict:
    try:
        return GatewayExecutionService(db).record_execution(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/execution-events')
def record_gateway_execution_event(payload: GatewayExecutionEvent, db: Session = Depends(get_db)) -> dict:
    return GatewayExecutionService(db).record_event(payload)


@router.post('/heartbeat')
def gateway_heartbeat(payload: GatewayHeartbeat, db: Session = Depends(get_db)) -> dict:
    return GatewayExecutionService(db).heartbeat(payload.model_dump(mode='json'))
