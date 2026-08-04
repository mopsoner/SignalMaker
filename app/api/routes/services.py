from fastapi import APIRouter

from app.services.collector_service import CollectorService
from app.services.scheduler_service import SchedulerService
from app.services.signal_engine_service import SignalEngineService
from app.services.planner_service import PlannerService
from app.services.worker_control_service import WorkerControlService
from app.api.deps import get_db
from fastapi import Depends
from sqlalchemy.orm import Session

router = APIRouter()


@router.get("/services")
def services_status(db: Session = Depends(get_db)) -> dict:
    return {"collector": CollectorService().heartbeat(), "signal_engine": SignalEngineService().heartbeat(), "planner": PlannerService().heartbeat(), "scheduler": SchedulerService().heartbeat(), "workers": WorkerControlService(db).status()}
