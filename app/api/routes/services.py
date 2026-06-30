from fastapi import APIRouter

router = APIRouter()


@router.get("/services")
def services_status() -> dict:
    from app.services.collector_service import CollectorService
    from app.services.planner_service import PlannerService
    from app.services.scheduler_service import SchedulerService
    from app.services.signal_engine_service import SignalEngineService

    return {"collector": CollectorService().heartbeat(), "signal_engine": SignalEngineService().heartbeat(), "planner": PlannerService().heartbeat(), "scheduler": SchedulerService().heartbeat()}
