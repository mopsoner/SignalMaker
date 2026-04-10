from datetime import datetime, timezone


class PlannerService:
    def heartbeat(self) -> dict:
        return {
            "service": "planner",
            "status": "idle",
            "last_tick_at": datetime.now(timezone.utc).isoformat(),
            "note": "Phase 2 stub ready for trade candidate generation",
        }
