from datetime import datetime, timezone


class SignalEngineService:
    def heartbeat(self) -> dict:
        return {
            "service": "signal_engine",
            "status": "idle",
            "last_tick_at": datetime.now(timezone.utc).isoformat(),
            "note": "Phase 2 stub ready for strategy computation migration",
        }
