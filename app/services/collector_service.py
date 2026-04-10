from datetime import datetime, timezone


class CollectorService:
    def heartbeat(self) -> dict:
        return {
            "service": "collector",
            "status": "idle",
            "last_tick_at": datetime.now(timezone.utc).isoformat(),
            "note": "Phase 2 stub ready for Binance ingestion integration",
        }
