from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy.orm import Session

from app.services.asset_state_service import AssetStateService
from app.services.collector_service import CollectorService
from app.services.live_run_service import LiveRunService
from app.services.planner_service import PlannerService
from app.services.signal_engine_service import SignalEngineService
from app.services.trade_candidate_service import TradeCandidateService


class PipelineService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.collector = CollectorService()
        self.engine = SignalEngineService()
        self.planner = PlannerService()
        self.asset_states = AssetStateService(db)
        self.live_runs = LiveRunService(db)
        self.trade_candidates = TradeCandidateService(db)

    def run_once(self, limit: int | None = None) -> dict:
        symbols = self.collector.discover_symbols(limit=limit)
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
        self.live_runs.start_run(run_id=run_id, mode="paper", symbols_total=len(symbols))
        scanned = 0
        candidates = 0
        errors: list[dict] = []
        for symbol in symbols:
            try:
                candles = self.collector.collect_symbol_bundle(symbol)
                signal = self.engine.compute_signal(symbol, candles)
                self.asset_states.upsert_from_signal(signal)
                candidate = self.planner.build_candidate_from_signal(signal)
                if candidate:
                    self.trade_candidates.upsert_open_candidate(**candidate)
                    candidates += 1
                scanned += 1
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)})
        stats = {"candidates_created": candidates, "errors": errors, "symbols_requested": len(symbols)}
        self.live_runs.complete_run(run_id, symbols_scanned=scanned, stats=stats)
        return {"run_id": run_id, "symbols_total": len(symbols), "symbols_scanned": scanned, "candidates_created": candidates, "errors": errors}
