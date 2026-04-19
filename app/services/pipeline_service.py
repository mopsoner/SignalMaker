from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy.orm import Session

from app.services.asset_state_service import AssetStateService
from app.services.collector_service import CollectorService
from app.services.live_run_service import LiveRunService
from app.services.market_data_service import MarketDataService
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
        self.market_data = MarketDataService(db)

    def _bundle_limits(self) -> dict[str, int]:
        runtime = self.collector.runtime["binance"]
        return {
            "1m": int(runtime["binance_lookback_1m"]),
            "5m": int(runtime["binance_lookback_5m"]),
            "1h": int(runtime["binance_lookback_1h"]),
            "4h": int(runtime["binance_lookback_4h"]),
        }

    def run_once(self, limit: int | None = None) -> dict:
        symbols = self.collector.discover_symbols(limit=limit)
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
        self.live_runs.start_run(run_id=run_id, mode="paper", symbols_total=len(symbols))

        scanned = 0
        candidates = 0
        candles_written = 0
        errors: list[dict] = []
        collected_symbols: list[str] = []
        latest_close_times = self.market_data.get_latest_close_times(symbols)

        max_workers = max(1, int(self.collector.runtime["binance"].get("binance_collect_max_workers", 4)))
        worker_count = min(max_workers, max(1, len(symbols)))
        fetched_bundles: dict[str, dict[str, list[dict]]] = {}

        def _collect(symbol: str):
            return symbol, self.collector.collect_symbol_bundle(symbol, latest_close_times.get(symbol.upper(), {}))

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = {pool.submit(_collect, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    fetched_symbol, bundle = future.result()
                    fetched_bundles[fetched_symbol] = bundle
                except Exception as exc:
                    errors.append({"symbol": symbol, "phase": "collect", "error": str(exc)})

        for symbol in symbols:
            bundle = fetched_bundles.get(symbol)
            if not bundle:
                continue
            try:
                wrote_any = False
                for interval, rows in bundle.items():
                    if rows:
                        candles_written += self.market_data.upsert_candles(symbol, interval, rows)
                        wrote_any = True
                if wrote_any:
                    collected_symbols.append(symbol)
            except Exception as exc:
                errors.append({"symbol": symbol, "phase": "store", "error": str(exc)})

        limits = self._bundle_limits()
        for symbol in collected_symbols:
            try:
                candles = self.market_data.load_symbol_bundle(symbol, limits)
                if not all(candles.get(tf) for tf in ("1m", "5m", "1h", "4h")):
                    errors.append({"symbol": symbol, "phase": "analyze", "error": "missing timeframe candles"})
                    continue
                signal = self.engine.compute_signal(symbol, candles)
                assessment = self.planner.assess_signal(signal)
                signal['planner_candidate_status'] = 'open_candidate' if assessment['accepted'] else 'rejected'
                signal['planner_candidate_reason'] = assessment['reason']
                signal['planner_candidate_rr'] = assessment.get('rr_ratio')
                self.asset_states.upsert_from_signal(signal)
                candidate = assessment['candidate']
                if candidate:
                    self.trade_candidates.upsert_open_candidate(**candidate)
                    candidates += 1
                scanned += 1
            except Exception as exc:
                errors.append({"symbol": symbol, "phase": "analyze", "error": str(exc)})

        stats = {
            "candidates_created": candidates,
            "candles_written": candles_written,
            "errors": errors,
            "symbols_requested": len(symbols),
            "symbols_collected": len(collected_symbols),
            "collect_workers": worker_count,
            "incremental_fetch_enabled": bool(self.collector.runtime["binance"].get("binance_incremental_fetch_enabled", True)),
        }
        self.live_runs.complete_run(run_id, symbols_scanned=scanned, stats=stats)
        return {
            "run_id": run_id,
            "symbols_total": len(symbols),
            "symbols_collected": len(collected_symbols),
            "symbols_scanned": scanned,
            "candles_written": candles_written,
            "candidates_created": candidates,
            "collect_workers": worker_count,
            "errors": errors,
        }
