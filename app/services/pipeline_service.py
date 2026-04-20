from collections import Counter
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

        pipeline_counts = Counter()
        planner_reason_counts = Counter()
        state_counts = Counter()
        bias_counts = Counter()
        trigger_counts = Counter()
        confirm_source_counts = Counter()
        zone_quality_counts = Counter()
        session_counts = Counter()
        data_quality_counts = Counter()
        structure_counts = Counter()

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
                    data_quality_counts["missing_timeframe_bundle"] += 1
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

                pipeline = signal.get('pipeline', {}) or {}
                pipeline_counts['collect'] += 1
                for stage in ('liquidity', 'zone', 'confirm', 'trade'):
                    if pipeline.get(stage):
                        pipeline_counts[stage] += 1

                planner_reason_counts[assessment.get('reason', 'unknown')] += 1
                state_counts[signal.get('state', 'unknown')] += 1
                bias_counts[signal.get('bias', 'unknown')] += 1
                trigger_counts[signal.get('trigger', 'unknown')] += 1
                confirm_source_counts[signal.get('confirm_source', 'none') or 'none'] += 1
                zone_quality_counts[signal.get('zone_quality', 'unknown')] += 1
                session_counts[signal.get('session', 'unknown')] += 1

                if signal.get('mss_bull'):
                    structure_counts['mss_bull'] += 1
                if signal.get('mss_bear'):
                    structure_counts['mss_bear'] += 1
                if signal.get('bos_bull'):
                    structure_counts['bos_bull'] += 1
                if signal.get('bos_bear'):
                    structure_counts['bos_bear'] += 1
                if signal.get('confirm_blocked_by_session'):
                    structure_counts['confirm_blocked_by_session'] += 1
                if signal.get('tp_zone'):
                    structure_counts['tp_zone'] += 1

                volume_debug = signal.get('volume_debug', {}) or {}
                market_quality_debug = signal.get('market_quality_debug', {}) or {}
                if (volume_debug.get('last') or 0) == 0:
                    data_quality_counts['volume_last_zero'] += 1
                if (volume_debug.get('average') or 0) == 0:
                    data_quality_counts['volume_average_zero'] += 1
                if (market_quality_debug.get('avg_range_pct') or 0) == 0:
                    data_quality_counts['market_range_zero'] += 1
                if signal.get('signal_interval') == '5m' and signal.get('rsi_main') in (0, 100):
                    data_quality_counts['rsi_main_extreme_edge'] += 1
                if signal.get('internal_bear_pivot_high') == signal.get('internal_bull_pivot_low'):
                    data_quality_counts['internal_pivots_flat'] += 1
                if signal.get('external_swing_high') == signal.get('external_swing_low'):
                    data_quality_counts['external_swings_flat'] += 1

                scanned += 1
            except Exception as exc:
                errors.append({"symbol": symbol, "phase": "analyze", "error": str(exc)})

        stats = {
            "candidates_created": candidates,
            "candles_written": candles_written,
            "errors": errors,
            "symbols_requested": len(symbols),
            "symbols_collected": len(collected_symbols),
            "symbols_scanned": scanned,
            "collect_workers": worker_count,
            "incremental_fetch_enabled": bool(self.collector.runtime["binance"].get("binance_incremental_fetch_enabled", True)),
            "pipeline_counts": dict(pipeline_counts),
            "planner_reason_counts": dict(planner_reason_counts),
            "state_counts": dict(state_counts),
            "bias_counts": dict(bias_counts),
            "trigger_counts": dict(trigger_counts),
            "confirm_source_counts": dict(confirm_source_counts),
            "zone_quality_counts": dict(zone_quality_counts),
            "session_counts": dict(session_counts),
            "structure_counts": dict(structure_counts),
            "data_quality_counts": dict(data_quality_counts),
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
            "pipeline_counts": dict(pipeline_counts),
            "planner_reason_counts": dict(planner_reason_counts),
            "data_quality_counts": dict(data_quality_counts),
        }
