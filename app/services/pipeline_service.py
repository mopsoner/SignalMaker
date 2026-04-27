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


EXECUTION_INTERVAL = "15m"


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
            EXECUTION_INTERVAL: int(runtime.get("binance_lookback_15m", runtime.get("binance_lookback_5m", 180))),
            "1h": int(runtime["binance_lookback_1h"]),
            "4h": int(runtime["binance_lookback_4h"]),
        }

    def _collect_interval_parallel(self, symbols: list[str], interval: str, latest_close_times: dict[str, dict[str, int]], worker_count: int) -> tuple[dict[str, list[dict]], list[dict]]:
        fetched: dict[str, list[dict]] = {}
        errors: list[dict] = []

        def _collect(symbol: str):
            latest_close_time = latest_close_times.get(symbol.upper(), {}).get(interval)
            return symbol, self.collector.collect_interval(symbol, interval, latest_close_time)

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = {pool.submit(_collect, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    fetched_symbol, rows = future.result()
                    fetched[fetched_symbol] = rows
                except Exception as exc:
                    errors.append({"symbol": symbol, "phase": f"collect_{interval}", "error": str(exc)})
        return fetched, errors

    def run_once(self, limit: int | None = None) -> dict:
        symbols = self.collector.discover_symbols(limit=limit)
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
        self.live_runs.start_run(run_id=run_id, mode="paper", symbols_total=len(symbols))

        scanned = 0
        candidates = 0
        candles_written = 0
        errors: list[dict] = []
        collected_symbols: set[str] = set()
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
        interval_write_counts = Counter()

        max_workers = max(1, int(self.collector.runtime["binance"].get("binance_collect_max_workers", 4)))
        worker_count = min(max_workers, max(1, len(symbols)))

        # Phase 1: collect/store execution TF first for all assets.
        fetched_exec, collect_errors = self._collect_interval_parallel(symbols, EXECUTION_INTERVAL, latest_close_times, worker_count)
        errors.extend(collect_errors)
        for symbol in symbols:
            rows = fetched_exec.get(symbol)
            if rows is None:
                continue
            try:
                if rows:
                    candles_written += self.market_data.upsert_candles(symbol, EXECUTION_INTERVAL, rows)
                    interval_write_counts[EXECUTION_INTERVAL] += len(rows)
                    collected_symbols.add(symbol)
            except Exception as exc:
                errors.append({"symbol": symbol, "phase": f"store_{EXECUTION_INTERVAL}", "error": str(exc)})

        # Phase 2: collect/store HTF only when due.
        for interval in ("1h", "4h"):
            fetched_htf, collect_errors = self._collect_interval_parallel(symbols, interval, latest_close_times, worker_count)
            errors.extend(collect_errors)
            for symbol in symbols:
                rows = fetched_htf.get(symbol)
                if rows is None:
                    continue
                try:
                    if rows:
                        candles_written += self.market_data.upsert_candles(symbol, interval, rows)
                        interval_write_counts[interval] += len(rows)
                        collected_symbols.add(symbol)
                except Exception as exc:
                    errors.append({"symbol": symbol, "phase": f"store_{interval}", "error": str(exc)})

        limits = self._bundle_limits()
        analyzed_symbols = sorted(collected_symbols)
        for symbol in analyzed_symbols:
            try:
                candles = self.market_data.load_symbol_bundle(symbol, limits)
                quality_exec = self.market_data.validate_candle_series(EXECUTION_INTERVAL, candles.get(EXECUTION_INTERVAL, []), min_count=30)
                if not quality_exec["valid"]:
                    errors.append({"symbol": symbol, "phase": "diagnostic", "warning": f"invalid_{EXECUTION_INTERVAL}_quality", "issues": quality_exec["issues"]})
                    for issue in quality_exec["issues"]:
                        data_quality_counts[issue] += 1
                if not candles.get("1h"):
                    errors.append({"symbol": symbol, "phase": "analyze", "error": "missing_1h_candles"})
                    data_quality_counts["missing_1h_bundle"] += 1
                    continue
                if not candles.get("4h"):
                    errors.append({"symbol": symbol, "phase": "analyze", "error": "missing_4h_candles"})
                    data_quality_counts["missing_4h_bundle"] += 1
                    continue

                # Low-impact migration: the legacy engine still reads the primary execution series as "5m".
                # Feed it with the cleaner 15m execution candles while preserving the existing engine API.
                candles["5m"] = candles.get(EXECUTION_INTERVAL, [])
                signal = self.engine.compute_signal(symbol, candles)
                signal[f"candle_quality_{EXECUTION_INTERVAL}"] = quality_exec
                signal["execution_timeframe"] = EXECUTION_INTERVAL
                signal["signal_interval"] = EXECUTION_INTERVAL
                if "rsi_main_timeframe" in signal:
                    signal["rsi_main_timeframe"] = EXECUTION_INTERVAL
                if signal.get("confirm_source") == "5m_bos":
                    signal["confirm_source"] = "15m_bos"

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
                if signal.get('signal_interval') == EXECUTION_INTERVAL and signal.get('rsi_main') in (0, 100):
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
            "execution_interval": EXECUTION_INTERVAL,
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
            "interval_write_counts": dict(interval_write_counts),
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
            "execution_interval": EXECUTION_INTERVAL,
            "errors": errors,
            "pipeline_counts": dict(pipeline_counts),
            "planner_reason_counts": dict(planner_reason_counts),
            "data_quality_counts": dict(data_quality_counts),
            "interval_write_counts": dict(interval_write_counts),
        }
