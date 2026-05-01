from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from uuid import uuid4
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.asset_state import AssetStateCurrent
from app.services.asset_state_service import AssetStateService
from app.services.collector_service import CollectorService
from app.services.hierarchical_gate_service import apply_hierarchical_stage_gates
from app.services.live_run_service import LiveRunService
from app.services.market_data_service import MarketDataService
from app.services.planner_service import PlannerService
from app.services.signal_context_service import apply_context_driven_progression
from app.services.signal_engine_service import SignalEngineService
from app.services.trade_candidate_service import TradeCandidateService


EXECUTION_INTERVAL = "15m"
LEGACY_ENGINE_INTERVAL = "5m"

PUBLIC_STATUS_REPLACEMENTS = {
    "blocked_no_5m_confirm": "blocked_no_confirm",
    "blocked_no_15m_confirm": "blocked_no_confirm",
    "reclaimed_waiting_5m_confirm": "reclaimed_waiting_confirm",
    "reclaimed_waiting_15m_confirm": "reclaimed_waiting_confirm",
    "rejected_waiting_5m_confirm": "rejected_waiting_confirm",
    "rejected_waiting_15m_confirm": "rejected_waiting_confirm",
    "waiting_5m_confirm": "waiting_confirm",
    "waiting_15m_confirm": "waiting_confirm",
    "5m_confirm": "confirm",
    "15m_confirm": "confirm",
}


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

    def _execution_interval(self) -> str:
        return EXECUTION_INTERVAL

    def _lookback_key(self, interval: str) -> str:
        return f"binance_lookback_{interval}"

    def _bundle_limits(self, execution_interval: str) -> dict[str, int]:
        runtime = self.collector.runtime["binance"]
        return {
            execution_interval: int(runtime.get(self._lookback_key(execution_interval), 180)),
            "1h": int(runtime["binance_lookback_1h"]),
            "4h": int(runtime["binance_lookback_4h"]),
        }

    def _clean_public_text(self, value):
        if isinstance(value, str):
            cleaned = value
            for old, new in PUBLIC_STATUS_REPLACEMENTS.items():
                cleaned = cleaned.replace(old, new)
            cleaned = re.sub(r"(?<!\d)5m\b", "15m", cleaned)
            cleaned = re.sub(r"(?<!\d)5M\b", "15M", cleaned)
            return cleaned
        if isinstance(value, list):
            return [self._clean_public_text(item) for item in value]
        if isinstance(value, dict):
            return {key: self._clean_public_text(item) for key, item in value.items()}
        return value

    def _public_signal(self, signal: dict) -> dict:
        payload = self._clean_public_text(dict(signal))
        legacy_trigger = payload.pop("execution_trigger_5m", None)
        if legacy_trigger and "execution_trigger" not in payload:
            payload["execution_trigger"] = {**legacy_trigger, "timeframe": EXECUTION_INTERVAL}
        if isinstance(payload.get("execution_trigger"), dict):
            payload["execution_trigger"]["timeframe"] = EXECUTION_INTERVAL
        payload.pop("rsi_5m", None)
        payload["rsi_15m"] = payload.get("rsi_main")
        payload["rsi_main_timeframe"] = EXECUTION_INTERVAL
        payload["signal_interval"] = EXECUTION_INTERVAL
        payload["execution_timeframe"] = EXECUTION_INTERVAL
        if payload.get("confirm_source") == "5m_bos":
            payload["confirm_source"] = "15m_bos"
        return payload

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

    def _order_symbols_for_analysis(self, symbols: list[str]) -> list[str]:
        """Analyze the strongest existing assets first.

        The pipeline must collect candles for the whole universe, but the analyze/planner
        pass should prioritize assets already closest to confirmation. We use the latest
        persisted 360/table score as the pre-run priority signal, then fall back to
        alphabetical order for new symbols that have no stored row yet.
        """
        normalized_symbols = sorted({symbol.upper() for symbol in symbols})
        if not normalized_symbols:
            return []

        rows = self.db.execute(
            select(AssetStateCurrent.symbol, AssetStateCurrent.score, AssetStateCurrent.updated_at)
            .where(AssetStateCurrent.symbol.in_(normalized_symbols))
        ).all()
        priority: dict[str, tuple[float, float]] = {}
        for symbol, score, updated_at in rows:
            updated_ts = updated_at.timestamp() if updated_at else 0.0
            priority[symbol.upper()] = (float(score or 0.0), updated_ts)

        def sort_key(symbol: str) -> tuple[float, float, str]:
            score, updated_ts = priority.get(symbol, (-1.0, 0.0))
            return (-score, -updated_ts, symbol)

        return sorted(normalized_symbols, key=sort_key)

    def run_once(self, limit: int | None = None) -> dict:
        execution_interval = self._execution_interval()
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

        fetched_exec, collect_errors = self._collect_interval_parallel(symbols, execution_interval, latest_close_times, worker_count)
        errors.extend(collect_errors)
        for symbol in symbols:
            rows = fetched_exec.get(symbol)
            if rows is None:
                continue
            try:
                if rows:
                    candles_written += self.market_data.upsert_candles(symbol, execution_interval, rows)
                    interval_write_counts[execution_interval] += len(rows)
                    collected_symbols.add(symbol)
            except Exception as exc:
                errors.append({"symbol": symbol, "phase": f"store_{execution_interval}", "error": str(exc)})

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

        limits = self._bundle_limits(execution_interval)
        analyzed_symbols = self._order_symbols_for_analysis(symbols)
        for symbol in analyzed_symbols:
            try:
                candles = self.market_data.load_symbol_bundle(symbol, limits)
                execution_candles = candles.get(execution_interval, [])
                quality_exec = self.market_data.validate_candle_series(execution_interval, execution_candles, min_count=30)
                if not quality_exec["valid"]:
                    errors.append({"symbol": symbol, "phase": "diagnostic", "warning": f"invalid_{execution_interval}_quality", "issues": quality_exec["issues"]})
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

                # Internal compatibility only: legacy strategy code reads the primary execution series through this key.
                candles[LEGACY_ENGINE_INTERVAL] = execution_candles
                raw_signal = self.engine.compute_signal(symbol, candles)
                raw_signal = apply_context_driven_progression(raw_signal)
                raw_signal[f"candle_quality_{execution_interval}"] = quality_exec
                raw_signal["execution_timeframe"] = execution_interval
                raw_signal["signal_interval"] = execution_interval
                raw_signal["rsi_main_timeframe"] = execution_interval
                legacy_trigger = raw_signal.get("execution_trigger_5m")
                if legacy_trigger:
                    raw_signal["execution_trigger"] = {**legacy_trigger, "timeframe": execution_interval}
                if raw_signal.get("confirm_source") == "5m_bos":
                    raw_signal["confirm_source"] = "15m_bos"

                # 4H context/target are finalized here. Re-apply the 1H candidate
                # progression immediately after so the planner receives a complete
                # trade object even when the first pass lacked the ranked 4H target.
                raw_signal = apply_hierarchical_stage_gates(raw_signal)
                raw_signal = apply_context_driven_progression(raw_signal)

                if raw_signal.get("confirm_blocked_by_hierarchy"):
                    assessment = {
                        "accepted": False,
                        "reason": raw_signal.get("planner_candidate_reason") or raw_signal.get("confirm_block_reason") or "blocked_before_planner",
                        "rr_ratio": None,
                        "candidate": None,
                    }
                else:
                    assessment = self.planner.assess_signal(raw_signal)
                    raw_signal['planner_candidate_status'] = 'open_candidate' if assessment['accepted'] else 'rejected'
                    raw_signal['planner_candidate_reason'] = self._clean_public_text(assessment['reason'])
                    raw_signal['planner_candidate_rr'] = assessment.get('rr_ratio')
                signal = self._public_signal(raw_signal)
                self.asset_states.upsert_from_signal(signal)
                candidate = assessment['candidate']
                if candidate:
                    candidate['payload'] = signal
                    candidate['notes'] = self._clean_public_text(candidate.get('notes'))
                    self.trade_candidates.upsert_open_candidate(**candidate)
                    candidates += 1

                pipeline = signal.get('pipeline', {}) or {}
                pipeline_counts['collect'] += 1
                for stage in ('liquidity', 'zone', 'confirm', 'trade'):
                    if pipeline.get(stage):
                        pipeline_counts[stage] += 1

                planner_reason_counts[signal.get('planner_candidate_reason', 'unknown')] += 1
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
                if signal.get('confirm_blocked_by_hierarchy'):
                    structure_counts['confirm_blocked_by_hierarchy'] += 1
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
                if signal.get('signal_interval') == execution_interval and signal.get('rsi_main') in (0, 100):
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
            "execution_interval": execution_interval,
            "analysis_ordering": "score_desc_existing_asset_state",
            "analysis_top_symbols": analyzed_symbols[:10],
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
            "execution_interval": execution_interval,
            "analysis_ordering": "score_desc_existing_asset_state",
            "analysis_top_symbols": analyzed_symbols[:10],
            "errors": errors,
            "pipeline_counts": dict(pipeline_counts),
            "planner_reason_counts": dict(planner_reason_counts),
            "data_quality_counts": dict(data_quality_counts),
            "interval_write_counts": dict(interval_write_counts),
        }
