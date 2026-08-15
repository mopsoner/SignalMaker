from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from uuid import uuid4
import logging
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.asset_state import AssetStateCurrent
from app.services.asset_state_service import AssetStateService
from app.services.collector_service import CollectorService
from app.services.live_run_service import LiveRunService
from app.services.market_data_service import MarketDataService
from app.services.momentum_service import MomentumService
from app.services.planner_service import PlannerService
from app.services.signal_engine_service import SignalEngineService
from app.services.signal_score_service import SignalScoreService
from app.services.trade_candidate_service import TradeCandidateService
from app.services.wyckoff_pipeline_service import WyckoffPipelineService


EXECUTION_INTERVAL = "15m"
logger = logging.getLogger(__name__)

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
        self.momentum = MomentumService(db)
        self.signal_score = SignalScoreService(db)
        self.wyckoff_pipeline = WyckoffPipelineService(
            engine=self.engine, planner=self.planner, score_signal=self.signal_score.apply
        )

    def _execution_interval(self) -> str:
        return EXECUTION_INTERVAL

    def _bundle_limits(self, execution_interval: str) -> dict[str, int]:
        return {execution_interval: 180, "1h": 180, "4h": 120}

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
        return self.wyckoff_pipeline.public_signal(signal, self._execution_interval())

    def _enforce_one_hour_decision_gate(self, signal: dict) -> dict:
        return self.wyckoff_pipeline._enforce_one_hour_gate(signal)

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
        # Persist the run marker before collection/analysis so no transaction is
        # idle while external collectors or CPU-heavy engines execute.
        self.db.commit()

        scanned = 0
        candidates = 0
        candles_written = 0
        momentum_rows_upserted = 0
        asset_states_upserted = 0
        errors: list[dict] = []
        collected_symbols: set[str] = set()
        latest_close_times = self.market_data.get_latest_close_times(symbols)
        self.db.rollback()

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
        error_counts = Counter()

        def record_error(error: dict) -> None:
            errors.append(error)
            key = str(error.get("error") or error.get("warning") or error.get("phase") or "unknown")
            error_counts[key] += 1
            logger.warning("Pipeline issue run_id=%s details=%s", run_id, error)

        logger.info("Pipeline run started run_id=%s symbols_requested=%s limit=%s", run_id, len(symbols), limit)

        max_workers = 1
        worker_count = min(max_workers, max(1, len(symbols)))

        fetched_exec, collect_errors = self._collect_interval_parallel(symbols, execution_interval, latest_close_times, worker_count)
        for error in collect_errors:
            record_error(error)
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
                self.db.rollback()
                record_error({"symbol": symbol, "phase": f"store_{execution_interval}", "error": str(exc)})

        for interval in ("1h", "4h"):
            fetched_htf, collect_errors = self._collect_interval_parallel(symbols, interval, latest_close_times, worker_count)
            for error in collect_errors:
                record_error(error)
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
                    self.db.rollback()
                    record_error({"symbol": symbol, "phase": f"store_{interval}", "error": str(exc)})

        try:
            momentum_result = self.momentum.recalculate_and_store(symbols=list(collected_symbols or symbols))
            momentum_rows_upserted = int(momentum_result.get("momentum_rows_upserted", 0))
        except Exception as exc:
            self.db.rollback()
            record_error({"phase": "momentum_recalculate", "error": str(exc)})

        limits = self._bundle_limits(execution_interval)
        analyzed_symbols = self._order_symbols_for_analysis(symbols)
        for symbol in analyzed_symbols:
            try:
                candles = self.market_data.load_symbol_bundle(symbol, limits)
                # All engine inputs are now detached dictionaries.  End the read
                # transaction before validation and signal computation.
                self.db.rollback()
                # Do not calculate indicators across an ingestion outage.  The
                # external feed may resume without backfilling while this app
                # was stopped, leaving one permanent gap in every loaded window.
                candles = {
                    interval: self.market_data.latest_contiguous_candles(interval, series)
                    for interval, series in candles.items()
                }
                execution_candles = candles.get(execution_interval, [])
                candle_qualities = {
                    interval: self.market_data.validate_candle_series(
                        interval,
                        execution_candles if interval == execution_interval else candles.get(interval, []),
                        min_count=30,
                    )
                    for interval in (execution_interval, "1h", "4h")
                }

                for interval, quality in candle_qualities.items():
                    if quality["valid"]:
                        continue
                    record_error({"symbol": symbol, "phase": "diagnostic", "warning": f"invalid_{interval}_quality", "issues": quality["issues"]})
                    for issue in quality["issues"]:
                        counter_key = issue if interval == execution_interval else f"{interval}:{issue}"
                        data_quality_counts[counter_key] += 1

                if not candles.get("1h"):
                    record_error({"symbol": symbol, "phase": "analyze", "error": "missing_1h_candles"})
                    data_quality_counts["missing_1h_bundle"] += 1
                if not candles.get("4h"):
                    record_error({"symbol": symbol, "phase": "analyze", "error": "missing_4h_candles"})
                    data_quality_counts["missing_4h_bundle"] += 1

                if any(not quality["valid"] for quality in candle_qualities.values()):
                    continue

                try:
                    signal, assessment = self.wyckoff_pipeline.analyze(
                        symbol=symbol,
                        candles=candles,
                        market_context={"provider": "kraken", "provider_symbol": symbol, "asset_type": "crypto"},
                        execution_interval=execution_interval,
                    )
                except Exception as exc:
                    record_error({"symbol": symbol, "phase": "compute_signal", "error": "compute_signal_error", "detail": str(exc)})
                    continue
                signal[f"candle_quality_{execution_interval}"] = candle_qualities[execution_interval]
                try:
                    self.asset_states.upsert_from_signal(signal)
                    asset_states_upserted += 1
                except Exception as exc:
                    self.db.rollback()
                    record_error({"symbol": symbol, "phase": "asset_state_upsert", "error": "asset_state_upsert_error", "detail": str(exc)})
                    continue
                candidate = assessment['candidate']
                if candidate:
                    try:
                        candidate['payload'] = signal
                        candidate['notes'] = self._clean_public_text(candidate.get('notes'))
                        self.trade_candidates.upsert_open_candidate(**candidate)
                        candidates += 1
                    except Exception as exc:
                        self.db.rollback()
                        record_error({"symbol": symbol, "phase": "trade_candidate_upsert", "error": "trade_candidate_upsert_error", "detail": str(exc)})

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
                # Bound each symbol's writes to its own short transaction.
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                record_error({"symbol": symbol, "phase": "analyze", "error": str(exc)})

        stats = {
            "candidates_created": candidates,
            "candles_written": candles_written,
            "momentum_rows_upserted": momentum_rows_upserted,
            "asset_states_upserted": asset_states_upserted,
            "errors": errors,
            "error_counts": dict(error_counts),
            "symbols_requested": len(symbols),
            "symbols_collected": len(collected_symbols),
            "symbols_scanned": scanned,
            "collect_workers": worker_count,
            "execution_interval": execution_interval,
            "analysis_ordering": "score_desc_existing_asset_state",
            "analysis_top_symbols": analyzed_symbols[:10],
            "external_ingest_only": True,
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
        logger.info(
            "Pipeline run completed run_id=%s symbols_requested=%s symbols_collected=%s symbols_scanned=%s candles_written=%s momentum_rows_upserted=%s asset_states_upserted=%s errors=%s",
            run_id,
            len(symbols),
            len(collected_symbols),
            scanned,
            candles_written,
            momentum_rows_upserted,
            asset_states_upserted,
            len(errors),
        )
        return {
            "run_id": run_id,
            "symbols_total": len(symbols),
            "symbols_requested": len(symbols),
            "symbols_collected": len(collected_symbols),
            "symbols_scanned": scanned,
            "candles_written": candles_written,
            "momentum_rows_upserted": momentum_rows_upserted,
            "asset_states_upserted": asset_states_upserted,
            "candidates_created": candidates,
            "collect_workers": worker_count,
            "execution_interval": execution_interval,
            "analysis_ordering": "score_desc_existing_asset_state",
            "analysis_top_symbols": analyzed_symbols[:10],
            "errors": errors,
            "error_counts": dict(error_counts),
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
