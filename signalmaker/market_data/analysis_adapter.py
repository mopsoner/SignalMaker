from __future__ import annotations

from datetime import datetime, timezone

from signalmaker.market_data.repository import MarketDataRepository
from app.services.wyckoff_pipeline_service import WyckoffPipelineService
from app.services.momentum_service import MomentumService


class MarketAnalysisAdapter:
    """Boundary between exchange candles and the shared crypto engines.

    Stock/ETF analysis uses adjusted_close as the engine's ``close`` when it is
    available (and reports the policy in diagnostics). OHLC is otherwise passed
    through unchanged. This handles split/dividend discontinuities without
    changing any shared-engine rule; providers must supply adjusted history.
    Exchange-local timestamps must already have been converted to UTC at ingest.
    We preserve holidays, regular-session edges and overnight gaps rather than
    pretending that equities trade continuously.
    """

    TIMEFRAME_MAPPING = {"15m": "15m", "1h": "1h", "4h": "4h"}
    TIMEFRAME_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}
    WYCKOFF_MINIMUM_HISTORY = {"15m": 24, "1h": 24, "4h": 24}
    CLOSE_PRICE_POLICY = "adjusted_close_when_available_else_close"
    def __init__(self, repo: MarketDataRepository):
        self.repo = repo

    async def load_stock_etf_candles_for_asset(self, asset_id, timeframe="1d"):
        return await self.repo.load_stock_etf_candles_for_asset(asset_id, timeframe)

    async def load_stock_etf_candle_bundle(self, asset_id, timeframes=("15m", "1h", "4h")):
        if hasattr(self.repo, "load_stock_etf_candle_bundle"):
            return await self.repo.load_stock_etf_candle_bundle(asset_id, timeframes)
        return {tf: await self.load_stock_etf_candles_for_asset(asset_id, tf) for tf in timeframes}

    def normalize_engine_candles(self, candles, *, symbol: str, timeframe: str):
        """Produce the crypto-engine OHLCV contract.

        ``timestamp``/``open_time`` and ``close_time`` are UTC epoch milliseconds;
        prices and volume are floats. ``is_closed`` is explicit and only closed
        candles are returned. No exchange gap is filled and no timeframe is
        resampled. Duplicate bar opens are collapsed deterministically.
        """
        if timeframe not in self.TIMEFRAME_MS:
            raise ValueError(f"unsupported_engine_timeframe:{timeframe}")
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        normalized = {}
        for candle in candles:
            timestamp = candle.get("timestamp") or candle.get("open_time")
            if isinstance(timestamp, datetime):
                aware = timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp.astimezone(timezone.utc)
                timestamp = int(aware.timestamp() * 1000)
            else:
                timestamp = int(float(timestamp))
                if timestamp < 10_000_000_000:
                    timestamp *= 1000
            close_time = candle.get("close_time")
            if isinstance(close_time, datetime):
                aware_close = close_time.replace(tzinfo=timezone.utc) if close_time.tzinfo is None else close_time.astimezone(timezone.utc)
                close_time = int(aware_close.timestamp() * 1000)
            elif close_time is not None:
                close_time = int(float(close_time))
                if close_time < 10_000_000_000:
                    close_time *= 1000
            else:
                close_time = timestamp + self.TIMEFRAME_MS[timeframe]
            is_closed = bool(candle.get("is_closed", close_time <= now_ms))
            if not is_closed or close_time > now_ms:
                continue
            adjusted = candle.get("adjusted_close")
            normalized[timestamp] = {
                "timestamp": timestamp, "open_time": timestamp, "close_time": close_time,
                "open": float(candle["open"]), "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(adjusted if adjusted is not None else candle["close"]),
                "volume": float(candle.get("volume") or 0), "symbol": symbol,
                "timeframe": timeframe, "is_closed": True,
            }
        return [normalized[key] for key in sorted(normalized)]

    async def run_momentum_analysis(self, asset_id, timeframe="1d"):
        timeframes = MomentumService.INTERVALS
        raw_bundle = await self.load_stock_etf_candle_bundle(asset_id, timeframes)
        symbol = str(asset_id)
        bundle = {tf: self.normalize_engine_candles(raw_bundle.get(tf, []), symbol=symbol, timeframe=tf) for tf in timeframes}
        missing = [tf for tf in timeframes if len(bundle[tf]) < MomentumService.LOOKBACKS[tf]]
        if missing:
            return self._insufficient_data("momentum", bundle, MomentumService.LOOKBACKS, missing, timeframe)
        row = MomentumService.calculate_bundle(str(asset_id), bundle)
        available = [tf for tf in timeframes if row[f"momentum_{tf}"] is not None]
        timeframe_metadata = {
            "market_type": "stock_etf", "requested_timeframe": timeframe,
            "timeframe_mapping": {tf: (tf if tf in available else None) for tf in timeframes},
            "unavailable_timeframes": [tf for tf in timeframes if tf not in available],
        }
        signal = "NO_SIGNAL" if not available else "BUY" if row["momentum_score"] >= 10 else "SELL" if row["momentum_score"] < -10 else "HOLD"
        return {
            "engine_name": "momentum", "signal": signal, "score": row["momentum_score"] if available else None,
            "trend": row["classification"] if available else None,
            "confidence": min(1.0, abs(row["momentum_score"]) / 25) if available else None,
            "payload": {**row, **timeframe_metadata},
        }

    async def run_wyckoff_smc_analysis(self, asset_id, timeframe="15m", *, asset: dict | None = None):
        execution_timeframe = "15m" if timeframe in {"1d", "5m", "15m"} else timeframe
        timeframes = tuple(dict.fromkeys((execution_timeframe, "1h", "4h")))
        if any(tf not in self.TIMEFRAME_MAPPING for tf in timeframes):
            return self._insufficient_data("wyckoff_smc", {}, self.WYCKOFF_MINIMUM_HISTORY, list(timeframes), timeframe)
        raw_bundle = await self.load_stock_etf_candle_bundle(asset_id, timeframes)
        identity = dict(asset or {})
        context = {
            key: identity.get(key)
            for key in ("asset_id", "provider_symbol", "universe_name", "asset_type", "currency", "exchange_code")
        }
        context["asset_id"] = context.get("asset_id") or identity.get("id") or asset_id
        symbol = identity.get("provider_symbol") or identity.get("symbol") or str(asset_id)
        bundle = {tf: self.normalize_engine_candles(raw_bundle.get(tf, []), symbol=symbol, timeframe=tf) for tf in timeframes}
        minimums = {tf: self.WYCKOFF_MINIMUM_HISTORY[tf] for tf in timeframes}
        missing = [tf for tf in timeframes if len(bundle[tf]) < minimums[tf]]
        if missing:
            return self._insufficient_data("wyckoff_smc", bundle, minimums, missing, timeframe)
        state, _assessment = WyckoffPipelineService().analyze(
            symbol=symbol, candles=bundle, market_context=context, execution_interval=execution_timeframe
        )
        bias = str(state.get("bias") or "neutral")
        decision = "BUY" if bias.startswith("bull") else "SELL" if bias.startswith("bear") else "HOLD"
        return {
            "engine_name": "wyckoff_smc", "signal": decision, "score": state.get("score"),
            "trend": state.get("state"), "confidence": state.get("confidence"),
            "stage": state.get("stage"), "bias": state.get("bias"),
            "state_payload": state, "payload": state,
            **{key: state.get(key) for key in (
                "hierarchy_gate", "wyckoff_requirement", "one_hour_decision", "confirmation_model",
                "execution_trigger", "liquidity_context", "macro_liquidity_context",
                "entry_liquidity_context", "projected_target", "execution_target")},
        }

    def _no_signal(self, engine, count, minimum):
        return {"engine_name": engine, "signal": "NO_SIGNAL", "score": None, "trend": None, "confidence": None, "payload": {"reason": "NOT_ENOUGH_CANDLES", "candles_count": count, "minimum_candles": minimum}}

    def _insufficient_data(self, engine, bundle, minimums, missing, requested_timeframe):
        counts = {tf: len(bundle.get(tf, [])) for tf in minimums}
        # Keep the historical no-signal construction path for callers that
        # instrument it, then enrich it with the structured diagnostic contract.
        result = self._no_signal(engine, sum(counts.values()), sum(minimums.values()))
        payload = {
            "status": "insufficient_data", "reason": "INSUFFICIENT_TIMEFRAME_HISTORY",
            "missing_timeframes": missing, "unavailable_timeframes": missing,
            "candle_counts": counts, "minimum_candles_by_timeframe": dict(minimums),
            "requested_timeframe": requested_timeframe,
            "timeframe_mapping": {tf: (self.TIMEFRAME_MAPPING.get(tf) if tf not in missing else None) for tf in minimums},
            "market_type": "stock_etf", "price_policy": self.CLOSE_PRICE_POLICY,
        }
        result["status"] = "insufficient_data"
        result["payload"] = payload
        return result
