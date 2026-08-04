from __future__ import annotations

from statistics import fmean
from typing import Any

from signalmaker.market_data.repository import MarketDataRepository
from app.services.wyckoff_pipeline_service import WyckoffPipelineService


class MarketAnalysisAdapter:
    def __init__(self, repo: MarketDataRepository):
        self.repo = repo

    async def load_stock_etf_candles_for_asset(self, asset_id, timeframe="1d"):
        return await self.repo.load_stock_etf_candles_for_asset(asset_id, timeframe)

    async def load_stock_etf_candle_bundle(self, asset_id, timeframes=("15m", "1h", "4h")):
        if hasattr(self.repo, "load_stock_etf_candle_bundle"):
            return await self.repo.load_stock_etf_candle_bundle(asset_id, timeframes)
        return {tf: await self.load_stock_etf_candles_for_asset(asset_id, tf) for tf in timeframes}

    def to_engine_input(self, candles):
        normalized = []
        for candle in candles:
            timestamp = candle.get("timestamp") or candle.get("open_time")
            if hasattr(timestamp, "timestamp"):
                timestamp = int(timestamp.timestamp() * 1000)
            normalized.append({
                **candle, "timestamp": timestamp, "open_time": candle.get("open_time") or timestamp,
                "close_time": candle.get("close_time") or timestamp,
                "open": float(candle["open"]), "high": float(candle["high"]),
                "low": float(candle["low"]), "close": float(candle.get("adjusted_close") or candle["close"]),
                "raw_close": float(candle["close"]), "volume": float(candle.get("volume") or 0),
            })
        return normalized

    async def run_momentum_analysis(self, asset_id, timeframe="1d"):
        candles = self.to_engine_input(await self.load_stock_etf_candles_for_asset(asset_id, timeframe))
        if len(candles) < 200:
            return self._no_signal("momentum", len(candles), 200)
        closes = [c["close"] for c in candles]
        ma50 = fmean(closes[-50:]); ma200 = fmean(closes[-200:]); last = closes[-1]
        ret_20 = (last / closes[-21] - 1) * 100 if closes[-21] else 0
        score = round((last / ma200 - 1) * 100 + ret_20, 4)
        signal = "BUY" if last > ma50 > ma200 and score > 0 else "SELL" if last < ma200 else "HOLD"
        return {"engine_name": "momentum", "signal": signal, "score": score, "trend": "UP" if last > ma200 else "DOWN", "confidence": min(1.0, abs(score) / 25), "payload": {"ma50": ma50, "ma200": ma200, "return_20d_pct": ret_20, "candles_count": len(candles)}}

    async def run_wyckoff_smc_analysis(self, asset_id, timeframe="15m", *, asset: dict | None = None):
        execution_timeframe = "15m" if timeframe in {"1d", "5m", "15m"} else timeframe
        timeframes = tuple(dict.fromkeys((execution_timeframe, "1h", "4h")))
        raw_bundle = await self.load_stock_etf_candle_bundle(asset_id, timeframes)
        bundle = {tf: self.to_engine_input(rows) for tf, rows in raw_bundle.items()}
        identity = dict(asset or {})
        context = {
            key: identity.get(key)
            for key in ("asset_id", "provider_symbol", "universe_name", "asset_type", "currency", "exchange_code")
        }
        context["asset_id"] = context.get("asset_id") or identity.get("id") or asset_id
        symbol = identity.get("provider_symbol") or identity.get("symbol") or str(asset_id)
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
