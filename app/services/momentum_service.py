from __future__ import annotations

from datetime import datetime, timezone
from statistics import fmean
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.momentum_current import MomentumCurrent
from app.services.market_data_service import MarketDataService


class MomentumService:
    """Momentum calculation and snapshot service.

    Heavy calculations happen explicitly after candle ingestion through
    recalculate_and_store(). Read paths use the persisted momentum_current table.
    """

    INTERVALS = ("15m", "1h", "4h")
    WEIGHTS = {"15m": 0.35, "1h": 0.40, "4h": 0.25}
    LOOKBACKS = {"15m": 16, "1h": 24, "4h": 30}
    EMA_PERIOD = 20
    RSI_PERIOD = 14

    def __init__(self, db: Session) -> None:
        self.db = db
        self.market_data = MarketDataService(db)

    def list_rankings(self, *, limit: int = 200) -> list[dict[str, Any]]:
        stmt = select(MomentumCurrent).order_by(MomentumCurrent.momentum_score.desc(), MomentumCurrent.symbol.asc()).limit(limit)
        rows = list(self.db.scalars(stmt).all())
        return [self._row_to_payload(row, index) for index, row in enumerate(rows, start=1)]

    def recalculate_and_store(self, *, symbols: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
        target_symbols = sorted({symbol.upper() for symbol in (symbols or self.market_data.list_symbols(limit=None))})
        if limit is not None:
            target_symbols = target_symbols[:limit]

        calculated_rows = [self._build_symbol_row(symbol) for symbol in target_symbols]
        calculated_rows.sort(key=lambda row: row["momentum_score"], reverse=True)
        now = datetime.now(timezone.utc)
        for index, payload in enumerate(calculated_rows, start=1):
            payload["rank"] = index
            self._upsert_current(payload, calculated_at=now)
        self.db.commit()
        return {"symbols": len(target_symbols), "momentum_rows_upserted": len(calculated_rows), "calculated_at": now.isoformat()}

    def _row_to_payload(self, row: MomentumCurrent, rank: int) -> dict[str, Any]:
        return {
            "rank": rank,
            "symbol": row.symbol,
            "price": row.price,
            "momentum_15m": row.momentum_15m,
            "momentum_1h": row.momentum_1h,
            "momentum_4h": row.momentum_4h,
            "momentum_score": row.momentum_score,
            "classification": row.classification,
            "rsi_15m": row.rsi_15m,
            "rsi_1h": row.rsi_1h,
            "rsi_4h": row.rsi_4h,
            "change_15m": row.change_15m,
            "change_1h": row.change_1h,
            "change_4h": row.change_4h,
            "ema_trend_15m": row.ema_trend_15m,
            "ema_trend_1h": row.ema_trend_1h,
            "ema_trend_4h": row.ema_trend_4h,
            "updated_at": row.updated_at,
            "data_quality": row.data_quality,
            "calculated_at": row.calculated_at,
        }

    def _upsert_current(self, payload: dict[str, Any], *, calculated_at: datetime) -> None:
        row = self.db.get(MomentumCurrent, payload["symbol"])
        if row is None:
            row = MomentumCurrent(symbol=payload["symbol"])
            self.db.add(row)
        for key in (
            "price", "momentum_15m", "momentum_1h", "momentum_4h", "momentum_score", "classification",
            "rsi_15m", "rsi_1h", "rsi_4h", "change_15m", "change_1h", "change_4h",
            "ema_trend_15m", "ema_trend_1h", "ema_trend_4h", "data_quality", "rank", "updated_at",
        ):
            setattr(row, key, payload.get(key))
        row.calculated_at = calculated_at

    def _build_symbol_row(self, symbol: str) -> dict[str, Any]:
        bundle = self.market_data.load_symbol_bundle(symbol, {interval: self.LOOKBACKS[interval] for interval in self.INTERVALS})
        interval_payloads = {interval: self._interval_momentum(bundle.get(interval) or []) for interval in self.INTERVALS}

        weighted_score = 0.0
        available_weight = 0.0
        for interval, payload in interval_payloads.items():
            value = payload["momentum"]
            if value is None:
                continue
            weight = self.WEIGHTS[interval]
            weighted_score += value * weight
            available_weight += weight
        momentum_score = weighted_score / available_weight if available_weight else 0.0

        latest_times = [payload["updated_at"] for payload in interval_payloads.values() if payload["updated_at"]]
        latest_price = self._latest_price(interval_payloads)
        available_count = sum(1 for payload in interval_payloads.values() if payload["momentum"] is not None)

        return {
            "rank": 0,
            "symbol": symbol,
            "price": latest_price,
            "momentum_15m": interval_payloads["15m"]["momentum"],
            "momentum_1h": interval_payloads["1h"]["momentum"],
            "momentum_4h": interval_payloads["4h"]["momentum"],
            "momentum_score": round(momentum_score, 4),
            "classification": self._classification(momentum_score),
            "rsi_15m": interval_payloads["15m"]["rsi"],
            "rsi_1h": interval_payloads["1h"]["rsi"],
            "rsi_4h": interval_payloads["4h"]["rsi"],
            "change_15m": interval_payloads["15m"]["change"],
            "change_1h": interval_payloads["1h"]["change"],
            "change_4h": interval_payloads["4h"]["change"],
            "ema_trend_15m": interval_payloads["15m"]["ema_trend"],
            "ema_trend_1h": interval_payloads["1h"]["ema_trend"],
            "ema_trend_4h": interval_payloads["4h"]["ema_trend"],
            "updated_at": max(latest_times) if latest_times else None,
            "data_quality": "complete" if available_count == len(self.INTERVALS) else f"partial:{available_count}/{len(self.INTERVALS)}",
        }

    def _interval_momentum(self, candles: list[dict[str, Any]]) -> dict[str, Any]:
        if len(candles) < 2:
            return self._empty_interval_payload()

        closes = [float(candle["close"]) for candle in candles if candle.get("close") is not None]
        if len(closes) < 2 or closes[0] == 0:
            return self._empty_interval_payload()

        first_close = closes[0]
        latest_close = closes[-1]
        price_change = ((latest_close - first_close) / first_close) * 100
        rsi = self._rsi(closes)
        ema = self._ema(closes, min(self.EMA_PERIOD, len(closes)))
        ema_bonus = 0.0
        ema_trend = "unknown"
        if ema is not None:
            if latest_close > ema:
                ema_bonus = 5.0
                ema_trend = "above_ema"
            elif latest_close < ema:
                ema_bonus = -5.0
                ema_trend = "below_ema"
            else:
                ema_trend = "at_ema"

        rsi_component = ((rsi - 50.0) / 2.0) if rsi is not None else 0.0
        momentum = price_change + rsi_component + ema_bonus
        latest_ingested = candles[-1].get("ingested_at")

        return {
            "momentum": round(momentum, 4),
            "change": round(price_change, 4),
            "rsi": round(rsi, 4) if rsi is not None else None,
            "ema_trend": ema_trend,
            "updated_at": latest_ingested if isinstance(latest_ingested, datetime) else None,
            "price": latest_close,
        }

    def _empty_interval_payload(self) -> dict[str, Any]:
        return {"momentum": None, "change": None, "rsi": None, "ema_trend": "insufficient_data", "updated_at": None, "price": None}

    def _latest_price(self, interval_payloads: dict[str, dict[str, Any]]) -> float | None:
        for interval in ("15m", "1h", "4h"):
            price = interval_payloads[interval].get("price")
            if price is not None:
                return float(price)
        return None

    def _rsi(self, closes: list[float]) -> float | None:
        if len(closes) <= self.RSI_PERIOD:
            return None
        changes = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
        recent = changes[-self.RSI_PERIOD:]
        gains = [max(change, 0.0) for change in recent]
        losses = [abs(min(change, 0.0)) for change in recent]
        avg_gain = fmean(gains)
        avg_loss = fmean(losses)
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _ema(self, values: list[float], period: int) -> float | None:
        if not values or period <= 0:
            return None
        alpha = 2.0 / (period + 1.0)
        ema = values[0]
        for value in values[1:]:
            ema = (value * alpha) + (ema * (1.0 - alpha))
        return ema

    def _classification(self, score: float) -> str:
        if score >= 20:
            return "strong_bull"
        if score >= 10:
            return "bull"
        if score >= 0:
            return "neutral_bull"
        if score >= -10:
            return "neutral_bear"
        return "bear"
