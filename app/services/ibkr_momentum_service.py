from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.services.momentum_service import MomentumService
from app.services.runtime_settings import load_runtime_settings


class IBKRMomentumAdapter:
    """Adapt isolated IBKR daily candles to the existing MomentumService math.

    The crypto momentum engine remains unchanged; this adapter only reshapes IBKR
    daily OHLCV rows into the candle dictionaries consumed by MomentumService's
    interval momentum helpers.
    """

    WINDOWS = {
        "1d": 21,
        "1m": 63,
        "6m": 126,
    }
    WEIGHTS = {
        "1d": 0.35,
        "1m": 0.40,
        "6m": 0.25,
    }

    def __init__(self, momentum: MomentumService) -> None:
        self.momentum = momentum

    def build_row(self, symbol: str, conid: int | None, candles: list[dict[str, Any]], rank: int) -> dict[str, Any]:
        interval_payloads = {
            key: self.momentum._interval_momentum(candles[-lookback:])
            for key, lookback in self.WINDOWS.items()
        }
        weighted_score = 0.0
        available_weight = 0.0
        for key, payload in interval_payloads.items():
            value = payload["momentum"]
            if value is None:
                continue
            weight = self.WEIGHTS[key]
            weighted_score += value * weight
            available_weight += weight

        momentum_score = weighted_score / available_weight if available_weight else 0.0
        available_count = sum(1 for payload in interval_payloads.values() if payload["momentum"] is not None)
        latest_times = [payload["updated_at"] for payload in interval_payloads.values() if payload["updated_at"]]
        latest_price = self._latest_price(interval_payloads)

        return {
            "rank": rank,
            "symbol": symbol,
            "conid": conid,
            "price": latest_price,
            "momentum_1d": interval_payloads["1d"]["momentum"],
            "momentum_1m": interval_payloads["1m"]["momentum"],
            "momentum_6m": interval_payloads["6m"]["momentum"],
            "momentum_score": round(momentum_score, 4),
            "classification": self.momentum._classification(momentum_score),
            "rsi_1d": interval_payloads["1d"]["rsi"],
            "rsi_1m": interval_payloads["1m"]["rsi"],
            "rsi_6m": interval_payloads["6m"]["rsi"],
            "change_1d": interval_payloads["1d"]["change"],
            "change_1m": interval_payloads["1m"]["change"],
            "change_6m": interval_payloads["6m"]["change"],
            "ema_trend_1d": interval_payloads["1d"]["ema_trend"],
            "ema_trend_1m": interval_payloads["1m"]["ema_trend"],
            "ema_trend_6m": interval_payloads["6m"]["ema_trend"],
            "updated_at": max(latest_times) if latest_times else None,
            "data_quality": "complete" if available_count == len(self.WINDOWS) else "partial" if available_count else "insufficient",
            "candle_count": len(candles),
            "calculated_at": datetime.now(timezone.utc),
        }

    def _latest_price(self, interval_payloads: dict[str, dict[str, Any]]) -> float | None:
        for key in ("1d", "1m", "6m"):
            price = interval_payloads[key].get("price")
            if price is not None:
                return float(price)
        return None


class IBKRMomentumService:
    """Read-only IBKR stock/ETF momentum rankings from ibkr_candles."""

    def __init__(self, db: Session) -> None:
        self.db = db
        self._momentum = MomentumService(db)
        self._adapter = IBKRMomentumAdapter(self._momentum)

    def _has_table(self, table_name: str) -> bool:
        return inspect(self.db.get_bind()).has_table(table_name)

    def list_rankings(self, *, limit: int = 300) -> list[dict[str, Any]]:
        if not self._has_table("ibkr_candles") or not self._has_table("ibkr_contracts"):
            return []

        settings = load_runtime_settings(self.db).get("ibkr", {})
        lookback = int(settings.get("ibkr_momentum_lookback_days") or 180)

        rows = self.db.execute(text("""
            SELECT *
            FROM (
                SELECT
                    c.symbol,
                    c.conid,
                    c.timestamp,
                    c.open,
                    c.high,
                    c.low,
                    c.close,
                    c.volume,
                    c.created_at,
                    ct.currency,
                    ct.exchange,
                    ct.primary_exchange,
                    ct.local_symbol,
                    ct.trading_class,
                    ct.ambiguous,
                    ROW_NUMBER() OVER (PARTITION BY c.symbol, c.conid ORDER BY c.timestamp DESC) AS rn
                FROM ibkr_candles c
                LEFT JOIN ibkr_contracts ct
                  ON ct.symbol = c.symbol
                 AND (ct.conid = c.conid OR (ct.conid IS NULL AND c.conid IS NULL))
                WHERE c.timeframe = '1d'
            ) ranked
            WHERE rn <= :lookback
            ORDER BY symbol, conid, timestamp
        """), {"lookback": lookback}).mappings().all()

        grouped: dict[tuple[str, int | None], dict[str, Any]] = {}
        for row in rows:
            key = (row["symbol"], row["conid"])
            bucket = grouped.setdefault(key, {"meta": dict(row), "candles": []})
            bucket["candles"].append({
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "ingested_at": row["timestamp"],
            })

        rankings: list[dict[str, Any]] = []
        for (symbol, conid), bucket in grouped.items():
            row = self._adapter.build_row(symbol, conid, bucket["candles"], rank=0)
            meta = bucket["meta"]
            row.update({
                "currency": meta.get("currency"),
                "exchange": meta.get("exchange"),
                "primary_exchange": meta.get("primary_exchange"),
                "local_symbol": meta.get("local_symbol"),
                "trading_class": meta.get("trading_class"),
                "ambiguous": meta.get("ambiguous"),
            })
            rankings.append(row)

        rankings.sort(key=lambda item: (item["momentum_score"], item["symbol"]), reverse=True)
        for index, row in enumerate(rankings, start=1):
            row["rank"] = index
        return rankings[:limit]
