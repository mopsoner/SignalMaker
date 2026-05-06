from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.models.market_candle import MarketCandle

INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
}


class MarketDataService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self._ensure_optional_candle_columns()

    def _ensure_optional_candle_columns(self) -> None:
        """Keep existing Replit/Postgres databases compatible with new Binance kline fields."""
        columns = {
            "quote_volume": "DOUBLE PRECISION NOT NULL DEFAULT 0",
            "number_of_trades": "INTEGER NOT NULL DEFAULT 0",
            "taker_buy_base_volume": "DOUBLE PRECISION NOT NULL DEFAULT 0",
            "taker_buy_quote_volume": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        }
        for column, definition in columns.items():
            self.db.execute(text(f"ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS {column} {definition}"))
        self.db.commit()

    def list_symbols(self, limit: int | None = None) -> list[str]:
        stmt = select(MarketCandle.symbol).distinct().order_by(MarketCandle.symbol)
        if limit:
            stmt = stmt.limit(limit)
        return [str(symbol).upper() for symbol in self.db.scalars(stmt).all()]

    def list_candles(self, *, symbol: str | None = None, interval: str | None = None, limit: int = 200, latest: bool = False) -> list[MarketCandle]:
        if latest:
            filters = "WHERE 1=1"
            params: dict = {}
            if symbol:
                filters += " AND symbol = :symbol"
                params["symbol"] = symbol.upper()
            if interval:
                filters += " AND interval = :interval"
                params["interval"] = interval
            sql = text(f"""
                SELECT DISTINCT ON (symbol, interval) *
                FROM market_candles
                {filters}
                ORDER BY symbol, interval, ingested_at DESC
                LIMIT :limit
            """)
            params["limit"] = limit
            rows = self.db.execute(sql, params).mappings().all()
            return [MarketCandle(**dict(r)) for r in rows]

        stmt = select(MarketCandle)
        if symbol:
            stmt = stmt.where(MarketCandle.symbol == symbol.upper())
        if interval:
            stmt = stmt.where(MarketCandle.interval == interval)
        stmt = stmt.order_by(MarketCandle.ingested_at.desc()).limit(limit)
        return list(self.db.scalars(stmt).all())

    def candle_summary(self, symbol: str | None = None) -> list[dict[str, Any]]:
        filters = "WHERE 1=1"
        params: dict = {}
        if symbol:
            filters += " AND symbol = :symbol"
            params["symbol"] = symbol.upper()
        sql = text(f"""
            SELECT
                symbol,
                interval,
                COUNT(*) AS candle_count,
                to_timestamp(MIN(open_time) / 1000.0) AS first_open,
                to_timestamp(MAX(close_time) / 1000.0) AS last_close,
                ROUND(
                    EXTRACT(EPOCH FROM (
                        to_timestamp(MAX(close_time) / 1000.0)
                        - to_timestamp(MIN(open_time) / 1000.0)
                    )) / 3600.0, 1
                ) AS span_hours,
                MAX(ingested_at) AS last_ingested
            FROM market_candles
            {filters}
            GROUP BY symbol, interval
            ORDER BY symbol, interval
        """)
        rows = self.db.execute(sql, params).mappings().all()
        return [dict(r) for r in rows]

    def get_latest_close_times(self, symbols: list[str]) -> dict[str, dict[str, int]]:
        if not symbols:
            return {}
        normalized = [s.upper() for s in symbols]
        stmt = (
            select(MarketCandle.symbol, MarketCandle.interval, func.max(MarketCandle.close_time))
            .where(MarketCandle.symbol.in_(normalized))
            .group_by(MarketCandle.symbol, MarketCandle.interval)
        )
        out: dict[str, dict[str, int]] = {}
        for symbol, interval, close_time in self.db.execute(stmt).all():
            out.setdefault(symbol, {})[interval] = int(close_time)
        return out

    def load_symbol_bundle(self, symbol: str, limits: dict[str, int]) -> dict[str, list[dict[str, Any]]]:
        payload: dict[str, list[dict[str, Any]]] = {}
        upper_symbol = symbol.upper()
        for interval, limit in limits.items():
            stmt = (
                select(MarketCandle)
                .where(MarketCandle.symbol == upper_symbol, MarketCandle.interval == interval)
                .order_by(MarketCandle.open_time.desc())
                .limit(limit)
            )
            rows = list(self.db.scalars(stmt).all())
            rows.reverse()
            payload[interval] = [
                {
                    "open_time": row.open_time,
                    "close_time": row.close_time,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "quote_volume": row.quote_volume,
                    "number_of_trades": row.number_of_trades,
                    "taker_buy_base_volume": row.taker_buy_base_volume,
                    "taker_buy_quote_volume": row.taker_buy_quote_volume,
                }
                for row in rows
            ]
        return payload

    def validate_candle_series(self, interval: str, candles: list[dict[str, Any]], *, min_count: int = 1) -> dict[str, Any]:
        expected_step = INTERVAL_MS[interval]
        issues: list[str] = []
        if len(candles) < min_count:
            issues.append(f"insufficient_count:{len(candles)}<{min_count}")
        duplicate_count = 0
        gap_count = 0
        ohlc_error_count = 0
        previous_open_time: int | None = None
        seen_open_times: set[int] = set()
        for candle in candles:
            open_time = int(candle["open_time"])
            if open_time in seen_open_times:
                duplicate_count += 1
            seen_open_times.add(open_time)
            if previous_open_time is not None and open_time - previous_open_time != expected_step:
                gap_count += 1
            previous_open_time = open_time
            open_price = float(candle["open"])
            high_price = float(candle["high"])
            low_price = float(candle["low"])
            close_price = float(candle["close"])
            if not (high_price >= max(open_price, close_price, low_price) and low_price <= min(open_price, close_price, high_price)):
                ohlc_error_count += 1
        if duplicate_count:
            issues.append(f"duplicate_open_times:{duplicate_count}")
        if gap_count:
            issues.append(f"time_gaps:{gap_count}")
        if ohlc_error_count:
            issues.append(f"ohlc_inconsistencies:{ohlc_error_count}")
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "duplicate_count": duplicate_count,
            "gap_count": gap_count,
            "ohlc_error_count": ohlc_error_count,
            "count": len(candles),
        }

    def upsert_candles(self, symbol: str, interval: str, candles: list[dict]) -> int:
        count = 0
        for candle in candles:
            candle_id = f"{symbol.upper()}-{interval}-{int(candle['open_time'])}"
            row = self.db.get(MarketCandle, candle_id)
            if row is None:
                row = MarketCandle(candle_id=candle_id, symbol=symbol.upper(), interval=interval)
                self.db.add(row)
            row.open_time = int(candle["open_time"])
            row.close_time = int(candle["close_time"])
            row.open = float(candle["open"])
            row.high = float(candle["high"])
            row.low = float(candle["low"])
            row.close = float(candle["close"])
            row.volume = float(candle["volume"])
            row.quote_volume = float(candle.get("quote_volume") or 0.0)
            row.number_of_trades = int(candle.get("number_of_trades") or 0)
            row.taker_buy_base_volume = float(candle.get("taker_buy_base_volume") or 0.0)
            row.taker_buy_quote_volume = float(candle.get("taker_buy_quote_volume") or 0.0)
            row.ingested_at = datetime.now(timezone.utc)
            count += 1
        self.db.commit()
        return count
