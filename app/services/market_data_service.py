from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.models.market_candle import MarketCandle


class MarketDataService:
    def __init__(self, db: Session) -> None:
        self.db = db

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
                }
                for row in rows
            ]
        return payload

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
            row.ingested_at = datetime.now(timezone.utc)
            count += 1
        self.db.commit()
        return count
