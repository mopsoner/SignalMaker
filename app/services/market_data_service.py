from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
        """Keep existing Replit/Postgres databases compatible with new Kraken kline fields."""
        columns = {
            "quote_volume": "DOUBLE PRECISION NOT NULL DEFAULT 0",
            "number_of_trades": "INTEGER NOT NULL DEFAULT 0",
            "taker_buy_base_volume": "DOUBLE PRECISION NOT NULL DEFAULT 0",
            "taker_buy_quote_volume": "DOUBLE PRECISION NOT NULL DEFAULT 0",
            "provider": "VARCHAR(32) NOT NULL DEFAULT 'KRAKEN'",
            "asset_id": "VARCHAR(96)",
            "provider_symbol": "VARCHAR(64)",
            "asset_type": "VARCHAR(32)",
            "currency": "VARCHAR(16)",
            "exchange": "VARCHAR(64)",
            "universe": "VARCHAR(128)",
            "metadata_json": "JSON",
        }
        for column, definition in columns.items():
            self.db.execute(text(f"ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS {column} {definition}"))
        try:
            self.db.execute(text("""
                CREATE TABLE IF NOT EXISTS market_data_import_runs (
                    id VARCHAR(96) PRIMARY KEY,
                    provider VARCHAR(32),
                    run_type VARCHAR(64),
                    status VARCHAR(32),
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    total_assets INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    error_message TEXT
                )
            """))
        except Exception:
            self.db.rollback()
        self.db.commit()

    def list_symbols(self, limit: int | None = None) -> list[str]:
        stmt = select(MarketCandle.symbol).distinct().order_by(MarketCandle.symbol)
        if limit:
            stmt = stmt.limit(limit)
        return [str(symbol).upper() for symbol in self.db.scalars(stmt).all()]

    def list_candles(
        self,
        *,
        symbol: str | None = None,
        interval: str | None = None,
        limit: int = 200,
        latest: bool = False,
        first: bool = False,
    ) -> list[MarketCandle]:
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
        if first:
            stmt = stmt.order_by(MarketCandle.open_time.asc()).limit(limit)
        else:
            stmt = stmt.order_by(MarketCandle.ingested_at.desc()).limit(limit)
        return list(self.db.scalars(stmt).all())

    def candle_summary(self, symbol: str | None = None, provider: str | None = None) -> list[dict[str, Any]]:
        filters = "WHERE 1=1"
        params: dict = {}
        if symbol:
            filters += " AND symbol = :symbol"
            params["symbol"] = symbol.upper()
        if provider:
            filters += " AND provider = :provider"
            params["provider"] = provider.upper()

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

    def validate_candle_series(
        self,
        interval: str,
        candles: list[dict[str, Any]],
        *,
        min_count: int = 1,
    ) -> dict[str, Any]:
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

            if not (
                high_price >= max(open_price, close_price, low_price)
                and low_price <= min(open_price, close_price, high_price)
            ):
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
        """
        True PostgreSQL upsert.

        The previous implementation did:
        - SELECT by primary key
        - then INSERT if not found

        That can fail under concurrency:
        two feeds can both see the row as missing, then both try to insert it.
        This version uses INSERT ... ON CONFLICT DO UPDATE, so duplicate candles
        update the existing row instead of throwing market_candles_pkey errors.
        """
        if not candles:
            return 0

        now = datetime.now(timezone.utc)
        normalized_symbol = symbol.upper()

        records: list[dict[str, Any]] = []

        for candle in candles:
            candle_id = f"{normalized_symbol}-{interval}-{int(candle['open_time'])}"

            records.append(
                {
                    "candle_id": candle_id,
                    "symbol": normalized_symbol,
                    "interval": interval,
                    "open_time": int(candle["open_time"]),
                    "close_time": int(candle["close_time"]),
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "volume": float(candle["volume"]),
                    "quote_volume": float(candle.get("quote_volume") or 0.0),
                    "number_of_trades": int(candle.get("number_of_trades") or 0),
                    "taker_buy_base_volume": float(candle.get("taker_buy_base_volume") or 0.0),
                    "taker_buy_quote_volume": float(candle.get("taker_buy_quote_volume") or 0.0),
                    "provider": str(candle.get("provider") or "KRAKEN").upper(),
                    "asset_id": candle.get("asset_id"),
                    "provider_symbol": candle.get("provider_symbol"),
                    "asset_type": candle.get("asset_type"),
                    "currency": candle.get("currency"),
                    "exchange": candle.get("exchange"),
                    "universe": candle.get("universe"),
                    "metadata_json": candle.get("metadata_json"),
                    "ingested_at": now,
                }
            )

        table = MarketCandle.__table__
        stmt = pg_insert(table).values(records)

        update_columns = {
            "symbol": stmt.excluded.symbol,
            "interval": stmt.excluded.interval,
            "open_time": stmt.excluded.open_time,
            "close_time": stmt.excluded.close_time,
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume,
            "quote_volume": stmt.excluded.quote_volume,
            "number_of_trades": stmt.excluded.number_of_trades,
            "taker_buy_base_volume": stmt.excluded.taker_buy_base_volume,
            "taker_buy_quote_volume": stmt.excluded.taker_buy_quote_volume,
            "provider": stmt.excluded.provider,
            "asset_id": stmt.excluded.asset_id,
            "provider_symbol": stmt.excluded.provider_symbol,
            "asset_type": stmt.excluded.asset_type,
            "currency": stmt.excluded.currency,
            "exchange": stmt.excluded.exchange,
            "universe": stmt.excluded.universe,
            "metadata_json": stmt.excluded.metadata_json,
            "ingested_at": stmt.excluded.ingested_at,
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.candle_id],
            set_=update_columns,
        )

        self.db.execute(stmt)
        self.db.commit()

        return len(records)

    def count_market_candles_by_provider(self, provider: str) -> int:
        return int(
            self.db.execute(
                text("SELECT COUNT(*) FROM market_candles WHERE provider = :provider"),
                {"provider": provider.upper()},
            ).scalar()
            or 0
        )

    def last_import_run(self, provider: str = "KRAKEN") -> dict[str, Any] | None:
        try:
            row = self.db.execute(
                text("SELECT * FROM market_data_import_runs WHERE provider = :provider ORDER BY started_at DESC LIMIT 1"),
                {"provider": provider.upper()},
            ).mappings().first()
            return dict(row) if row else None
        except Exception:
            self.db.rollback()
            return None

    def candle_quality(self, provider: str = "KRAKEN") -> dict[str, Any]:
        return {
            "provider": provider.upper(),
            "candles": self.count_market_candles_by_provider(provider),
        }

    def latest_analysis_results(self, provider: str | None = None) -> list[dict[str, Any]]:
        return []
