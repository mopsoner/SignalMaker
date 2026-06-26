from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text


class IBKRRepository:
    """Repository for phase-1 IBKR tables only.

    The repository accepts the project's synchronous SQLAlchemy ``Session`` and exposes
    async methods so IBKR jobs can stay aligned with the async ib_async client without
    writing to the existing market_candles pipeline tables.
    """

    def __init__(self, db):
        self.db = db

    async def upsert_contract(
        self,
        asset_id,
        symbol: str,
        sec_type: str,
        exchange: str,
        primary_exchange,
        currency: str,
        conid: int | None,
        local_symbol,
        trading_class,
        resolved: bool,
        ambiguous: bool,
        last_error=None,
    ):
        query = text("""
        INSERT INTO ibkr_contracts (
            asset_id, symbol, sec_type, exchange, primary_exchange, currency,
            conid, local_symbol, trading_class, resolved, ambiguous,
            last_error, last_resolved_at, updated_at
        )
        VALUES (
            :asset_id, :symbol, :sec_type, :exchange, :primary_exchange, :currency,
            :conid, :local_symbol, :trading_class, :resolved, :ambiguous,
            :last_error, now(), now()
        )
        ON CONFLICT (symbol, sec_type, exchange, currency, COALESCE(primary_exchange, ''))
        DO UPDATE SET
            asset_id = EXCLUDED.asset_id,
            conid = EXCLUDED.conid,
            local_symbol = EXCLUDED.local_symbol,
            trading_class = EXCLUDED.trading_class,
            resolved = EXCLUDED.resolved,
            ambiguous = EXCLUDED.ambiguous,
            last_error = EXCLUDED.last_error,
            last_resolved_at = now(),
            updated_at = now()
        """)
        self.db.execute(query, {
            "asset_id": asset_id,
            "symbol": symbol,
            "sec_type": sec_type,
            "exchange": exchange,
            "primary_exchange": primary_exchange,
            "currency": currency,
            "conid": conid,
            "local_symbol": local_symbol,
            "trading_class": trading_class,
            "resolved": resolved,
            "ambiguous": ambiguous,
            "last_error": last_error,
        })
        self.db.commit()

    async def list_active_resolved_contracts(self, limit: int | None = None):
        query = """
        SELECT id, asset_id, symbol, sec_type, exchange, primary_exchange,
               currency, conid, local_symbol, trading_class
        FROM ibkr_contracts
        WHERE resolved = true
          AND active = true
          AND conid IS NOT NULL
        ORDER BY symbol
        """
        params: dict[str, Any] = {}
        if limit:
            query += " LIMIT :limit"
            params["limit"] = limit
        result = self.db.execute(text(query), params)
        return [dict(row._mapping) for row in result]

    async def upsert_ibkr_candles(
        self,
        symbol: str,
        conid: int,
        asset_id,
        timeframe: str,
        candles: list,
    ) -> int:
        query = text("""
        INSERT INTO ibkr_candles (
            asset_id, symbol, conid, timeframe, timestamp,
            open, high, low, close, volume, source
        )
        VALUES (
            :asset_id, :symbol, :conid, :timeframe, :timestamp,
            :open, :high, :low, :close, :volume, 'IBKR'
        )
        ON CONFLICT (symbol, conid, timeframe, timestamp)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """)
        count = 0
        for candle in candles:
            self.db.execute(query, {
                "asset_id": asset_id,
                "symbol": symbol,
                "conid": conid,
                "timeframe": timeframe,
                "timestamp": candle.timestamp,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            })
            count += 1
        self.db.commit()
        return count

    async def create_import_run(self, run_type: str, total_assets: int = 0, metadata: Mapping[str, Any] | None = None) -> int:
        result = self.db.execute(text("""
            INSERT INTO ibkr_import_runs (run_type, status, total_assets, metadata)
            VALUES (:run_type, 'running', :total_assets, CAST(:metadata AS JSONB))
            RETURNING id
        """), {
            "run_type": run_type,
            "total_assets": total_assets,
            "metadata": json.dumps(metadata or {}),
        })
        run_id = int(result.scalar_one())
        self.db.commit()
        return run_id

    async def finish_import_run(
        self,
        run_id: int,
        status: str,
        success_count: int,
        failed_count: int,
        error_message: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.db.execute(text("""
            UPDATE ibkr_import_runs
            SET status = :status,
                finished_at = now(),
                success_count = :success_count,
                failed_count = :failed_count,
                error_message = :error_message,
                metadata = COALESCE(CAST(:metadata AS JSONB), metadata)
            WHERE id = :run_id
        """), {
            "run_id": run_id,
            "status": status,
            "success_count": success_count,
            "failed_count": failed_count,
            "error_message": error_message,
            "metadata": json.dumps(metadata) if metadata is not None else None,
        })
        self.db.commit()
