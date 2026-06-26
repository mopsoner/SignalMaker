from __future__ import annotations

from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session


class IBKRMarketDataService:
    """Read-only views over the isolated IBKR phase-1 tables."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def _has_table(self, table_name: str) -> bool:
        return inspect(self.db.get_bind()).has_table(table_name)

    def list_contracts(
        self,
        *,
        symbol: str | None = None,
        resolved: bool | None = None,
        active: bool | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        if not self._has_table("ibkr_contracts"):
            return []

        filters = ["1=1"]
        params: dict[str, Any] = {"limit": limit}
        if symbol:
            filters.append("symbol = :symbol")
            params["symbol"] = symbol.upper()
        if resolved is not None:
            filters.append("resolved = :resolved")
            params["resolved"] = resolved
        if active is not None:
            filters.append("active = :active")
            params["active"] = active

        rows = self.db.execute(text(f"""
            SELECT
                id,
                asset_id,
                symbol,
                sec_type,
                exchange,
                primary_exchange,
                currency,
                conid,
                local_symbol,
                trading_class,
                resolved,
                ambiguous,
                active,
                last_resolved_at,
                last_error,
                created_at,
                updated_at
            FROM ibkr_contracts
            WHERE {' AND '.join(filters)}
            ORDER BY symbol, currency, exchange, COALESCE(primary_exchange, '')
            LIMIT :limit
        """), params).mappings().all()
        return [dict(row) for row in rows]

    def candle_summary(self, *, symbol: str | None = None) -> list[dict[str, Any]]:
        if not self._has_table("ibkr_candles"):
            return []

        filters = ["1=1"]
        params: dict[str, Any] = {}
        if symbol:
            filters.append("symbol = :symbol")
            params["symbol"] = symbol.upper()

        rows = self.db.execute(text(f"""
            SELECT
                symbol,
                conid,
                timeframe,
                COUNT(*) AS candle_count,
                MIN(timestamp) AS first_timestamp,
                MAX(timestamp) AS last_timestamp,
                MAX(created_at) AS last_imported_at
            FROM ibkr_candles
            WHERE {' AND '.join(filters)}
            GROUP BY symbol, conid, timeframe
            ORDER BY symbol, timeframe
        """), params).mappings().all()
        return [dict(row) for row in rows]

    def list_candles(
        self,
        *,
        symbol: str | None = None,
        timeframe: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if not self._has_table("ibkr_candles"):
            return []

        filters = ["1=1"]
        params: dict[str, Any] = {"limit": limit}
        if symbol:
            filters.append("symbol = :symbol")
            params["symbol"] = symbol.upper()
        if timeframe:
            filters.append("timeframe = :timeframe")
            params["timeframe"] = timeframe

        rows = self.db.execute(text(f"""
            SELECT
                id,
                asset_id,
                symbol,
                conid,
                timeframe,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                source,
                created_at
            FROM ibkr_candles
            WHERE {' AND '.join(filters)}
            ORDER BY timestamp DESC, symbol
            LIMIT :limit
        """), params).mappings().all()
        return [dict(row) for row in rows]

    def import_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        if not self._has_table("ibkr_import_runs"):
            return []

        rows = self.db.execute(text("""
            SELECT
                id,
                run_type,
                status,
                started_at,
                finished_at,
                total_assets,
                success_count,
                failed_count,
                error_message,
                metadata
            FROM ibkr_import_runs
            ORDER BY started_at DESC
            LIMIT :limit
        """), {"limit": limit}).mappings().all()
        return [dict(row) for row in rows]
