from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.market_data_service import MarketDataService

router = APIRouter()


class ExternalMarketCandleIn(BaseModel):
    timestamp: str | None = None
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


class ExternalMarketCandleIngestRequest(BaseModel):
    provider: str = "IBKR"
    provider_symbol: str | None = None
    symbol: str
    asset_id: str | None = None
    asset_type: str | None = None
    timeframe: str = "1d"
    run_type: str = "raspberry_ibkr_feed"
    queue_analysis: bool = False
    conid: int | None = None
    gateway_id: str | None = None
    exchange: str | None = None
    currency: str | None = None
    universe: str | None = None
    candles: list[ExternalMarketCandleIn] = Field(default_factory=list)


@router.get("/assets")
def list_assets(limit: int = Query(default=500, ge=1, le=2000), universe: str | None = None, asset_type: str | None = None, db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    MarketDataService(db)._ensure_optional_candle_columns()
    where = "WHERE 1=1"
    params: dict[str, Any] = {"limit": limit}
    if universe:
        where += " AND universe = :universe"; params["universe"] = universe
    if asset_type:
        where += " AND asset_type = :asset_type"; params["asset_type"] = asset_type.upper()
    rows = db.execute(text(f"""
        SELECT COALESCE(asset_id, symbol) AS id, symbol, COALESCE(provider_symbol, symbol) AS provider_symbol,
               COALESCE(asset_type, 'ETF') AS asset_type, universe, currency, TRUE AS enabled
        FROM (
            SELECT DISTINCT symbol, provider_symbol, asset_id, asset_type, universe, currency
            FROM market_candles
            WHERE provider IN ('IBKR','EODHD') OR provider_symbol IS NOT NULL
        ) a
        {where}
        ORDER BY provider_symbol NULLS LAST, symbol
        LIMIT :limit
    """), params).mappings().all()
    return [dict(r) for r in rows]


@router.post("/ibkr/candles")
def ingest_ibkr_candles(payload: ExternalMarketCandleIngestRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    provider = payload.provider.upper()
    if provider not in {"IBKR", "IBKR_CP", "IBKR_REST"}:
        raise HTTPException(status_code=422, detail="provider must be IBKR, IBKR_CP, or IBKR_REST")
    timeframe = payload.timeframe or "1d"
    symbol = (payload.symbol or payload.provider_symbol or "").upper()
    if not symbol:
        raise HTTPException(status_code=422, detail="symbol is required")
    provider_symbol = (payload.provider_symbol or symbol).upper()
    svc = MarketDataService(db)
    run_id = str(uuid4())
    candles = [c.model_dump() | {"provider": "IBKR", "asset_id": payload.asset_id, "provider_symbol": provider_symbol, "asset_type": payload.asset_type, "currency": payload.currency, "exchange": payload.exchange, "universe": payload.universe, "metadata_json": {"source": payload.run_type, "gateway_id": payload.gateway_id, "conid": payload.conid, "provider_symbol": provider_symbol, "asset_type": payload.asset_type, "received": len(payload.candles)}} for c in payload.candles]
    upserted = svc.upsert_candles(symbol, timeframe, candles)
    if payload.candles:
        db.execute(text("""
            INSERT INTO market_data_import_runs(id, provider, run_type, status, started_at, finished_at, total_assets, success_count, failed_count, error_message)
            VALUES (:id, 'IBKR', :run_type, 'success', :now, :now, 1, 1, 0, NULL)
            ON CONFLICT DO NOTHING
        """), {"id": run_id, "run_type": payload.run_type, "now": datetime.now(timezone.utc)})
        db.commit()
    return {"status": "ok", "provider": "IBKR", "symbol": symbol, "provider_symbol": provider_symbol, "timeframe": timeframe, "received": len(payload.candles), "upserted": upserted, "import_run_id": run_id}
