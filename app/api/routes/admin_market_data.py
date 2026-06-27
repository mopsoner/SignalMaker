from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.market_data_service import MarketDataService

router = APIRouter()


@router.get("/market-data/ibkr-feed/status")
def ibkr_feed_status(db: Session = Depends(get_db)) -> dict[str, Any]:
    svc = MarketDataService(db)
    total = svc.count_market_candles_by_provider("IBKR")
    assets = db.execute(text("SELECT COUNT(DISTINCT COALESCE(provider_symbol, symbol)) FROM market_candles WHERE provider='IBKR'")).scalar() or 0
    return {"enabled": True, "ingest_endpoint": "/api/v1/stocks-etfs/ibkr/candles", "total_ibkr_candles": total, "total_ibkr_assets": int(assets), "last_ibkr_import_run": svc.last_import_run("IBKR"), "last_ibkr_errors": [], "ibkr_provider_breakdown": {"IBKR": total}, "warnings": ["IBKR Client Portal Gateway runs on Raspberry. SignalMaker does not connect to the gateway directly."]}


@router.post("/market-data/ibkr-feed/test-ingest")
def test_ibkr_feed_ingest() -> dict[str, Any]:
    return {"ok": True, "ingest_endpoint": "/api/v1/stocks-etfs/ibkr/candles", "message": "Route is available; Raspberry pushes normalized candles here."}


@router.get("/market-data/ibkr-feed/candles/summary")
def ibkr_candle_summary(db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    return MarketDataService(db).candle_summary(provider="IBKR")

@router.post("/market-data/analyze")
def analyze_market_data(payload: dict[str, Any]) -> dict[str, Any]:
    provider = str(payload.get("provider") or "AUTO").upper()
    return {"ok": True, "provider": provider, "engine": payload.get("engine") or "both", "queued": False, "message": "Provider-specific analysis request accepted; existing engines remain unchanged."}
