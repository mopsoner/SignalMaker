from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.ibkr_market_data_service import IBKRMarketDataService

router = APIRouter()


@router.get("/contracts")
def list_contracts(
    symbol: str | None = Query(default=None),
    resolved: bool | None = Query(default=None),
    active: bool | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    return IBKRMarketDataService(db).list_contracts(
        symbol=symbol,
        resolved=resolved,
        active=active,
        limit=limit,
    )


@router.get("/candles/summary")
def candle_summary(symbol: str | None = Query(default=None), db: Session = Depends(get_db)):
    return IBKRMarketDataService(db).candle_summary(symbol=symbol)


@router.get("/candles")
def list_candles(
    symbol: str | None = Query(default=None),
    timeframe: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    return IBKRMarketDataService(db).list_candles(symbol=symbol, timeframe=timeframe, limit=limit)


@router.get("/import-runs")
def import_runs(limit: int = Query(default=50, ge=1, le=500), db: Session = Depends(get_db)):
    return IBKRMarketDataService(db).import_runs(limit=limit)
