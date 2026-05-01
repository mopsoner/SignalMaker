from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.market_candle import CandleSummary, MarketCandleRead
from app.services.market_data_service import MarketDataService

router = APIRouter()


@router.get("/candles/summary", response_model=list[CandleSummary])
def candle_summary(symbol: str | None = Query(default=None), db: Session = Depends(get_db)) -> list[CandleSummary]:
    return MarketDataService(db).candle_summary(symbol=symbol)


@router.get("/candles", response_model=list[MarketCandleRead])
def list_candles(symbol: str | None = Query(default=None), interval: str | None = Query(default=None), limit: int = Query(default=200, ge=1, le=2000), latest: bool = Query(default=False), db: Session = Depends(get_db)) -> list[MarketCandleRead]:
    return MarketDataService(db).list_candles(symbol=symbol, interval=interval, limit=limit, latest=latest)
