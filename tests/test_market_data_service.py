from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.market_candle import MarketCandle
from app.services.market_data_service import MarketDataService


def _candle(*, candle_id: str, open_time: int, close_time: int, ingested_at: datetime, close: float) -> MarketCandle:
    return MarketCandle(
        candle_id=candle_id,
        symbol="BTCUSD",
        interval="1m",
        open_time=open_time,
        close_time=close_time,
        open=close - 1,
        high=close + 1,
        low=close - 2,
        close=close,
        volume=10,
        quote_volume=100,
        number_of_trades=1,
        taker_buy_base_volume=5,
        taker_buy_quote_volume=50,
        provider="KRAKEN",
        ingested_at=ingested_at,
    )


def test_list_candles_latest_uses_candle_time_not_ingestion_time(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(MarketDataService, "_ensure_optional_candle_columns", lambda self: None)

    with Session(engine) as session:
        session.add_all(
            [
                _candle(
                    candle_id="BTCUSD-1m-1000",
                    open_time=1_000,
                    close_time=59_999,
                    close=100,
                    ingested_at=datetime(2026, 1, 1, 12, 10, tzinfo=timezone.utc),
                ),
                _candle(
                    candle_id="BTCUSD-1m-60000",
                    open_time=60_000,
                    close_time=119_999,
                    close=200,
                    ingested_at=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
                ),
            ]
        )
        session.commit()

        candles = MarketDataService(session).list_candles(symbol="BTCUSD", interval="1m", latest=True)

    assert len(candles) == 1
    assert candles[0].candle_id == "BTCUSD-1m-60000"
    assert candles[0].close_time == 119_999
    assert candles[0].close == 200
