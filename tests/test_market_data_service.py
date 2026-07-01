from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.market_candle import MarketCandle
from app.services.market_data_service import MarketDataService


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def _candle(*, candle_id: str, open_time: int, close_time: int, ingested_at: datetime, close: float) -> MarketCandle:
    return MarketCandle(
        candle_id=candle_id,
        symbol="BTCUSDC",
        interval="1m",
        open_time=open_time,
        close_time=close_time,
        open=close - 1,
        high=close + 1,
        low=close - 2,
        close=close,
        volume=100.0,
        quote_volume=1000.0,
        number_of_trades=10,
        taker_buy_base_volume=50.0,
        taker_buy_quote_volume=500.0,
        ingested_at=ingested_at,
    )


def test_list_candles_latest_uses_temporal_close_time_not_ingested_at() -> None:
    with _make_session() as db:
        db.add_all(
            [
                _candle(
                    candle_id="BTCUSDC-1m-recent",
                    open_time=2_000,
                    close_time=2_999,
                    close=120.0,
                    ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
                _candle(
                    candle_id="BTCUSDC-1m-old-late-ingest",
                    open_time=1_000,
                    close_time=1_999,
                    close=100.0,
                    ingested_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                ),
            ]
        )
        db.commit()

        candles = MarketDataService(db).list_candles(symbol="btcusdc", interval="1m", latest=True)

        assert len(candles) == 1
        assert candles[0].candle_id == "BTCUSDC-1m-recent"
        assert candles[0].close_time == 2_999
