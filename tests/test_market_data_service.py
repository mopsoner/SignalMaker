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


def _candle(
    *,
    candle_id: str,
    open_time: int,
    close_time: int,
    ingested_at: datetime,
    close: float,
    symbol: str = "BTCUSDC",
    interval: str = "1m",
) -> MarketCandle:
    return MarketCandle(
        candle_id=candle_id,
        symbol=symbol,
        interval=interval,
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


def test_list_candles_first_uses_smallest_open_time() -> None:
    with _make_session() as db:
        db.add_all(
            [
                _candle(
                    candle_id="BTCUSDC-1m-middle",
                    open_time=2_000,
                    close_time=2_999,
                    close=110.0,
                    ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
                _candle(
                    candle_id="BTCUSDC-1m-first-late-ingest",
                    open_time=1_000,
                    close_time=1_999,
                    close=100.0,
                    ingested_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
                ),
                _candle(
                    candle_id="BTCUSDC-1m-latest",
                    open_time=3_000,
                    close_time=3_999,
                    close=120.0,
                    ingested_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                ),
            ]
        )
        db.commit()

        candles = MarketDataService(db).list_candles(symbol="btcusdc", interval="1m", first=True)

        assert len(candles) == 1
        assert candles[0].candle_id == "BTCUSDC-1m-first-late-ingest"
        assert candles[0].open_time == 1_000


def test_list_candles_first_filters_symbol_and_interval() -> None:
    with _make_session() as db:
        db.add_all(
            [
                _candle(
                    candle_id="BTCUSDC-1m-first",
                    open_time=1_000,
                    close_time=1_999,
                    close=100.0,
                    ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
                _candle(
                    candle_id="BTCUSDC-5m-first",
                    open_time=500,
                    close_time=4_999,
                    close=105.0,
                    interval="5m",
                    ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
                _candle(
                    candle_id="ETHUSDC-1m-first",
                    open_time=250,
                    close_time=1_249,
                    close=200.0,
                    symbol="ETHUSDC",
                    ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
                _candle(
                    candle_id="BTCUSDC-1m-second",
                    open_time=2_000,
                    close_time=2_999,
                    close=110.0,
                    ingested_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                ),
            ]
        )
        db.commit()

        candles = MarketDataService(db).list_candles(symbol="btcusdc", interval="1m", limit=1, first=True)

        assert len(candles) == 1
        assert candles[0].candle_id == "BTCUSDC-1m-first"
        assert candles[0].symbol == "BTCUSDC"
        assert candles[0].interval == "1m"
