from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.base import Base
from app.models.market_candle import MarketCandle
from app.services.market_data_service import MarketDataService


@pytest.fixture()
def db_session(monkeypatch):
    monkeypatch.setattr(MarketDataService, "_ensure_optional_candle_columns", lambda self: None)
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


def add_candle(db, *, symbol="BTCUSDT", interval="1h", open_time=1, close_time=2, ingested_offset=0):
    candle = MarketCandle(
        candle_id=f"{symbol}-{interval}-{open_time}",
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        close_time=close_time,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10.0,
        quote_volume=0.0,
        number_of_trades=0,
        taker_buy_base_volume=0.0,
        taker_buy_quote_volume=0.0,
        provider="KRAKEN",
        ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=ingested_offset),
    )
    db.add(candle)
    return candle


def test_first_returns_candle_with_smallest_open_time(db_session):
    add_candle(db_session, symbol="BTCUSDT", interval="1h", open_time=3000, close_time=3999)
    oldest = add_candle(db_session, symbol="BTCUSDT", interval="1h", open_time=1000, close_time=1999)
    add_candle(db_session, symbol="ETHUSDT", interval="1h", open_time=5000, close_time=5999)
    eth_oldest = add_candle(db_session, symbol="ETHUSDT", interval="1h", open_time=4000, close_time=4999)
    db_session.commit()

    rows = MarketDataService(db_session).list_candles(first=True)

    assert [row.candle_id for row in rows] == [oldest.candle_id, eth_oldest.candle_id]


def test_latest_returns_greatest_close_time_not_most_recently_ingested(db_session):
    latest_by_time = add_candle(db_session, open_time=1000, close_time=1999, ingested_offset=1)
    add_candle(db_session, open_time=500, close_time=999, ingested_offset=99)
    db_session.commit()

    rows = MarketDataService(db_session).list_candles(latest=True)

    assert [row.candle_id for row in rows] == [latest_by_time.candle_id]


def test_first_with_symbol_interval_and_limit_filters_to_oldest_pair_candle(db_session):
    expected = add_candle(db_session, symbol="BTCUSDT", interval="1h", open_time=1000, close_time=1999)
    add_candle(db_session, symbol="BTCUSDT", interval="1h", open_time=2000, close_time=2999)
    add_candle(db_session, symbol="BTCUSDT", interval="5m", open_time=500, close_time=799)
    add_candle(db_session, symbol="ETHUSDT", interval="1h", open_time=100, close_time=1099)
    db_session.commit()

    rows = MarketDataService(db_session).list_candles(symbol="BTCUSDT", interval="1h", limit=1, first=True)

    assert [row.candle_id for row in rows] == [expected.candle_id]
