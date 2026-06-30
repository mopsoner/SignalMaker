import asyncio
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.models.base import Base
from signalmaker.data_providers.eodhd.repository import EODHDRepository


def test_momentum_decision_and_account_mode_defaults_match_raspberry_executor_contract():
    cfg = Settings(_env_file=None)

    assert cfg.momentum_decision_path == "/api/v1/momentum"
    assert cfg.momentum_decision_method == "GET"
    assert cfg.momentum_decision_limit == 25
    assert cfg.binance_account_mode == "cross_margin"


def test_external_market_candle_upsert_populates_legacy_required_candle_columns():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as db:
        repo = EODHDRepository(db)
        repo.ensure_schema()
        universe_id = asyncio.run(repo.create_or_update_universe("smoke", asset_type="stock"))
        asset_id = asyncio.run(repo.upsert_market_asset(
            universe_id,
            symbol="AIR",
            provider_symbol="AIR.PA",
            exchange_code="PA",
            name="Airbus",
            asset_type="stock",
            region="EU",
            country="FR",
            currency="EUR",
        ))
        candle = SimpleNamespace(
            timestamp=datetime(2026, 6, 29),
            open=Decimal("10"),
            high=Decimal("11"),
            low=Decimal("9"),
            close=Decimal("10.5"),
            adjusted_close=Decimal("10.5"),
            volume=Decimal("1234"),
        )

        upserted = asyncio.run(repo.upsert_market_candles(asset_id, "EODHD", "AIR.PA", "1d", [candle]))
        db.commit()

        assert upserted == 1
        row = db.execute(text("SELECT candle_id, symbol, interval, open_time, close_time FROM market_candles")).mappings().one()
        assert row["candle_id"] == "AIR.PA-1d-1782691200000"
        assert row["symbol"] == "AIR.PA"
        assert row["interval"] == "1d"
        assert row["open_time"] == 1782691200000
        assert row["close_time"] == 1782777599999
