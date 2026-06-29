from sqlalchemy import Column, Float, MetaData, String, Table, create_engine, insert, inspect, select
from sqlalchemy.orm import Session

from app.services.asset_state_service import AssetStateService


def test_ensure_15m_columns_skips_missing_legacy_rsi_5m_column():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    asset_state = Table(
        "asset_state_current",
        metadata,
        Column("symbol", String(32), primary_key=True),
        Column("rsi_15m", Float),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(insert(asset_state).values(symbol="BTCUSDC", rsi_15m=None))

    with Session(engine) as session:
        AssetStateService(session)

    with engine.connect() as connection:
        row = connection.execute(select(asset_state.c.rsi_15m)).one()
    assert row.rsi_15m is None


def test_ensure_15m_columns_backfills_from_legacy_rsi_5m_when_present():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    asset_state = Table(
        "asset_state_current",
        metadata,
        Column("symbol", String(32), primary_key=True),
        Column("rsi_5m", Float),
        Column("rsi_15m", Float),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(insert(asset_state).values(symbol="BTCUSDC", rsi_5m=42.5, rsi_15m=None))

    with Session(engine) as session:
        AssetStateService(session)

    with engine.connect() as connection:
        row = connection.execute(select(asset_state.c.rsi_15m)).one()
    assert row.rsi_15m == 42.5


def test_ensure_15m_columns_adds_rsi_15m_when_missing():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    asset_state = Table(
        "asset_state_current",
        metadata,
        Column("symbol", String(32), primary_key=True),
    )
    metadata.create_all(engine)

    with Session(engine) as session:
        AssetStateService(session)

    columns = {column["name"] for column in inspect(engine).get_columns("asset_state_current")}
    assert "rsi_15m" in columns
