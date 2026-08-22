import asyncio
import json
from unittest.mock import Mock

from sqlalchemy import create_engine, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from signalmaker.market_data.repository import MarketDataRepository


def _repository():
    db = Session(create_engine("sqlite:///:memory:"))
    repository = MarketDataRepository(db)
    repository.ensure_schema()
    universe_id = asyncio.run(repository.create_or_update_universe("Europe ETF", asset_type="ETF"))
    asset_id = asyncio.run(repository.upsert_market_asset(
        universe_id, "CW8", "CW8.PA", "PA", "Amundi", "ETF", "EU", "FR", "EUR", pea_eligible=False, ucits=True
    ))
    return db, repository, asset_id


def test_complete_result_json_round_trip_and_latest_retention():
    db, repository, asset_id = _repository()
    run1 = asyncio.run(repository.create_analysis_run("wyckoff_smc"))
    complete = {
        "signal": "BUY", "score": 91, "stage": "execution", "bias": "bullish",
        "state": "READY", "hierarchy_gate": {"passed": True},
        "wyckoff_requirement": "spring", "one_hour_decision": "confirm",
        "confirmation_model": {"kind": "bos"}, "execution_trigger": {"price": 10.2},
        "liquidity_context": {"sweep": "sell-side"}, "target": {"price": 12},
        "blocking_reasons": [], "extra_engine_diagnostic": {"lossless": [1, 2]},
    }
    asyncio.run(repository.insert_analysis_result(run1, asset_id, "wyckoff_smc", "1h", complete))
    run2 = asyncio.run(repository.create_analysis_run("wyckoff_smc"))
    asyncio.run(repository.insert_analysis_result(
        run2, asset_id, "wyckoff_smc", "1h", {**complete, "signal": "HOLD", "score": 70}
    ))
    rows = asyncio.run(repository.latest_analysis_results(engine_name="wyckoff_smc"))
    assert len(rows) == 1
    assert rows[0]["signal"] == "HOLD"
    assert rows[0]["payload"]["schema_version"] == 2
    assert rows[0]["payload"]["raw_result"]["extra_engine_diagnostic"]["lossless"] == [1, 2]
    assert rows[0]["run_id"] == run2
    assert db.execute(text("SELECT COUNT(*) FROM market_analysis_results")).scalar_one() == 2


def test_legacy_rows_and_payload_version_filter_are_compatible():
    db, repository, asset_id = _repository()
    db.execute(text("""
        INSERT INTO market_analysis_results
          (asset_id, engine_name, timeframe, signal, payload_version, payload, created_at)
        VALUES (:asset, 'momentum', '1d', 'BUY', 1, :payload, '2026-01-01')
    """), {"asset": asset_id, "payload": json.dumps({"score": 4})})
    db.commit()
    row = asyncio.run(repository.latest_analysis_results(
        engine_name="momentum", asset_type="ETF", payload_version=1
    ))[0]
    assert row["schema_version"] == 1
    assert row["payload"] == {"schema_version": 1, "score": 4}


def test_latest_result_across_versions_when_payload_version_is_omitted():
    db, repository, asset_id = _repository()
    db.execute(text("""
        INSERT INTO market_analysis_results
          (asset_id, engine_name, timeframe, signal, payload_version, payload, created_at)
        VALUES
          (:asset, 'momentum', '1d', 'BUY', 1, :version_one, '2026-01-01'),
          (:asset, 'momentum', '1d', 'SELL', 2, :version_two, '2026-01-02')
    """), {
        "asset": asset_id,
        "version_one": json.dumps({"score": 4}),
        "version_two": json.dumps({"schema_version": 2, "score": 8}),
    })
    db.commit()

    row = asyncio.run(repository.latest_analysis_results(engine_name="momentum"))[0]

    assert row["signal"] == "SELL"
    assert row["payload_version"] == 2


def test_latest_result_for_explicit_payload_version():
    db, repository, asset_id = _repository()
    db.execute(text("""
        INSERT INTO market_analysis_results
          (asset_id, engine_name, timeframe, signal, payload_version, payload, created_at)
        VALUES
          (:asset, 'momentum', '1d', 'BUY', 1, :version_one, '2026-01-01'),
          (:asset, 'momentum', '1d', 'SELL', 2, :version_two, '2026-01-02')
    """), {
        "asset": asset_id,
        "version_one": json.dumps({"score": 4}),
        "version_two": json.dumps({"schema_version": 2, "score": 8}),
    })
    db.commit()

    row = asyncio.run(repository.latest_analysis_results(
        engine_name="momentum", payload_version=1
    ))[0]

    assert row["signal"] == "BUY"
    assert row["payload_version"] == 1


def test_omitted_payload_version_has_no_postgresql_parameter():
    result = Mock()
    result.all.return_value = []
    db = Mock()
    db.execute.return_value = result
    repository = MarketDataRepository(db)

    assert asyncio.run(repository.latest_analysis_results(engine_name="momentum")) == []

    statement, params = db.execute.call_args.args
    compiled = statement.compile(dialect=postgresql.dialect())
    assert "payload_version" not in str(compiled)
    assert "payload_version" not in compiled.params
    assert "payload_version" not in params


def test_results_are_isolated_from_crypto_tables_and_other_universes():
    db, repository, asset_id = _repository()
    db.execute(text("CREATE TABLE market_candles (id INTEGER PRIMARY KEY, symbol TEXT)"))
    db.execute(text("INSERT INTO market_candles (symbol) VALUES ('BTCUSD')"))
    asyncio.run(repository.insert_analysis_result(None, asset_id, "momentum", "1d", {"signal": "BUY"}))
    assert asyncio.run(repository.latest_analysis_results(asset_type="STOCK")) == []
    assert len(asyncio.run(repository.latest_analysis_results(universe_name="Europe ETF"))) == 1
    assert db.execute(text("SELECT COUNT(*) FROM market_candles")).scalar_one() == 1
