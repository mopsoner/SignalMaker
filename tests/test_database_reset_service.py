from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models.app_setting import AppSetting
from app.models.market_candle import MarketCandle
from app.models.trade_candidate import TradeCandidate
from app.services.database_reset_service import reset_database_preserving_config
from app.services import runtime_settings


def test_reset_database_deletes_momentum_runtime_data_but_preserves_config(monkeypatch, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "executor.log"
    log_file.write_text("old error\nold activity\n")

    from app.services import database_reset_service

    monkeypatch.setattr(database_reset_service, "LOG_DIRS", (log_dir,))
    monkeypatch.setattr(database_reset_service, "reset_positions_db", lambda: {"status": "ok", "deleted": {"events": 2}, "preserved": {"settings": 1}, "errors": {}})

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as db:
        db.execute(text("CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT NOT NULL)"))
        db.execute(text("INSERT INTO config(key, value) VALUES ('mode', 'live')"))
        db.add(AppSetting(category="momentum", key="enabled", value="true"))
        db.add(TradeCandidate(candidate_id="momentum-btcusdc", symbol="BTCUSDC", side="long", stage="momentum", status="momentum_ready", score=12.5, payload={"stage": "momentum"}))
        db.add(MarketCandle(candle_id="BTCUSDC-1h-1", symbol="BTCUSDC", interval="1h", open_time=1, close_time=2, open=1, high=2, low=1, close=2, volume=100))
        db.commit()

        result = reset_database_preserving_config(db)

        assert result["status"] == "ok"
        assert result["mode"] == "delete_runtime_data_reset_settings_except_api_secrets_and_clear_logs"
        assert result["raspberry_executor"]["deleted"]["events"] == 2
        assert result["logs"]["cleared"][str(log_file)] > 0
        assert result["deleted"]["trade_candidates"] == 1
        assert result["deleted"]["market_candles"] == 1
        assert result["deleted"]["app_settings"] == 1
        assert result["preserved"]["app_settings"] == 0
        assert result["preserved"]["config"] == 1
        assert db.execute(text("SELECT COUNT(*) FROM trade_candidates")).scalar_one() == 0
        assert db.execute(text("SELECT COUNT(*) FROM market_candles")).scalar_one() == 0
        assert db.execute(text("SELECT COUNT(*) FROM app_settings")).scalar_one() == 0
        assert db.execute(text("SELECT COUNT(*) FROM config")).scalar_one() == 1

        assert log_file.read_text() == ""


def test_reset_database_preserves_only_kraken_api_secrets_and_runtime_settings_fall_back(monkeypatch, tmp_path):
    from app.services import database_reset_service

    monkeypatch.setattr(database_reset_service, "LOG_DIRS", (tmp_path / "missing-logs",))
    monkeypatch.setattr(
        database_reset_service,
        "reset_positions_db",
        lambda: {"status": "ok", "deleted": {}, "preserved": {}, "errors": {}},
    )
    monkeypatch.setattr(runtime_settings, "_legacy_bootstrap_values", lambda: {})

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as db:
        db.add_all(
            [
                AppSetting(category="kraken", key="kraken_api_key", value="secret-api-key"),
                AppSetting(category="kraken", key="kraken_secret_key", value="secret-api-secret"),
                AppSetting(category="live", key="live_trading_enabled", value=True),
                AppSetting(category="executor", key="quote_assets", value=["EUR", "USD"]),
                AppSetting(category="notifications", key="telegram_chat_id", value="chat-from-db"),
            ]
        )
        db.commit()

        result = reset_database_preserving_config(db)

        remaining = {
            (row.category, row.key): row.value
            for row in db.execute(select(AppSetting)).scalars().all()
        }
        assert result["status"] == "ok"
        assert result["deleted"]["app_settings"] == 3
        assert result["preserved"]["app_settings"] == 2
        assert remaining == {
            ("kraken", "kraken_api_key"): "secret-api-key",
            ("kraken", "kraken_secret_key"): "secret-api-secret",
        }

        loaded_settings = runtime_settings.load_runtime_settings(db)

        assert loaded_settings["kraken"]["kraken_api_key"] == "secret-api-key"
        assert loaded_settings["kraken"]["kraken_secret_key"] == "secret-api-secret"
        assert loaded_settings["live"]["live_trading_enabled"] == runtime_settings.DEFAULT_SETTINGS["live"]["live_trading_enabled"]
        assert loaded_settings["executor"]["quote_assets"] == runtime_settings.DEFAULT_SETTINGS["executor"]["quote_assets"]
        assert loaded_settings["notifications"]["telegram_chat_id"] == runtime_settings.DEFAULT_SETTINGS["notifications"]["telegram_chat_id"]
