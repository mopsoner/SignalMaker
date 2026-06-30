from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models.app_setting import AppSetting
from app.models.market_candle import MarketCandle
from app.models.trade_candidate import TradeCandidate
from app.services.database_reset_service import reset_database_preserving_config


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
        assert result["mode"] == "delete_all_except_settings_and_clear_logs"
        assert result["raspberry_executor"]["deleted"]["events"] == 2
        assert result["logs"]["cleared"][str(log_file)] > 0
        assert result["deleted"]["trade_candidates"] == 1
        assert result["deleted"]["market_candles"] == 1
        assert result["preserved"]["app_settings"] == 1
        assert result["preserved"]["config"] == 1
        assert db.execute(text("SELECT COUNT(*) FROM trade_candidates")).scalar_one() == 0
        assert db.execute(text("SELECT COUNT(*) FROM market_candles")).scalar_one() == 0
        assert db.execute(text("SELECT COUNT(*) FROM app_settings")).scalar_one() == 1
        assert db.execute(text("SELECT COUNT(*) FROM config")).scalar_one() == 1

        assert log_file.read_text() == ""
