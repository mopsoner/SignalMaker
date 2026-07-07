from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.base import Base
from app.models.app_setting import AppSetting
from app.models.order import Order
from app.services.executor_service import ExecutorService
from app.services.trade_candidate_service import TradeCandidateService


@pytest.fixture()
def db_session():
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


def test_executor_live_uses_take_profit_order_without_stop_loss(db_session, monkeypatch):
    class FakeKraken:
        def __init__(self):
            self.oco_calls = []
            self.limit_sell_calls = []

        def current_price(self, symbol):
            return 100.0

        def is_configured(self):
            return True

        def normalize_order(self, symbol, quantity, target_price, stop_price):
            assert stop_price is None
            return {"quantity": quantity, "mark_price": 100.0, "target_price": target_price}

        def place_market_buy(self, symbol, quantity):
            return {"orderId": 101, "status": "FILLED", "executedQty": quantity, "cummulativeQuoteQty": quantity * 100.0}

        def average_fill_price(self, order_payload):
            return 100.0

        def place_limit_sell(self, symbol, quantity, price):
            self.limit_sell_calls.append({"symbol": symbol, "quantity": quantity, "price": price})
            return {"orderId": 202, "status": "NEW"}

        def place_oco_sell(self, *args, **kwargs):
            self.oco_calls.append({"args": args, "kwargs": kwargs})
            raise AssertionError("OCO stop-loss order should not be used")

    db_session.add_all([
        AppSetting(category="live", key="live_trading_enabled", value=True),
        AppSetting(category="live", key="live_require_tp_sl", value=False),
        AppSetting(category="live", key="live_max_open_positions", value=10),
        AppSetting(category="live", key="live_max_notional_per_trade", value=1000.0),
    ])
    db_session.commit()
    TradeCandidateService(db_session).upsert_open_candidate(
        candidate_id="BTCUSDT-momentum",
        symbol="BTCUSDT",
        side="long",
        stage="momentum",
        score=8.0,
        entry_price=100.0,
        stop_price=90.0,
        target_price=120.0,
        rr_ratio=2.0,
        execution_target=None,
        liquidity_context=None,
        notes=None,
        payload={"source": "test"},
    )
    service = ExecutorService(db_session)
    fake_kraken = FakeKraken()
    service.kraken = fake_kraken

    result = service.execute_open_candidates(limit=10, quantity=0.25, mode="live")

    assert result["executed"], result
    assert result["executed"][0]["exchange_tp_order_id"] == 202
    assert fake_kraken.limit_sell_calls == [{"symbol": "BTCUSDT", "quantity": 0.25, "price": 120.0}]
    assert fake_kraken.oco_calls == []
    orders = db_session.query(Order).order_by(Order.created_at).all()
    assert [order.order_type for order in orders] == ["market", "take_profit"]
    assert all(order.order_type != "stop_loss" for order in orders)


def test_momentum_business_listing_excludes_smoke_and_dry_run_candidates(db_session):
    service = TradeCandidateService(db_session)
    service.upsert_open_candidate(
        candidate_id="momentum-LIVE-open",
        symbol="LIVEUSDC",
        side="long",
        stage="momentum",
        score=20,
        entry_price=1,
        stop_price=None,
        target_price=2,
        rr_ratio=2,
        execution_target={"source": "momentum_ranking"},
        liquidity_context=None,
        notes=None,
        payload={"source": "momentum_candidates"},
    )
    service.upsert_open_candidate(
        candidate_id="momentum-SMOKE-open",
        symbol="SMOKEUSDC",
        side="long",
        stage="momentum",
        score=99,
        entry_price=1,
        stop_price=None,
        target_price=2,
        rr_ratio=2,
        execution_target={"source": "smoke_test"},
        liquidity_context=None,
        notes=None,
        payload={"dry_run": True, "test_run_id": "kraken-smoke"},
    )

    rows = service.list_candidates(limit=10, stage="momentum", exclude_test_data=True)

    assert [row.candidate_id for row in rows] == ["momentum-LIVE-open"]


def test_risk_service_uses_live_runtime_app_settings(db_session):
    from app.services.risk_service import RiskService

    db_session.add_all(
        [
            AppSetting(category="live", key="live_trading_enabled", value=True),
            AppSetting(category="live", key="live_require_tp_sl", value=False),
            AppSetting(category="live", key="live_max_open_positions", value=3),
            AppSetting(category="live", key="live_max_notional_per_trade", value=50.0),
        ]
    )
    db_session.commit()

    RiskService(db_session).validate_live_candidate(
        symbol="BTCUSDT",
        side="long",
        entry_price=100.0,
        stop_price=None,
        target_price=None,
        quantity=0.25,
    )

    with pytest.raises(RuntimeError, match="exceeds max per trade 50.00"):
        RiskService(db_session).validate_live_candidate(
            symbol="BTCUSDT",
            side="long",
            entry_price=100.0,
            stop_price=None,
            target_price=None,
            quantity=0.75,
        )


def test_exchange_adapter_uses_env_credentials_not_runtime_app_setting(db_session, monkeypatch):
    from app.services import exchange_adapter

    monkeypatch.setenv("KRAKEN_API_KEY", "env-key")
    monkeypatch.setenv("KRAKEN_SECRET_KEY", "env-secret")
    db_session.add_all(
        [
            AppSetting(category="kraken", key="kraken_base_url", value="https://runtime.kraken"),
            AppSetting(category="kraken", key="kraken_api_key", value="runtime-key"),
            AppSetting(category="kraken", key="kraken_secret_key", value="runtime-secret"),
            AppSetting(category="live", key="live_trading_enabled", value=True),
        ]
    )
    db_session.commit()

    captured = {}

    class FakeKrakenClient:
        def __init__(self, base_url, api_key, secret_key, *, dry_run):
            captured.update(base_url=base_url, api_key=api_key, secret_key=secret_key, dry_run=dry_run)

        def is_configured(self):
            return True

    monkeypatch.setattr(exchange_adapter, "KrakenClient", FakeKrakenClient)

    exchange_adapter.KrakenExchangeAdapter(db_session)

    assert captured == {
        "base_url": "https://runtime.kraken",
        "api_key": "env-key",
        "secret_key": "env-secret",
        "dry_run": False,
    }


def test_executor_reconcile_uses_live_runtime_app_setting(db_session, monkeypatch):
    monkeypatch.setattr("app.services.executor_service.create_execution_adapter", lambda db=None: object())
    db_session.add(AppSetting(category="live", key="live_reconcile_enabled", value=True))
    db_session.commit()

    service = ExecutorService(db_session)
    service.positions.list_positions = lambda limit, status: []

    assert service.reconcile_live_positions() == {"enabled": True, "checked": 0, "closed": [], "updated": []}
