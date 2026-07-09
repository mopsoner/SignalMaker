from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.base import Base
from app.models.app_setting import AppSetting
from app.models.order import Order
from app.models.position import Position
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
        candidate_id="BTCUSDT-trade",
        symbol="BTCUSDT",
        side="long",
        stage="trade",
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


def _enable_live_settings(db_session):
    db_session.add_all([
        AppSetting(category="live", key="live_trading_enabled", value=True),
        AppSetting(category="live", key="live_require_tp_sl", value=False),
        AppSetting(category="live", key="live_max_open_positions", value=10),
        AppSetting(category="live", key="live_max_notional_per_trade", value=1000.0),
    ])
    db_session.commit()


def _add_live_candidate(db_session, candidate_id="BTCUSDC-margin-trade"):
    TradeCandidateService(db_session).upsert_open_candidate(
        candidate_id=candidate_id,
        symbol="BTCUSDC",
        side="long",
        stage="trade",
        score=8.0,
        entry_price=100.0,
        stop_price=None,
        target_price=120.0,
        rr_ratio=2.0,
        execution_target=None,
        liquidity_context=None,
        notes=None,
        payload={"source": "test"},
    )


class _FakeLiveKraken:
    exchange_name = "kraken"

    def __init__(self):
        self.client = type("Client", (), {"dry_run": False})()

    def is_configured(self):
        return True

    def normalize_order(self, symbol, quantity, target_price, stop_price):
        return {"quantity": quantity, "mark_price": 100.0, "target_price": target_price, "stop_price": stop_price}

    def current_price(self, symbol):
        return 100.0


class _SequencedMarginManager:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def open_long_with_margin_take_profit(self, **kwargs):
        self.calls.append(kwargs)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        leverage = kwargs["leverage"]
        return {
            "quantity": "0.2",
            "entry_price": 100.0,
            "entry_order_id": f"margin-entry-{leverage}",
            "tp_order_id": f"margin-tp-{leverage}",
            "entry_payload": {"orderId": f"margin-entry-{leverage}", "status": "FILLED"},
            "tp_payload": {"orderId": f"margin-tp-{leverage}", "status": "OPEN"},
        }


def test_executor_live_candidate_margin_retries_shared_leverages(db_session, monkeypatch):
    _enable_live_settings(db_session)
    _add_live_candidate(db_session)
    manager = _SequencedMarginManager([RuntimeError("leverage rejected"), "success"])

    monkeypatch.setattr("app.services.executor_service.margin_enabled", lambda: True)
    monkeypatch.setattr("app.services.executor_service.margin_leverage_attempts", lambda: (5, 3))
    monkeypatch.setattr("app.services.executor_service.KrakenSymbolRules", lambda *args, **kwargs: object())
    monkeypatch.setattr("app.services.executor_service.KrakenMarginClient", lambda *args, **kwargs: object())
    monkeypatch.setattr("app.services.executor_service.MarginOrderManager", lambda *args, **kwargs: manager)

    service = ExecutorService(db_session)
    service.kraken = _FakeLiveKraken()

    result = service.execute_open_candidates(limit=10, quantity=0.2, mode="live")

    assert result["executed"], result
    assert [call["leverage"] for call in manager.calls] == [5, 3]
    assert result["executed"][0]["mode"] == "margin"
    position = db_session.query(Position).one()
    entry_order = db_session.query(Order).filter_by(order_type="market").one()
    assert position.meta["leverage"] == 3
    assert position.meta["margin_attempts"] == [{"leverage": 5, "error": "leverage rejected"}]
    assert entry_order.meta["leverage"] == 3
    assert entry_order.meta["margin_attempts"] == [{"leverage": 5, "error": "leverage rejected"}]


def test_executor_live_candidate_spot_fallback_records_margin_attempts(db_session, monkeypatch):
    _enable_live_settings(db_session)
    _add_live_candidate(db_session)
    manager = _SequencedMarginManager([RuntimeError("leverage 5 rejected"), RuntimeError("leverage 3 rejected")])
    spot_calls = []

    class FakeSpotOrderManager:
        def __init__(self, *args, **kwargs):
            pass

        def open_long_with_take_profit(self, **kwargs):
            spot_calls.append(kwargs)
            return {
                "quantity": "0.2",
                "entry_price": 100.0,
                "entry_payload": {"orderId": "spot-entry", "status": "FILLED"},
                "tp_payload": {"orderId": "spot-tp", "status": "OPEN"},
            }

    monkeypatch.setattr("app.services.executor_service.margin_enabled", lambda: True)
    monkeypatch.setattr("app.services.executor_service.margin_leverage_attempts", lambda: (5, 3))
    monkeypatch.setattr("app.services.executor_service.KrakenSymbolRules", lambda *args, **kwargs: object())
    monkeypatch.setattr("app.services.executor_service.KrakenMarginClient", lambda *args, **kwargs: object())
    monkeypatch.setattr("app.services.executor_service.MarginOrderManager", lambda *args, **kwargs: manager)
    monkeypatch.setattr("app.services.executor_service.SpotOrderManager", FakeSpotOrderManager)

    service = ExecutorService(db_session)
    service.kraken = _FakeLiveKraken()

    result = service.execute_open_candidates(limit=10, quantity=0.2, mode="live")

    assert result["executed"], result
    assert [call["leverage"] for call in manager.calls] == [5, 3]
    assert len(spot_calls) == 1
    position = db_session.query(Position).one()
    entry_order = db_session.query(Order).filter_by(order_type="market").one()
    expected_attempts = [
        {"leverage": 5, "error": "leverage 5 rejected"},
        {"leverage": 3, "error": "leverage 3 rejected"},
    ]
    assert position.meta["mode"] == "spot"
    assert position.meta["leverage"] is None
    assert position.meta["margin_attempts"] == expected_attempts
    assert entry_order.meta["margin_attempts"] == expected_attempts
    assert "margin attempts failed" in entry_order.meta["margin_error"]


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


def test_reconcile_live_positions_checks_cross_margin_take_profit(db_session):
    db_session.add(AppSetting(category="live", key="live_reconcile_enabled", value=True))
    db_session.commit()
    position = Position(
        position_id="pos-cross-margin-tp",
        symbol="BTCUSDC",
        side="long",
        status="open",
        quantity=0.2,
        entry_price=100.0,
        mark_price=100.0,
        target_price=120.0,
        meta={"mode": "margin", "tp_exchange_order_id": "margin-tp-3"},
    )
    db_session.add(position)
    db_session.commit()

    class FakeKraken:
        def __init__(self):
            self.get_order_calls = []

        def current_price(self, symbol):
            return 121.0

        def get_order(self, symbol, order_id):
            self.get_order_calls.append((symbol, order_id))
            return {"status": "FILLED", "price": "120.0"}

    service = ExecutorService(db_session)
    fake_kraken = FakeKraken()
    service.kraken = fake_kraken

    result = service.reconcile_live_positions()

    assert result["enabled"] is True
    assert result["checked"] == 1
    assert result["closed"] == [{"position_id": "pos-cross-margin-tp", "reason": "tp", "fill_price": 120.0}]
    assert fake_kraken.get_order_calls == [("BTCUSDC", "margin-tp-3")]
    db_session.refresh(position)
    assert position.status == "closed"
    assert position.unrealized_pnl == pytest.approx(4.0)


def test_reconcile_live_positions_reports_missing_take_profit_for_margin(db_session):
    db_session.add(AppSetting(category="live", key="live_reconcile_enabled", value=True))
    db_session.commit()
    position = Position(
        position_id="pos-cross-margin-missing-tp",
        symbol="ETHUSDC",
        side="long",
        status="open",
        quantity=1.0,
        entry_price=50.0,
        mark_price=50.0,
        target_price=60.0,
        meta={"mode": "margin"},
    )
    db_session.add(position)
    db_session.commit()

    class FakeKraken:
        def current_price(self, symbol):
            return 55.0

        def get_order(self, symbol, order_id):
            raise AssertionError("missing TP exchange order id should not query exchange")

    service = ExecutorService(db_session)
    service.kraken = FakeKraken()

    result = service.reconcile_live_positions()

    assert result["checked"] == 1
    assert result["closed"] == []
    assert {"position_id": "pos-cross-margin-missing-tp", "reason": "missing_take_profit_order"} in result["updated"]
    db_session.refresh(position)
    assert position.status == "open"
    assert position.mark_price == 55.0
