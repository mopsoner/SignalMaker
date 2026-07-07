from __future__ import annotations

import requests
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.base import Base
from app.models.app_setting import AppSetting
from app.models.order import Order
from app.models.trade_candidate import TradeCandidate
from app.services.executor_service import ExecutorService
from app.services.momentum_candidate_sync_service import MomentumCandidateSyncService
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


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.url = "https://central.test/api/v1/momentum"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def remote_candidate(**overrides):
    payload = {
        "candidate_id": "remote-btc",
        "symbol": "BTCUSDT",
        "side": "long",
        "stage": "momentum",
        "status": "momentum_ready",
        "score": 8.5,
        "entry_price": 100.0,
        "stop_price": 90.0,
        "target_price": 120.0,
        "rr_ratio": 2.0,
        "execution_target": {"notional": 25},
        "liquidity_context": {"spread": "ok"},
        "notes": "ready",
        "payload": {"rank": 1},
    }
    payload.update(overrides)
    return payload


def patch_get(monkeypatch, payload=None, exc=None):
    calls = []

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        if exc:
            raise exc
        return FakeResponse(payload if payload is not None else [remote_candidate()])

    monkeypatch.setattr("app.services.momentum_candidate_sync_service.requests.get", fake_get)
    return calls


def test_sync_uses_existing_signalmaker_base_url_setting(db_session, monkeypatch):
    db_session.add(AppSetting(category="momentum", key="signalmaker_base_url", value="https://central.local"))
    db_session.commit()
    calls = patch_get(monkeypatch)

    MomentumCandidateSyncService(db_session).sync()

    assert calls[0]["url"] == "https://central.local/api/v1/momentum"


def test_sync_momentum_candidates_success(db_session, monkeypatch):
    calls = patch_get(monkeypatch)

    summary = MomentumCandidateSyncService(db_session).sync(limit=25, min_momentum_score=3, min_rr=1.2, require_wyckoff_context=False)

    assert summary["fetched"] == 1
    assert summary["upserted"] == 1
    assert summary["skipped"] == []
    assert summary["errors"] == []
    assert calls[0]["params"] == {"limit": 25, "min_momentum_score": 3, "min_rr": 1.2, "require_wyckoff_context": False}
    row = db_session.get(TradeCandidate, "momentum-BTCUSDT-open")
    assert row is not None
    assert row.symbol == "BTCUSDT"
    assert row.status == "open"
    assert row.payload["source"] == "momentum_candidates"
    assert row.payload["remote_candidate_id"] == "remote-btc"

def test_sync_converts_momentum_ranking_asset_to_trade_candidate(db_session, monkeypatch):
    patch_get(monkeypatch, [{"symbol": "ETHUSDT", "rank": 1, "price": 200.0, "momentum_score": 12.5, "classification": "bull"}])

    summary = MomentumCandidateSyncService(db_session).sync(min_momentum_score=10)

    row = db_session.get(TradeCandidate, "momentum-ETHUSDT-open")
    assert summary["upserted"] == 1
    assert row is not None
    assert row.stage == "momentum"
    assert row.score == 12.5
    assert row.entry_price == 200.0
    assert row.target_price == pytest.approx(206.0)
    assert row.payload["source"] == "momentum_rankings"


def test_skip_candidate_without_entry_target(db_session, monkeypatch):
    patch_get(monkeypatch, [remote_candidate(entry_price=None)])

    summary = MomentumCandidateSyncService(db_session).sync()

    assert summary["fetched"] == 1
    assert summary["upserted"] == 0
    assert summary["skipped"][0]["reason"] == "missing_entry_or_target"
    assert db_session.get(TradeCandidate, "momentum-BTCUSDT-open") is None



def test_sync_accepts_candidate_without_stop_loss(db_session, monkeypatch):
    patch_get(monkeypatch, [remote_candidate(stop_price=None)])

    summary = MomentumCandidateSyncService(db_session).sync()

    row = db_session.get(TradeCandidate, "momentum-BTCUSDT-open")
    assert summary["upserted"] == 1
    assert summary["skipped"] == []
    assert row is not None
    assert row.stop_price is None
    assert row.target_price == 120.0


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

def test_remote_status_momentum_ready_becomes_local_open(db_session, monkeypatch):
    patch_get(monkeypatch, [remote_candidate(status="momentum_ready")])

    MomentumCandidateSyncService(db_session).sync()

    assert db_session.get(TradeCandidate, "momentum-BTCUSDT-open").status == "open"


def test_does_not_overwrite_executed_local_candidate(db_session, monkeypatch):
    existing = TradeCandidate(
        candidate_id="momentum-BTCUSDT-open",
        symbol="BTCUSDT",
        side="long",
        stage="momentum",
        status="executed",
        score=1.0,
        entry_price=50.0,
        stop_price=45.0,
        target_price=60.0,
        payload={"source": "old"},
    )
    db_session.add(existing)
    db_session.commit()
    patch_get(monkeypatch, [remote_candidate(entry_price=100.0)])

    summary = MomentumCandidateSyncService(db_session).sync()

    row = db_session.get(TradeCandidate, "momentum-BTCUSDT-open")
    assert summary["upserted"] == 0
    assert summary["skipped"][0]["reason"] == "local_candidate_already_executed"
    assert row.status == "executed"
    assert row.entry_price == 50.0
    assert row.payload == {"source": "old"}


def test_executor_can_execute_synced_candidate_in_paper_mode(db_session, monkeypatch):
    patch_get(monkeypatch, [remote_candidate()])

    result = ExecutorService(db_session).execute_open_candidates(limit=10, quantity=0.25, mode="paper", sync_momentum_first=True)

    assert result["sync"]["upserted"] == 1
    assert result["executed"][0]["candidate_id"] == "momentum-BTCUSDT-open"
    assert db_session.get(TradeCandidate, "momentum-BTCUSDT-open").status == "executed"


def test_api_failure_returns_clean_error_and_does_not_crash_executor(db_session, monkeypatch):
    TradeCandidateService(db_session).upsert_open_candidate(
        candidate_id="ETHUSDT-accumulation",
        symbol="ETHUSDT",
        side="long",
        stage="accumulation",
        score=4.0,
        entry_price=100.0,
        stop_price=90.0,
        target_price=120.0,
        rr_ratio=2.0,
        execution_target=None,
        liquidity_context=None,
        notes=None,
        payload={"source": "wyckoff"},
    )
    patch_get(monkeypatch, exc=requests.ConnectionError("central down"))

    result = ExecutorService(db_session).execute_open_candidates(limit=10, quantity=1.0, mode="paper", sync_momentum_first=True)

    assert result["sync"]["errors"][0]["reason"] == "api_error"
    assert result["executed"][0]["candidate_id"] == "ETHUSDT-accumulation"


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


def test_momentum_sync_uses_runtime_params_from_app_settings(db_session, monkeypatch):
    db_session.add_all(
        [
            AppSetting(category="momentum", key="signalmaker_base_url", value="https://ops.local/api/v1"),
            AppSetting(category="momentum", key="momentum_candidates_source_path", value="/api/v1/custom-momentum"),
            AppSetting(category="momentum", key="momentum_candidates_limit", value=7),
            AppSetting(category="momentum", key="momentum_candidates_min_score", value=4.5),
            AppSetting(category="momentum", key="momentum_candidates_min_rr", value=1.7),
            AppSetting(category="momentum", key="momentum_candidates_require_wyckoff_context", value=False),
            AppSetting(category="momentum", key="momentum_candidates_http_timeout_sec", value=2.5),
            AppSetting(category="momentum", key="momentum_candidates_target_pct", value=10.0),
        ]
    )
    db_session.commit()
    calls = patch_get(monkeypatch, [{"symbol": "ETHUSDT", "rank": 1, "price": 200.0, "momentum_score": 5.0}])

    summary = MomentumCandidateSyncService(db_session).sync()

    assert calls[0]["url"] == "https://ops.local/api/v1/custom-momentum"
    assert calls[0]["timeout"] == 2.5
    assert calls[0]["params"] == {
        "limit": 7,
        "min_momentum_score": 4.5,
        "require_wyckoff_context": False,
        "min_rr": 1.7,
    }
    assert summary["upserted"] == 1
    row = db_session.get(TradeCandidate, "momentum-ETHUSDT-open")
    assert row.target_price == pytest.approx(220.0)


def test_executor_reconcile_uses_live_runtime_app_setting(db_session, monkeypatch):
    monkeypatch.setattr("app.services.executor_service.create_execution_adapter", lambda db=None: object())
    db_session.add(AppSetting(category="live", key="live_reconcile_enabled", value=True))
    db_session.commit()

    service = ExecutorService(db_session)
    service.positions.list_positions = lambda limit, status: []

    assert service.reconcile_live_positions() == {"enabled": True, "checked": 0, "closed": [], "updated": []}
