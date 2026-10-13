from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.position import Position
from app.services import wyckoff_goal_service as module
from app.services.wyckoff_goal_service import WyckoffGoalService


class Exchange:
    def __init__(self, profit):
        self.profit = profit
        self.calls = []
        self.closed = False
        self.client = self
        self.rules = self

    def open_margin_positions(self):
        return {} if self.closed else {"external": {"ordertxid": "entry", "pair": "BTCUSD", "vol": "2", "cost": "200", "type": "buy"}}

    def quote_asset(self, symbol):
        return "USD"

    def current_price(self, symbol):
        return 100 + self.profit / 2

    def pair_info(self, symbol):
        return {"pair_key": symbol}

    def open_orders(self):
        return [{"order_id": "tp", "descr": {"pair": "BTCUSD", "type": "sell", "ordertype": "limit"}}]

    def get_order(self, symbol, order_id, **kwargs):
        if order_id == "exit":
            return {"status": "filled" if self.closed else "open", "average_price": 150}
        return {"leverage": 2}

    def cancel_order(self, symbol, order_id, **kwargs):
        self.calls.append(("cancel", order_id))

    def close_market(self, symbol, side, quantity, **kwargs):
        self.calls.append(("close", side, quantity))
        return {"order_id": "exit"}


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(module, "settings", SimpleNamespace(
        wyckoff_live_enabled=False, kraken_profit_worker_enabled=True, kraken_execution_enabled=True,
        kraken_dry_run=False, wyckoff_live_mode="margin", wyckoff_live_goal_usd=100))
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        yield db


@pytest.mark.parametrize("profit,trigger", [(99.99, False), (100, True), (101, True)])
def test_account_positions_imported_and_threshold(setup, profit, trigger):
    ex = Exchange(profit)
    service = WyckoffGoalService(setup, ex)
    result = service.run()
    assert result["goal"]["profitUsd"] == pytest.approx(profit)
    assert result["blockEntries"] == trigger
    assert ex.calls == ([("cancel", "tp"), ("close", "sell", 2)] if trigger else [])
    assert setup.get(Position, "kraken-external") is not None


def test_pending_close_is_not_duplicated_and_confirmed(setup):
    ex = Exchange(100)
    service = WyckoffGoalService(setup, ex)
    service.run()
    service.run()
    assert len(ex.calls) == 2
    ex.closed = True
    service.run()
    assert setup.get(Position, "kraken-external").status == "closed"


def test_cancel_failure_does_not_submit_and_retries_latched_goal(setup):
    ex = Exchange(100)
    cancel = ex.cancel_order
    ex.cancel_order = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("cancel failed"))
    service = WyckoffGoalService(setup, ex)
    assert service.run()["goal"]["errors"] == ["cancel failed"]
    assert ex.calls == []
    ex.cancel_order = cancel
    ex.profit = 80
    service.run()
    assert ex.calls[-1] == ("close", "sell", 2)


def test_unavailable_price_blocks_entire_goal(setup):
    ex = Exchange(100)
    ex.current_price = lambda symbol: float("nan")
    result = WyckoffGoalService(setup, ex).run()
    assert result["blockEntries"]
    assert result["goal"]["profitUsd"] is None
    assert ex.calls == []


def test_dry_run_never_contacts_exchange(setup, monkeypatch):
    monkeypatch.setattr(module.settings, "kraken_dry_run", True)
    assert WyckoffGoalService(setup, object()).run()["enabled"] is False


@pytest.mark.parametrize("timestamps", [
    [], [None], [None, "2026-10-13T21:00:00+00:00"],
    ["missing", "2026-10-13T21:00:00+00:00"],
    ["2026-10-13T21:01:00+00:00", "2026-10-13T21:00:00+00:00"],
])
def test_summary_handles_missing_and_null_verification_dates(setup, timestamps):
    for index, timestamp in enumerate(timestamps):
        meta = {"execution_mode": "live"}
        if timestamp != "missing":
            meta["goal_verified_at"] = timestamp
        setup.add(Position(position_id=f"summary-{index}", symbol="BTCUSD", side="long",
                           status="open", quantity=1, unrealized_pnl=10, meta=meta))
    setup.commit()
    summary = WyckoffGoalService(setup).summary()
    verified = all(t and t != "missing" for t in timestamps)
    assert summary["verified"] == verified
    assert summary["lastCheckedAt"] == (min(timestamps) if verified and timestamps else None)
    assert summary["profitUsd"] == (10 * len(timestamps) if verified else None)


def test_margin_scan_repairs_stale_local_entry_price(setup):
    ex = Exchange(50)
    service = WyckoffGoalService(setup, ex)
    service.run()
    row = setup.get(Position, "kraken-external")
    row.entry_price = 0
    setup.commit()
    result = service.run()
    assert row.entry_price == 100
    assert result["goal"]["profitUsd"] == 50
    assert result["goal"]["errors"] == []


@pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf"), None])
def test_invalid_mark_price_identifies_symbol_and_field(setup, price):
    ex = Exchange(100)
    ex.current_price = lambda symbol: price
    result = WyckoffGoalService(setup, ex).run()
    assert "BTCUSD: invalid Kraken mark price" in result["goal"]["errors"][0]
    assert result["goal"]["profitUsd"] is None
    assert ex.calls == []


def test_spot_scan_refreshes_entry_from_confirmed_order(setup):
    ex = Exchange(100)
    ex.get_order = lambda *args, **kwargs: {"status": "filled", "executed_quantity": "2", "average_price": "100"}
    row = Position(position_id="spot", symbol="BTCUSD", side="long", status="open",
                   quantity=2, entry_price=0, entry_order_id="spot-entry",
                   meta={"execution_mode": "live", "kraken_mode": "spot"})
    setup.add(row)
    setup.commit()
    WyckoffGoalService(setup, ex)._scan(row, {})
    assert row.entry_price == 100
    assert row.unrealized_pnl == 100


def test_inconsistent_margin_volume_cannot_close_local_position(setup):
    ex = Exchange(50)
    service = WyckoffGoalService(setup, ex)
    service.run()
    positions = ex.open_margin_positions()
    positions["external"]["vol_closed"] = "3"
    ex.open_margin_positions = lambda: positions
    result = service.run()
    assert "invalid Kraken closed volume" in result["goal"]["errors"][0]
    assert setup.get(Position, "kraken-external").status == "open"
    assert ex.calls == []
