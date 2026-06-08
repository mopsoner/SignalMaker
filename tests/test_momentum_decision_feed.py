from __future__ import annotations

from types import SimpleNamespace

import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.momentum_decision_feed import buy_symbol, sell_symbol
from raspberry_executor.state import StateStore


class FakeRules:
    def base_asset(self, symbol: str) -> str:
        return symbol.upper().removesuffix("USDC")

    def normalize_market_quantity(self, symbol: str, qty: float) -> str:
        return f"{qty:.8f}"

    def ensure_exit_notional(self, symbol: str, qty: str, price: float, label: str) -> None:
        assert float(qty) * price >= 1.0

    def quantity_from_quote(self, symbol: str, notional: float, price: float, market: bool = True) -> str:
        return f"{notional / price:.8f}"


class FakeBinance:
    dry_run = False

    def __init__(self, *, quote_balances: list[float] | None = None, base_balance: float = 0.0) -> None:
        self.quote_balances = list(quote_balances or [0.0])
        self.base_balance = base_balance
        self.orders: list[dict] = []

    def current_price(self, symbol: str) -> float:
        return 1.0

    def free_balance(self, asset: str) -> float:
        if asset.upper() == "USDC":
            if len(self.quote_balances) > 1:
                return self.quote_balances.pop(0)
            return self.quote_balances[0]
        return self.base_balance

    def place_market_entry(self, symbol: str, side: str, quantity: str) -> dict:
        order = {"orderId": len(self.orders) + 1, "symbol": symbol, "side": side, "executedQty": quantity, "fills": [{"price": "1", "qty": quantity}]}
        self.orders.append(order)
        if side == "short":
            self.base_balance = 0.0
            self.quote_balances = [25.0]
        return order

    def average_fill_price(self, order: dict, fallback: float | None = None) -> float | None:
        return fallback


def settings() -> SimpleNamespace:
    return SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"])


def test_buy_waits_for_post_sell_quote_balance_before_no_cash_log(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.time.sleep", lambda _: None)
    monkeypatch.setenv("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", "4")
    monkeypatch.setenv("MOMENTUM_DECISION_BALANCE_CONFIRM_SLEEP", "0.2")
    state = StateStore()
    binance = FakeBinance(quote_balances=[0.0, 0.0, 12.0, 12.0])

    result = buy_symbol(settings(), binance, FakeRules(), state, "ALLUSDC", {"action": "ROTATE"})

    assert result.startswith("bought:ALLUSDC"), result
    assert [event["event_type"] for event in state.events()] == ["position_opened", "momentum_bought"]


def test_sell_records_single_realized_sell_event(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.time.sleep", lambda _: None)
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "quantity": "10", "entry_price": 1.2})
    binance = FakeBinance(quote_balances=[0.0], base_balance=10.0)

    result = sell_symbol(binance, FakeRules(), state, "BANKUSDC", {"action": "ROTATE"})

    assert result.startswith("sell_confirmed:BANKUSDC"), result
    event_types = [event["event_type"] for event in state.events()]
    assert event_types == ["position_opened", "momentum_sell_attempt", "momentum_sold"]
