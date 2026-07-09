from __future__ import annotations

import pytest

import raspberry_executor.margin_order_manager as manager_module
from raspberry_executor.margin_order_manager import MarginOrderManager


class FakeKraken:
    dry_run = False

    def current_price(self, symbol: str) -> float:
        return 10.0

    def free_balance(self, asset: str) -> float:
        raise AssertionError("spot balance must not be used for margin quote sizing")


class FakeMargin:
    dry_run = False
    isolated = True

    def __init__(self, margin_free: float = 15.0):
        self.margin_free = margin_free
        self.transfers = []
        self.orders = []

    def ensure_margin_account(self, symbol: str) -> dict:
        return {"status": "already_enabled", "symbol": symbol}

    def margin_free_balance(self, symbol: str, asset: str) -> float:
        assert symbol == "BTCUSDC"
        assert asset == "USDC"
        return self.margin_free

    def transfer_spot_to_margin(self, symbol: str, asset: str, amount: str) -> dict:
        self.transfers.append({"symbol": symbol, "asset": asset, "amount": amount})
        return {"tranId": "transfer-1", "amount": amount}

    def max_borrowable(self, symbol: str, asset: str) -> float:
        return 0.0

    def margin_order(self, symbol: str, side: str, quantity: str, order_type: str, **kwargs) -> dict:
        self.orders.append({"symbol": symbol, "side": side, "quantity": quantity, "type": order_type, **kwargs})
        if side == "BUY":
            return {"orderId": "entry-1", "status": "FILLED", "executedQty": quantity, "fills": [{"price": "10", "qty": quantity}]}
        return {"orderId": "tp-1", "status": "NEW"}

    def get_margin_order(self, symbol: str, order_id: str) -> dict:
        buy_order = next(order for order in self.orders if order["side"] == "BUY")
        qty = str(buy_order["quantity"])
        quote = str(float(qty) * 10.0)
        return {"orderId": order_id, "symbol": symbol, "side": "BUY", "status": "FILLED", "executedQty": qty, "cummulativeQuoteQty": quote}


class FakeRules:
    def symbol_info(self, symbol: str) -> dict:
        return {"quoteAsset": "USDC"}

    def quantity_from_quote(self, symbol: str, quote_amount: float, current_price: float, *, market: bool = True) -> str:
        return str(float(quote_amount) / float(current_price))

    def normalize_exit_quantity(self, symbol: str, quantity: float | str) -> str:
        return str(quantity)

    def normalize_exit_price(self, symbol: str, price: float) -> str:
        return str(price)

    def ensure_exit_notional(self, symbol: str, quantity: str, price: float | str, *, label: str) -> None:
        return None


def test_take_profit_long_uses_requested_quote_without_default_margin_balance_clamp(monkeypatch):
    monkeypatch.setattr(manager_module, "margin_transfer_spot_balance", lambda: True)
    monkeypatch.setattr(manager_module, "margin_multiplier", lambda: 1.0)
    kraken = FakeKraken()
    margin = FakeMargin(margin_free=15.0)
    manager = MarginOrderManager(kraken, margin, FakeRules())

    def fail_clamp(**kwargs):
        raise AssertionError("default Kraken margin entry must not clamp quote to free balance")

    monkeypatch.setattr(manager, "_clamp_own_quote_to_available", fail_clamp)

    result = manager.open_long_with_margin_take_profit(symbol="BTCUSDC", quote_amount=100.0, target_price=12.0)

    assert margin.transfers == []
    assert margin.orders[0]["quantity"] == "10.0"
    assert result["transfer_payload"] == {}
    assert result["own_quote_amount"] == 100.0
    assert result["requested_own_quote_amount"] == 100.0
    assert result["total_quote_amount"] == 100.0
    assert result["quote_balance_guard"]["quote_balance_source"] == "margin"
    assert result["quote_balance_guard"]["quote_balance_guard"] == "diagnostic_only"
    assert result["quote_balance_guard"]["available_quote_amount"] == 15.0
    assert result["quote_balance_guard"]["clamp_to_available"] is False


def test_take_profit_long_keeps_order_quote_when_margin_balance_is_enough(monkeypatch):
    monkeypatch.setattr(manager_module, "margin_transfer_spot_balance", lambda: True)
    monkeypatch.setattr(manager_module, "margin_multiplier", lambda: 1.0)
    kraken = FakeKraken()
    margin = FakeMargin(margin_free=150.0)
    manager = MarginOrderManager(kraken, margin, FakeRules())

    result = manager.open_long_with_margin_take_profit(symbol="BTCUSDC", quote_amount=100.0, target_price=12.0)

    assert margin.transfers == []
    assert margin.orders[0]["quantity"] == "10.0"
    assert result["transfer_payload"] == {}
    assert result["own_quote_amount"] == 100.0
    assert result["quote_balance_guard"]["quote_balance_source"] == "margin"
    assert result["quote_balance_guard"]["quote_balance_guard"] == "diagnostic_only"


def test_take_profit_long_does_not_raise_margin_insufficient_quote_balance_by_default(monkeypatch):
    monkeypatch.setattr(manager_module, "margin_transfer_spot_balance", lambda: True)
    monkeypatch.setattr(manager_module, "margin_multiplier", lambda: 1.0)
    manager = MarginOrderManager(FakeKraken(), FakeMargin(margin_free=0.0), FakeRules())

    result = manager.open_long_with_margin_take_profit(symbol="BTCUSDC", quote_amount=100.0, target_price=12.0)

    assert result["own_quote_amount"] == 100.0
    assert result["quote_balance_guard"]["available_quote_amount"] == 0.0
    assert result["quote_balance_guard"]["quote_balance_guard"] == "diagnostic_only"


def test_take_profit_long_can_still_raise_margin_insufficient_quote_balance_when_clamp_requested(monkeypatch):
    monkeypatch.setattr(manager_module, "margin_transfer_spot_balance", lambda: True)
    monkeypatch.setattr(manager_module, "margin_multiplier", lambda: 1.0)
    manager = MarginOrderManager(FakeKraken(), FakeMargin(margin_free=0.0), FakeRules())

    with pytest.raises(RuntimeError, match="margin_insufficient_quote_balance"):
        manager.place_margin_market_entry(symbol="BTCUSDC", quote_amount=100.0, clamp_to_available=True)


def test_place_margin_market_entry_zero_quote_still_reports_margin_long_no_quote_available(monkeypatch):
    monkeypatch.setattr(manager_module, "margin_multiplier", lambda: 2.0)
    manager = MarginOrderManager(FakeKraken(), FakeMargin(margin_free=15.0), FakeRules())

    with pytest.raises(RuntimeError, match="margin_long_no_quote_available"):
        manager.place_margin_market_entry(symbol="BTCUSDC", quote_amount=0.0)
