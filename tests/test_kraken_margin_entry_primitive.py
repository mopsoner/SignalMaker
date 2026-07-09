from __future__ import annotations

from types import SimpleNamespace

import raspberry_executor.momentum_decision_feed as momentum_module
import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.kraken_margin_client import KrakenMarginClient
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.momentum_decision_feed import buy_symbol
from raspberry_executor.state import StateStore


class FakeRules:
    def symbol_info(self, symbol: str) -> dict:
        return {"quoteAsset": "USDC"}

    def base_asset(self, symbol: str) -> str:
        return symbol.upper().removesuffix("USDC")

    def quantity_from_quote(self, symbol: str, quote_amount: float, current_price: float, *, market: bool = True) -> str:
        return f"{quote_amount / current_price:.8f}"

    def normalize_exit_quantity(self, symbol: str, quantity: float | str) -> str:
        return str(quantity)

    def normalize_exit_price(self, symbol: str, price: float) -> str:
        return str(price)

    def ensure_exit_notional(self, symbol: str, quantity: str, price: float | str, *, label: str) -> None:
        return None


class FakeKraken:
    dry_run = False

    def __init__(self) -> None:
        self.signed_calls = []

    def _pair_key(self, symbol: str) -> str:
        return "XBTUSDC"

    def _signed(self, method: str, path: str, params: dict) -> dict:
        self.signed_calls.append({"method": method, "path": path, "params": params})
        return {"txid": ["entry-1"]}

    def current_price(self, symbol: str) -> float:
        return 10.0

    def get_order(self, symbol: str, order_id: str) -> dict:
        return {"orderId": order_id, "symbol": symbol, "side": "BUY", "status": "FILLED", "executedQty": "2.00000000", "cummulativeQuoteQty": "20"}

    def free_balance(self, asset: str) -> float:
        return 10.0


class NoBorrowKrakenMargin(KrakenMarginClient):
    def borrow(self, symbol: str, asset: str, amount: str) -> dict:  # pragma: no cover - must not be called
        raise AssertionError("Kraken long margin entry must not call explicit borrow")


class FakeMomentumKraken(FakeKraken):
    def average_fill_price(self, order: dict, fallback: float | None = None) -> float | None:
        return fallback

    def free_balance(self, asset: str) -> float:
        return 25.0 if asset.upper() == "USDC" else 0.0


class FakeMomentumMargin(NoBorrowKrakenMargin):
    instances: list["FakeMomentumMargin"] = []

    def __init__(self, kraken, *, isolated: bool, dry_run: bool, leverage=None) -> None:
        super().__init__(kraken, isolated=isolated, dry_run=True, leverage=leverage)
        self.dry_run = True
        FakeMomentumMargin.instances.append(self)

    def margin_free_balance(self, symbol: str, asset: str) -> float:
        return 25.0


def test_kraken_margin_order_add_order_payload_contains_leverage():
    kraken = FakeKraken()
    margin = KrakenMarginClient(kraken, dry_run=False, leverage=3)

    order = margin.margin_order("BTCUSDC", "BUY", "0.1", "MARKET")

    assert order["leverage"] == "3"
    assert kraken.signed_calls[0]["path"] == "/0/private/AddOrder"
    assert kraken.signed_calls[0]["params"]["leverage"] == "3"


def test_kraken_margin_order_accepts_per_call_leverage_override():
    kraken = FakeKraken()
    margin = KrakenMarginClient(kraken, dry_run=False, leverage=5)

    observed = []
    for attempt in (5, 4, 3, 2):
        order = margin.margin_order("BTCUSDC", "BUY", "0.1", "MARKET", leverage=attempt)
        observed.append((order["leverage"], kraken.signed_calls[-1]["params"]["leverage"]))

    assert observed == [("5", "5"), ("4", "4"), ("3", "3"), ("2", "2")]


def test_kraken_take_profit_sell_does_not_receive_entry_attempt_override():
    kraken = FakeKraken()
    margin = NoBorrowKrakenMargin(kraken, dry_run=False, leverage=5)
    manager = MarginOrderManager(kraken, margin, FakeRules())

    result = manager.open_long_with_margin_take_profit(symbol="BTCUSDC", quote_amount=10.0, target_price=12.0, leverage=2)

    assert result["entry_payload"]["leverage"] == "2"
    assert kraken.signed_calls[0]["params"]["leverage"] == "2"
    assert kraken.signed_calls[1]["params"]["type"] == "sell"
    assert kraken.signed_calls[1]["params"]["reduce_only"] is True
    assert kraken.signed_calls[1]["params"]["leverage"] == "5"


def test_shared_entry_primitive_uses_implicit_kraken_leverage_without_borrow():
    kraken = FakeKraken()
    margin = NoBorrowKrakenMargin(kraken, dry_run=False, leverage=2)
    manager = MarginOrderManager(kraken, margin, FakeRules())

    result = manager.place_margin_market_entry(symbol="BTCUSDC", quote_amount=10.0, leverage=2)

    assert result["mode"] == "margin"
    assert result["leverage"] == "2"
    assert result["quantity"] == "2.00000000"
    assert result["entry_price"] == 10.0
    assert result["entry_order_id"] == "entry-1"
    assert result["implicit_margin"] is True
    assert result["implicit_leverage_payload"]["status"] == "implicit_margin"
    assert result["leverage_notional"] == 10.0
    assert kraken.signed_calls[0]["params"]["leverage"] == "2"


def test_candidate_and_momentum_reuse_same_margin_entry_primitive(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_USE_CROSS_MARGIN", "true")
    monkeypatch.setattr(momentum_module, "MarginClient", FakeMomentumMargin)
    monkeypatch.setattr(momentum_module, "margin_dry_run", lambda: False)
    calls = []
    original = MarginOrderManager.place_margin_market_entry

    def spy(self, **kwargs):
        calls.append(kwargs["symbol"])
        return original(self, **kwargs)

    monkeypatch.setattr(MarginOrderManager, "place_margin_market_entry", spy)

    kraken = FakeMomentumKraken()
    candidate_manager = MarginOrderManager(kraken, FakeMomentumMargin(kraken, isolated=False, dry_run=True, leverage=2), FakeRules())
    candidate_manager.open_long_with_margin_take_profit(symbol="BTCUSDC", quote_amount=10.0, target_price=12.0)
    result = buy_symbol(SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], exchange="kraken"), kraken, FakeRules(), StateStore(), "ETHUSDC", {"action": "BUY"})

    assert calls == ["BTCUSDC", "ETHUSDC"]
    assert result.startswith("bought_cross_margin:ETHUSDC")


def test_confirmed_entry_payload_keeps_submitted_leverage_metadata():
    kraken = FakeKraken()
    margin = NoBorrowKrakenMargin(kraken, dry_run=False, leverage=2)
    manager = MarginOrderManager(kraken, margin, FakeRules())

    result = manager.place_margin_market_entry(symbol="BTCUSDC", quote_amount=10.0, leverage=2)

    assert result["entry_payload"]["leverage"] == "2"
    assert result["entry_confirm_payload"]["leverage"] == "2"
    assert result["entry_confirm_payload"]["entry_request_payload"]["leverage"] == "2"


class RejectingKraken(FakeKraken):
    def _signed(self, method: str, path: str, params: dict) -> dict:
        self.signed_calls.append({"method": method, "path": path, "params": params})
        raise RuntimeError("Kraken POST /0/private/AddOrder failed errors=['EOrder:Insufficient initial margin']")


def test_kraken_margin_order_error_includes_leverage_and_payload():
    kraken = RejectingKraken()
    margin = KrakenMarginClient(kraken, dry_run=False, leverage=5)

    try:
        margin.margin_order("BTCUSDC", "BUY", "0.1", "MARKET", leverage=3)
    except RuntimeError as exc:
        message = str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected margin order rejection")

    assert "kraken_margin_add_order_failed" in message
    assert "leverage=3" in message
    assert "'leverage': '3'" in message
    assert "Insufficient initial margin" in message
