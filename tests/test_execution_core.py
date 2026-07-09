from __future__ import annotations


from raspberry_executor.execution_core import (
    open_market_entry_margin_first,
    place_leveraged_market_entry,
    place_spot_market_entry,
)


class FakeKraken:
    dry_run = False

    def __init__(self):
        self.spot_orders = []

    def current_price(self, symbol: str) -> float:
        return 10.0

    def average_fill_price(self, order: dict, fallback: float | None = None) -> float | None:
        return fallback

    def place_market_entry(self, symbol: str, side: str, quantity: str) -> dict:
        self.spot_orders.append((symbol, side, quantity))
        return {"orderId": "spot-1", "status": "FILLED", "executedQty": quantity}


class FakeRules:
    def symbol_info(self, symbol: str) -> dict:
        return {"quoteAsset": "USDC"}

    def quantity_from_quote(self, symbol: str, quote_amount: float, current_price: float, *, market: bool = True) -> str:
        return f"{quote_amount / current_price:.8f}"


class FakeMargin:
    dry_run = False
    isolated = False

    def __init__(self, *, fail: bool = False, leverage: str = "5"):
        self.fail = fail
        self._leverage = leverage
        self.orders = []
        self.borrow_calls = []

    def leverage(self) -> str:
        return self._leverage

    def ensure_isolated_account(self, symbol: str) -> dict:
        return {"status": "cross_margin", "symbol": symbol}

    def borrow(self, *args, **kwargs):
        self.borrow_calls.append((args, kwargs))
        raise AssertionError("Kraken margin entry must not borrow explicitly")

    def margin_order(self, symbol: str, side: str, quantity: str, order_type: str = "MARKET", **kwargs) -> dict:
        self.orders.append((symbol, side, quantity, order_type, kwargs))
        if self.fail:
            raise RuntimeError("margin rejected")
        return {"orderId": "margin-1", "status": "FILLED", "executedQty": quantity, "cummulativeQuoteQty": "100", "leverage": str(kwargs.get("leverage", self._leverage))}


def test_place_leveraged_market_entry_calls_buy_market_and_returns_leverage_payload():
    kraken = FakeKraken()
    margin = FakeMargin(leverage="5")

    result = place_leveraged_market_entry(kraken=kraken, margin=margin, rules=FakeRules(), symbol="btcusdc", quote_amount=20.0, leverage="5")

    assert margin.orders == [("BTCUSDC", "BUY", "10.00000000", "MARKET", {"leverage": "5"})]
    assert margin.borrow_calls == []
    assert result["mode"] == "margin"
    assert result["symbol"] == "BTCUSDC"
    assert result["side"] == "long"
    assert result["quantity"] == "10.00000000"
    assert result["entry_order_id"] == "margin-1"
    assert result["leverage"] == "5"
    assert result["entry_payload"]["leverage"] == "5"
    assert result["borrow_payload"]["status"] == "implicit_margin"


def test_place_leveraged_market_entry_propagates_each_attempt_leverage_to_order():
    kraken = FakeKraken()

    for attempt in (5, 4, 3, 2):
        margin = FakeMargin(leverage="5")

        result = place_leveraged_market_entry(kraken=kraken, margin=margin, rules=FakeRules(), symbol="BTCUSDC", quote_amount=20.0, leverage=attempt)

        assert margin.orders[-1][4]["leverage"] == str(attempt)
        assert result["entry_payload"]["leverage"] == str(attempt)
        assert result["leverage"] == str(attempt)


def test_place_spot_market_entry_calls_spot_long_market_entry():
    kraken = FakeKraken()

    result = place_spot_market_entry(kraken=kraken, rules=FakeRules(), symbol="btcusdc", quote_amount=20.0)

    assert kraken.spot_orders == [("BTCUSDC", "long", "2.00000000")]
    assert result["mode"] == "spot"
    assert result["entry_order_id"] == "spot-1"


def test_open_market_entry_margin_first_falls_back_to_spot_after_margin_errors():
    kraken = FakeKraken()
    margins: list[FakeMargin] = []

    def factory(leverage):
        margin = FakeMargin(fail=True, leverage=str(leverage))
        margins.append(margin)
        return margin

    result = open_market_entry_margin_first(kraken=kraken, rules=FakeRules(), symbol="BTCUSDC", quote_amount=20.0, margin_factory=factory, leverages=[5, 3])

    assert [m.orders[0][1:4] for m in margins] == [("BUY", "10.00000000", "MARKET"), ("BUY", "6.00000000", "MARKET")]
    assert [m.orders[0][4]["leverage"] for m in margins] == ["5", "3"]
    assert kraken.spot_orders == [("BTCUSDC", "long", "2.00000000")]
    assert result["mode"] == "spot"
    assert [attempt["leverage"] for attempt in result["margin_attempts"]] == [5, 3]
    assert all("margin rejected" in attempt["error"] for attempt in result["margin_attempts"])
