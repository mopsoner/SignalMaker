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


def test_build_decision_from_candidates_buys_top_supported_symbol(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")

    from raspberry_executor.momentum_decision_feed import build_decision_from_candidates

    decision = build_decision_from_candidates([
        {"symbol": "BANKUSDC", "rank": 1, "momentum_score": 12.5},
        {"symbol": "ALLUSDC", "rank": 2, "momentum_score": 8.0},
    ])

    assert decision["action"] == "BUY"
    assert decision["should_trade"] is True
    assert decision["buy_symbol"] == "BANKUSDC"
    assert decision["source"] == "momentum_rankings"
    assert decision["executor_contract"]["buy_candidates"][0]["symbol"] == "BANKUSDC"


def test_build_decision_from_candidates_rotates_existing_momentum_position(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()
    state.add_open_position("momentum-ALLUSDC", {"candidate_id": "momentum-ALLUSDC", "execution_symbol": "ALLUSDC", "signal_symbol": "ALLUSDC", "side": "long", "quantity": "10", "entry_price": 1.0, "strategy": "momentum_rotation"})

    from raspberry_executor.momentum_decision_feed import build_decision_from_candidates

    decision = build_decision_from_candidates([{"symbol": "BANKUSDC", "rank": 1, "momentum_score": 12.5}])

    assert decision["action"] == "ROTATE"
    assert decision["should_trade"] is True
    assert decision["sell_symbol"] == "ALLUSDC"
    assert decision["buy_symbol"] == "BANKUSDC"


def test_fetch_decision_uses_main_momentum_rankings_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_PATH", "")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.load_settings", lambda: SimpleNamespace(signalmaker_base_url="https://central.test", quote_assets=["USDC"]))
    calls = []

    class FakeResponse:
        def __init__(self, payload, status_code, url):
            self._payload = payload
            self.status_code = status_code
            self.url = url
            self.ok = status_code < 400
            self.headers = {"content-type": "application/json"}
            self.text = "{}"

        def json(self):
            return self._payload

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse([
            {"symbol": "BADUSDT", "rank": 1, "momentum_score": 99.0},
            {"symbol": "BANKUSDC", "rank": 2, "momentum_score": 12.5},
        ], 200, url)

    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.requests.get", fake_get)

    from raspberry_executor.momentum_decision_feed import fetch_decision

    decision = fetch_decision()

    assert [call["url"] for call in calls] == ["https://central.test/api/v1/momentum"]
    assert decision["action"] == "BUY"
    assert decision["buy_symbol"] == "BANKUSDC"
    assert decision["source"] == "momentum_rankings"
    assert [row["symbol"] for row in decision["buy_candidates"]] == ["BANKUSDC"]


def test_fetch_decision_falls_back_to_momentum_rankings_for_custom_missing_endpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_PATH", "/api/v1/custom-decision")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.load_settings", lambda: SimpleNamespace(signalmaker_base_url="https://central.test", quote_assets=["USDC"]))
    calls = []

    class FakeResponse:
        def __init__(self, payload, status_code, url):
            self._payload = payload
            self.status_code = status_code
            self.url = url
            self.ok = status_code < 400
            self.headers = {"content-type": "application/json"}
            self.text = "{}"

        def json(self):
            return self._payload

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        if url.endswith("/api/v1/custom-decision"):
            return FakeResponse({"detail": "Not Found"}, 404, url)
        return FakeResponse([{"symbol": "BANKUSDC", "rank": 1, "momentum_score": 12.5}], 200, url)

    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.requests.get", fake_get)

    from raspberry_executor.momentum_decision_feed import fetch_decision

    decision = fetch_decision()

    assert [call["url"] for call in calls] == [
        "https://central.test/api/v1/custom-decision",
        "https://central.test/api/v1/momentum",
    ]
    assert decision["source"] == "momentum_decision_endpoint_fallback"
    assert decision["buy_symbol"] == "BANKUSDC"


def test_buy_symbol_skips_unsupported_quote_asset(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()

    result = buy_symbol(settings(), FakeBinance(quote_balances=[20.0]), FakeRules(), state, "BADUSDT", {"action": "BUY"})

    assert result == "unsupported_quote:BADUSDT:configured=USDC"
    assert [event["event_type"] for event in state.events()] == ["momentum_buy_skipped_unsupported_quote"]
