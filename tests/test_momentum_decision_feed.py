from __future__ import annotations

from types import SimpleNamespace

import pytest

import raspberry_executor.sqlite_db as sqlite_db
import raspberry_executor.momentum_decision_feed as momentum_module
from raspberry_executor.momentum_decision_feed import buy_best_available, buy_symbol, sell_symbol
from raspberry_executor.state import StateStore


@pytest.fixture(autouse=True)
def default_momentum_env(monkeypatch):
    monkeypatch.setenv("MOMENTUM_DECISION_EXECUTE_ENABLED", "true")
    monkeypatch.setenv("MOMENTUM_DECISION_USE_MARGIN", "false")
    monkeypatch.setenv("MARGIN_LEVERAGE_ATTEMPTS", "5,3")
    monkeypatch.delenv("MOMENTUM_DECISION_PATH", raising=False)
    monkeypatch.delenv("MOMENTUM_DECISION_METHOD", raising=False)
    monkeypatch.delenv("MOMENTUM_DECISION_USE_REMOTE_RUN_ONCE", raising=False)


class FakeRules:
    def base_asset(self, symbol: str) -> str:
        return symbol.upper().removesuffix("USDC")

    def normalize_market_quantity(self, symbol: str, qty: float) -> str:
        return f"{qty:.8f}"

    def ensure_exit_notional(self, symbol: str, qty: str, price: float, label: str) -> None:
        assert float(qty) * price >= 1.0

    def quantity_from_quote(self, symbol: str, notional: float, price: float, market: bool = True) -> str:
        return f"{notional / price:.8f}"


class FakeKraken:
    dry_run = False

    def __init__(self, *, quote_balances: list[float] | None = None, base_balance: float = 0.0) -> None:
        self.quote_balances = list(quote_balances or [0.0])
        self.base_balance = base_balance
        self.orders: list[dict] = []
        self.get_order_calls: list[tuple[str, int | str]] = []

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

    def get_order(self, symbol: str, order_id: int | str) -> dict:
        self.get_order_calls.append((symbol, order_id))
        for order in self.orders:
            if order.get("orderId") == order_id:
                return {**order, "side": "BUY", "status": "FILLED"}
        return {"orderId": order_id, "symbol": symbol, "side": "BUY", "status": "NEW", "executedQty": "0"}

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
    kraken = FakeKraken(quote_balances=[0.0, 0.0, 12.0, 12.0])

    result = buy_symbol(settings(), kraken, FakeRules(), state, "ALLUSDC", {"action": "ROTATE"})

    assert result.startswith("bought:ALLUSDC"), result
    assert [event["event_type"] for event in state.events()] == ["position_opened", "momentum_bought"]


def test_buy_symbol_keeps_confirmed_quote_when_next_account_read_is_stale(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.time.sleep", lambda _: None)
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setenv("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", "3")
    state = StateStore()
    kraken = FakeKraken(quote_balances=[0.0, 12.0, 0.0])

    result = buy_symbol(settings(), kraken, FakeRules(), state, "ALLUSDC", {"action": "ROTATE"})

    assert result == "bought:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert [event["event_type"] for event in state.events()] == ["position_opened", "momentum_bought"]



def test_spot_buy_confirms_order_before_recording_open_position(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.time.sleep", lambda _: None)
    calls: list[str] = []

    class RecordingKraken(FakeKraken):
        def get_order(self, symbol: str, order_id: int | str) -> dict:
            calls.append("get_order")
            return super().get_order(symbol, order_id)

    class RecordingState(StateStore):
        def add_open_position(self, candidate_id: str, payload: dict) -> None:
            calls.append("add_open_position")
            assert "get_order" in calls
            super().add_open_position(candidate_id, payload)

    state = RecordingState()
    kraken = RecordingKraken(quote_balances=[35.0])

    result = buy_symbol(settings(), kraken, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert calls == ["get_order", "add_open_position"]
    position = state.open_positions()["momentum-ALLUSDC"]
    assert position["entry_confirmed"] is True
    assert position["entry_confirm_status"] == "FILLED"
    assert position["entry_confirm_payload"]["orderId"] == 1
    event = state.events()[1]
    assert event["event_type"] == "momentum_bought"
    assert event["payload"]["entry_confirmed"] is True
    assert event["payload"]["entry_confirm_status"] == "FILLED"
    assert event["payload"]["entry_confirm_payload"]["orderId"] == 1


def test_buy_symbol_keeps_order_quote_when_more_quote_is_available(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    state = StateStore()
    kraken = FakeKraken(quote_balances=[35.0])

    result = buy_symbol(settings(), kraken, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert kraken.orders[0]["executedQty"] == "10.00000000"
    assert state.open_positions()["momentum-ALLUSDC"]["notional_used"] == 10.0


def test_buy_symbol_ignores_full_quote_env_and_keeps_order_quote(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_WITH_FULL_QUOTE", "true")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    state = StateStore()
    kraken = FakeKraken(quote_balances=[35.0])

    result = buy_symbol(settings(), kraken, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert kraken.orders[0]["executedQty"] == "10.00000000"


def test_buy_symbol_uses_margin_when_requested(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_USE_MARGIN", "true")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setattr(momentum_module, "margin_dry_run", lambda: False)
    monkeypatch.setattr(momentum_module, "margin_multiplier", lambda: 1.0)

    class FakeMargin:
        dry_run = False

        def __init__(self, kraken, *, dry_run: bool) -> None:
            self.kraken = kraken
            self.margin_account_mode_value = "cross"
            self.dry_run = dry_run
            self.orders = []

        def ensure_margin_account(self, symbol: str) -> dict:
            return {"status": "margin"}

        def margin_free_balance(self, symbol: str, asset: str) -> float:
            if asset == "USDC":
                return 35.0
            return 35.0

        def max_borrowable(self, symbol: str, asset: str) -> float:
            return 0.0

        def margin_order(self, symbol: str, side: str, quantity: str, order_type: str = "MARKET", **kwargs) -> dict:
            self.orders.append({"symbol": symbol, "side": side, "quantity": quantity, "type": order_type, **kwargs})
            return {"orderId": "margin-entry-1", "symbol": symbol, "side": side, "status": "FILLED", "executedQty": quantity, "fills": [{"price": "1", "qty": quantity}], "isIsolated": "FALSE"}

    instances = []

    def fake_margin_client(*args, **kwargs):
        margin = FakeMargin(*args, **kwargs)
        instances.append(margin)
        return margin

    monkeypatch.setattr(momentum_module, "MarginClient", fake_margin_client)
    state = StateStore()
    kraken = FakeKraken(quote_balances=[35.0])

    result = buy_symbol(settings(), kraken, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought_margin:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert kraken.orders == []
    assert instances[0].orders[0]["type"] == "MARKET"
    position = state.open_positions()["momentum-ALLUSDC"]
    assert position["mode"] == "margin"
    assert position["margin_isolated"] is False
    assert position["entry_payload"]["isIsolated"] == "FALSE"
    assert position["notional_used"] == 10.0


def test_margin_buy_uses_available_quote_when_less_than_order_quote(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_USE_MARGIN", "true")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setattr(momentum_module, "margin_dry_run", lambda: False)
    monkeypatch.setattr(momentum_module, "margin_multiplier", lambda: 1.0)

    class FakeMargin:
        dry_run = False

        def __init__(self, kraken, *, dry_run: bool) -> None:
            self.kraken = kraken
            self.margin_account_mode_value = "cross"
            self.dry_run = dry_run
            self.orders = []

        def ensure_margin_account(self, symbol: str) -> dict:
            return {"status": "margin"}

        def margin_free_balance(self, symbol: str, asset: str) -> float:
            if asset == "USDC":
                return 7.0
            return 7.0

        def max_borrowable(self, symbol: str, asset: str) -> float:
            return 0.0

        def margin_order(self, symbol: str, side: str, quantity: str, order_type: str = "MARKET", **kwargs) -> dict:
            self.orders.append({"symbol": symbol, "side": side, "quantity": quantity, "type": order_type, **kwargs})
            return {"orderId": "margin-entry-1", "symbol": symbol, "side": side, "status": "FILLED", "executedQty": quantity, "fills": [{"price": "1", "qty": quantity}], "isIsolated": "FALSE"}

    instances = []

    def fake_margin_client(*args, **kwargs):
        margin = FakeMargin(*args, **kwargs)
        instances.append(margin)
        return margin

    monkeypatch.setattr(momentum_module, "MarginClient", fake_margin_client)
    state = StateStore()
    kraken = FakeKraken(quote_balances=[7.0])

    result = buy_symbol(settings(), kraken, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought_margin:ALLUSDC:qty=7.00000000:notional=7.0000"
    assert instances[0].orders[0]["quantity"] == "7.00000000"
    assert state.open_positions()["momentum-ALLUSDC"]["notional_used"] == 7.0


def test_sell_symbol_uses_margin_for_margin_position(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setattr(momentum_module, "load_settings", settings)
    monkeypatch.setattr(momentum_module, "margin_dry_run", lambda: False)

    class FakeMargin:
        dry_run = False

        def __init__(self, kraken, *, dry_run: bool) -> None:
            self.kraken = kraken
            self.margin_account_mode_value = "cross"
            self.dry_run = dry_run
            self.base_balance = 10.0
            self.quote_balance = 0.0
            self.orders = []

        def ensure_margin_account(self, symbol: str) -> dict:
            return {"status": "margin"}

        def margin_free_balance(self, symbol: str, asset: str) -> float:
            if asset == "USDC":
                return self.quote_balance
            return self.base_balance

        def margin_order(self, symbol: str, side: str, quantity: str, order_type: str = "MARKET", **kwargs) -> dict:
            self.orders.append({"symbol": symbol, "side": side, "quantity": quantity, "type": order_type, **kwargs})
            if side == "SELL":
                self.base_balance = 0.0
                self.quote_balance = 25.0
            return {"orderId": "margin-sell-1", "symbol": symbol, "side": side, "status": "FILLED", "executedQty": quantity, "fills": [{"price": "1", "qty": quantity}], "isIsolated": "FALSE"}

    instances = []

    def fake_margin_client(*args, **kwargs):
        margin = FakeMargin(*args, **kwargs)
        instances.append(margin)
        return margin

    monkeypatch.setattr(momentum_module, "MarginClient", fake_margin_client)
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "mode": "margin", "margin_isolated": False, "quantity": "10", "entry_price": 1.2})
    kraken = FakeKraken(quote_balances=[0.0], base_balance=0.0)

    result = sell_symbol(kraken, FakeRules(), state, "BANKUSDC", {"action": "ROTATE"})

    assert result.startswith("sell_confirmed_margin:BANKUSDC"), result
    assert kraken.orders == []
    assert instances[0].orders[0]["side"] == "SELL"
    assert state.open_positions() == {}
    sold_event = state.events()[-1]
    assert sold_event["event_type"] == "momentum_sold"
    assert sold_event["payload"]["mode"] == "margin"


def test_rotate_sells_margin_position_before_restarting_buy_pattern(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "mode": "margin", "margin_isolated": False, "quantity": "10", "entry_price": 1.2})
    calls = []

    monkeypatch.setattr(momentum_module, "StateStore", lambda: state)
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[0.0], base_balance=0.0))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())

    def fake_sell(kraken, rules, store, symbol, decision, *, require_confirmed: bool = True):
        calls.append(("sell", symbol, bool(decision.get("force_margin")), list(store.open_positions())))
        store.close_position("momentum-BANKUSDC", "momentum_sell", {}, record_event=False)
        return "sell_confirmed_margin:BANKUSDC:remaining_value=0.0000:quote=25.0000"

    def fake_buy(settings_arg, kraken, rules, store, decision, *, exclude=None):
        calls.append(("buy", decision.get("buy_symbol"), bool(decision.get("force_margin")), list(store.open_positions())))
        return "fallback_buy:ALLUSDC:bought_margin:ALLUSDC:qty=25.00000000:notional=25.0000"

    monkeypatch.setattr(momentum_module, "sell_symbol", fake_sell)
    monkeypatch.setattr(momentum_module, "_buy_with_momentum_cadence", fake_buy)

    result = momentum_module.execute_decision({"action": "ROTATE", "should_trade": True, "symbol": "BANKUSDC", "sell_symbol": "BANKUSDC", "buy_symbol": "ALLUSDC"})

    assert result.startswith("rotate:sell_confirmed_margin:BANKUSDC"), result
    assert calls == [
        ("sell", "BANKUSDC", False, ["momentum-BANKUSDC"]),
        ("buy", "ALLUSDC", False, []),
    ]



def test_fetch_decision_reads_structured_central_decision_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.load_settings", lambda: SimpleNamespace(signalmaker_base_url="https://central.test", quote_assets=["USDC"]))
    calls = []

    class FakeResponse:
        status_code = 200
        ok = True
        headers = {"content-type": "application/json"}
        text = "{}"

        def __init__(self, url):
            self.url = url

        def json(self):
            return {
                "decision_action": "BUY",
                "symbol": "ALLUSDC",
                "target_symbol": "ALLUSDC",
                "status": "ready",
                "reason": "central_momentum_signal",
                "order_ids": ["order-1"],
                "fill_ids": ["fill-1"],
            }

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(url)

    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.requests.get", fake_get)

    decision = momentum_module.fetch_decision()

    assert [call["url"] for call in calls] == ["https://central.test/api/v1/momentum-engine/decision"]
    assert decision["decision_action"] == "BUY"
    assert decision["action"] == "BUY"
    assert decision["symbol"] == "ALLUSDC"
    assert decision["target_symbol"] == "ALLUSDC"
    assert decision["status"] == "ready"
    assert decision["reason"] == "central_momentum_signal"
    assert decision["order_ids"] == ["order-1"]
    assert decision["fill_ids"] == ["fill-1"]


def test_fetch_decision_can_display_central_run_once_result(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_METHOD", "POST")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.load_settings", lambda: SimpleNamespace(signalmaker_base_url="https://central.test", quote_assets=["USDC"]))
    calls = []

    class FakeResponse:
        status_code = 200
        ok = True
        headers = {"content-type": "application/json"}
        text = "{}"

        def __init__(self, url):
            self.url = url

        def json(self):
            return {"decision": {"decision_action": "HOLD", "symbol": "ALLUSDC", "target_symbol": "ALLUSDC"}, "result": {"status": "completed", "reason": "already_positioned", "order_ids": [], "fill_ids": []}}

    def fake_post(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(url)

    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.requests.post", fake_post)

    decision = momentum_module.fetch_decision()

    assert [call["url"] for call in calls] == ["https://central.test/api/v1/executor/momentum/run-once"]
    assert decision["decision_action"] == "HOLD"
    assert decision["symbol"] == "ALLUSDC"
    assert decision["target_symbol"] == "ALLUSDC"
    assert decision["status"] == "completed"
    assert decision["reason"] == "already_positioned"



def test_execute_decision_accepts_decision_action_only_buy(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    monkeypatch.setattr(momentum_module, "StateStore", StateStore)
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[20.0]))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())
    calls = []

    def fake_buy(settings, kraken, rules, state, decision, *, exclude=None):
        calls.append({"action": decision["action"], "decision_action": decision["decision_action"], "exclude": exclude})
        return "bought:ALLUSDC"

    monkeypatch.setattr(momentum_module, "_buy_with_momentum_cadence", fake_buy)

    decision = {"decision_action": "BUY", "target_symbol": "ALLUSDC", "should_trade": True}

    assert momentum_module.execute_decision(decision) == "bought:ALLUSDC"
    assert calls == [{"action": "BUY", "decision_action": "BUY", "exclude": None}]


def test_execute_decision_accepts_action_only_buy(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    monkeypatch.setattr(momentum_module, "StateStore", StateStore)
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[20.0]))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())
    calls = []

    def fake_buy(settings, kraken, rules, state, decision, *, exclude=None):
        calls.append({"action": decision["action"], "decision_action": decision["decision_action"], "exclude": exclude})
        return "bought:ALLUSDC"

    monkeypatch.setattr(momentum_module, "_buy_with_momentum_cadence", fake_buy)

    decision = {"action": "BUY", "target_symbol": "ALLUSDC", "should_trade": True}

    assert momentum_module.execute_decision(decision) == "bought:ALLUSDC"
    assert calls == [{"action": "BUY", "decision_action": "BUY", "exclude": None}]


def test_execute_decision_rejects_conflicting_action_fields(monkeypatch):
    monkeypatch.setattr(momentum_module, "load_settings", lambda: (_ for _ in ()).throw(AssertionError("load_settings should not be called")))

    decision = {"action": "WAIT", "decision_action": "BUY", "target_symbol": "ALLUSDC", "should_trade": True}

    assert momentum_module.execute_decision(decision) == "unsupported_action_conflict:action=WAIT:decision_action=BUY"


def test_execute_decision_accepts_matching_wait_action_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    monkeypatch.setattr(momentum_module, "StateStore", StateStore)
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[20.0]))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())

    decision = {"action": "WAIT", "decision_action": "WAIT", "reason": "no_signal", "should_trade": False}

    assert momentum_module.execute_decision(decision) == "wait:no_signal"
    assert decision["action"] == "WAIT"
    assert decision["decision_action"] == "WAIT"

def test_execute_buy_decision_rotates_when_different_momentum_asset_is_held(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "quantity": "10", "entry_price": 1.2})
    calls = []

    monkeypatch.setattr(momentum_module, "StateStore", lambda: state)
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[0.0], base_balance=0.0))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())

    def fake_sell(kraken, rules, store, symbol, decision, *, require_confirmed: bool = True):
        calls.append(("sell", symbol))
        return "sell_confirmed:BANKUSDC:remaining_value=0.0000:quote=25.0000"

    def fake_buy(settings_arg, kraken, rules, store, decision, *, exclude=None):
        calls.append(("buy", decision.get("buy_symbol"), decision["action"], sorted(exclude or []), list(store.open_positions())))
        return "bought:ALLUSDC"

    monkeypatch.setattr(momentum_module, "sell_symbol", fake_sell)
    monkeypatch.setattr(momentum_module, "_buy_with_momentum_cadence", fake_buy)

    result = momentum_module.execute_decision({"action": "BUY", "should_trade": True, "symbol": "ALLUSDC", "buy_symbol": "ALLUSDC"})

    assert result == "bought:ALLUSDC"
    assert calls == [("buy", "ALLUSDC", "BUY", [], ["momentum-BANKUSDC"])]


def test_execute_hold_without_position_buys_target_asset(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_CADENCE_HOURS", "0")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    state = StateStore()
    calls = []

    monkeypatch.setattr(momentum_module, "StateStore", lambda: state)
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[20.0], base_balance=0.0))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())

    def fake_buy(settings_arg, kraken, rules, store, decision, *, exclude=None):
        calls.append((decision["action"], decision["buy_symbol"], decision["symbol"], decision["target_symbol"]))
        return "bought:ALLUSDC:qty=10.00000000:notional=10.0000"

    monkeypatch.setattr(momentum_module, "_buy_with_momentum_cadence", fake_buy)

    result = momentum_module.execute_decision({"action": "HOLD", "should_trade": False, "target_asset": {"symbol": "ALLUSDC"}})

    assert result == "hold_without_position:ALLUSDC"
    assert calls == []
    assert [event["event_type"] for event in state.events()] == []


def test_execute_wait_with_target_differs_from_held_momentum_position_stays_passive(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "quantity": "10", "entry_price": 1.2})
    calls = []

    monkeypatch.setattr(momentum_module, "StateStore", lambda: state)
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[0.0], base_balance=0.0))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())

    def fake_sell(kraken, rules, store, symbol, decision, *, require_confirmed: bool = True):
        calls.append(("sell", symbol, decision["action"], decision["buy_symbol"]))
        store.close_position("momentum-BANKUSDC", "momentum_sell", {}, record_event=False)
        return "sell_confirmed:BANKUSDC:remaining_value=0.0000:quote=25.0000"

    def fake_buy(settings_arg, kraken, rules, store, decision, *, exclude=None):
        calls.append(("buy", decision["buy_symbol"], decision["action"], sorted(exclude or [])))
        return "bought:ALLUSDC:qty=10.00000000:notional=10.0000"

    monkeypatch.setattr(momentum_module, "sell_symbol", fake_sell)
    monkeypatch.setattr(momentum_module, "buy_best_available", fake_buy)

    result = momentum_module.execute_decision({"action": "WAIT", "should_trade": False, "target_symbol": "ALLUSDC"})

    assert result == "wait:WAIT"
    assert calls == []


def test_execute_wait_holds_existing_momentum_target(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    state = StateStore()
    state.add_open_position("momentum-ALLUSDC", {"candidate_id": "momentum-ALLUSDC", "execution_symbol": "ALLUSDC", "signal_symbol": "ALLUSDC", "side": "long", "quantity": "10", "entry_price": 1.0})

    monkeypatch.setattr(momentum_module, "StateStore", lambda: state)
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[20.0], base_balance=10.0))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())

    result = momentum_module.execute_decision({"action": "WAIT", "should_trade": False, "target_symbol": "ALLUSDC"})

    assert result == "wait:WAIT"


def test_confirmed_recorded_buy_turns_new_buy_into_sell_then_buy_rotation(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()
    state.add_event("momentum-BANKUSDC", "momentum_bought", {"symbol": "BANKUSDC"})

    decision = momentum_module.apply_previous_buy_rotation(
        {"action": "BUY", "should_trade": True, "symbol": "ALLUSDC", "buy_symbol": "ALLUSDC"},
        state,
    )

    assert decision["action"] == "ROTATE"
    assert decision["sell_symbol"] == "BANKUSDC"
    assert decision["buy_symbol"] == "ALLUSDC"
    assert decision["order_sequence"] == [
        {"step": 1, "action": "SELL", "symbol": "BANKUSDC", "role": "exit_held_momentum_asset"},
        {"step": 2, "action": "BUY", "symbol": "ALLUSDC", "role": "enter_new_momentum_asset"},
    ]
    assert decision["executor_contract"]["order_sequence"] == decision["order_sequence"]


def test_unconfirmed_recorded_buy_does_not_create_fake_hold_or_rotation(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()
    momentum_module.record_decision(
        {"action": "BUY", "should_trade": True, "symbol": "JUPUSDC", "buy_symbol": "JUPUSDC"},
        execution_result="quote_balance_wait:USDC:free=0.0000:usable=0.0000",
    )

    decision = momentum_module.apply_previous_buy_rotation(
        {"action": "BUY", "should_trade": True, "symbol": "ALLUSDC", "buy_symbol": "ALLUSDC"},
        state,
    )

    assert decision["action"] == "BUY"
    assert decision["sell_symbol"] is None
    assert decision["buy_symbol"] == "ALLUSDC"


def test_sell_records_single_realized_sell_event(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.time.sleep", lambda _: None)
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "quantity": "10", "entry_price": 1.2})
    kraken = FakeKraken(quote_balances=[0.0], base_balance=10.0)

    result = sell_symbol(kraken, FakeRules(), state, "BANKUSDC", {"action": "ROTATE"})

    assert result.startswith("sell_confirmed:BANKUSDC"), result
    event_types = [event["event_type"] for event in state.events()]
    assert event_types == ["position_opened", "momentum_sell_attempt", "momentum_sold"]


def test_rotation_sell_then_buy_uses_quote_balance_after_sale(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.time.sleep", lambda _: None)
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "quantity": "10", "entry_price": 1.2})
    kraken = FakeKraken(quote_balances=[0.0], base_balance=10.0)

    sell_result = sell_symbol(kraken, FakeRules(), state, "BANKUSDC", {"action": "ROTATE"})
    buy_result = buy_symbol(settings(), kraken, FakeRules(), state, "ALLUSDC", {"action": "ROTATE"})

    assert sell_result.startswith("sell_confirmed:BANKUSDC"), sell_result
    assert buy_result == "bought:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert [order["symbol"] for order in kraken.orders] == ["BANKUSDC", "ALLUSDC"]
    assert kraken.orders[1]["executedQty"] == "10.00000000"


def test_buy_best_available_uses_only_central_target_without_ranking_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")

    state = StateStore()

    result = buy_best_available(settings(), FakeKraken(quote_balances=[20.0]), FakeRules(), state, {"decision_action": "BUY", "target_symbol": "ALLUSDC"})

    assert result == "bought:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert "momentum-ALLUSDC" in state.open_positions()


def test_buy_best_available_skips_missing_central_target(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()

    result = buy_best_available(settings(), FakeKraken(quote_balances=[20.0]), FakeRules(), state, {"decision_action": "BUY"})

    assert result == "buy_skipped_missing_target"
    assert state.open_positions() == {}


def test_buy_symbol_skips_unsupported_quote_asset(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()

    result = buy_symbol(settings(), FakeKraken(quote_balances=[20.0]), FakeRules(), state, "BADUSDT", {"action": "BUY"})

    assert result == "unsupported_quote:BADUSDT:configured=USDC"
    assert [event["event_type"] for event in state.events()] == ["momentum_buy_skipped_unsupported_quote"]


def test_execute_decision_waits_for_cadence_after_quote_balance_skip(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_CADENCE_HOURS", "4")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setenv("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", "1")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], kraken_base_url="https://kraken.test", kraken_api_key="key", kraken_secret_key="secret", dry_run=False))
    balances = iter([0.0, 20.0])
    monkeypatch.setattr(momentum_module, "KrakenClient", lambda *args, **kwargs: FakeKraken(quote_balances=[next(balances)]))
    monkeypatch.setattr(momentum_module, "KrakenSymbolRules", lambda *args, **kwargs: FakeRules())

    decision = {"action": "BUY", "should_trade": True, "buy_symbol": "ALLUSDC", "symbol": "ALLUSDC"}

    first = momentum_module.execute_decision(dict(decision))
    second = momentum_module.execute_decision(dict(decision))

    assert first == "quote_balance_wait:USDC:free=0.0000:usable=0.0000"
    assert second.startswith("wait:momentum_cadence:next_check_at="), second
    assert [event["event_type"] for event in StateStore().events()] == [
        "momentum_decision_cadence_checked",
        "momentum_buy_skipped_quote_balance",
        "momentum_decision_cadence_wait",
    ]


def test_momentum_buy_attempts_margin_5_then_3_before_spot_fallback_when_margin_enabled(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_USE_MARGIN", "true")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setattr(momentum_module, "margin_dry_run", lambda: False)

    attempts = []

    class FakeMargin:
        dry_run = False

        def __init__(self, kraken, *, dry_run: bool, leverage=None) -> None:
            self.leverage_value = leverage
            self.dry_run = dry_run

        def leverage(self):
            return str(self.leverage_value)

        def ensure_margin_account(self, symbol: str) -> dict:
            return {"status": "margin"}

        def margin_free_balance(self, symbol: str, asset: str) -> float:
            return 10.0

        def max_borrowable(self, symbol: str, asset: str) -> float:
            return 0.0

        def margin_order(self, symbol: str, side: str, quantity: str, order_type: str = "MARKET", **kwargs) -> dict:
            attempts.append(self.leverage_value)
            raise RuntimeError(f"pair_not_margin_enabled_leverage_{self.leverage_value}")

    monkeypatch.setattr(momentum_module, "MarginClient", FakeMargin)
    state = StateStore()
    kraken = FakeKraken(quote_balances=[10.0, 10.0])
    cfg = SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], exchange="kraken")

    result = buy_symbol(cfg, kraken, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert attempts == [5, 4, 3, 2]
    assert result == "bought:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert kraken.orders[0]["executedQty"] == "10.00000000"
    assert [event["event_type"] for event in state.events()] == [
        "momentum_buy_margin_fallback_spot",
        "position_opened",
        "momentum_bought",
    ]


def _stub_execute_decision_runtime(monkeypatch, held_symbol: str | None = None):
    calls: list[tuple[str, str | None]] = []
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], exchange="kraken"))
    monkeypatch.setattr(momentum_module, "create_spot_exchange", lambda cfg: (FakeKraken(quote_balances=[25.0]), FakeRules()))
    monkeypatch.setattr(momentum_module, "StateStore", lambda: SimpleNamespace(add_event=lambda *args, **kwargs: None))
    held_position = {"execution_symbol": held_symbol, "signal_symbol": held_symbol} if held_symbol else None
    monkeypatch.setattr(momentum_module, "current_momentum_position", lambda state: held_position)

    def fake_buy_with_cadence(cfg, kraken, rules, state, decision, *, exclude=None):
        calls.append(("buy", decision.get("buy_symbol") or decision.get("target_symbol") or decision.get("symbol")))
        return f"bought:{decision.get('buy_symbol') or decision.get('target_symbol') or decision.get('symbol')}"

    def fake_rotate(cfg, kraken, rules, state, current_symbol, buy_symbol, decision):
        calls.append(("rotate", f"{current_symbol}->{buy_symbol}"))
        return f"rotate:{current_symbol}->{buy_symbol}"

    def fake_sell(kraken, rules, state, symbol, decision, *, require_confirmed=True):
        calls.append(("sell", symbol))
        return f"sell_confirmed:{symbol}"

    def fake_buy_best(cfg, kraken, rules, state, decision, *, exclude=None):
        symbol = decision.get("buy_symbol") or decision.get("target_symbol") or decision.get("symbol")
        calls.append(("buy_best", symbol))
        return f"bought:{symbol}"

    monkeypatch.setattr(momentum_module, "_buy_with_momentum_cadence", fake_buy_with_cadence)
    monkeypatch.setattr(momentum_module, "_rotate_momentum_position", fake_rotate)
    monkeypatch.setattr(momentum_module, "sell_symbol", fake_sell)
    monkeypatch.setattr(momentum_module, "buy_best_available", fake_buy_best)
    return calls


def test_execute_decision_wait_without_held_symbol_never_orders(monkeypatch):
    calls = _stub_execute_decision_runtime(monkeypatch, held_symbol=None)

    result = momentum_module.execute_decision({"action": "WAIT", "should_trade": False, "target_symbol": "OMGUSD"})

    assert calls == []
    assert result.startswith("wait:"), result


def test_execute_decision_hold_without_held_symbol_buys_target(monkeypatch):
    calls = _stub_execute_decision_runtime(monkeypatch, held_symbol=None)

    result = momentum_module.execute_decision({"action": "HOLD", "should_trade": False, "target_symbol": "OMGUSD"})

    assert calls == []
    assert result == "hold_without_position:OMGUSD"


def test_execute_decision_buy_without_held_symbol_allows_buy(monkeypatch):
    calls = _stub_execute_decision_runtime(monkeypatch, held_symbol=None)

    result = momentum_module.execute_decision({"action": "BUY", "should_trade": True, "target_symbol": "OMGUSD"})

    assert calls == [("buy", "OMGUSD")]
    assert result == "bought:OMGUSD"


def test_execute_decision_rotate_different_held_symbol_sells_then_buys(monkeypatch):
    calls = _stub_execute_decision_runtime(monkeypatch, held_symbol="ALLUSD")

    result = momentum_module.execute_decision({"action": "ROTATE", "should_trade": True, "sell_symbol": "ALLUSD", "buy_symbol": "OMGUSD", "target_symbol": "OMGUSD"})

    assert calls == [("sell", "ALLUSD"), ("buy", "OMGUSD")]
    assert result == "rotate:sell_confirmed:ALLUSD:bought:OMGUSD"


def test_execute_decision_sell_with_held_symbol_sells_only(monkeypatch):
    calls = _stub_execute_decision_runtime(monkeypatch, held_symbol="ALLUSD")

    result = momentum_module.execute_decision({"action": "SELL", "should_trade": True, "sell_symbol": "ALLUSD", "symbol": "ALLUSD"})

    assert calls == [("sell", "ALLUSD")]
    assert result == "sell_confirmed:ALLUSD"


def test_execute_decision_hold_existing_momentum_position(monkeypatch):
    calls = _stub_execute_decision_runtime(monkeypatch, held_symbol="OMGUSD")

    result = momentum_module.execute_decision({"action": "HOLD", "should_trade": False, "target_symbol": "OMGUSD"})

    assert calls == []
    assert result == "hold_existing_momentum_position:OMGUSD"


def test_execute_decision_sell_blocked_when_target_is_not_held(monkeypatch):
    calls = _stub_execute_decision_runtime(monkeypatch, held_symbol="ALLUSD")

    result = momentum_module.execute_decision({"action": "SELL", "should_trade": True, "sell_symbol": "OMGUSD", "symbol": "OMGUSD"})

    assert calls == []
    assert result == "sell_blocked:decision_not_on_held_momentum_asset:held=ALLUSD:sell=OMGUSD"


def test_execute_decision_unsupported_action(monkeypatch):
    calls = _stub_execute_decision_runtime(monkeypatch, held_symbol=None)

    result = momentum_module.execute_decision({"action": "CANCEL", "should_trade": True, "target_symbol": "OMGUSD"})

    assert calls == []
    assert result == "unsupported_action:CANCEL"
