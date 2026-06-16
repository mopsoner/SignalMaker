from __future__ import annotations

from types import SimpleNamespace

import raspberry_executor.sqlite_db as sqlite_db
import raspberry_executor.momentum_decision_feed as momentum_module
from raspberry_executor.momentum_decision_feed import buy_best_available, buy_symbol, sell_symbol
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


def test_buy_symbol_keeps_confirmed_quote_when_next_account_read_is_stale(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.time.sleep", lambda _: None)
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setenv("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", "3")
    state = StateStore()
    binance = FakeBinance(quote_balances=[0.0, 12.0, 0.0])

    result = buy_symbol(settings(), binance, FakeRules(), state, "ALLUSDC", {"action": "ROTATE"})

    assert result == "bought:ALLUSDC:qty=12.00000000:notional=12.0000"
    assert [event["event_type"] for event in state.events()] == ["position_opened", "momentum_bought"]


def test_buy_symbol_uses_full_available_quote_balance(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    state = StateStore()
    binance = FakeBinance(quote_balances=[35.0])

    result = buy_symbol(settings(), binance, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought:ALLUSDC:qty=35.00000000:notional=35.0000"
    assert binance.orders[0]["executedQty"] == "35.00000000"
    assert state.open_positions()["momentum-ALLUSDC"]["notional_used"] == 35.0


def test_buy_symbol_can_keep_fixed_order_quote_when_full_quote_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_WITH_FULL_QUOTE", "false")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    state = StateStore()
    binance = FakeBinance(quote_balances=[35.0])

    result = buy_symbol(settings(), binance, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert binance.orders[0]["executedQty"] == "10.00000000"


def test_buy_symbol_uses_cross_margin_when_requested(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_USE_CROSS_MARGIN", "true")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setattr(momentum_module, "margin_dry_run", lambda: False)
    monkeypatch.setattr(momentum_module, "margin_multiplier", lambda: 1.0)

    class FakeMargin:
        dry_run = False
        isolated = False

        def __init__(self, binance, *, isolated: bool, dry_run: bool) -> None:
            self.binance = binance
            self.isolated = isolated
            self.dry_run = dry_run
            self.orders = []

        def ensure_isolated_account(self, symbol: str) -> dict:
            return {"status": "cross_margin"}

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
    binance = FakeBinance(quote_balances=[35.0])

    result = buy_symbol(settings(), binance, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought_cross_margin:ALLUSDC:qty=10.00000000:notional=10.0000"
    assert binance.orders == []
    assert instances[0].orders[0]["type"] == "MARKET"
    position = state.open_positions()["momentum-ALLUSDC"]
    assert position["mode"] == "cross_margin"
    assert position["margin_isolated"] is False
    assert position["entry_payload"]["isIsolated"] == "FALSE"
    assert position["notional_used"] == 10.0


def test_cross_margin_buy_uses_available_quote_when_less_than_order_quote(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_USE_CROSS_MARGIN", "true")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setattr(momentum_module, "margin_dry_run", lambda: False)
    monkeypatch.setattr(momentum_module, "margin_multiplier", lambda: 1.0)

    class FakeMargin:
        dry_run = False
        isolated = False

        def __init__(self, binance, *, isolated: bool, dry_run: bool) -> None:
            self.binance = binance
            self.isolated = isolated
            self.dry_run = dry_run
            self.orders = []

        def ensure_isolated_account(self, symbol: str) -> dict:
            return {"status": "cross_margin"}

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
    binance = FakeBinance(quote_balances=[7.0])

    result = buy_symbol(settings(), binance, FakeRules(), state, "ALLUSDC", {"action": "BUY"})

    assert result == "bought_cross_margin:ALLUSDC:qty=7.00000000:notional=7.0000"
    assert instances[0].orders[0]["quantity"] == "7.00000000"
    assert state.open_positions()["momentum-ALLUSDC"]["notional_used"] == 7.0


def test_sell_symbol_uses_cross_margin_for_cross_margin_position(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setattr(momentum_module, "load_settings", settings)
    monkeypatch.setattr(momentum_module, "margin_dry_run", lambda: False)

    class FakeMargin:
        dry_run = False
        isolated = False

        def __init__(self, binance, *, isolated: bool, dry_run: bool) -> None:
            self.binance = binance
            self.isolated = isolated
            self.dry_run = dry_run
            self.base_balance = 10.0
            self.quote_balance = 0.0
            self.orders = []

        def ensure_isolated_account(self, symbol: str) -> dict:
            return {"status": "cross_margin"}

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
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "mode": "cross_margin", "margin_isolated": False, "quantity": "10", "entry_price": 1.2})
    binance = FakeBinance(quote_balances=[0.0], base_balance=0.0)

    result = sell_symbol(binance, FakeRules(), state, "BANKUSDC", {"action": "ROTATE"})

    assert result.startswith("sell_confirmed_cross_margin:BANKUSDC"), result
    assert binance.orders == []
    assert instances[0].orders[0]["side"] == "SELL"
    assert state.open_positions() == {}
    sold_event = state.events()[-1]
    assert sold_event["event_type"] == "momentum_sold"
    assert sold_event["payload"]["mode"] == "cross_margin"


def test_rotate_sells_cross_margin_position_before_forced_cross_margin_buy(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], binance_base_url="https://binance.test", binance_api_key="key", binance_secret_key="secret", dry_run=False))
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "mode": "cross_margin", "margin_isolated": False, "quantity": "10", "entry_price": 1.2})
    calls = []

    monkeypatch.setattr(momentum_module, "StateStore", lambda: state)
    monkeypatch.setattr(momentum_module, "BinanceClient", lambda *args, **kwargs: FakeBinance(quote_balances=[0.0], base_balance=0.0))
    monkeypatch.setattr(momentum_module, "BinanceSymbolRules", lambda *args, **kwargs: FakeRules())

    def fake_sell(binance, rules, store, symbol, decision, *, require_confirmed: bool = True):
        calls.append(("sell", symbol, bool(decision.get("force_cross_margin")), list(store.open_positions())))
        store.close_position("momentum-BANKUSDC", "momentum_sell", {}, record_event=False)
        return "sell_confirmed_cross_margin:BANKUSDC:remaining_value=0.0000:quote=25.0000"

    def fake_buy(settings_arg, binance, rules, store, decision, *, exclude=None):
        calls.append(("buy", decision.get("buy_symbol"), bool(decision.get("force_cross_margin")), list(store.open_positions())))
        return "fallback_buy:ALLUSDC:bought_cross_margin:ALLUSDC:qty=25.00000000:notional=25.0000"

    monkeypatch.setattr(momentum_module, "sell_symbol", fake_sell)
    monkeypatch.setattr(momentum_module, "buy_best_available", fake_buy)

    result = momentum_module.execute_decision({"action": "ROTATE", "should_trade": True, "symbol": "BANKUSDC", "sell_symbol": "BANKUSDC", "buy_symbol": "ALLUSDC"})

    assert result.startswith("rotate:sell_confirmed_cross_margin:BANKUSDC"), result
    assert calls == [
        ("sell", "BANKUSDC", True, ["momentum-BANKUSDC"]),
        ("buy", "ALLUSDC", True, []),
    ]



def test_build_decision_rotate_includes_sell_before_buy_order_sequence(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()
    state.add_open_position("momentum-ALLUSDC", {"candidate_id": "momentum-ALLUSDC", "execution_symbol": "ALLUSDC", "signal_symbol": "ALLUSDC", "side": "long", "quantity": "10", "entry_price": 1.0, "strategy": "momentum_rotation"})

    from raspberry_executor.momentum_decision_feed import build_decision_from_candidates

    decision = build_decision_from_candidates([{"symbol": "BANKUSDC", "rank": 1, "momentum_score": 12.5, "rsi_1h": 50}])

    assert decision["action"] == "ROTATE"
    assert decision["order_sequence"] == [
        {"step": 1, "action": "SELL", "symbol": "ALLUSDC", "role": "exit_held_momentum_asset"},
        {"step": 2, "action": "BUY", "symbol": "BANKUSDC", "role": "enter_new_momentum_asset"},
    ]
    assert decision["executor_contract"]["order_sequence"] == decision["order_sequence"]


def test_execute_buy_decision_rotates_when_different_momentum_asset_is_held(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], binance_base_url="https://binance.test", binance_api_key="key", binance_secret_key="secret", dry_run=False))
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "quantity": "10", "entry_price": 1.2})
    calls = []

    monkeypatch.setattr(momentum_module, "StateStore", lambda: state)
    monkeypatch.setattr(momentum_module, "BinanceClient", lambda *args, **kwargs: FakeBinance(quote_balances=[0.0], base_balance=0.0))
    monkeypatch.setattr(momentum_module, "BinanceSymbolRules", lambda *args, **kwargs: FakeRules())

    def fake_sell(binance, rules, store, symbol, decision, *, require_confirmed: bool = True):
        calls.append(("sell", symbol, decision["action"], decision["order_sequence"], list(store.open_positions())))
        store.close_position("momentum-BANKUSDC", "momentum_sell", {}, record_event=False)
        return "sell_confirmed:BANKUSDC:remaining_value=0.0000:quote=25.0000"

    def fake_buy(settings_arg, binance, rules, store, decision, *, exclude=None):
        calls.append(("buy", decision.get("buy_symbol"), decision["action"], decision["order_sequence"], list(store.open_positions())))
        return "fallback_buy:ALLUSDC:bought:ALLUSDC:qty=25.00000000:notional=25.0000"

    monkeypatch.setattr(momentum_module, "sell_symbol", fake_sell)
    monkeypatch.setattr(momentum_module, "buy_best_available", fake_buy)

    result = momentum_module.execute_decision({"action": "BUY", "should_trade": True, "symbol": "ALLUSDC", "buy_symbol": "ALLUSDC"})

    expected_sequence = [
        {"step": 1, "action": "SELL", "symbol": "BANKUSDC", "role": "exit_held_momentum_asset"},
        {"step": 2, "action": "BUY", "symbol": "ALLUSDC", "role": "enter_new_momentum_asset"},
    ]
    assert result.startswith("rotate:sell_confirmed:BANKUSDC"), result
    assert calls == [
        ("sell", "BANKUSDC", "ROTATE", expected_sequence, ["momentum-BANKUSDC"]),
        ("buy", "ALLUSDC", "ROTATE", expected_sequence, []),
    ]


def test_previous_recorded_buy_turns_new_buy_into_sell_then_buy_rotation(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()
    momentum_module.record_decision({"action": "BUY", "should_trade": True, "symbol": "BANKUSDC", "buy_symbol": "BANKUSDC"})

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


def test_rotation_sell_then_buy_uses_quote_balance_after_sale(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.time.sleep", lambda _: None)
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "quantity": "10", "entry_price": 1.2})
    binance = FakeBinance(quote_balances=[0.0], base_balance=10.0)

    sell_result = sell_symbol(binance, FakeRules(), state, "BANKUSDC", {"action": "ROTATE"})
    buy_result = buy_symbol(settings(), binance, FakeRules(), state, "ALLUSDC", {"action": "ROTATE"})

    assert sell_result.startswith("sell_confirmed:BANKUSDC"), sell_result
    assert buy_result == "bought:ALLUSDC:qty=25.00000000:notional=25.0000"
    assert [order["symbol"] for order in binance.orders] == ["BANKUSDC", "ALLUSDC"]
    assert binance.orders[1]["executedQty"] == "25.00000000"


def test_build_decision_from_candidates_buys_top_supported_symbol(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")

    from raspberry_executor.momentum_decision_feed import build_decision_from_candidates

    decision = build_decision_from_candidates([
        {"symbol": "BANKUSDC", "rank": 1, "momentum_score": 12.5, "rsi_1h": 50},
        {"symbol": "ALLUSDC", "rank": 2, "momentum_score": 8.0, "rsi_1h": 50},
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

    decision = build_decision_from_candidates([{"symbol": "BANKUSDC", "rank": 1, "momentum_score": 12.5, "rsi_1h": 50}])

    assert decision["action"] == "ROTATE"
    assert decision["should_trade"] is True
    assert decision["sell_symbol"] == "ALLUSDC"
    assert decision["buy_symbol"] == "BANKUSDC"



def test_build_decision_buys_best_rsi_buyable_candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")

    from raspberry_executor.momentum_decision_feed import build_decision_from_candidates

    decision = build_decision_from_candidates([
        {"symbol": "BANKUSDC", "rank": 1, "momentum_score": 20.0, "rsi_1h": 61},
        {"symbol": "ALLUSDC", "rank": 2, "momentum_score": 12.5, "rsi_1h": 50},
    ])

    assert decision["action"] == "BUY"
    assert decision["buy_symbol"] == "ALLUSDC"
    assert decision["buy_candidates"][0]["symbol"] == "ALLUSDC"
    assert decision["skipped_candidates"][0]["reason"].startswith("rsi_1h_out_of_range")


def test_build_decision_holds_when_held_rank_beats_buyable_candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    state = StateStore()
    state.add_open_position("momentum-BANKUSDC", {"candidate_id": "momentum-BANKUSDC", "execution_symbol": "BANKUSDC", "signal_symbol": "BANKUSDC", "side": "long", "quantity": "10", "entry_price": 1.0, "strategy": "momentum_rotation"})

    from raspberry_executor.momentum_decision_feed import build_decision_from_candidates

    decision = build_decision_from_candidates([
        {"symbol": "BANKUSDC", "rank": 1, "momentum_score": 20.0, "rsi_1h": 61},
        {"symbol": "ALLUSDC", "rank": 2, "momentum_score": 12.5, "rsi_1h": 50},
    ])

    assert decision["action"] == "HOLD"
    assert decision["should_trade"] is False
    assert decision["buy_symbol"] == "ALLUSDC"
    assert "held_rank=1" in decision["reason"]


def test_buy_best_available_tries_second_buyable_after_first_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.fetch_momentum_candidates", lambda limit: [
        {"symbol": "BANKUSDC", "rank": 1, "momentum_score": 20.0, "rsi_1h": 50},
        {"symbol": "ALLUSDC", "rank": 2, "momentum_score": 12.5, "rsi_1h": 50},
    ])
    state = StateStore()

    class FirstBuyFailsBinance(FakeBinance):
        def place_market_entry(self, symbol: str, side: str, quantity: str) -> dict:
            if side == "long" and symbol == "BANKUSDC":
                raise RuntimeError("first candidate rejected")
            return super().place_market_entry(symbol, side, quantity)

    result = buy_best_available(settings(), FirstBuyFailsBinance(quote_balances=[20.0]), FakeRules(), state, {"action": "BUY"})

    assert result.startswith("fallback_buy:ALLUSDC:bought:ALLUSDC"), result
    event_types = [event["event_type"] for event in state.events()]
    assert "momentum_fallback_buy_failed" in event_types
    assert "momentum_bought" in event_types

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
            {"symbol": "BADUSDT", "rank": 1, "momentum_score": 99.0, "rsi_1h": 50},
            {"symbol": "BANKUSDC", "rank": 2, "momentum_score": 12.5, "rsi_1h": 50},
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
        return FakeResponse([{"symbol": "BANKUSDC", "rank": 1, "momentum_score": 12.5, "rsi_1h": 50}], 200, url)

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


def test_build_decision_falls_back_to_lowest_received_rsi_when_none_buyable(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")

    from raspberry_executor.momentum_decision_feed import build_decision_from_candidates

    decision = build_decision_from_candidates([
        {"symbol": "BANKUSDC", "rank": 1, "momentum_score": 20.0, "rsi_1h": 70},
        {"symbol": "ALLUSDC", "rank": 2, "momentum_score": 12.5, "rsi_1h": 38},
        {"symbol": "LOWUSDC", "rank": 3, "momentum_score": 9.0, "rsi_1h": 34},
    ])

    assert decision["action"] == "BUY"
    assert decision["should_trade"] is True
    assert decision["buy_symbol"] == "LOWUSDC"
    assert decision["buy_candidates"][0]["buyable_reason"] == "fallback_lowest_rsi_1h:34"
    assert decision["reason"].startswith("buy_lowest_rsi_momentum_fallback:LOWUSDC")


def test_buy_best_available_falls_back_to_lowest_received_rsi_when_none_buyable(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setattr("raspberry_executor.momentum_decision_feed.fetch_momentum_candidates", lambda limit: [
        {"symbol": "BANKUSDC", "rank": 1, "momentum_score": 20.0, "rsi_1h": 70},
        {"symbol": "ALLUSDC", "rank": 2, "momentum_score": 12.5, "rsi_1h": 38},
        {"symbol": "LOWUSDC", "rank": 3, "momentum_score": 9.0, "rsi_1h": 34},
    ])
    state = StateStore()

    result = buy_best_available(settings(), FakeBinance(quote_balances=[20.0]), FakeRules(), state, {"action": "BUY"})

    assert result.startswith("fallback_buy:LOWUSDC:bought:LOWUSDC"), result
    assert state.open_positions()["momentum-LOWUSDC"]["execution_symbol"] == "LOWUSDC"


def test_execute_decision_waits_for_cadence_after_quote_balance_skip(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    monkeypatch.setenv("MOMENTUM_DECISION_CADENCE_HOURS", "4")
    monkeypatch.setenv("MOMENTUM_DECISION_QUOTE_RESERVE", "0")
    monkeypatch.setenv("MOMENTUM_DECISION_BUY_BALANCE_RATIO", "1")
    monkeypatch.setenv("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", "1")
    monkeypatch.setattr(momentum_module, "load_settings", lambda: SimpleNamespace(order_quote_amount=10.0, quote_assets=["USDC"], binance_base_url="https://binance.test", binance_api_key="key", binance_secret_key="secret", dry_run=False))
    balances = iter([0.0, 20.0])
    monkeypatch.setattr(momentum_module, "BinanceClient", lambda *args, **kwargs: FakeBinance(quote_balances=[next(balances)]))
    monkeypatch.setattr(momentum_module, "BinanceSymbolRules", lambda *args, **kwargs: FakeRules())
    monkeypatch.setattr(momentum_module, "fetch_momentum_candidates", lambda limit: [{"symbol": "ALLUSDC", "rank": 1, "momentum_score": 10, "rsi_1h": 50}])

    decision = {"action": "BUY", "should_trade": True, "buy_symbol": "ALLUSDC", "symbol": "ALLUSDC"}

    first = momentum_module.execute_decision(dict(decision))
    second = momentum_module.execute_decision(dict(decision))

    assert first == "fallback_buy_exhausted:attempts=1"
    assert second.startswith("wait:momentum_cadence:next_check_at="), second
    assert [event["event_type"] for event in StateStore().events()] == [
        "momentum_decision_cadence_checked",
        "momentum_buy_skipped_quote_balance",
        "momentum_fallback_buy_exhausted",
        "momentum_decision_cadence_wait",
    ]
