import os
import time
from typing import Any

import requests

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-momentum-decision")


def _bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/api/v1") and path.startswith("/api/v1"):
        return f"{base}{path[len('/api/v1'):] }"
    return f"{base}{path}"


def fetch_decision() -> dict[str, Any]:
    settings = load_settings()
    params = {
        "cadence_hours": _int_env("MOMENTUM_DECISION_CADENCE_HOURS", 4),
        "starting_capital": _float_env("MOMENTUM_DECISION_STARTING_CAPITAL", 1000.0),
        "min_momentum_score": _float_env("MOMENTUM_DECISION_MIN_SCORE", 0.0),
    }
    response = requests.get(_url(settings.signalmaker_base_url, "/api/v1/momentum/decision"), params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected_momentum_decision_response:{type(data).__name__}")
    return data


def _candidate_id(symbol: str) -> str:
    return f"momentum-{symbol.upper()}"


def _quote_asset(symbol: str, quote_assets: list[str]) -> str | None:
    upper = symbol.upper()
    for quote in sorted([q.upper() for q in quote_assets], key=len, reverse=True):
        if upper.endswith(quote):
            return quote
    return None


def _already_have(binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, symbol: str, min_value: float) -> tuple[bool, str]:
    symbol = symbol.upper()
    if state.has_open_position_for(symbol, "long"):
        return True, "local_position_open"
    if binance.dry_run:
        return False, "dry_run_no_wallet_check"
    base = rules.base_asset(symbol)
    qty = binance.free_balance(base)
    price = binance.current_price(symbol)
    value = qty * price
    if value >= min_value:
        return True, f"wallet_has_{base}:value={value:.4f}"
    return False, f"wallet_value_below_threshold:{base}:value={value:.4f}"


def buy_symbol(settings, binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any]) -> str:
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    min_value = max(5.0, float(settings.order_quote_amount) * 0.5)
    already, reason = _already_have(binance, rules, state, symbol, min_value)
    if already:
        state.add_event(cid, "momentum_buy_skipped_already_have", {"symbol": symbol, "reason": reason, "decision": decision})
        return f"already_have:{reason}"

    quote = _quote_asset(symbol, settings.quote_assets)
    if quote and not binance.dry_run:
        free_quote = binance.free_balance(quote)
        if free_quote < float(settings.order_quote_amount):
            state.add_event(cid, "momentum_buy_skipped_quote_balance", {"symbol": symbol, "quote": quote, "free_quote": free_quote, "decision": decision})
            return f"quote_balance_wait:{quote}:{free_quote}"

    price = binance.current_price(symbol)
    qty = rules.quantity_from_quote(symbol, float(settings.order_quote_amount), price, market=True)
    order = binance.place_market_entry(symbol, "long", qty)
    fill_price = binance.average_fill_price(order, fallback=price) or price
    state.mark_executed(cid)
    state.add_open_position(cid, {
        "candidate_id": cid,
        "signal_symbol": symbol,
        "execution_symbol": symbol,
        "side": "long",
        "quantity": qty,
        "entry_price": float(fill_price),
        "stop_price": None,
        "target_price": None,
        "entry_order_id": order.get("orderId"),
        "momentum_decision": decision,
        "entry_payload": order,
    })
    state.add_event(cid, "momentum_bought", {"symbol": symbol, "quantity": qty, "price": fill_price, "order": order, "decision": decision})
    return f"bought:{symbol}:qty={qty}"


def sell_symbol(binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any]) -> str:
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    base = rules.base_asset(symbol)
    qty = binance.free_balance(base)
    if qty <= 0:
        state.add_event(cid, "momentum_sell_skipped_no_balance", {"symbol": symbol, "base": base, "decision": decision})
        return f"no_balance:{base}"
    market_qty = rules.normalize_market_quantity(symbol, qty)
    order = binance.place_market_entry(symbol, "short", market_qty)
    state.close_position(cid, "momentum_sell", {"symbol": symbol, "base": base, "quantity": market_qty, "order": order, "decision": decision})
    state.add_event(cid, "momentum_sold", {"symbol": symbol, "base": base, "quantity": market_qty, "order": order, "decision": decision})
    return f"sold:{symbol}:qty={market_qty}"


def execute_decision(decision: dict[str, Any]) -> str:
    if not _bool(os.getenv("MOMENTUM_DECISION_EXECUTE_ENABLED"), default=True):
        return "execution_disabled"
    settings = load_settings()
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    rules = BinanceSymbolRules(settings.binance_base_url)
    state = StateStore()
    action = str(decision.get("action") or "WAIT").upper()
    should_trade = bool(decision.get("should_trade"))
    buy = str(decision.get("buy_symbol") or "").upper()
    sell = str(decision.get("sell_symbol") or "").upper()

    if not should_trade or action in {"WAIT", "HOLD"}:
        return f"wait:{action}"
    if action == "BUY" and buy:
        return buy_symbol(settings, binance, rules, state, buy, decision)
    if action == "SELL" and sell:
        return sell_symbol(binance, rules, state, sell, decision)
    if action == "ROTATE":
        sell_result = sell_symbol(binance, rules, state, sell, decision) if sell else "no_sell_symbol"
        buy_result = buy_symbol(settings, binance, rules, state, buy, decision) if buy else "no_buy_symbol"
        return f"rotate:{sell_result}:{buy_result}"
    return f"unsupported_action:{action}"


def record_decision(decision: dict[str, Any], execution_result: str | None = None) -> None:
    action = str(decision.get("action") or "WAIT").upper()
    symbol = str(decision.get("symbol") or decision.get("buy_symbol") or decision.get("sell_symbol") or "momentum")
    StateStore().add_event("momentum-decision", "momentum_decision", {
        "action": action,
        "symbol": symbol,
        "should_trade": bool(decision.get("should_trade")),
        "buy_symbol": decision.get("buy_symbol"),
        "sell_symbol": decision.get("sell_symbol"),
        "execution_result": execution_result,
        "reason": decision.get("reason"),
        "due_now": decision.get("due_now"),
        "next_check_at": decision.get("next_check_at"),
        "decision": decision,
    })


def run_once() -> dict[str, Any]:
    decision = fetch_decision()
    result = execute_decision(decision)
    record_decision(decision, result)
    return {"decision": decision, "execution_result": result}


def run_loop() -> None:
    if not _bool(os.getenv("MOMENTUM_DECISION_ENABLED"), default=True):
        logger.info("momentum decision feed disabled")
        return
    poll_seconds = max(30, _int_env("MOMENTUM_DECISION_POLL_SECONDS", 60))
    logger.info("momentum decision feed started poll_seconds=%s execute=%s", poll_seconds, os.getenv("MOMENTUM_DECISION_EXECUTE_ENABLED", "true"))
    while True:
        try:
            output = run_once()
            decision = output["decision"]
            logger.info("momentum decision action=%s symbol=%s should_trade=%s result=%s", decision.get("action"), decision.get("symbol"), decision.get("should_trade"), output.get("execution_result"))
        except Exception as exc:
            logger.error("momentum decision feed error=%s", str(exc))
            try:
                StateStore().add_event("momentum-decision", "momentum_decision_error", {"error": str(exc)})
            except Exception:
                pass
        time.sleep(poll_seconds)


if __name__ == "__main__":
    print(run_once())
