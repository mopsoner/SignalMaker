import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules
from raspberry_executor.exchange_factory import create_margin_exchange, create_spot_exchange
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import read_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import execution_mode, margin_dry_run, margin_enabled, margin_leverage_attempts, margin_multiplier
from raspberry_executor.state import StateStore
from raspberry_executor.spot_order_manager import SpotOrderManager

logger = setup_logging("raspberry-momentum-decision")

DEFAULT_DECISION_PATH = "/api/v1/momentum-engine/decision"
DEFAULT_DECISION_METHOD = "GET"
DEFAULT_DECISION_LIMIT = 25
DEFAULT_EXECUTOR_RUN_ONCE_PATH = "/api/v1/executor/momentum/run-once"


def _bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is not None:
        return value
    try:
        return read_env().get(name, default)
    except Exception:
        return default


def _int_env(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)) or default)
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)) or default)
    except Exception:
        return default


def _url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    if base.endswith("/api/v1") and path.startswith("/api/v1"):
        return f"{base}{path[len('/api/v1'):] }".strip()
    return f"{base}{path}"


def _decision_path() -> str:
    path = (_env("MOMENTUM_DECISION_PATH", DEFAULT_DECISION_PATH) or DEFAULT_DECISION_PATH).strip()
    if path.rstrip("/") in {"/api/v1/momentum", "/api/v1/momentum/ranking"}:
        return DEFAULT_DECISION_PATH
    return path


def _decision_method() -> str:
    return (_env("MOMENTUM_DECISION_METHOD", DEFAULT_DECISION_METHOD) or DEFAULT_DECISION_METHOD).strip().upper()


def _decision_limit() -> int:
    return _int_env("MOMENTUM_DECISION_LIMIT", DEFAULT_DECISION_LIMIT)


def _executor_run_once_path() -> str:
    return (_env("MOMENTUM_EXECUTOR_RUN_ONCE_PATH", DEFAULT_EXECUTOR_RUN_ONCE_PATH) or DEFAULT_EXECUTOR_RUN_ONCE_PATH).strip()


def _read_json_payload(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(
            "momentum_non_json_response "
            f"status={response.status_code} content_type={response.headers.get('content-type')} "
            f"url={response.url} body={response.text[:500]!r}"
        ) from exc


def _read_json_response(response: requests.Response) -> dict[str, Any]:
    data = _read_json_payload(response)
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected_momentum_decision_response:{type(data).__name__}:url={response.url}")
    return data


def _use_remote_executor_run_once() -> bool:
    return _decision_method() == "POST" or _bool(_env("MOMENTUM_DECISION_USE_REMOTE_RUN_ONCE"), default=False)


def fetch_decision() -> dict[str, Any]:
    """Fetch the central structured momentum decision/result contract.

    Raspberry no longer builds BUY/HOLD/ROTATE locally from momentum rankings.
    Use GET /api/v1/momentum-engine/decision for a decision-only contract, or
    POST /api/v1/executor/momentum/run-once when the central executor should
    return the action/result that the Raspberry displays.
    """
    settings = load_settings()
    if _use_remote_executor_run_once():
        response = requests.post(
            _url(settings.signalmaker_base_url, _executor_run_once_path()),
            timeout=30,
            headers={"accept": "application/json", "cache-control": "no-cache"},
        )
        data = _read_json_response(response)
        if not response.ok:
            raise RuntimeError(f"momentum_run_once_http_error status={response.status_code} url={response.url} payload={data}")
        return normalize_decision(data)

    if _decision_method() != "GET":
        raise RuntimeError(f"unsupported_momentum_decision_method:{_decision_method()}")

    response = requests.get(
        _url(settings.signalmaker_base_url, _decision_path()),
        timeout=30,
        headers={"accept": "application/json", "cache-control": "no-cache"},
    )
    data = _read_json_response(response)
    if not response.ok:
        raise RuntimeError(f"momentum_decision_http_error status={response.status_code} url={response.url} payload={data}")
    return apply_previous_buy_rotation(normalize_decision(data))

def normalize_decision(decision: dict[str, Any]) -> dict[str, Any]:
    payload = dict(decision)
    nested_decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else {}
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    contract = payload.get("executor_contract") if isinstance(payload.get("executor_contract"), dict) else {}

    for source in (nested_decision, result, contract):
        for key in (
            "decision_action",
            "action",
            "raw_action",
            "symbol",
            "target_symbol",
            "buy_symbol",
            "sell_symbol",
            "status",
            "reason",
            "should_trade",
            "order_sequence",
            "order_ids",
            "fill_ids",
            "execution_result",
        ):
            if key in source and payload.get(key) is None:
                payload[key] = source.get(key)

    payload.setdefault("mode", "momentum_rotation")
    payload["decision_action"] = str(payload.get("decision_action") or payload.get("action") or "WAIT").upper()
    payload.setdefault("action", payload["decision_action"])
    payload["action"] = str(payload.get("action") or payload["decision_action"]).upper()
    payload.setdefault("target_symbol", payload.get("buy_symbol") or payload.get("symbol"))
    payload.setdefault("symbol", payload.get("target_symbol") or payload.get("buy_symbol") or payload.get("sell_symbol"))
    payload.setdefault("buy_symbol", payload.get("target_symbol") if payload["action"] in {"BUY", "ROTATE"} else None)
    payload.setdefault("sell_symbol", payload.get("symbol") if payload["action"] == "SELL" else None)
    payload.setdefault("should_trade", payload["action"] in {"BUY", "SELL", "ROTATE"})
    payload.setdefault("status", payload.get("execution_status") or result.get("status") or "decision")
    payload.setdefault("reason", payload.get("recommendation") or payload.get("message") or "")
    payload.setdefault("order_ids", [])
    payload.setdefault("fill_ids", [])
    payload.setdefault("fallback_policy", {})
    return payload


def _event_payload_buy_symbol(payload: dict[str, Any]) -> str:
    decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else {}
    return str(payload.get("buy_symbol") or decision.get("buy_symbol") or payload.get("symbol") or decision.get("symbol") or "").upper()


def _event_payload_sell_symbol(payload: dict[str, Any]) -> str:
    decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else {}
    return str(payload.get("sell_symbol") or decision.get("sell_symbol") or "").upper()


def _last_recorded_buy_without_later_sell(state: StateStore) -> str:
    """Return the last confirmed momentum buy that has not been superseded by a sell.

    A stored ``momentum_decision`` is only an intent plus execution result. The
    dashboard and rotation logic must not treat a BUY/HOLD decision as a held
    asset unless the executor actually recorded a buy. Otherwise a failed or
    skipped BUY can make later details say "HOLD <symbol>" even though the
    asset was never bought, while the previous momentum asset remains open.
    """
    sold_symbols: set[str] = set()
    fallback_decision_buy = ""
    for event in reversed(state.events(limit=1000)):
        event_type = str(event.get("event_type") or "")
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}

        if event_type == "momentum_sold":
            sell_symbol = str(payload.get("symbol") or _event_payload_sell_symbol(payload) or "").upper()
            if sell_symbol:
                sold_symbols.add(sell_symbol)
            continue

        if event_type == "momentum_bought":
            buy_symbol = str(payload.get("symbol") or _event_payload_buy_symbol(payload) or "").upper()
            if buy_symbol and buy_symbol not in sold_symbols:
                return buy_symbol
            continue

        if event_type != "momentum_decision":
            continue

        action = str(payload.get("action") or "").upper()
        execution_result = str(payload.get("execution_result") or "")
        sell_symbol = _event_payload_sell_symbol(payload)
        if action in {"SELL", "ROTATE"} and sell_symbol and execution_result.startswith(("sell_confirmed", "rotate:")):
            sold_symbols.add(sell_symbol)

        buy_symbol = _event_payload_buy_symbol(payload)
        buy_confirmed = execution_result.startswith(("bought:", "bought_margin:", "already_have:")) or (
            execution_result.startswith("rotate:") and ":bought" in execution_result
        )
        if buy_confirmed and buy_symbol and buy_symbol not in sold_symbols and not fallback_decision_buy:
            fallback_decision_buy = buy_symbol

    return fallback_decision_buy


def apply_previous_buy_rotation(decision: dict[str, Any], state: StateStore | None = None) -> dict[str, Any]:
    """Upgrade a BUY Y decision to ROTATE sell X/buy Y when the last buy was X."""
    payload = normalize_decision(decision)
    action = str(payload.get("action") or "WAIT").upper()
    buy_symbol = str(payload.get("buy_symbol") or payload.get("symbol") or "").upper()
    if action != "BUY" or not buy_symbol or payload.get("sell_symbol"):
        return payload
    previous_buy = _last_recorded_buy_without_later_sell(state or StateStore())
    if not previous_buy or previous_buy == buy_symbol:
        return payload

    order_sequence = _rotation_order_sequence(previous_buy, buy_symbol)
    payload.update({
        "action": "ROTATE",
        "raw_action": payload.get("raw_action") or "BUY",
        "symbol": buy_symbol,
        "buy_symbol": buy_symbol,
        "sell_symbol": previous_buy,
        "order_sequence": order_sequence,
        "should_trade": True,
        "reason": f"rotate_after_previous_buy:{previous_buy}->{buy_symbol}:{payload.get('reason') or ''}".rstrip(":"),
    })
    contract = payload.get("executor_contract") if isinstance(payload.get("executor_contract"), dict) else {}
    payload["executor_contract"] = {
        **contract,
        "action": payload["action"],
        "raw_action": payload["raw_action"],
        "symbol": payload["symbol"],
        "buy_symbol": payload["buy_symbol"],
        "sell_symbol": payload["sell_symbol"],
        "order_sequence": order_sequence,
        "should_trade": payload["should_trade"],
        "reason": payload["reason"],
    }
    return payload


def _candidate_id(symbol: str) -> str:
    return f"momentum-{symbol.upper()}"


def _quote_asset(symbol: str, quote_assets: list[str]) -> str | None:
    upper = symbol.upper()
    for quote in sorted([q.upper() for q in quote_assets], key=len, reverse=True):
        if upper.endswith(quote):
            return quote
    return None


def _already_have(kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, symbol: str, min_value: float) -> tuple[bool, str]:
    symbol = symbol.upper()
    if state.has_open_position_for(symbol, "long"):
        return True, "local_position_open"
    if kraken.dry_run:
        return False, "dry_run_no_wallet_check"
    base = rules.base_asset(symbol)
    qty = kraken.free_balance(base)
    price = kraken.current_price(symbol)
    value = qty * price
    if value >= min_value:
        return True, f"wallet_has_{base}:value={value:.4f}"
    return False, f"wallet_value_below_threshold:{base}:value={value:.4f}"


def _wait_for_quote_increase(kraken: KrakenClient, quote: str, before_quote: float, *, attempts: int, sleep_sec: float) -> float:
    if kraken.dry_run:
        return before_quote
    last = before_quote
    for _ in range(max(1, attempts)):
        time.sleep(max(0.1, sleep_sec))
        last = kraken.free_balance(quote)
        if last > before_quote:
            return last
    return last


def _wait_for_quote_notional(kraken: KrakenClient, quote: str, minimum: float, *, attempts: int, sleep_sec: float) -> float:
    """Wait for the quote balance to be spendable after a just-filled sell.

    Kraken spot balances can lag for a few account reads immediately after a
    market sell. A rotation buy should not create a misleading no-cash event
    until the post-sell quote balance has had the same confirmation window used
    by the sell leg.
    """
    if kraken.dry_run:
        return minimum
    last = 0.0
    for _ in range(max(1, attempts)):
        last = kraken.free_balance(quote)
        if last >= minimum:
            return last
        time.sleep(max(0.1, sleep_sec))
    return last


def _wallet_value(kraken: KrakenClient, rules: KrakenSymbolRules, symbol: str) -> tuple[str, float, float, float]:
    base = rules.base_asset(symbol)
    qty = kraken.free_balance(base)
    price = kraken.current_price(symbol)
    return base, qty, price, qty * price


def _safe_quote_notional(settings, kraken: KrakenClient, quote: str, desired: float, *, use_full_available: bool = False, observed_free_quote: float | None = None) -> tuple[float, float]:
    desired = max(0.0, float(desired or 0.0))
    if kraken.dry_run:
        return desired, desired
    free_quote = kraken.free_balance(quote)
    if observed_free_quote is not None:
        free_quote = max(free_quote, float(observed_free_quote))
    reserve = max(0.0, _float_env("MOMENTUM_DECISION_QUOTE_RESERVE", 1.0))
    safety_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_BUY_BALANCE_RATIO", 0.995)))
    usable = max(0.0, (free_quote - reserve) * safety_ratio)
    if use_full_available:
        return usable, free_quote
    return min(desired, usable), free_quote


def _explicit_margin_requested() -> bool:
    explicit = _env("MOMENTUM_DECISION_USE_MARGIN")
    if explicit is None:
        # Compatibility boundary for older environment names; active routing is normalized to margin.
        explicit = _env("MOMENTUM_DECISION_USE_CROSS_MARGIN")
    if explicit is not None:
        return _bool(explicit, default=False)
    configured_mode = _env("EXECUTION_MODE") or _env("MARGIN_ACCOUNT_MODE")
    if configured_mode is None:
        return False
    return margin_enabled() and execution_mode() == "margin"


def _is_margin_position(position: dict[str, Any] | None) -> bool:
    if not position:
        return False
    mode = str(position.get("mode") or "").lower()
    return mode == "margin"


def _margin_stack(settings):
    if hasattr(settings, "exchange"):
        return create_margin_exchange(settings, dry_run=margin_dry_run())
    kraken = KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=getattr(settings, "dry_run", False) or margin_dry_run())
    rules = KrakenSymbolRules(settings.kraken_base_url)
    margin = MarginClient(kraken, dry_run=getattr(settings, "dry_run", False) or margin_dry_run())
    return kraken, margin, rules


def _safe_margin_quote_notional(margin: MarginClient, symbol: str, quote: str, desired: float, *, use_full_available: bool = False) -> tuple[float, float]:
    desired = max(0.0, float(desired or 0.0))
    if margin.dry_run:
        return desired, desired
    free_quote = float(margin.margin_free_balance(symbol, quote))
    reserve = max(0.0, _float_env("MOMENTUM_DECISION_QUOTE_RESERVE", 1.0))
    safety_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_BUY_BALANCE_RATIO", 0.995)))
    usable = max(0.0, (free_quote - reserve) * safety_ratio)
    if use_full_available:
        return usable, free_quote
    return min(desired, usable), free_quote



def _spot_order_id(payload: dict | None):
    if not payload:
        return None
    return payload.get("orderId") or payload.get("order_id")


def _spot_executed_qty(payload: dict, fallback: str | None = None) -> str:
    try:
        raw = payload.get("executedQty")
        if float(raw or 0) > 0:
            return str(raw)
    except Exception:
        pass
    return str(fallback or "0")


def _spot_avg_fill_price(kraken: KrakenClient, payload: dict, fallback: float) -> float:
    avg = kraken.average_fill_price(payload, fallback=fallback)
    if avg is not None:
        return float(avg)
    try:
        qty = float(payload.get("executedQty") or 0)
        quote_qty = float(payload.get("cummulativeQuoteQty") or payload.get("cumulativeQuoteQty") or 0)
        if qty > 0 and quote_qty > 0:
            return quote_qty / qty
    except Exception:
        pass
    try:
        price = payload.get("price")
        if price and float(price) > 0:
            return float(price)
    except Exception:
        pass
    return float(fallback)


def _confirm_spot_entry_order(kraken: KrakenClient, rules: KrakenSymbolRules, *, symbol: str, entry_order_id, submitted_payload: dict, fallback_price: float, fallback_qty: str) -> dict[str, Any]:
    confirm = SpotOrderManager(kraken, rules).confirm_spot_entry_order(
        symbol=symbol,
        entry_order_id=entry_order_id,
        submitted_payload={**submitted_payload, "quantity": submitted_payload.get("quantity") or fallback_qty},
        fallback_price=fallback_price,
    )
    return {**confirm, "executed_qty": _spot_executed_qty(confirm.get("entry_confirm_payload") or {}, fallback_qty)}


def _place_confirmed_spot_market_entry(*, kraken: KrakenClient, rules: KrakenSymbolRules, symbol: str, quote_amount: float) -> dict[str, Any]:
    symbol = symbol.upper()
    price = kraken.current_price(symbol)
    requested_qty = rules.quantity_from_quote(symbol, float(quote_amount), price, market=True)
    order = kraken.place_market_entry(symbol, "long", requested_qty)
    entry_order_id = _spot_order_id(order)
    confirm = _confirm_spot_entry_order(
        kraken,
        rules,
        symbol=symbol,
        entry_order_id=entry_order_id,
        submitted_payload=order,
        fallback_price=price,
        fallback_qty=requested_qty,
    )
    return {
        "mode": "spot",
        "symbol": symbol,
        "side": "long",
        "quantity": confirm["executed_qty"],
        "entry_price": float(confirm["entry_price"]),
        "entry_order_id": entry_order_id,
        "leverage": None,
        "entry_payload": order,
        "total_quote_amount": float(quote_amount),
        "entry_confirmed": confirm.get("entry_confirmed"),
        "entry_confirm_status": confirm.get("entry_confirm_status"),
        "entry_confirm_payload": confirm.get("entry_confirm_payload") or {},
    }

def _buy_symbol_margin(
    settings,
    kraken: KrakenClient,
    rules: KrakenSymbolRules,
    state: StateStore,
    symbol: str,
    decision: dict[str, Any],
    quote: str,
    *,
    use_available_quote: bool = True,
    leverage: float | int | None = None,
) -> str:
    cid = _candidate_id(symbol)
    margin_kwargs = {"dry_run": getattr(settings, "dry_run", False) or margin_dry_run()}
    if leverage is not None:
        margin_kwargs["leverage"] = leverage
    effective_leverage = leverage
    try:
        margin = MarginClient(kraken, **margin_kwargs)
    except TypeError:
        margin_kwargs.pop("leverage", None)
        margin = MarginClient(kraken, **margin_kwargs)
        effective_leverage = margin_multiplier()

    desired_notional = float(settings.order_quote_amount)
    quote_amount = desired_notional
    min_buy_notional = max(5.0, _float_env("MOMENTUM_DECISION_MIN_BUY_NOTIONAL", 5.0))
    manager = MarginOrderManager(kraken, margin, rules)
    try:
        entry = manager.place_margin_market_entry(symbol=symbol, quote_amount=quote_amount, min_notional=min_buy_notional, leverage=effective_leverage, clamp_to_available=use_available_quote)
    except RuntimeError as exc:
        if "margin_entry_notional_below_minimum" in str(exc) or "margin_insufficient_quote_balance" in str(exc):
            free_quote = margin.margin_free_balance(symbol, quote) if not margin.dry_run else desired_notional
            state.add_event(cid, "momentum_buy_skipped_margin_quote_balance", {"symbol": symbol, "quote": quote, "free_quote": free_quote, "usable_notional": 0.0, "min_buy_notional": min_buy_notional, "decision": decision, "mode": "margin", "margin_account_mode": "cross"})
            return f"margin_quote_balance_wait:{quote}:free={float(free_quote):.4f}:usable=0.0000"
        raise

    order = entry["entry_payload"]
    total_quote = float(entry["total_quote_amount"])
    fill_price = float(entry["entry_price"])
    qty = str(entry["quantity"])
    acquired = float(qty) if margin.dry_run else margin.margin_free_balance(symbol, rules.base_asset(symbol))
    state.mark_executed(cid)
    state.add_open_position(cid, {
        "candidate_id": cid, "signal_symbol": symbol, "execution_symbol": symbol, "side": "long", "strategy": "momentum_rotation", "mode": "margin", "margin_account_mode": "cross", "margin_isolated": False,
        "quantity": qty, "entry_price": fill_price, "stop_price": None, "target_price": None, "entry_order_id": entry.get("entry_order_id"), "momentum_decision": decision, "entry_payload": order,
        "implicit_leverage_payload": entry.get("implicit_leverage_payload") or {}, "implicit_margin": True, "leverage_notional": entry.get("leverage_notional"),
        "borrow_payload": entry.get("implicit_leverage_payload") or {}, "borrow_error": None, "notional_used": total_quote, "own_quote_amount": entry.get("own_quote_amount"), "borrow_quote_amount": entry.get("leverage_notional"), "quote_asset": quote, "leverage": entry.get("leverage"), "confirmed_base_balance": acquired,
    })
    state.add_event(cid, "momentum_bought", {"symbol": symbol, "quantity": qty, "price": fill_price, "notional_used": total_quote, "quote": quote, "confirmed_base_balance": acquired, "order": order, "implicit_leverage_payload": entry.get("implicit_leverage_payload") or {}, "decision": decision, "mode": "margin", "margin_account_mode": "cross", "leverage": entry.get("leverage")})
    return f"bought_margin:{symbol}:qty={qty}:notional={total_quote:.4f}"


def _buy_symbol_spot(settings, kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any], quote: str, *, use_available_quote: bool = True) -> str:
    cid = _candidate_id(symbol)
    desired_notional = float(settings.order_quote_amount)
    available_notional, free_quote = _safe_quote_notional(settings, kraken, quote, desired_notional) if use_available_quote else (desired_notional, desired_notional)
    notional = desired_notional
    min_buy_notional = max(5.0, _float_env("MOMENTUM_DECISION_MIN_BUY_NOTIONAL", 5.0))
    required_notional = max(min_buy_notional, desired_notional)
    if use_available_quote and available_notional < required_notional and not kraken.dry_run:
        wait_attempts = max(1, _int_env("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", 8))
        wait_sleep = max(0.2, _float_env("MOMENTUM_DECISION_BALANCE_CONFIRM_SLEEP", 1.0))
        reserve = max(0.0, _float_env("MOMENTUM_DECISION_QUOTE_RESERVE", 1.0))
        safety_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_BUY_BALANCE_RATIO", 0.995)))
        required_free_quote = reserve + (required_notional / safety_ratio)
        confirmed_free_quote = _wait_for_quote_notional(kraken, quote, required_free_quote, attempts=wait_attempts, sleep_sec=wait_sleep)
        available_notional, free_quote = _safe_quote_notional(
            settings,
            kraken,
            quote,
            desired_notional,
            observed_free_quote=confirmed_free_quote,
        )
    if available_notional < required_notional:
        state.add_event(cid, "momentum_buy_skipped_quote_balance", {"symbol": symbol, "quote": quote, "free_quote": free_quote, "usable_notional": available_notional, "min_buy_notional": min_buy_notional, "decision": decision})
        return f"quote_balance_wait:{quote}:free={free_quote:.4f}:usable={available_notional:.4f}"

    entry = _place_confirmed_spot_market_entry(kraken=kraken, rules=rules, symbol=symbol, quote_amount=notional)
    qty = str(entry["quantity"])
    order = entry["entry_payload"]
    fill_price = float(entry["entry_price"])
    acquired = float(qty) if kraken.dry_run else kraken.free_balance(rules.base_asset(symbol))
    state.mark_executed(cid)
    state.add_open_position(cid, {
        "candidate_id": cid,
        "signal_symbol": symbol,
        "execution_symbol": symbol,
        "side": "long",
        "strategy": "momentum_rotation",
        "mode": "spot",
        "quantity": qty,
        "entry_price": float(fill_price),
        "stop_price": None,
        "target_price": None,
        "entry_order_id": entry.get("entry_order_id"),
        "momentum_decision": decision,
        "entry_payload": order,
        "entry_confirmed": entry.get("entry_confirmed"),
        "entry_confirm_status": entry.get("entry_confirm_status"),
        "entry_confirm_payload": entry.get("entry_confirm_payload") or {},
        "notional_used": notional,
        "quote_asset": quote,
        "confirmed_base_balance": acquired,
    })
    state.add_event(cid, "momentum_bought", {"symbol": symbol, "quantity": qty, "price": fill_price, "notional_used": notional, "quote": quote, "confirmed_base_balance": acquired, "order": order, "decision": decision, "mode": "spot", "entry_confirmed": entry.get("entry_confirmed"), "entry_confirm_status": entry.get("entry_confirm_status"), "entry_confirm_payload": entry.get("entry_confirm_payload") or {}})
    return f"bought:{symbol}:qty={qty}:notional={notional:.4f}"


def _margin_wallet_value(margin: MarginClient, rules: KrakenSymbolRules, kraken: KrakenClient, symbol: str) -> tuple[str, float, float, float]:
    base = rules.base_asset(symbol)
    qty = margin.margin_free_balance(symbol, base)
    price = kraken.current_price(symbol)
    return base, qty, price, qty * price


def _sell_symbol_margin(kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any], *, require_confirmed: bool = True) -> str:
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    settings = load_settings()
    quote = _quote_asset(symbol, settings.quote_assets) or str(decision.get("quote_asset") or "USDC").upper()
    if hasattr(settings, "exchange"):
        kraken, margin, rules = _margin_stack(settings)
    else:
        margin = MarginClient(kraken, dry_run=getattr(settings, "dry_run", False) or margin_dry_run())
    margin.ensure_margin_account(symbol)
    base, qty, price, value = _margin_wallet_value(margin, rules, kraken, symbol)
    dust_value = max(1.0, _float_env("MOMENTUM_DECISION_SELL_DUST_VALUE", 5.0))
    max_attempts = max(1, _int_env("MOMENTUM_DECISION_SELL_MAX_ATTEMPTS", 5))
    chunk_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_SELL_CHUNK_RATIO", 0.995)))
    details: list[dict[str, Any]] = []

    if qty <= 0 or value < dust_value:
        state.add_event(cid, "momentum_sell_confirmed_no_balance", {"symbol": symbol, "base": base, "qty": qty, "value": value, "dust_value": dust_value, "decision": decision, "mode": "margin", "margin_account_mode": "cross"})
        return f"sell_confirmed_margin:no_balance:{base}:value={value:.4f}"

    for attempt in range(1, max_attempts + 1):
        before_quote = margin.margin_free_balance(symbol, quote) if not margin.dry_run else 0.0
        base, qty, price, value = _margin_wallet_value(margin, rules, kraken, symbol)
        if qty <= 0 or value < dust_value:
            state.add_event(cid, "momentum_sell_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "attempts": attempt - 1, "details": details, "decision": decision, "mode": "margin", "margin_account_mode": "cross"})
            return f"sell_confirmed_margin:{symbol}:remaining_value={value:.4f}"

        sell_qty = _market_sell_quantity(rules, symbol, qty * chunk_ratio, price) or _market_sell_quantity(rules, symbol, qty, price)
        if sell_qty is None:
            message = f"sell_quantity_not_tradeable_margin:{symbol}:qty={qty}:value={value:.4f}"
            state.add_event(cid, "momentum_sell_not_tradeable", {"symbol": symbol, "base": base, "qty": qty, "value": value, "dust_value": dust_value, "decision": decision, "mode": "margin", "margin_account_mode": "cross"})
            if require_confirmed:
                raise RuntimeError(message)
            return message

        try:
            order = margin.margin_order(symbol, "SELL", sell_qty, "MARKET")
            after_base, after_qty, after_price, after_value = _margin_wallet_value(margin, rules, kraken, symbol)
            after_quote = margin.margin_free_balance(symbol, quote) if not margin.dry_run else before_quote
            detail = {"attempt": attempt, "symbol": symbol, "base": base, "quote": quote, "sell_qty": sell_qty, "before_qty": qty, "before_value": value, "before_quote": before_quote, "after_qty": after_qty, "after_value": after_value, "after_quote": after_quote, "order": order, "mode": "margin", "margin_account_mode": "cross"}
            details.append(detail)
            state.add_event(cid, "momentum_sell_attempt", {"decision": decision, **detail})
            if margin.dry_run or after_value < dust_value:
                state.close_position(cid, "momentum_sell", {"symbol": symbol, "base": base, "quantity": sell_qty, "order": order, "decision": decision, "remaining_qty": after_qty, "remaining_value": after_value, "details": details, "mode": "margin", "margin_account_mode": "cross"}, record_event=False)
                state.add_event(cid, "momentum_sold", {"symbol": symbol, "base": base, "quantity": sell_qty, "order": order, "decision": decision, "remaining_qty": after_qty, "remaining_value": after_value, "quote_after": after_quote, "mode": "margin", "margin_account_mode": "cross"})
                return f"sell_confirmed_margin:{symbol}:remaining_value={after_value:.4f}:quote={after_quote:.4f}"
        except Exception as exc:
            details.append({"attempt": attempt, "symbol": symbol, "qty": sell_qty, "error": str(exc), "mode": "margin", "margin_account_mode": "cross"})
            state.add_event(cid, "momentum_sell_attempt_failed", {"symbol": symbol, "base": base, "qty": sell_qty, "attempt": attempt, "error": str(exc), "decision": decision, "mode": "margin", "margin_account_mode": "cross"})
            if attempt >= max_attempts:
                if require_confirmed:
                    raise RuntimeError(f"sell_not_confirmed_margin:{symbol}:attempts={max_attempts}:last_error={exc}") from exc
                return f"sell_failed_margin:{symbol}:{exc}"

    base, qty, price, value = _margin_wallet_value(margin, rules, kraken, symbol)
    if value < dust_value:
        state.add_event(cid, "momentum_sell_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "details": details, "decision": decision, "mode": "margin", "margin_account_mode": "cross"})
        return f"sell_confirmed_margin:{symbol}:remaining_value={value:.4f}"
    message = f"sell_not_confirmed_margin:{symbol}:remaining_value={value:.4f}:attempts={max_attempts}"
    state.add_event(cid, "momentum_sell_not_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "details": details, "decision": decision, "mode": "margin", "margin_account_mode": "cross"})
    if require_confirmed:
        raise RuntimeError(message)
    return message


def _market_sell_quantity(rules: KrakenSymbolRules, symbol: str, qty: float, price: float) -> str | None:
    try:
        normalized = rules.normalize_market_quantity(symbol, qty)
        rules.ensure_exit_notional(symbol, normalized, price, label="momentum_sell")
        return normalized
    except Exception:
        return None


def _candidate_symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("buy_symbol") or "").upper()


def decision_buy_candidates(decision: dict[str, Any], *, exclude: set[str] | None = None) -> list[dict[str, Any]]:
    """Return the ordered buy candidates embedded in the persisted main decision.

    The central momentum engine owns ranking and buyability. Older decisions may
    still embed candidate rows, but no ranking endpoint fallback is fetched here.
    """
    exclude = {item.upper() for item in (exclude or set()) if item}
    contract = decision.get("executor_contract") if isinstance(decision.get("executor_contract"), dict) else {}
    raw_candidates = decision.get("buy_candidates") or contract.get("buy_candidates") or []
    rows: list[dict[str, Any]] = []
    if isinstance(raw_candidates, list):
        rows.extend([row for row in raw_candidates if isinstance(row, dict)])

    # Backward compatibility: if main has not produced v3 buy_candidates yet,
    # keep the primary buy symbol only. No ranking endpoint fallback is used.
    primary_symbol = str(decision.get("buy_symbol") or decision.get("symbol") or "").upper()
    primary = decision.get("target_asset")
    if not rows and isinstance(primary, dict) and _candidate_symbol(primary):
        rows.append(primary)
    elif not rows and primary_symbol:
        rows.append({"symbol": primary_symbol, "source": "decision_primary_legacy"})

    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        symbol = _candidate_symbol(row)
        if not symbol or symbol in seen or symbol in exclude:
            continue
        seen.add(symbol)
        out.append(row)
    return out


def sell_symbol(kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any], *, require_confirmed: bool = True) -> str:
    """Sell with wallet confirmation and chunk retries before any rotation buy."""
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    position = state.open_positions().get(cid)
    if _is_margin_position(position):
        return _sell_symbol_margin(kraken, rules, state, symbol, decision, require_confirmed=require_confirmed)
    quote = _quote_asset(symbol, load_settings().quote_assets) or str(decision.get("quote_asset") or "USDC").upper()
    base, qty, price, value = _wallet_value(kraken, rules, symbol)
    dust_value = max(1.0, _float_env("MOMENTUM_DECISION_SELL_DUST_VALUE", 5.0))
    max_attempts = max(1, _int_env("MOMENTUM_DECISION_SELL_MAX_ATTEMPTS", 5))
    chunk_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_SELL_CHUNK_RATIO", 0.995)))
    wait_attempts = max(1, _int_env("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", 8))
    wait_sleep = max(0.2, _float_env("MOMENTUM_DECISION_BALANCE_CONFIRM_SLEEP", 1.0))
    details: list[dict[str, Any]] = []

    if qty <= 0 or value < dust_value:
        state.add_event(cid, "momentum_sell_confirmed_no_balance", {"symbol": symbol, "base": base, "qty": qty, "value": value, "dust_value": dust_value, "decision": decision})
        return f"sell_confirmed:no_balance:{base}:value={value:.4f}"

    for attempt in range(1, max_attempts + 1):
        before_quote = kraken.free_balance(quote) if not kraken.dry_run else 0.0
        base, qty, price, value = _wallet_value(kraken, rules, symbol)
        if qty <= 0 or value < dust_value:
            state.add_event(cid, "momentum_sell_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "attempts": attempt - 1, "details": details, "decision": decision})
            return f"sell_confirmed:{symbol}:remaining_value={value:.4f}"

        sell_qty = _market_sell_quantity(rules, symbol, qty * chunk_ratio, price) or _market_sell_quantity(rules, symbol, qty, price)
        if sell_qty is None:
            message = f"sell_quantity_not_tradeable:{symbol}:qty={qty}:value={value:.4f}"
            state.add_event(cid, "momentum_sell_not_tradeable", {"symbol": symbol, "base": base, "qty": qty, "value": value, "dust_value": dust_value, "decision": decision})
            if require_confirmed:
                raise RuntimeError(message)
            return message

        try:
            order = kraken.place_market_entry(symbol, "short", sell_qty)
            after_quote = _wait_for_quote_increase(kraken, quote, before_quote, attempts=wait_attempts, sleep_sec=wait_sleep) if not kraken.dry_run else before_quote
            after_base, after_qty, after_price, after_value = _wallet_value(kraken, rules, symbol)
            detail = {"attempt": attempt, "symbol": symbol, "base": base, "quote": quote, "sell_qty": sell_qty, "before_qty": qty, "before_value": value, "before_quote": before_quote, "after_qty": after_qty, "after_value": after_value, "after_quote": after_quote, "order": order}
            details.append(detail)
            state.add_event(cid, "momentum_sell_attempt", {"decision": decision, **detail})
            if kraken.dry_run or after_value < dust_value:
                state.close_position(cid, "momentum_sell", {"symbol": symbol, "base": base, "quantity": sell_qty, "order": order, "decision": decision, "remaining_qty": after_qty, "remaining_value": after_value, "details": details}, record_event=False)
                state.add_event(cid, "momentum_sold", {"symbol": symbol, "base": base, "quantity": sell_qty, "order": order, "decision": decision, "remaining_qty": after_qty, "remaining_value": after_value, "quote_after": after_quote})
                return f"sell_confirmed:{symbol}:remaining_value={after_value:.4f}:quote={after_quote:.4f}"
        except Exception as exc:
            details.append({"attempt": attempt, "symbol": symbol, "qty": sell_qty, "error": str(exc)})
            state.add_event(cid, "momentum_sell_attempt_failed", {"symbol": symbol, "base": base, "qty": sell_qty, "attempt": attempt, "error": str(exc), "decision": decision})
            if attempt >= max_attempts:
                if require_confirmed:
                    raise RuntimeError(f"sell_not_confirmed:{symbol}:attempts={max_attempts}:last_error={exc}") from exc
                return f"sell_failed:{symbol}:{exc}"
            time.sleep(wait_sleep)

    base, qty, price, value = _wallet_value(kraken, rules, symbol)
    if value < dust_value:
        state.add_event(cid, "momentum_sell_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "details": details, "decision": decision})
        return f"sell_confirmed:{symbol}:remaining_value={value:.4f}"
    message = f"sell_not_confirmed:{symbol}:remaining_value={value:.4f}:attempts={max_attempts}"
    state.add_event(cid, "momentum_sell_not_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "details": details, "decision": decision})
    if require_confirmed:
        raise RuntimeError(message)
    return message


def buy_symbol(settings, kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any], *, use_available_quote: bool = True) -> str:
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    quote = _quote_asset(symbol, settings.quote_assets)
    if not quote:
        configured_quotes = ",".join(settings.quote_assets) or "none"
        state.add_event(cid, "momentum_buy_skipped_unsupported_quote", {"symbol": symbol, "configured_quote_assets": settings.quote_assets, "decision": decision})
        return f"unsupported_quote:{symbol}:configured={configured_quotes}"
    min_value = max(5.0, float(settings.order_quote_amount) * 0.5)
    already, reason = _already_have(kraken, rules, state, symbol, min_value)
    if already:
        state.add_event(cid, "momentum_buy_skipped_already_have", {"symbol": symbol, "reason": reason, "decision": decision})
        return f"already_have:{reason}"
    # Compatibility boundary for older decision payloads that still send force_cross_margin.
    should_try_margin = bool(decision.get("force_margin")) or bool(decision.get("force_cross_margin")) or _explicit_margin_requested()
    if should_try_margin:
        margin_failures: list[dict[str, Any]] = []
        for leverage in margin_leverage_attempts():
            try:
                result = _buy_symbol_margin(settings, kraken, rules, state, symbol, decision, quote, use_available_quote=use_available_quote, leverage=leverage)
                if result.startswith("bought_margin:"):
                    return result
                margin_failures.append({"leverage": leverage, "result": result})
            except Exception as exc:
                margin_failures.append({"leverage": leverage, "error": str(exc)})
        state.add_event(cid, "momentum_buy_margin_fallback_spot", {"symbol": symbol, "quote": quote, "margin_attempts": margin_failures, "decision": decision})
        try:
            return _buy_symbol_spot(settings, kraken, rules, state, symbol, decision, quote, use_available_quote=use_available_quote)
        except Exception as exc:
            state.add_event(cid, "momentum_buy_skipped_margin_and_spot_failed", {"symbol": symbol, "quote": quote, "margin_attempts": margin_failures, "spot_error": str(exc), "decision": decision})
            return f"buy_skipped_margin_and_spot_failed:{symbol}:spot_error={exc}"

    return _buy_symbol_spot(settings, kraken, rules, state, symbol, decision, quote, use_available_quote=use_available_quote)


def _buy_result_ok(result: str) -> bool:
    return str(result).startswith("bought:") or str(result).startswith("bought_margin:") or str(result).startswith("already_have:")


def _buy_result_balance_wait(result: str) -> bool:
    text = str(result)
    return text.startswith("margin_quote_balance_wait:")


def buy_best_available(settings, kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, decision: dict[str, Any], *, exclude: set[str] | None = None) -> str:
    """Buy only the central decision target.

    Ranking-based local fallbacks and RSI filtering are intentionally not used:
    the central momentum engine owns candidate selection and buyability rules.
    """
    symbol = str(decision.get("target_symbol") or decision.get("buy_symbol") or decision.get("symbol") or "").upper()
    exclude_set = {item.upper() for item in (exclude or set()) if item}
    if not symbol:
        state.add_event("momentum-decision", "momentum_buy_skipped_missing_target", {"decision": decision})
        return "buy_skipped_missing_target"
    if symbol in exclude_set:
        state.add_event(_candidate_id(symbol), "momentum_buy_skipped_excluded_target", {"symbol": symbol, "decision": decision})
        return f"buy_skipped_excluded_target:{symbol}"
    return buy_symbol(settings, kraken, rules, state, symbol, decision, use_available_quote=True)

def current_momentum_position(state: StateStore) -> dict[str, Any] | None:
    for candidate_id, position in state.open_positions().items():
        if str(candidate_id).startswith("momentum-") or isinstance(position.get("momentum_decision"), dict):
            return position
    return None


def _position_symbol(position: dict[str, Any] | None) -> str:
    if not position:
        return ""
    return str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()


def _target_asset_symbol(target_asset: Any) -> str:
    if isinstance(target_asset, dict):
        return str(target_asset.get("buy_symbol") or target_asset.get("symbol") or target_asset.get("target_symbol") or "").upper()
    return str(target_asset or "").upper()


def _decision_expected_symbol(decision: dict[str, Any]) -> str:
    """Resolve the momentum target from the central decision contract."""
    return str(
        decision.get("buy_symbol")
        or decision.get("symbol")
        or decision.get("target_symbol")
        or _target_asset_symbol(decision.get("target_asset"))
        or ""
    ).upper()


def _decision_targets_held_symbol(decision: dict[str, Any], held_symbol: str) -> bool:
    held_symbol = held_symbol.upper()
    sell_symbol = str(decision.get("sell_symbol") or "").upper()
    if not held_symbol:
        return bool(sell_symbol)
    symbols = {sell_symbol, str(decision.get("symbol") or "").upper()}
    return held_symbol in symbols



def _parse_event_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _momentum_cadence_hours() -> float:
    return max(0.0, _float_env("MOMENTUM_DECISION_CADENCE_HOURS", 4.0))


def _last_momentum_buy_check(state: StateStore) -> datetime | None:
    cadence_events = {
        "momentum_decision_cadence_checked",
        "momentum_buy_skipped_quote_balance",
        "momentum_buy_skipped_margin_quote_balance",
        "momentum_bought",
    }
    last: datetime | None = None
    for event in state.events(limit=1000):
        event_type = str(event.get("event_type") or "")
        if event_type not in cadence_events:
            continue
        ts = _parse_event_timestamp(event.get("timestamp"))
        if ts is not None and (last is None or ts > last):
            last = ts
    return last


def _momentum_buy_cadence_status(state: StateStore) -> dict[str, Any]:
    cadence_hours = _momentum_cadence_hours()
    last_check = _last_momentum_buy_check(state)
    now = datetime.now(timezone.utc)
    if cadence_hours <= 0 or last_check is None:
        return {
            "due_now": True,
            "cadence_hours": cadence_hours,
            "last_check_at": last_check.isoformat() if last_check else None,
            "next_check_at": now.isoformat(),
        }
    next_check = last_check + timedelta(hours=cadence_hours)
    return {
        "due_now": now >= next_check,
        "cadence_hours": cadence_hours,
        "last_check_at": last_check.isoformat(),
        "next_check_at": next_check.isoformat(),
    }


def _mark_momentum_buy_cadence_checked(state: StateStore, decision: dict[str, Any], status: dict[str, Any]) -> None:
    state.add_event("momentum-decision", "momentum_decision_cadence_checked", {
        "action": decision.get("action"),
        "buy_symbol": decision.get("buy_symbol"),
        "symbol": decision.get("symbol"),
        "cadence": status,
        "decision": decision,
    })


def _rotation_order_sequence(sell_symbol: str | None, buy_symbol: str | None) -> list[dict[str, Any]]:
    sequence: list[dict[str, Any]] = []
    if sell_symbol:
        sequence.append({"step": 1, "action": "SELL", "symbol": sell_symbol, "role": "exit_held_momentum_asset"})
    if buy_symbol:
        sequence.append({"step": len(sequence) + 1, "action": "BUY", "symbol": buy_symbol, "role": "enter_new_momentum_asset"})
    return sequence


def _buy_with_momentum_cadence(settings, kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, decision: dict[str, Any], *, exclude: set[str] | None = None) -> str:
    cadence = _momentum_buy_cadence_status(state)
    decision.setdefault("due_now", cadence["due_now"])
    decision.setdefault("next_check_at", cadence["next_check_at"])
    if not cadence["due_now"]:
        state.add_event("momentum-decision", "momentum_decision_cadence_wait", {"cadence": cadence, "decision": decision})
        return f"wait:momentum_cadence:next_check_at={cadence['next_check_at']}"
    _mark_momentum_buy_cadence_checked(state, decision, cadence)
    return buy_best_available(settings, kraken, rules, state, decision, exclude=exclude)


def _rotate_momentum_position(settings, kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, held_symbol: str, buy_symbol: str, decision: dict[str, Any]) -> str:
    rotation_decision = {
        **decision,
        "action": "ROTATE",
        "raw_action": decision.get("raw_action") or decision.get("action") or "BUY",
        "sell_symbol": held_symbol,
        "buy_symbol": buy_symbol,
        "symbol": buy_symbol,
        "target_symbol": decision.get("target_symbol") or buy_symbol,
        "should_trade": True,
        "order_sequence": _rotation_order_sequence(held_symbol, buy_symbol),
    }
    state.add_event("momentum-decision", "momentum_rotation_started", {"sell_symbol": held_symbol, "buy_symbol": buy_symbol, "order_sequence": rotation_decision.get("order_sequence"), "decision": rotation_decision})
    sell_result = sell_symbol(kraken, rules, state, held_symbol, rotation_decision, require_confirmed=True)
    buy_result = buy_best_available(settings, kraken, rules, state, rotation_decision, exclude={held_symbol})
    return f"rotate:{sell_result}:{buy_result}"


def execute_decision(decision: dict[str, Any]) -> str:
    if not _bool(_env("MOMENTUM_DECISION_EXECUTE_ENABLED"), default=True):
        return "execution_disabled"
    settings = load_settings()
    if hasattr(settings, "exchange"):
        kraken, rules = create_spot_exchange(settings)
    else:
        kraken = KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=settings.dry_run)
        rules = KrakenSymbolRules(settings.kraken_base_url)
    state = StateStore()
    action = str(decision.get("action") or "WAIT").upper()
    should_trade = bool(decision.get("should_trade"))
    sell = str(decision.get("sell_symbol") or "").upper()
    held_position = current_momentum_position(state)
    held_symbol = _position_symbol(held_position) or sell
    expected_symbol = _decision_expected_symbol(decision)

    if action == "WAIT" or (not should_trade and action != "HOLD"):
        if held_symbol:
            return f"wait_existing_momentum_position:{held_symbol}"
        reason = str(decision.get("reason") or "WAIT").strip() or "WAIT"
        return f"wait:{reason}"
    if action == "HOLD":
        if held_symbol and (not expected_symbol or expected_symbol == held_symbol):
            return f"hold_existing_momentum_position:{held_symbol}"
        if held_symbol:
            return f"hold_target_not_held:held={held_symbol}:target={expected_symbol or 'none'}"
        return f"hold_no_existing_momentum_position:{expected_symbol or 'none'}"
    if action == "BUY":
        buy = expected_symbol
        if held_symbol and (not buy or buy == held_symbol):
            return f"hold_existing_momentum_position:{held_symbol}"
        if held_symbol and buy != held_symbol:
            return _rotate_momentum_position(settings, kraken, rules, state, held_symbol, buy, decision)
        return _buy_with_momentum_cadence(settings, kraken, rules, state, decision, exclude={sell})
    if action == "SELL":
        if not _decision_targets_held_symbol(decision, held_symbol):
            return f"sell_blocked:decision_not_on_held_momentum_asset:held={held_symbol or 'none'}:sell={sell or 'none'}"
        return sell_symbol(kraken, rules, state, held_symbol, decision, require_confirmed=True)
    if action == "ROTATE":
        if not _decision_targets_held_symbol(decision, held_symbol):
            return f"rotate_blocked:decision_not_on_held_momentum_asset:held={held_symbol or 'none'}:sell={sell or 'none'}"
        rotation_decision = dict(decision)
        state.add_event("momentum-decision", "momentum_rotation_started", {"sell_symbol": held_symbol, "buy_symbol": rotation_decision.get("buy_symbol"), "order_sequence": rotation_decision.get("order_sequence"), "decision": rotation_decision})
        sell_result = sell_symbol(kraken, rules, state, held_symbol, rotation_decision, require_confirmed=True)
        buy_result = buy_best_available(settings, kraken, rules, state, rotation_decision, exclude={held_symbol})
        return f"rotate:{sell_result}:{buy_result}"
    return f"unsupported_action:{action}"


def record_decision(decision: dict[str, Any], execution_result: str | None = None) -> None:
    action = str(decision.get("action") or "WAIT").upper()
    symbol = str(decision.get("symbol") or decision.get("buy_symbol") or decision.get("sell_symbol") or "momentum")
    StateStore().add_event("momentum-decision", "momentum_decision", {
        "decision_action": decision.get("decision_action") or action,
        "action": action,
        "symbol": symbol,
        "target_symbol": decision.get("target_symbol"),
        "status": decision.get("status"),
        "should_trade": bool(decision.get("should_trade")),
        "buy_symbol": decision.get("buy_symbol"),
        "sell_symbol": decision.get("sell_symbol"),
        "execution_result": execution_result or decision.get("execution_result"),
        "reason": decision.get("reason"),
        "order_ids": decision.get("order_ids") or [],
        "fill_ids": decision.get("fill_ids") or [],
        "due_now": decision.get("due_now"),
        "next_check_at": decision.get("next_check_at"),
        "fallback_policy": decision.get("fallback_policy"),
        "decision": decision,
    })


def _log_decision(prefix: str, decision: dict[str, Any], execution_result: str | None = None) -> None:
    logger.info(
        "%s action=%s symbol=%s should_trade=%s buy_symbol=%s sell_symbol=%s execution_result=%s",
        prefix,
        decision.get("action"),
        decision.get("symbol"),
        decision.get("should_trade"),
        decision.get("buy_symbol"),
        decision.get("sell_symbol"),
        execution_result,
    )


def run_once() -> dict[str, Any]:
    decision = fetch_decision()
    _log_decision("momentum decision received", decision)
    result = execute_decision(decision)
    _log_decision("momentum decision executed", decision, result)
    record_decision(decision, result)
    return {"decision": decision, "execution_result": result}


def run_loop() -> None:
    if not _bool(_env("MOMENTUM_DECISION_ENABLED"), default=True):
        logger.info("momentum decision feed disabled")
        return
    poll_seconds = max(30, _int_env("MOMENTUM_DECISION_POLL_SECONDS", 300))
    logger.info("momentum decision feed started poll_seconds=%s execute=%s path=%s", poll_seconds, _env("MOMENTUM_DECISION_EXECUTE_ENABLED", "true"), _decision_path())
    while True:
        try:
            output = run_once()
            decision = output["decision"]
            logger.info("momentum decision action=%s symbol=%s target_symbol=%s status=%s result=%s", decision.get("decision_action") or decision.get("action"), decision.get("symbol"), decision.get("target_symbol"), decision.get("status"), output.get("execution_result"))
        except Exception as exc:
            logger.error("momentum decision feed error=%s", str(exc))
            try:
                StateStore().add_event("momentum-decision", "momentum_decision_error", {"error": str(exc)})
            except Exception:
                pass
        time.sleep(poll_seconds)


if __name__ == "__main__":
    run_loop()
