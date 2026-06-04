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

DEFAULT_DECISION_PATH = "/api/v1/momentum-engine/decision"
DEFAULT_MOMENTUM_RANKING_PATH = "/api/v1/momentum"


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
    if not path.startswith("/"):
        path = "/" + path
    if base.endswith("/api/v1") and path.startswith("/api/v1"):
        return f"{base}{path[len('/api/v1'):] }".strip()
    return f"{base}{path}"


def _decision_path() -> str:
    return os.getenv("MOMENTUM_DECISION_PATH", DEFAULT_DECISION_PATH) or DEFAULT_DECISION_PATH


def _ranking_path() -> str:
    return os.getenv("MOMENTUM_RANKING_PATH", DEFAULT_MOMENTUM_RANKING_PATH) or DEFAULT_MOMENTUM_RANKING_PATH


def _read_json_response(response: requests.Response) -> dict[str, Any] | list[Any]:
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "momentum_decision_non_json_response "
            f"status={response.status_code} content_type={response.headers.get('content-type')} "
            f"url={response.url} body={response.text[:500]!r}"
        ) from exc
    return data


def fetch_decision() -> dict[str, Any]:
    settings = load_settings()
    response = requests.get(
        _url(settings.signalmaker_base_url, _decision_path()),
        timeout=30,
        headers={"accept": "application/json", "cache-control": "no-cache"},
    )
    data = _read_json_response(response)
    if not response.ok:
        raise RuntimeError(f"momentum_decision_http_error status={response.status_code} url={response.url} payload={data}")
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected_momentum_decision_response:{type(data).__name__}:url={response.url}")
    return normalize_decision(data)


def fetch_momentum_rankings(limit: int | None = None) -> list[dict[str, Any]]:
    settings = load_settings()
    fetch_limit = int(limit or _int_env("MOMENTUM_DECISION_FALLBACK_LIMIT", 30))
    response = requests.get(
        _url(settings.signalmaker_base_url, _ranking_path()),
        params={"limit": fetch_limit},
        timeout=30,
        headers={"accept": "application/json", "cache-control": "no-cache"},
    )
    data = _read_json_response(response)
    if not response.ok:
        raise RuntimeError(f"momentum_ranking_http_error status={response.status_code} url={response.url} payload={data}")
    if not isinstance(data, list):
        raise RuntimeError(f"unexpected_momentum_ranking_response:{type(data).__name__}:url={response.url}")
    return [row for row in data if isinstance(row, dict)]


def normalize_decision(decision: dict[str, Any]) -> dict[str, Any]:
    payload = dict(decision)
    contract = payload.get("executor_contract")
    if isinstance(contract, dict):
        for key in ("action", "raw_action", "symbol", "buy_symbol", "sell_symbol", "reason", "should_trade", "order_sequence"):
            payload.setdefault(key, contract.get(key))
    payload.setdefault("mode", "momentum_rotation")
    payload.setdefault("action", "WAIT")
    payload["action"] = str(payload.get("action") or "WAIT").upper()
    payload.setdefault("symbol", payload.get("buy_symbol") or payload.get("sell_symbol"))
    payload.setdefault("buy_symbol", payload.get("symbol") if payload["action"] in {"BUY", "ROTATE"} else None)
    payload.setdefault("sell_symbol", payload.get("symbol") if payload["action"] == "SELL" else None)
    payload.setdefault("should_trade", payload["action"] in {"BUY", "SELL", "ROTATE"})
    payload.setdefault("reason", payload.get("recommendation") or payload.get("message") or "")
    return payload


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


def _wait_for_quote_increase(binance: BinanceClient, quote: str, before_quote: float, *, attempts: int, sleep_sec: float) -> float:
    if binance.dry_run:
        return before_quote
    last = before_quote
    for _ in range(max(1, attempts)):
        time.sleep(max(0.1, sleep_sec))
        last = binance.free_balance(quote)
        if last > before_quote:
            return last
    return last


def _wallet_value(binance: BinanceClient, rules: BinanceSymbolRules, symbol: str) -> tuple[str, float, float, float]:
    base = rules.base_asset(symbol)
    qty = binance.free_balance(base)
    price = binance.current_price(symbol)
    return base, qty, price, qty * price


def _safe_quote_notional(settings, binance: BinanceClient, quote: str, desired: float) -> tuple[float, float]:
    desired = max(0.0, float(desired or 0.0))
    if binance.dry_run:
        return desired, desired
    free_quote = binance.free_balance(quote)
    reserve = max(0.0, _float_env("MOMENTUM_DECISION_QUOTE_RESERVE", 1.0))
    safety_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_BUY_BALANCE_RATIO", 0.995)))
    usable = max(0.0, (free_quote - reserve) * safety_ratio)
    return min(desired, usable), free_quote


def _market_sell_quantity(rules: BinanceSymbolRules, symbol: str, qty: float, price: float) -> str | None:
    try:
        normalized = rules.normalize_market_quantity(symbol, qty)
        rules.ensure_exit_notional(symbol, normalized, price, label="momentum_sell")
        return normalized
    except Exception:
        return None


def _ranking_symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or "").upper()


def _fallback_row_is_eligible(row: dict[str, Any]) -> bool:
    min_score = _float_env("MOMENTUM_DECISION_FALLBACK_MIN_SCORE", 0.0)
    symbol = _ranking_symbol(row)
    if not symbol:
        return False
    if float(row.get("momentum_score") or 0.0) < min_score:
        return False
    if row.get("data_quality") not in (None, "complete"):
        return False
    if row.get("structure_15m_status") in {"broken_bearish", "invalid", "opposed"}:
        return False
    if row.get("in_entry_pool") is False:
        return False
    entry_status = str(row.get("entry_status") or "")
    if entry_status and not entry_status.startswith("ready"):
        return False
    return True


def fallback_candidates(decision: dict[str, Any], *, exclude: set[str] | None = None) -> list[dict[str, Any]]:
    exclude = {item.upper() for item in (exclude or set()) if item}
    rows: list[dict[str, Any]] = []
    primary_symbol = str(decision.get("buy_symbol") or decision.get("symbol") or "").upper()
    primary = decision.get("target_asset")
    if isinstance(primary, dict) and _ranking_symbol(primary):
        rows.append(primary)
    elif primary_symbol:
        rows.append({"symbol": primary_symbol, "source": "decision_primary"})

    try:
        rows.extend(fetch_momentum_rankings())
    except Exception as exc:
        StateStore().add_event("momentum-decision", "momentum_fallback_ranking_error", {"error": str(exc), "decision": decision})

    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        symbol = _ranking_symbol(row)
        if not symbol or symbol in seen or symbol in exclude:
            continue
        if row.get("source") == "decision_primary" or _fallback_row_is_eligible(row):
            seen.add(symbol)
            out.append(row)
    return out


def sell_symbol(binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any], *, require_confirmed: bool = True) -> str:
    """Sell with wallet confirmation and chunk retries before any rotation buy."""
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    quote = _quote_asset(symbol, load_settings().quote_assets) or str(decision.get("quote_asset") or "USDC").upper()
    base, qty, price, value = _wallet_value(binance, rules, symbol)
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
        before_quote = binance.free_balance(quote) if not binance.dry_run else 0.0
        base, qty, price, value = _wallet_value(binance, rules, symbol)
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
            order = binance.place_market_entry(symbol, "short", sell_qty)
            after_quote = _wait_for_quote_increase(binance, quote, before_quote, attempts=wait_attempts, sleep_sec=wait_sleep) if not binance.dry_run else before_quote
            after_base, after_qty, after_price, after_value = _wallet_value(binance, rules, symbol)
            detail = {"attempt": attempt, "symbol": symbol, "base": base, "quote": quote, "sell_qty": sell_qty, "before_qty": qty, "before_value": value, "before_quote": before_quote, "after_qty": after_qty, "after_value": after_value, "after_quote": after_quote, "order": order}
            details.append(detail)
            state.add_event(cid, "momentum_sell_attempt", {"decision": decision, **detail})
            if binance.dry_run or after_value < dust_value:
                state.close_position(cid, "momentum_sell", {"symbol": symbol, "base": base, "quantity": sell_qty, "order": order, "decision": decision, "remaining_qty": after_qty, "remaining_value": after_value, "details": details})
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

    base, qty, price, value = _wallet_value(binance, rules, symbol)
    if value < dust_value:
        state.add_event(cid, "momentum_sell_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "details": details, "decision": decision})
        return f"sell_confirmed:{symbol}:remaining_value={value:.4f}"
    message = f"sell_not_confirmed:{symbol}:remaining_value={value:.4f}:attempts={max_attempts}"
    state.add_event(cid, "momentum_sell_not_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "details": details, "decision": decision})
    if require_confirmed:
        raise RuntimeError(message)
    return message


def buy_symbol(settings, binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any], *, use_available_quote: bool = True) -> str:
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    quote = _quote_asset(symbol, settings.quote_assets) or str(decision.get("quote_asset") or "USDC").upper()
    min_value = max(5.0, float(settings.order_quote_amount) * 0.5)
    already, reason = _already_have(binance, rules, state, symbol, min_value)
    if already:
        state.add_event(cid, "momentum_buy_skipped_already_have", {"symbol": symbol, "reason": reason, "decision": decision})
        return f"already_have:{reason}"

    desired_notional = float(settings.order_quote_amount)
    notional, free_quote = _safe_quote_notional(settings, binance, quote, desired_notional) if use_available_quote else (desired_notional, desired_notional)
    min_buy_notional = max(5.0, _float_env("MOMENTUM_DECISION_MIN_BUY_NOTIONAL", 5.0))
    if notional < min_buy_notional:
        state.add_event(cid, "momentum_buy_skipped_quote_balance", {"symbol": symbol, "quote": quote, "free_quote": free_quote, "usable_notional": notional, "min_buy_notional": min_buy_notional, "decision": decision})
        return f"quote_balance_wait:{quote}:free={free_quote:.4f}:usable={notional:.4f}"

    price = binance.current_price(symbol)
    qty = rules.quantity_from_quote(symbol, notional, price, market=True)
    order = binance.place_market_entry(symbol, "long", qty)
    fill_price = binance.average_fill_price(order, fallback=price) or price
    acquired = float(qty) if binance.dry_run else binance.free_balance(rules.base_asset(symbol))
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
        "notional_used": notional,
        "quote_asset": quote,
        "confirmed_base_balance": acquired,
    })
    state.add_event(cid, "momentum_bought", {"symbol": symbol, "quantity": qty, "price": fill_price, "notional_used": notional, "quote": quote, "confirmed_base_balance": acquired, "order": order, "decision": decision})
    return f"bought:{symbol}:qty={qty}:notional={notional:.4f}"


def _buy_result_ok(result: str) -> bool:
    return str(result).startswith("bought:") or str(result).startswith("already_have:")


def buy_best_available(settings, binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, decision: dict[str, Any], *, exclude: set[str] | None = None) -> str:
    if not _bool(os.getenv("MOMENTUM_DECISION_FALLBACK_ENABLED"), default=True):
        symbol = str(decision.get("buy_symbol") or decision.get("symbol") or "").upper()
        return buy_symbol(settings, binance, rules, state, symbol, decision, use_available_quote=True)

    attempts: list[dict[str, Any]] = []
    max_attempts = max(1, _int_env("MOMENTUM_DECISION_FALLBACK_MAX_ATTEMPTS", 10))
    candidates = fallback_candidates(decision, exclude=exclude)
    for row in candidates[:max_attempts]:
        symbol = _ranking_symbol(row)
        trial_decision = {**decision, "buy_symbol": symbol, "symbol": symbol, "fallback_target_asset": row}
        try:
            result = buy_symbol(settings, binance, rules, state, symbol, trial_decision, use_available_quote=True)
            attempts.append({"symbol": symbol, "result": result, "rank": row.get("rank"), "score": row.get("momentum_score")})
            if _buy_result_ok(result):
                state.add_event("momentum-decision", "momentum_fallback_buy_selected", {"selected_symbol": symbol, "attempts": attempts, "decision": decision})
                return f"fallback_buy:{symbol}:{result}"
        except Exception as exc:
            attempts.append({"symbol": symbol, "error": str(exc), "rank": row.get("rank"), "score": row.get("momentum_score")})
            StateStore().add_event(_candidate_id(symbol), "momentum_fallback_buy_failed", {"symbol": symbol, "error": str(exc), "rank": row.get("rank"), "score": row.get("momentum_score"), "decision": decision})
            continue

    state.add_event("momentum-decision", "momentum_fallback_buy_exhausted", {"attempts": attempts, "decision": decision})
    return f"fallback_buy_exhausted:attempts={len(attempts)}"


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
        return buy_best_available(settings, binance, rules, state, decision, exclude={sell})
    if action == "SELL" and sell:
        return sell_symbol(binance, rules, state, sell, decision, require_confirmed=True)
    if action == "ROTATE":
        sell_result = sell_symbol(binance, rules, state, sell, decision, require_confirmed=True) if sell else "no_sell_symbol"
        buy_result = buy_best_available(settings, binance, rules, state, decision, exclude={sell}) if buy else "no_buy_symbol"
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
    logger.info("momentum decision feed started poll_seconds=%s execute=%s path=%s", poll_seconds, os.getenv("MOMENTUM_DECISION_EXECUTE_ENABLED", "true"), _decision_path())
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
