import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import read_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_order_manager import amount_str
from raspberry_executor.margin_settings import execution_mode, margin_dry_run, margin_enabled, margin_multiplier
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-momentum-decision")

DEFAULT_DECISION_PATH = ""
DEFAULT_CANDIDATES_PATH = "/api/v1/momentum"
DECISION_RANKINGS_SOURCE = "momentum_rankings"
DECISION_ENDPOINT_FALLBACK_SOURCE = "momentum_decision_endpoint_fallback"
LEGACY_DECISION_PATH = "/api/v1/momentum-engine/decision"


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
    return (_env("MOMENTUM_DECISION_PATH", DEFAULT_DECISION_PATH) or DEFAULT_DECISION_PATH).strip()


def _candidates_path() -> str:
    return _env("MOMENTUM_CANDIDATES_PATH", DEFAULT_CANDIDATES_PATH) or DEFAULT_CANDIDATES_PATH


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


def _decision_candidates_fallback_enabled() -> bool:
    return _bool(_env("MOMENTUM_DECISION_CANDIDATES_FALLBACK_ENABLED"), default=True)


def fetch_decision() -> dict[str, Any]:
    """Fetch the actionable momentum decision for the Raspberry executor.

    The current main SignalMaker API exposes rankings at /api/v1/momentum. It
    does not expose the older experimental /api/v1/momentum-engine/decision
    route, so rankings are the default source. MOMENTUM_DECISION_PATH remains
    supported for deployments that explicitly provide a decision endpoint.
    """
    decision_path = _decision_path()
    if not decision_path or decision_path == LEGACY_DECISION_PATH:
        return build_decision_from_candidates(
            fetch_momentum_candidates(limit=_int_env("MOMENTUM_DECISION_FALLBACK_LIMIT", 50)),
            source=DECISION_RANKINGS_SOURCE,
        )

    settings = load_settings()
    response = requests.get(
        _url(settings.signalmaker_base_url, decision_path),
        timeout=30,
        headers={"accept": "application/json", "cache-control": "no-cache"},
    )
    if not response.ok and response.status_code in {404, 405} and _decision_candidates_fallback_enabled():
        logger.warning(
            "momentum decision endpoint unavailable status=%s url=%s; falling back to %s",
            response.status_code,
            response.url,
            _candidates_path(),
        )
        return build_decision_from_candidates(
            fetch_momentum_candidates(limit=_int_env("MOMENTUM_DECISION_FALLBACK_LIMIT", 50)),
            source=DECISION_ENDPOINT_FALLBACK_SOURCE,
        )
    data = _read_json_response(response)
    if not response.ok:
        raise RuntimeError(f"momentum_decision_http_error status={response.status_code} url={response.url} payload={data}")
    return apply_previous_buy_rotation(normalize_decision(data))


def normalize_decision(decision: dict[str, Any]) -> dict[str, Any]:
    payload = dict(decision)
    contract = payload.get("executor_contract")
    if isinstance(contract, dict):
        for key in ("action", "raw_action", "symbol", "buy_symbol", "sell_symbol", "reason", "should_trade", "order_sequence", "buy_candidates", "fallback_policy"):
            payload.setdefault(key, contract.get(key))
    payload.setdefault("mode", "momentum_rotation")
    payload.setdefault("action", "WAIT")
    payload["action"] = str(payload.get("action") or "WAIT").upper()
    payload.setdefault("symbol", payload.get("buy_symbol") or payload.get("sell_symbol"))
    payload.setdefault("buy_symbol", payload.get("symbol") if payload["action"] in {"BUY", "ROTATE"} else None)
    payload.setdefault("sell_symbol", payload.get("symbol") if payload["action"] == "SELL" else None)
    payload.setdefault("should_trade", payload["action"] in {"BUY", "SELL", "ROTATE"})
    payload.setdefault("reason", payload.get("recommendation") or payload.get("message") or "")
    payload.setdefault("buy_candidates", [])
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
        buy_confirmed = execution_result.startswith(("bought:", "bought_cross_margin:", "already_have:")) or (
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


def _wait_for_quote_notional(binance: BinanceClient, quote: str, minimum: float, *, attempts: int, sleep_sec: float) -> float:
    """Wait for the quote balance to be spendable after a just-filled sell.

    Binance spot balances can lag for a few account reads immediately after a
    market sell. A rotation buy should not create a misleading no-cash event
    until the post-sell quote balance has had the same confirmation window used
    by the sell leg.
    """
    if binance.dry_run:
        return minimum
    last = 0.0
    for _ in range(max(1, attempts)):
        last = binance.free_balance(quote)
        if last >= minimum:
            return last
        time.sleep(max(0.1, sleep_sec))
    return last


def _wallet_value(binance: BinanceClient, rules: BinanceSymbolRules, symbol: str) -> tuple[str, float, float, float]:
    base = rules.base_asset(symbol)
    qty = binance.free_balance(base)
    price = binance.current_price(symbol)
    return base, qty, price, qty * price


def _safe_quote_notional(settings, binance: BinanceClient, quote: str, desired: float, *, use_full_available: bool = False, observed_free_quote: float | None = None) -> tuple[float, float]:
    desired = max(0.0, float(desired or 0.0))
    if binance.dry_run:
        return desired, desired
    free_quote = binance.free_balance(quote)
    if observed_free_quote is not None:
        free_quote = max(free_quote, float(observed_free_quote))
    reserve = max(0.0, _float_env("MOMENTUM_DECISION_QUOTE_RESERVE", 1.0))
    safety_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_BUY_BALANCE_RATIO", 0.995)))
    usable = max(0.0, (free_quote - reserve) * safety_ratio)
    if use_full_available:
        return usable, free_quote
    return min(desired, usable), free_quote


def _explicit_cross_margin_requested() -> bool:
    explicit = _env("MOMENTUM_DECISION_USE_CROSS_MARGIN")
    if explicit is not None:
        return _bool(explicit, default=False)
    configured_mode = _env("EXECUTION_MODE") or _env("MARGIN_ACCOUNT_MODE")
    if configured_mode is None:
        return False
    return margin_enabled() and execution_mode() == "cross"


def _is_cross_margin_position(position: dict[str, Any] | None) -> bool:
    if not position:
        return False
    mode = str(position.get("mode") or "").lower()
    if mode == "cross_margin":
        return True
    if position.get("margin_isolated") is False and mode in {"margin", "momentum_rotation"}:
        return True
    return False


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


def _buy_symbol_cross_margin(
    settings,
    binance: BinanceClient,
    rules: BinanceSymbolRules,
    state: StateStore,
    symbol: str,
    decision: dict[str, Any],
    quote: str,
    *,
    use_available_quote: bool = True,
) -> str:
    cid = _candidate_id(symbol)
    margin = MarginClient(binance, isolated=False, dry_run=getattr(settings, "dry_run", False) or margin_dry_run())
    margin.ensure_isolated_account(symbol)

    desired_notional = float(settings.order_quote_amount)
    use_full_available_quote = use_available_quote and _bool(_env("MOMENTUM_DECISION_BUY_WITH_FULL_QUOTE", "false"), default=False)
    own_quote, free_quote = _safe_margin_quote_notional(margin, symbol, quote, desired_notional, use_full_available=use_full_available_quote) if use_available_quote else (desired_notional, desired_notional)
    min_buy_notional = max(5.0, _float_env("MOMENTUM_DECISION_MIN_BUY_NOTIONAL", 5.0))
    if own_quote < min_buy_notional:
        state.add_event(cid, "momentum_buy_skipped_margin_quote_balance", {"symbol": symbol, "quote": quote, "free_quote": free_quote, "usable_notional": own_quote, "min_buy_notional": min_buy_notional, "decision": decision, "mode": "cross_margin"})
        return f"margin_quote_balance_wait:{quote}:free={free_quote:.4f}:usable={own_quote:.4f}"

    wanted_borrow_quote = max(0.0, own_quote * max(0.0, margin_multiplier() - 1.0))
    borrow_quote = 0.0
    borrow_payload: dict[str, Any] = {}
    borrow_error = None
    if wanted_borrow_quote > 0:
        try:
            max_borrow = margin.max_borrowable(symbol, quote)
            borrow_quote = min(wanted_borrow_quote, max_borrow) if max_borrow > 0 else wanted_borrow_quote
            if borrow_quote > 0:
                borrow_payload = margin.borrow(symbol, quote, amount_str(borrow_quote))
        except Exception as exc:
            borrow_quote = 0.0
            borrow_error = str(exc)
            borrow_payload = {"status": "borrow_failed_continued", "error": borrow_error, "wanted_borrow_quote": wanted_borrow_quote}

    total_quote = own_quote + borrow_quote
    price = binance.current_price(symbol)
    qty = rules.quantity_from_quote(symbol, total_quote, price, market=True)
    order = margin.margin_order(symbol, "BUY", qty, "MARKET")
    fill_price = binance.average_fill_price(order, fallback=price) or price
    acquired = float(order.get("executedQty") or qty) if margin.dry_run else margin.margin_free_balance(symbol, rules.base_asset(symbol))
    state.mark_executed(cid)
    state.add_open_position(cid, {
        "candidate_id": cid,
        "signal_symbol": symbol,
        "execution_symbol": symbol,
        "side": "long",
        "strategy": "momentum_rotation",
        "mode": "cross_margin",
        "margin_isolated": False,
        "quantity": str(order.get("executedQty") or qty),
        "entry_price": float(fill_price),
        "stop_price": None,
        "target_price": None,
        "entry_order_id": order.get("orderId"),
        "momentum_decision": decision,
        "entry_payload": order,
        "borrow_payload": borrow_payload,
        "borrow_error": borrow_error,
        "notional_used": total_quote,
        "own_quote_amount": own_quote,
        "borrow_quote_amount": borrow_quote,
        "quote_asset": quote,
        "confirmed_base_balance": acquired,
    })
    state.add_event(cid, "momentum_bought", {"symbol": symbol, "quantity": str(order.get("executedQty") or qty), "price": fill_price, "notional_used": total_quote, "quote": quote, "confirmed_base_balance": acquired, "order": order, "borrow_payload": borrow_payload, "borrow_error": borrow_error, "decision": decision, "mode": "cross_margin"})
    return f"bought_cross_margin:{symbol}:qty={qty}:notional={total_quote:.4f}"


def _margin_wallet_value(margin: MarginClient, rules: BinanceSymbolRules, binance: BinanceClient, symbol: str) -> tuple[str, float, float, float]:
    base = rules.base_asset(symbol)
    qty = margin.margin_free_balance(symbol, base)
    price = binance.current_price(symbol)
    return base, qty, price, qty * price


def _sell_symbol_cross_margin(binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any], *, require_confirmed: bool = True) -> str:
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    quote = _quote_asset(symbol, load_settings().quote_assets) or str(decision.get("quote_asset") or "USDC").upper()
    margin = MarginClient(binance, isolated=False, dry_run=getattr(load_settings(), "dry_run", False) or margin_dry_run())
    margin.ensure_isolated_account(symbol)
    base, qty, price, value = _margin_wallet_value(margin, rules, binance, symbol)
    dust_value = max(1.0, _float_env("MOMENTUM_DECISION_SELL_DUST_VALUE", 5.0))
    max_attempts = max(1, _int_env("MOMENTUM_DECISION_SELL_MAX_ATTEMPTS", 5))
    chunk_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_SELL_CHUNK_RATIO", 0.995)))
    details: list[dict[str, Any]] = []

    if qty <= 0 or value < dust_value:
        state.add_event(cid, "momentum_sell_confirmed_no_balance", {"symbol": symbol, "base": base, "qty": qty, "value": value, "dust_value": dust_value, "decision": decision, "mode": "cross_margin"})
        return f"sell_confirmed_cross_margin:no_balance:{base}:value={value:.4f}"

    for attempt in range(1, max_attempts + 1):
        before_quote = margin.margin_free_balance(symbol, quote) if not margin.dry_run else 0.0
        base, qty, price, value = _margin_wallet_value(margin, rules, binance, symbol)
        if qty <= 0 or value < dust_value:
            state.add_event(cid, "momentum_sell_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "attempts": attempt - 1, "details": details, "decision": decision, "mode": "cross_margin"})
            return f"sell_confirmed_cross_margin:{symbol}:remaining_value={value:.4f}"

        sell_qty = _market_sell_quantity(rules, symbol, qty * chunk_ratio, price) or _market_sell_quantity(rules, symbol, qty, price)
        if sell_qty is None:
            message = f"sell_quantity_not_tradeable_cross_margin:{symbol}:qty={qty}:value={value:.4f}"
            state.add_event(cid, "momentum_sell_not_tradeable", {"symbol": symbol, "base": base, "qty": qty, "value": value, "dust_value": dust_value, "decision": decision, "mode": "cross_margin"})
            if require_confirmed:
                raise RuntimeError(message)
            return message

        try:
            order = margin.margin_order(symbol, "SELL", sell_qty, "MARKET")
            after_base, after_qty, after_price, after_value = _margin_wallet_value(margin, rules, binance, symbol)
            after_quote = margin.margin_free_balance(symbol, quote) if not margin.dry_run else before_quote
            detail = {"attempt": attempt, "symbol": symbol, "base": base, "quote": quote, "sell_qty": sell_qty, "before_qty": qty, "before_value": value, "before_quote": before_quote, "after_qty": after_qty, "after_value": after_value, "after_quote": after_quote, "order": order, "mode": "cross_margin"}
            details.append(detail)
            state.add_event(cid, "momentum_sell_attempt", {"decision": decision, **detail})
            if margin.dry_run or after_value < dust_value:
                state.close_position(cid, "momentum_sell", {"symbol": symbol, "base": base, "quantity": sell_qty, "order": order, "decision": decision, "remaining_qty": after_qty, "remaining_value": after_value, "details": details, "mode": "cross_margin"}, record_event=False)
                state.add_event(cid, "momentum_sold", {"symbol": symbol, "base": base, "quantity": sell_qty, "order": order, "decision": decision, "remaining_qty": after_qty, "remaining_value": after_value, "quote_after": after_quote, "mode": "cross_margin"})
                return f"sell_confirmed_cross_margin:{symbol}:remaining_value={after_value:.4f}:quote={after_quote:.4f}"
        except Exception as exc:
            details.append({"attempt": attempt, "symbol": symbol, "qty": sell_qty, "error": str(exc), "mode": "cross_margin"})
            state.add_event(cid, "momentum_sell_attempt_failed", {"symbol": symbol, "base": base, "qty": sell_qty, "attempt": attempt, "error": str(exc), "decision": decision, "mode": "cross_margin"})
            if attempt >= max_attempts:
                if require_confirmed:
                    raise RuntimeError(f"sell_not_confirmed_cross_margin:{symbol}:attempts={max_attempts}:last_error={exc}") from exc
                return f"sell_failed_cross_margin:{symbol}:{exc}"

    base, qty, price, value = _margin_wallet_value(margin, rules, binance, symbol)
    if value < dust_value:
        state.add_event(cid, "momentum_sell_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "details": details, "decision": decision, "mode": "cross_margin"})
        return f"sell_confirmed_cross_margin:{symbol}:remaining_value={value:.4f}"
    message = f"sell_not_confirmed_cross_margin:{symbol}:remaining_value={value:.4f}:attempts={max_attempts}"
    state.add_event(cid, "momentum_sell_not_confirmed", {"symbol": symbol, "base": base, "remaining_qty": qty, "remaining_value": value, "details": details, "decision": decision, "mode": "cross_margin"})
    if require_confirmed:
        raise RuntimeError(message)
    return message


def _market_sell_quantity(rules: BinanceSymbolRules, symbol: str, qty: float, price: float) -> str | None:
    try:
        normalized = rules.normalize_market_quantity(symbol, qty)
        rules.ensure_exit_notional(symbol, normalized, price, label="momentum_sell")
        return normalized
    except Exception:
        return None


def _candidate_symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("buy_symbol") or "").upper()


def _candidate_score(row: dict[str, Any]) -> float:
    try:
        return float(row.get("momentum_score") if row.get("momentum_score") is not None else row.get("score") or 0.0)
    except Exception:
        return 0.0


def _nested_dict(row: dict[str, Any], key: str) -> dict[str, Any]:
    value = row.get(key)
    return value if isinstance(value, dict) else {}


def _candidate_payload(row: dict[str, Any]) -> dict[str, Any]:
    return _nested_dict(row, "payload")


def _candidate_remote(row: dict[str, Any]) -> dict[str, Any]:
    payload = _candidate_payload(row)
    return _nested_dict(payload, "remote_candidate") or _nested_dict(payload, "momentum_trade_candidate")


def _candidate_rank(row: dict[str, Any]) -> int | None:
    remote = _candidate_remote(row)
    for value in (row.get("rank"), remote.get("rank"), _candidate_payload(row).get("rank")):
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _candidate_rsi_1h(row: dict[str, Any]) -> float | None:
    remote = _candidate_remote(row)
    for value in (row.get("rsi_1h"), remote.get("rsi_1h"), _candidate_payload(row).get("rsi_1h")):
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _rsi_buy_range() -> tuple[float, float]:
    lower = _float_env("MOMENTUM_BUYABLE_RSI_1H_MIN", 45.0)
    upper = _float_env("MOMENTUM_BUYABLE_RSI_1H_MAX", 55.0)
    return (min(lower, upper), max(lower, upper))


def _candidate_buyable(row: dict[str, Any]) -> tuple[bool, str]:
    rsi = _candidate_rsi_1h(row)
    lower, upper = _rsi_buy_range()
    if rsi is None:
        return False, "missing_rsi_1h"
    if not lower <= rsi <= upper:
        return False, f"rsi_1h_out_of_range:{rsi:g}:range={lower:g}-{upper:g}"
    return True, "buyable_rsi_1h"


def _ordered_momentum_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    indexed = []
    for index, row in enumerate(candidates):
        rank = _candidate_rank(row)
        indexed.append((rank if rank is not None else 10_000 + index, -_candidate_score(row), index, row))
    indexed.sort(key=lambda item: (item[0], item[1], item[2]))
    return [row for _, _, _, row in indexed]


def _enrich_candidate(row: dict[str, Any], *, buyable: bool, reason: str) -> dict[str, Any]:
    return {
        **row,
        "buyable": buyable,
        "buyable_reason": reason,
        "rsi_1h": _candidate_rsi_1h(row),
        "rank": _candidate_rank(row) or row.get("rank"),
    }


def _candidate_quote_supported(row: dict[str, Any], quote_assets: list[str]) -> bool:
    return _quote_asset(_candidate_symbol(row), quote_assets) is not None


def _extract_candidate_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        raw_candidates = payload.get("candidates") or payload.get("items") or payload.get("data") or payload.get("rankings") or []
    elif isinstance(payload, list):
        raw_candidates = payload
    else:
        raw_candidates = []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in raw_candidates:
        if not isinstance(row, dict):
            continue
        symbol = _candidate_symbol(row)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        rows.append({**row, "symbol": symbol, "source": row.get("source") or "momentum_rankings_endpoint"})
    return rows


def fetch_momentum_candidates(limit: int = 50) -> list[dict[str, Any]]:
    settings = load_settings()
    response = requests.get(
        _url(settings.signalmaker_base_url, _candidates_path()),
        params={"limit": limit},
        timeout=30,
        headers={"accept": "application/json", "cache-control": "no-cache"},
    )
    data = _read_json_payload(response)
    if not response.ok:
        raise RuntimeError(f"momentum_rankings_http_error status={response.status_code} url={response.url} payload={data}")
    min_score = _float_env("MOMENTUM_DECISION_MIN_SCORE", 0.0)
    candidates = _extract_candidate_rows(data)
    return [
        row
        for row in candidates
        if _candidate_quote_supported(row, settings.quote_assets) and _candidate_score(row) >= min_score
    ]


def build_decision_from_candidates(candidates: list[dict[str, Any]], *, source: str = DECISION_RANKINGS_SOURCE) -> dict[str, Any]:
    state = StateStore()
    held_symbol = _position_symbol(current_momentum_position(state)) or _last_recorded_buy_without_later_sell(state)
    ordered_candidates = _ordered_momentum_candidates([row for row in candidates if _candidate_symbol(row)])
    buy_candidates: list[dict[str, Any]] = []
    skipped_candidates: list[dict[str, Any]] = []
    held_candidate: dict[str, Any] | None = None

    for row in ordered_candidates:
        symbol = _candidate_symbol(row)
        if symbol == held_symbol:
            held_candidate = row
        buyable, reason = _candidate_buyable(row)
        enriched = _enrich_candidate(row, buyable=buyable, reason=reason)
        if buyable:
            buy_candidates.append(enriched)
        else:
            skipped_candidates.append({"symbol": symbol, "rank": _candidate_rank(row), "rsi_1h": _candidate_rsi_1h(row), "reason": reason})

    top = buy_candidates[0] if buy_candidates else {}
    buy_symbol = _candidate_symbol(top)
    top_rank = _candidate_rank(top) if top else None
    held_rank = _candidate_rank(held_candidate) if held_candidate else None
    if not buy_symbol:
        action = "WAIT"
        should_trade = False
        reason = "no_buyable_momentum_candidates_rsi_1h_45_55"
    elif not held_symbol:
        action = "BUY"
        should_trade = True
        reason = f"buy_best_buyable_momentum:{buy_symbol}:rsi_1h={_candidate_rsi_1h(top)}"
    elif held_symbol == buy_symbol:
        action = "HOLD"
        should_trade = False
        reason = f"hold_best_buyable_momentum:{held_symbol}:rank={held_rank or top_rank or '-'}"
    elif held_rank is None or top_rank is None or top_rank < held_rank:
        action = "ROTATE"
        should_trade = True
        rank_text = f"rank={held_rank or 'unknown'}->{top_rank or 'unknown'}"
        reason = f"rotate_to_better_buyable_momentum:{held_symbol}->{buy_symbol}:{rank_text}:rsi_1h={_candidate_rsi_1h(top)}"
    else:
        action = "HOLD"
        should_trade = False
        reason = f"hold_current_momentum_rank:{held_symbol}:held_rank={held_rank}:best_buyable_rank={top_rank}"

    sell_symbol = held_symbol if action == "ROTATE" else None
    decision_buy_symbol = buy_symbol if action in {"BUY", "ROTATE", "HOLD"} else None
    order_sequence = _rotation_order_sequence(sell_symbol, decision_buy_symbol) if action == "ROTATE" else []
    decision = {
        "mode": "momentum_rotation",
        "action": action,
        "raw_action": action,
        "symbol": buy_symbol or held_symbol or None,
        "buy_symbol": decision_buy_symbol,
        "sell_symbol": sell_symbol,
        "order_sequence": order_sequence,
        "should_trade": should_trade,
        "reason": reason,
        "source": source,
        "buy_candidates": buy_candidates,
        "skipped_candidates": skipped_candidates,
        "target_asset": top or None,
        "fallback_policy": {
            "source": _candidates_path(),
            "min_score": _float_env("MOMENTUM_DECISION_MIN_SCORE", 0.0),
            "rsi_1h_min": _rsi_buy_range()[0],
            "rsi_1h_max": _rsi_buy_range()[1],
        },
    }
    decision["executor_contract"] = {
        "action": action,
        "raw_action": action,
        "symbol": decision["symbol"],
        "buy_symbol": decision["buy_symbol"],
        "sell_symbol": decision["sell_symbol"],
        "order_sequence": order_sequence,
        "should_trade": should_trade,
        "reason": reason,
        "buy_candidates": buy_candidates,
        "skipped_candidates": skipped_candidates,
        "fallback_policy": decision["fallback_policy"],
    }
    return normalize_decision(decision)

def decision_buy_candidates(decision: dict[str, Any], *, exclude: set[str] | None = None) -> list[dict[str, Any]]:
    """Return the ordered buy candidates embedded in the persisted main decision.

    Local fallback for older main deployments. The preferred source is now
    /api/v1/momentum, fetched immediately before buying.
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


def sell_symbol(binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, symbol: str, decision: dict[str, Any], *, require_confirmed: bool = True) -> str:
    """Sell with wallet confirmation and chunk retries before any rotation buy."""
    symbol = symbol.upper()
    cid = _candidate_id(symbol)
    position = state.open_positions().get(cid)
    if _is_cross_margin_position(position):
        return _sell_symbol_cross_margin(binance, rules, state, symbol, decision, require_confirmed=require_confirmed)
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
    quote = _quote_asset(symbol, settings.quote_assets)
    if not quote:
        configured_quotes = ",".join(settings.quote_assets) or "none"
        state.add_event(cid, "momentum_buy_skipped_unsupported_quote", {"symbol": symbol, "configured_quote_assets": settings.quote_assets, "decision": decision})
        return f"unsupported_quote:{symbol}:configured={configured_quotes}"
    min_value = max(5.0, float(settings.order_quote_amount) * 0.5)
    already, reason = _already_have(binance, rules, state, symbol, min_value)
    if already:
        state.add_event(cid, "momentum_buy_skipped_already_have", {"symbol": symbol, "reason": reason, "decision": decision})
        return f"already_have:{reason}"
    if bool(decision.get("force_cross_margin")) or _explicit_cross_margin_requested():
        return _buy_symbol_cross_margin(settings, binance, rules, state, symbol, decision, quote, use_available_quote=use_available_quote)

    desired_notional = float(settings.order_quote_amount)
    use_full_available_quote = use_available_quote and _bool(_env("MOMENTUM_DECISION_BUY_WITH_FULL_QUOTE", "true"), default=True)
    notional, free_quote = _safe_quote_notional(settings, binance, quote, desired_notional, use_full_available=use_full_available_quote) if use_available_quote else (desired_notional, desired_notional)
    min_buy_notional = max(5.0, _float_env("MOMENTUM_DECISION_MIN_BUY_NOTIONAL", 5.0))
    if use_available_quote and notional < min_buy_notional and not binance.dry_run:
        wait_attempts = max(1, _int_env("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", 8))
        wait_sleep = max(0.2, _float_env("MOMENTUM_DECISION_BALANCE_CONFIRM_SLEEP", 1.0))
        reserve = max(0.0, _float_env("MOMENTUM_DECISION_QUOTE_RESERVE", 1.0))
        safety_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_BUY_BALANCE_RATIO", 0.995)))
        required_free_quote = reserve + (min_buy_notional / safety_ratio)
        confirmed_free_quote = _wait_for_quote_notional(binance, quote, required_free_quote, attempts=wait_attempts, sleep_sec=wait_sleep)
        notional, free_quote = _safe_quote_notional(
            settings,
            binance,
            quote,
            desired_notional,
            use_full_available=use_full_available_quote,
            observed_free_quote=confirmed_free_quote,
        )
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
        "strategy": "momentum_rotation",
        "mode": "momentum_rotation",
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
    return str(result).startswith("bought:") or str(result).startswith("bought_cross_margin:") or str(result).startswith("already_have:")


def _buy_result_balance_wait(result: str) -> bool:
    text = str(result)
    return text.startswith("margin_quote_balance_wait:")


def buy_best_available(settings, binance: BinanceClient, rules: BinanceSymbolRules, state: StateStore, decision: dict[str, Any], *, exclude: set[str] | None = None) -> str:
    if not _bool(_env("MOMENTUM_DECISION_FALLBACK_ENABLED"), default=True):
        symbol = str(decision.get("buy_symbol") or decision.get("symbol") or "").upper()
        row = {"symbol": symbol, **(decision.get("target_asset") if isinstance(decision.get("target_asset"), dict) else {})}
        buyable, reason = _candidate_buyable(row)
        if not buyable:
            state.add_event(_candidate_id(symbol), "momentum_buy_candidate_not_buyable", {"symbol": symbol, "rank": _candidate_rank(row), "rsi_1h": _candidate_rsi_1h(row), "reason": reason, "decision": decision})
            return f"not_buyable:{symbol}:{reason}"
        return buy_symbol(settings, binance, rules, state, symbol, decision, use_available_quote=True)

    attempts: list[dict[str, Any]] = []
    max_attempts = max(1, _int_env("MOMENTUM_DECISION_FALLBACK_MAX_ATTEMPTS", 10))
    exclude_set = {item.upper() for item in (exclude or set()) if item}
    try:
        candidates = []
        for row in fetch_momentum_candidates(limit=max_attempts * 2):
            symbol = _candidate_symbol(row)
            buyable, reason = _candidate_buyable(row)
            if symbol in exclude_set or not _candidate_quote_supported(row, settings.quote_assets):
                continue
            if not buyable:
                state.add_event(_candidate_id(symbol), "momentum_buy_candidate_not_buyable", {"symbol": symbol, "rank": _candidate_rank(row), "rsi_1h": _candidate_rsi_1h(row), "reason": reason, "decision": decision})
                continue
            candidates.append(_enrich_candidate(row, buyable=True, reason=reason))
    except Exception as exc:
        candidates = []
        state.add_event("momentum-decision", "momentum_rankings_fetch_failed", {"error": str(exc), "decision": decision})
    decision_candidates = []
    for row in decision_buy_candidates(decision, exclude=exclude_set | {_candidate_symbol(row) for row in candidates}):
        buyable, reason = _candidate_buyable(row)
        if buyable:
            decision_candidates.append(_enrich_candidate(row, buyable=True, reason=reason))
        else:
            state.add_event(_candidate_id(_candidate_symbol(row)), "momentum_buy_candidate_not_buyable", {"symbol": _candidate_symbol(row), "rank": _candidate_rank(row), "rsi_1h": _candidate_rsi_1h(row), "reason": reason, "decision": decision})
    candidates.extend(_ordered_momentum_candidates(decision_candidates))
    candidates = _ordered_momentum_candidates(candidates)
    if not candidates:
        state.add_event("momentum-decision", "momentum_fallback_no_candidates", {"decision": decision})
        return "fallback_buy_exhausted:no_candidates"

    for row in candidates[:max_attempts]:
        symbol = _candidate_symbol(row)
        trial_decision = {**decision, "buy_symbol": symbol, "symbol": symbol, "fallback_target_asset": row}
        try:
            result = buy_symbol(settings, binance, rules, state, symbol, trial_decision, use_available_quote=True)
            attempts.append({"symbol": symbol, "result": result, "rank": row.get("rank"), "score": row.get("momentum_score") or row.get("score"), "rsi_1h": _candidate_rsi_1h(row), "buyable_reason": row.get("buyable_reason"), "source": row.get("source")})
            if _buy_result_ok(result):
                state.add_event("momentum-decision", "momentum_fallback_buy_selected", {"selected_symbol": symbol, "attempts": attempts, "decision": decision})
                return f"fallback_buy:{symbol}:{result}"
            if _buy_result_balance_wait(result):
                state.add_event("momentum-decision", "momentum_fallback_buy_balance_wait", {"selected_symbol": symbol, "attempts": attempts, "decision": decision})
                return f"fallback_buy_balance_wait:{symbol}:{result}"
        except Exception as exc:
            attempts.append({"symbol": symbol, "error": str(exc), "rank": row.get("rank"), "score": row.get("momentum_score") or row.get("score"), "rsi_1h": _candidate_rsi_1h(row), "buyable_reason": row.get("buyable_reason"), "source": row.get("source")})
            state.add_event(_candidate_id(symbol), "momentum_fallback_buy_failed", {"symbol": symbol, "error": str(exc), "rank": row.get("rank"), "score": row.get("momentum_score") or row.get("score"), "rsi_1h": _candidate_rsi_1h(row), "buyable_reason": row.get("buyable_reason"), "source": row.get("source"), "decision": decision})
            continue

    state.add_event("momentum-decision", "momentum_fallback_buy_exhausted", {"attempts": attempts, "decision": decision})
    return f"fallback_buy_exhausted:attempts={len(attempts)}"


def current_momentum_position(state: StateStore) -> dict[str, Any] | None:
    for candidate_id, position in state.open_positions().items():
        if str(candidate_id).startswith("momentum-") or isinstance(position.get("momentum_decision"), dict):
            return position
    return None


def _position_symbol(position: dict[str, Any] | None) -> str:
    if not position:
        return ""
    return str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()


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


def execute_decision(decision: dict[str, Any]) -> str:
    if not _bool(_env("MOMENTUM_DECISION_EXECUTE_ENABLED"), default=True):
        return "execution_disabled"
    settings = load_settings()
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    rules = BinanceSymbolRules(settings.binance_base_url)
    state = StateStore()
    action = str(decision.get("action") or "WAIT").upper()
    should_trade = bool(decision.get("should_trade"))
    sell = str(decision.get("sell_symbol") or "").upper()
    held_position = current_momentum_position(state)
    held_symbol = _position_symbol(held_position) or sell

    if not should_trade or action in {"WAIT", "HOLD"}:
        return f"wait:{action}"
    if action == "BUY":
        buy = str(decision.get("buy_symbol") or decision.get("symbol") or "").upper()
        if held_symbol and (not buy or buy == held_symbol):
            return f"hold_existing_momentum_position:{held_symbol}"
        if held_symbol and buy != held_symbol:
            force_cross_margin = _is_cross_margin_position(held_position)
            rotation_decision = {
                **decision,
                "action": "ROTATE",
                "raw_action": decision.get("raw_action") or "BUY",
                "sell_symbol": held_symbol,
                "buy_symbol": buy,
                "symbol": buy,
                "order_sequence": _rotation_order_sequence(held_symbol, buy),
                **({"force_cross_margin": True} if force_cross_margin else {}),
            }
            state.add_event("momentum-decision", "momentum_rotation_started", {"sell_symbol": held_symbol, "buy_symbol": buy, "order_sequence": rotation_decision.get("order_sequence"), "decision": rotation_decision})
            sell_result = sell_symbol(binance, rules, state, held_symbol, rotation_decision, require_confirmed=True)
            buy_result = buy_best_available(settings, binance, rules, state, rotation_decision, exclude={held_symbol})
            return f"rotate:{sell_result}:{buy_result}"
        cadence = _momentum_buy_cadence_status(state)
        decision.setdefault("due_now", cadence["due_now"])
        decision.setdefault("next_check_at", cadence["next_check_at"])
        if not cadence["due_now"]:
            state.add_event("momentum-decision", "momentum_decision_cadence_wait", {"cadence": cadence, "decision": decision})
            return f"wait:momentum_cadence:next_check_at={cadence['next_check_at']}"
        _mark_momentum_buy_cadence_checked(state, decision, cadence)
        return buy_best_available(settings, binance, rules, state, decision, exclude={sell})
    if action == "SELL":
        if not _decision_targets_held_symbol(decision, held_symbol):
            return f"sell_blocked:decision_not_on_held_momentum_asset:held={held_symbol or 'none'}:sell={sell or 'none'}"
        return sell_symbol(binance, rules, state, held_symbol, decision, require_confirmed=True)
    if action == "ROTATE":
        if not _decision_targets_held_symbol(decision, held_symbol):
            return f"rotate_blocked:decision_not_on_held_momentum_asset:held={held_symbol or 'none'}:sell={sell or 'none'}"
        force_cross_margin = _is_cross_margin_position(held_position)
        rotation_decision = {**decision, "force_cross_margin": True} if force_cross_margin else decision
        state.add_event("momentum-decision", "momentum_rotation_started", {"sell_symbol": held_symbol, "buy_symbol": rotation_decision.get("buy_symbol"), "order_sequence": rotation_decision.get("order_sequence"), "decision": rotation_decision})
        sell_result = sell_symbol(binance, rules, state, held_symbol, rotation_decision, require_confirmed=True)
        buy_result = buy_best_available(settings, binance, rules, state, rotation_decision, exclude={held_symbol})
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
        "buy_candidates_count": len(decision_buy_candidates(decision)),
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
            logger.info("momentum decision action=%s symbol=%s should_trade=%s buy_symbol=%s sell_symbol=%s candidates=%s result=%s", decision.get("action"), decision.get("symbol"), decision.get("should_trade"), decision.get("buy_symbol"), decision.get("sell_symbol"), len(decision_buy_candidates(decision)), output.get("execution_result"))
        except Exception as exc:
            logger.error("momentum decision feed error=%s", str(exc))
            try:
                StateStore().add_event("momentum-decision", "momentum_decision_error", {"error": str(exc)})
            except Exception:
                pass
        time.sleep(poll_seconds)


if __name__ == "__main__":
    run_loop()
