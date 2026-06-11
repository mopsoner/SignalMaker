import os
import time
from typing import Any

import requests

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import read_env
from raspberry_executor.logging_setup import setup_logging
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
    return normalize_decision(data)


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
    held_symbol = _position_symbol(current_momentum_position(state))
    ordered_candidates = _ordered_momentum_candidates([row for row in candidates if _candidate_symbol(row)])
    buy_candidates: list[dict[str, Any]] = []
    skipped_candidates: list[dict[str, Any]] = []
    held_candidate: dict[str, Any] | None = None

    for row in ordered_candidates:
        symbol = _candidate_symbol(row)
        if symbol == held_symbol:
            held_candidate = row
        buyable, reason = _candidate_buyable(row)
        enriched = {**row, "buyable": buyable, "buyable_reason": reason, "rsi_1h": _candidate_rsi_1h(row), "rank": _candidate_rank(row) or row.get("rank")}
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

    decision = {
        "mode": "momentum_rotation",
        "action": action,
        "raw_action": action,
        "symbol": buy_symbol or held_symbol or None,
        "buy_symbol": buy_symbol if action in {"BUY", "ROTATE", "HOLD"} else None,
        "sell_symbol": held_symbol if action == "ROTATE" else None,
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

    desired_notional = float(settings.order_quote_amount)
    notional, free_quote = _safe_quote_notional(settings, binance, quote, desired_notional) if use_available_quote else (desired_notional, desired_notional)
    min_buy_notional = max(5.0, _float_env("MOMENTUM_DECISION_MIN_BUY_NOTIONAL", 5.0))
    if use_available_quote and notional < min_buy_notional and not binance.dry_run:
        wait_attempts = max(1, _int_env("MOMENTUM_DECISION_BALANCE_CONFIRM_ATTEMPTS", 8))
        wait_sleep = max(0.2, _float_env("MOMENTUM_DECISION_BALANCE_CONFIRM_SLEEP", 1.0))
        reserve = max(0.0, _float_env("MOMENTUM_DECISION_QUOTE_RESERVE", 1.0))
        safety_ratio = min(1.0, max(0.1, _float_env("MOMENTUM_DECISION_BUY_BALANCE_RATIO", 0.995)))
        required_free_quote = reserve + (min_buy_notional / safety_ratio)
        free_quote = _wait_for_quote_notional(binance, quote, required_free_quote, attempts=wait_attempts, sleep_sec=wait_sleep)
        notional, free_quote = _safe_quote_notional(settings, binance, quote, desired_notional)
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
    return str(result).startswith("bought:") or str(result).startswith("already_have:")


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
            candidates.append({**row, "buyable": True, "buyable_reason": reason, "rsi_1h": _candidate_rsi_1h(row), "rank": _candidate_rank(row) or row.get("rank")})
    except Exception as exc:
        candidates = []
        state.add_event("momentum-decision", "momentum_rankings_fetch_failed", {"error": str(exc), "decision": decision})
    decision_candidates = []
    for row in decision_buy_candidates(decision, exclude=exclude_set | {_candidate_symbol(row) for row in candidates}):
        buyable, reason = _candidate_buyable(row)
        if buyable:
            decision_candidates.append({**row, "buyable": True, "buyable_reason": reason, "rsi_1h": _candidate_rsi_1h(row), "rank": _candidate_rank(row) or row.get("rank")})
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
    if not held_symbol:
        return False
    symbols = {str(decision.get("sell_symbol") or "").upper(), str(decision.get("symbol") or "").upper()}
    return held_symbol in symbols


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
    held_symbol = _position_symbol(held_position)

    if not should_trade or action in {"WAIT", "HOLD"}:
        return f"wait:{action}"
    if action == "BUY":
        if held_symbol:
            return f"hold_existing_momentum_position:{held_symbol}"
        return buy_best_available(settings, binance, rules, state, decision, exclude={sell})
    if action == "SELL":
        if not _decision_targets_held_symbol(decision, held_symbol):
            return f"sell_blocked:decision_not_on_held_momentum_asset:held={held_symbol or 'none'}:sell={sell or 'none'}"
        return sell_symbol(binance, rules, state, held_symbol, decision, require_confirmed=True)
    if action == "ROTATE":
        if not _decision_targets_held_symbol(decision, held_symbol):
            return f"rotate_blocked:decision_not_on_held_momentum_asset:held={held_symbol or 'none'}:sell={sell or 'none'}"
        sell_result = sell_symbol(binance, rules, state, held_symbol, decision, require_confirmed=True)
        buy_result = buy_best_available(settings, binance, rules, state, decision, exclude={held_symbol})
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
