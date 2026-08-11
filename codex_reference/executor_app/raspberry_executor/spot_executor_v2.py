import os
import time
from typing import Any

from raspberry_executor.exchange_factory import create_spot_exchange
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.pending_trade_queue import add_pending, bump_pending, list_pending, remove_pending
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-executor-v2")


def candidate_fetch_limit() -> int:
    try:
        return max(10, int(os.getenv("CANDIDATE_FETCH_LIMIT", "100") or "100"))
    except Exception:
        return 100


def quote_asset_for_symbol(symbol: str, quote_assets: list[str]) -> str | None:
    upper = symbol.upper()
    for quote in sorted([q.upper() for q in quote_assets], key=len, reverse=True):
        if upper.endswith(quote):
            return quote
    return None


def take_profit_window_still_valid(kraken: Any, symbol: str, target_price: float) -> tuple[bool, float, str]:
    current = kraken.current_price(symbol)
    if not (float(target_price) > current):
        return False, current, f"invalid_take_profit_window target={target_price} current={current}"
    return True, current, "ok"


def sell_all_spot_balance(kraken: Any, rules: Any, state: StateStore, candidate: dict, symbol: str) -> str:
    candidate_id = candidate["candidate_id"]
    base = rules.base_asset(symbol)
    free_qty = kraken.free_balance(base)
    if free_qty <= 0:
        return f"no_free_balance:{base}"
    price = kraken.current_price(symbol)
    qty = rules.normalize_market_quantity(symbol, free_qty)
    rules.ensure_exit_notional(symbol, qty, price, label="spot_sell_on_short")
    order = kraken.place_market_entry(symbol, "short", qty)
    state.mark_executed(candidate_id)
    state.add_event(candidate_id, "spot_balance_sold_on_short", {
        "symbol": symbol,
        "base_asset": base,
        "quantity": qty,
        "price": price,
        "candidate": candidate,
        "order": order,
    })
    logger.info("spot balance sold on short candidate=%s symbol=%s base=%s qty=%s", candidate_id, symbol, base, qty)
    return "sold"


def open_long(settings, order_manager: SpotOrderManager, state: StateStore, candidate: dict, symbol: str) -> str:
    candidate_id = candidate["candidate_id"]
    result = order_manager.open_long_with_take_profit(
        symbol=symbol,
        quote_amount=settings.order_quote_amount,
        target_price=float(candidate["target_price"]),
    )
    state.mark_executed(candidate_id)
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": candidate["symbol"],
        "execution_symbol": symbol,
        "side": "long",
        "quantity": result["quantity"],
        "entry_price": float(result["entry_price"]),
        "stop_price": candidate.get("stop_price"),
        "target_price": float(candidate["target_price"]),
        "entry_order_id": result.get("entry_order_id"),
        "oco_order_list_id": None,
        "tp_order_id": result.get("tp_order_id"),
        "sl_order_id": None,
        "exit_strategy": "take_profit_only",
        "candidate": candidate,
        "entry_payload": result.get("entry_payload") or {},
        "tp_payload": result.get("tp_payload") or {},
        "needs_tp_replay": not bool(result.get("tp_order_id")),
        "tp_error": result.get("tp_error"),
    })
    logger.info("long opened candidate=%s symbol=%s qty=%s tp=%s", candidate_id, symbol, result["quantity"], result.get("tp_order_id"))
    return "opened"


def should_queue_long_error(error_text: str) -> bool:
    low = error_text.lower()
    return "insufficient balance" in low or "account has insufficient balance" in low or "-2010" in low


def process_candidate(settings, kraken, rules, order_manager, state, guard, candidate: dict, *, from_queue: bool = False) -> str:
    candidate_id = candidate.get("candidate_id")
    if not candidate_id:
        return "missing_candidate_id"
    accepted, reason = guard.accept(candidate, already_executed=state.already_executed(candidate_id))
    if not accepted:
        if from_queue and reason == "already_executed_locally":
            remove_pending(candidate_id)
        return reason

    symbol = guard.execution_symbol(candidate)
    side = guard.normalize_side(str(candidate.get("side", "")))

    if side == "short":
        result = sell_all_spot_balance(kraken, rules, state, candidate, symbol)
        if result == "sold" and from_queue:
            remove_pending(candidate_id)
        return result

    valid, current, why = take_profit_window_still_valid(kraken, symbol, float(candidate["target_price"]))
    if not valid:
        if from_queue:
            bump_pending(candidate_id, why)
        return why

    quote = quote_asset_for_symbol(symbol, settings.quote_assets)
    if quote:
        free_quote = kraken.free_balance(quote)
        if not kraken.dry_run and free_quote < float(settings.order_quote_amount):
            add_pending(candidate, f"quote_balance_wait:{quote}:{free_quote}")
            return f"queued_quote_balance_wait:{quote}"

    try:
        result = open_long(settings, order_manager, state, candidate, symbol)
        if from_queue:
            remove_pending(candidate_id)
        return result
    except Exception as exc:
        error_text = str(exc)
        if should_queue_long_error(error_text):
            add_pending(candidate, error_text)
            return "queued_insufficient_balance"
        logger.error("long execution failed candidate=%s error=%s", candidate_id, error_text)
        state.add_event(candidate_id, "execution_error", {"error": error_text, "candidate": candidate})
        return "error"


def process_pending(settings, kraken, rules, order_manager, state, guard) -> dict:
    stats = {"pending_checked": 0, "pending_opened": 0, "pending_sold": 0, "pending_waiting": 0, "pending_errors": 0}
    for item in list_pending(limit=50):
        stats["pending_checked"] += 1
        result = process_candidate(settings, kraken, rules, order_manager, state, guard, item["candidate"], from_queue=True)
        if result == "opened":
            stats["pending_opened"] += 1
        elif result == "sold":
            stats["pending_sold"] += 1
        elif result == "error":
            stats["pending_errors"] += 1
        else:
            stats["pending_waiting"] += 1
    return stats


def main() -> None:
    settings = load_settings()
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    kraken, rules = create_spot_exchange(settings)
    order_manager = SpotOrderManager(kraken, rules)
    state = StateStore()
    guard = RiskGuard(settings.quote_assets, settings.max_candidate_age_seconds)
    fetch_limit = candidate_fetch_limit()

    logger.info("Raspberry spot executor v2 started gateway_id=%s dry_run=%s quote_assets=%s order_quote_amount=%s candidate_fetch_limit=%s", settings.gateway_id, settings.dry_run, settings.quote_assets, settings.order_quote_amount, fetch_limit)
    while True:
        try:
            candidates = signalmaker.get_open_candidates(limit=fetch_limit)
            stats = {"fetched": len(candidates), "opened": 0, "sold": 0, "queued": 0, "errors": 0, "skipped": 0}
            for candidate in candidates:
                result = process_candidate(settings, kraken, rules, order_manager, state, guard, candidate)
                if result == "opened":
                    stats["opened"] += 1
                elif result == "sold":
                    stats["sold"] += 1
                elif str(result).startswith("queued"):
                    stats["queued"] += 1
                elif result == "error":
                    stats["errors"] += 1
                else:
                    stats["skipped"] += 1
            pending_stats = process_pending(settings, kraken, rules, order_manager, state, guard)
            logger.info("executor summary=%s pending=%s", stats, pending_stats)
        except Exception as exc:
            logger.error("executor loop error=%s", str(exc))
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
