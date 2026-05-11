import os
import time

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import margin_dry_run, margin_enabled, margin_isolated, shorts_enabled
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.spot_executor_v2 import process_candidate as process_spot_candidate
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-margin-executor")


def candidate_fetch_limit() -> int:
    return max(10, int(os.getenv("CANDIDATE_FETCH_LIMIT", "100") or "100"))


def log_skipped_disabled_shorts() -> bool:
    return str(os.getenv("LOG_SKIPPED_DISABLED_SHORTS", "false") or "false").lower() in {"1", "true", "yes", "on"}


def is_margin_unavailable(text: str) -> bool:
    low = str(text or "").lower()
    return any(x in low for x in ["not support", "not supported", "not exist", "does not exist", "margin account", "isolated", "invalid symbol", "-1121", "-11001", "-3028"])


def is_insufficient_balance(text: str) -> bool:
    low = str(text or "").lower()
    return any(x in low for x in [
        "insufficient balance", "insufficient account balance", "balance was too low", "available balance was too low",
        "margin_insufficient_quote_balance", "margin_long_no_quote_available", "-2010", "-2019",
    ])


def fallback_spot(settings, binance, rules, spot_manager, state, guard, candidate, reason: str) -> str:
    cid = str(candidate.get("candidate_id") or "")
    logger.warning("margin unavailable fallback spot candidate=%s reason=%s", cid, reason)
    state.add_event(cid, "margin_fallback_spot", {"reason": reason, "candidate": candidate})
    return process_spot_candidate(settings, binance, rules, spot_manager, state, guard, candidate)


def save_short_position(state: StateStore, candidate_id: str, candidate: dict, symbol: str, result: dict) -> None:
    state.mark_executed(candidate_id)
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": candidate["symbol"],
        "execution_symbol": symbol,
        "side": "short",
        "mode": result.get("mode") or "margin",
        "margin_isolated": result.get("margin_isolated"),
        "quantity": result.get("quantity"),
        "entry_price": float(result.get("entry_price") or 0),
        "stop_price": candidate.get("stop_price"),
        "target_price": candidate.get("target_price"),
        "entry_order_id": result.get("entry_order_id"),
        "borrow_base_amount": result.get("borrow_base_amount"),
        "base_asset": result.get("base_asset"),
        "candidate": candidate,
        "margin_payload": result,
        "borrow_payload": result.get("borrow_payload") or {},
        "entry_payload": result.get("entry_payload") or {},
    })


def save_long_position(state: StateStore, candidate_id: str, candidate: dict, symbol: str, manager: MarginOrderManager, result: dict) -> None:
    state.mark_executed(candidate_id)
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": candidate["symbol"],
        "execution_symbol": symbol,
        "side": "long",
        "mode": "isolated_margin" if manager.margin.isolated else "cross_margin",
        "margin_isolated": manager.margin.isolated,
        "quantity": result["quantity"],
        "entry_price": float(result["entry_price"]),
        "stop_price": float(candidate["stop_price"]),
        "target_price": float(candidate["target_price"]),
        "entry_order_id": result.get("entry_order_id"),
        "oco_order_list_id": result.get("oco_order_list_id"),
        "tp_order_id": result.get("tp_order_id"),
        "sl_order_id": result.get("sl_order_id"),
        "candidate": candidate,
        "margin_payload": result,
        "entry_payload": result.get("entry_payload") or {},
        "oco_payload": result.get("oco_payload") or {},
        "needs_oco_repair": not bool(result.get("tp_order_id") and result.get("sl_order_id")),
        "oco_error": result.get("oco_error"),
    })


def process_candidate(settings, binance, rules, manager: MarginOrderManager, spot_manager: SpotOrderManager, state: StateStore, guard: RiskGuard, candidate: dict) -> str:
    candidate_id = candidate.get("candidate_id")
    if not candidate_id:
        return "missing_candidate_id"
    accepted, reason = guard.accept(candidate, already_executed=state.already_executed(candidate_id))
    if not accepted:
        return reason

    symbol = guard.execution_symbol(candidate)
    side = guard.normalize_side(str(candidate.get("side", "")))

    if state.has_open_position_for(symbol, side):
        state.mark_executed(candidate_id)
        state.add_event(candidate_id, "candidate_skipped_open_position_exists", {"symbol": symbol, "side": side, "candidate": candidate})
        logger.warning("candidate skipped because open position already exists candidate=%s symbol=%s side=%s", candidate_id, symbol, side)
        return "open_position_exists"

    if side == "short" and not shorts_enabled():
        if log_skipped_disabled_shorts():
            state.add_event(candidate_id, "short_skipped_disabled", {"symbol": symbol, "candidate": candidate})
        return "shorts_disabled"

    try:
        manager.margin.ensure_isolated_account(symbol)
    except Exception as exc:
        text = str(exc)
        if is_margin_unavailable(text):
            return fallback_spot(settings, binance, rules, spot_manager, state, guard, candidate, text)
        state.add_event(candidate_id, "margin_setup_error", {"error": text, "candidate": candidate})
        logger.error("margin setup failed candidate=%s error=%s", candidate_id, text)
        return "error"

    if side == "short":
        try:
            result = manager.open_short_with_margin_borrow_sell(symbol=symbol, quote_amount=float(settings.order_quote_amount))
            save_short_position(state, candidate_id, candidate, symbol, result)
            logger.info("margin short opened candidate=%s symbol=%s mode=%s qty=%s", candidate_id, symbol, result.get("mode"), result.get("quantity"))
            return "opened"
        except Exception as exc:
            text = str(exc)
            if is_margin_unavailable(text):
                return fallback_spot(settings, binance, rules, spot_manager, state, guard, candidate, text)
            if is_insufficient_balance(text):
                state.add_event(candidate_id, "margin_skipped_insufficient_balance", {"error": text, "symbol": symbol, "side": side, "candidate": candidate})
                logger.warning("margin short skipped insufficient balance candidate=%s symbol=%s error=%s", candidate_id, symbol, text)
                return "insufficient_balance"
            state.add_event(candidate_id, "margin_execution_error", {"error": text, "candidate": candidate})
            logger.error("margin short failed candidate=%s error=%s", candidate_id, text)
            return "error"

    try:
        result = manager.open_long_with_margin_oco(symbol=symbol, quote_amount=float(settings.order_quote_amount), target_price=float(candidate["target_price"]), stop_price=float(candidate["stop_price"]))
    except Exception as exc:
        text = str(exc)
        if is_margin_unavailable(text):
            return fallback_spot(settings, binance, rules, spot_manager, state, guard, candidate, text)
        if is_insufficient_balance(text):
            state.add_event(candidate_id, "margin_skipped_insufficient_balance", {"error": text, "symbol": symbol, "side": side, "candidate": candidate})
            logger.warning("margin long skipped insufficient balance candidate=%s symbol=%s error=%s", candidate_id, symbol, text)
            return "insufficient_balance"
        state.add_event(candidate_id, "margin_execution_error", {"error": text, "candidate": candidate})
        logger.error("margin long failed candidate=%s error=%s", candidate_id, text)
        return "error"

    save_long_position(state, candidate_id, candidate, symbol, manager, result)
    if result.get("oco_error"):
        state.add_event(candidate_id, "position_opened_needs_oco_repair", {"symbol": symbol, "error": result.get("oco_error"), "result": result})
        logger.warning("margin long opened without oco candidate=%s symbol=%s qty=%s error=%s", candidate_id, symbol, result.get("quantity"), result.get("oco_error"))
        return "opened_needs_oco_repair"
    logger.info("margin long opened candidate=%s symbol=%s qty=%s oco=%s quote_guard=%s", candidate_id, symbol, result["quantity"], result.get("oco_order_list_id"), result.get("quote_balance_guard"))
    return "opened"


def main() -> None:
    settings = load_settings()
    if not margin_enabled():
        logger.warning("margin executor started while MARGIN_MODE_ENABLED is false")
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run or margin_dry_run())
    rules = BinanceSymbolRules(settings.binance_base_url)
    margin = MarginClient(binance, isolated=margin_isolated(), dry_run=settings.dry_run or margin_dry_run())
    manager = MarginOrderManager(binance, margin, rules)
    spot_manager = SpotOrderManager(binance, rules)
    state = StateStore()
    guard = RiskGuard(settings.quote_assets, settings.max_candidate_age_seconds)
    limit = candidate_fetch_limit()
    logger.info("Raspberry margin executor started dry_run=%s isolated=%s shorts_enabled=%s log_disabled_shorts=%s fallback=spot", margin.dry_run, margin.isolated, shorts_enabled(), log_skipped_disabled_shorts())
    while True:
        try:
            candidates = signalmaker.get_open_candidates(limit=limit)
            stats = {"fetched": len(candidates), "opened": 0, "opened_needs_oco_repair": 0, "sold": 0, "errors": 0, "skipped": 0, "shorts_disabled": 0, "insufficient_balance": 0}
            for candidate in candidates:
                result = process_candidate(settings, binance, rules, manager, spot_manager, state, guard, candidate)
                if result == "opened": stats["opened"] += 1
                elif result == "opened_needs_oco_repair": stats["opened_needs_oco_repair"] += 1
                elif result == "sold": stats["sold"] += 1
                elif result == "error": stats["errors"] += 1
                elif result == "insufficient_balance": stats["insufficient_balance"] += 1
                elif result == "shorts_disabled": stats["shorts_disabled"] += 1
                else: stats["skipped"] += 1
            logger.info("margin executor summary=%s", stats)
        except Exception as exc:
            logger.error("margin executor loop error=%s", str(exc))
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
