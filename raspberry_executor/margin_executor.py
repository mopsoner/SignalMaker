import os
import time
from datetime import datetime, timezone

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import margin_dry_run, margin_enabled, margin_isolated, shorts_enabled
from raspberry_executor.pending_trade_queue import add_pending, bump_pending, list_pending, remove_pending
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-margin-executor")


def candidate_fetch_limit() -> int:
    return max(10, int(os.getenv("CANDIDATE_FETCH_LIMIT", "100") or "100"))


def token_limit_retry_seconds() -> int:
    return max(60, int(os.getenv("TOKEN_COLLATERAL_RETRY_SECONDS", "900") or "900"))


def token_limit_max_attempts() -> int:
    return max(1, int(os.getenv("TOKEN_COLLATERAL_MAX_ATTEMPTS", "24") or "24"))


def pending_retry_limit() -> int:
    return max(1, int(os.getenv("PENDING_TRADE_RETRY_LIMIT", "30") or "30"))


def signal_fingerprint_enabled() -> bool:
    return str(os.getenv("SIGNAL_FINGERPRINT_DEDUPE_ENABLED", "true") or "true").lower() in {"1", "true", "yes", "on"}


def log_skipped_disabled_shorts() -> bool:
    return str(os.getenv("LOG_SKIPPED_DISABLED_SHORTS", "false") or "false").lower() in {"1", "true", "yes", "on"}


def _price_key(value) -> str:
    try:
        return f"{float(value):.10f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value or "")


def signal_fingerprint(symbol: str, side: str, candidate: dict) -> str:
    return "|".join([
        str(symbol or candidate.get("symbol") or "").upper(),
        str(side or candidate.get("side") or "").lower(),
        _price_key(candidate.get("stop_price")),
        _price_key(candidate.get("target_price")),
    ])


def is_margin_unavailable(text: str) -> bool:
    low = str(text or "").lower()
    return any(x in low for x in ["not support", "not supported", "not exist", "does not exist", "margin account does not exist", "invalid symbol", "-1121", "-11001", "-3028"])


def is_margin_token_collateral_limit(text: str) -> bool:
    low = str(text or "").lower()
    return any(x in low for x in ["-3087", "platform max pledged collateral amount", "max transfer in quantity is 0", "reaches platform max pledged collateral"])


def is_insufficient_balance(text: str) -> bool:
    low = str(text or "").lower()
    return any(x in low for x in ["insufficient balance", "insufficient account balance", "balance was too low", "available balance was too low", "margin_insufficient_quote_balance", "margin_long_no_quote_available", "-2010", "-2019"])


def _age_seconds(iso_value: str | None) -> float:
    if not iso_value:
        return 10**9
    try:
        dt = datetime.fromisoformat(str(iso_value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
    except Exception:
        return 10**9


def result_is_dry_run(result: dict) -> bool:
    return bool(
        result.get("dry_run")
        or (isinstance(result.get("entry_payload"), dict) and result["entry_payload"].get("dry_run"))
        or (isinstance(result.get("entry_confirm_payload"), dict) and result["entry_confirm_payload"].get("confirmed_dry_run"))
    )


def margin_unavailable_error(state: StateStore, candidate_id: str, candidate: dict, symbol: str, side: str, error: str) -> str:
    state.mark_executed(candidate_id)
    remove_pending(candidate_id)
    state.add_event(candidate_id, "margin_unavailable_error", {"error": error, "symbol": symbol, "side": side, "candidate": candidate})
    logger.error("margin unavailable candidate=%s symbol=%s side=%s error=%s", candidate_id, symbol, side, error)
    return "margin_unavailable_error"


def mark_signal_done(state: StateStore, candidate_id: str, fingerprint: str) -> None:
    state.mark_executed(candidate_id)
    if signal_fingerprint_enabled():
        state.mark_executed_fingerprint(fingerprint)


def save_dry_run_simulation(state: StateStore, candidate_id: str, fingerprint: str, candidate: dict, symbol: str, side: str, result: dict) -> None:
    mark_signal_done(state, candidate_id, fingerprint)
    remove_pending(candidate_id)
    state.add_event(candidate_id, "position_simulated_dry_run", {"symbol": symbol, "side": side, "quantity": result.get("quantity"), "entry_price": result.get("entry_price"), "target_price": candidate.get("target_price"), "stop_price": candidate.get("stop_price"), "entry_order_id": result.get("entry_order_id"), "dry_run": True, "candidate": candidate, "margin_payload": result})


def save_short_position(state: StateStore, candidate_id: str, fingerprint: str, candidate: dict, symbol: str, result: dict) -> None:
    if result_is_dry_run(result):
        save_dry_run_simulation(state, candidate_id, fingerprint, candidate, symbol, "short", result)
        return
    mark_signal_done(state, candidate_id, fingerprint)
    remove_pending(candidate_id)
    state.add_open_position(candidate_id, {"candidate_id": candidate_id, "signal_fingerprint": fingerprint, "signal_symbol": candidate["symbol"], "execution_symbol": symbol, "side": "short", "mode": result.get("mode") or "margin", "margin_isolated": result.get("margin_isolated"), "quantity": result.get("quantity"), "entry_price": float(result.get("entry_price") or 0), "stop_price": candidate.get("stop_price"), "target_price": candidate.get("target_price"), "entry_order_id": result.get("entry_order_id"), "borrow_base_amount": result.get("borrow_base_amount"), "base_asset": result.get("base_asset"), "candidate": candidate, "margin_payload": result, "borrow_payload": result.get("borrow_payload") or {}, "entry_payload": result.get("entry_payload") or {}})


def save_long_position(state: StateStore, candidate_id: str, fingerprint: str, candidate: dict, symbol: str, manager: MarginOrderManager, result: dict) -> None:
    if result_is_dry_run(result):
        save_dry_run_simulation(state, candidate_id, fingerprint, candidate, symbol, "long", result)
        return
    mark_signal_done(state, candidate_id, fingerprint)
    remove_pending(candidate_id)
    state.add_open_position(candidate_id, {"candidate_id": candidate_id, "signal_fingerprint": fingerprint, "signal_symbol": candidate["symbol"], "execution_symbol": symbol, "side": "long", "mode": "isolated_margin" if manager.margin.isolated else "cross_margin", "margin_isolated": manager.margin.isolated, "quantity": result["quantity"], "entry_price": float(result["entry_price"]), "stop_price": float(candidate["stop_price"]), "target_price": float(candidate["target_price"]), "entry_order_id": result.get("entry_order_id"), "oco_order_list_id": result.get("oco_order_list_id"), "tp_order_id": result.get("tp_order_id"), "sl_order_id": result.get("sl_order_id"), "candidate": candidate, "margin_payload": result, "entry_payload": result.get("entry_payload") or {}, "oco_payload": result.get("oco_payload") or {}, "needs_oco_repair": not bool(result.get("tp_order_id") and result.get("sl_order_id")), "oco_error": result.get("oco_error")})


def queue_margin_token_limit(state: StateStore, candidate_id: str, candidate: dict, symbol: str, side: str, error: str, *, from_queue: bool = False) -> str:
    if from_queue:
        bump_pending(candidate_id, f"token_collateral_limit_retry:{error}")
    else:
        add_pending(candidate, f"token_collateral_limit:{error}")
    state.add_event(candidate_id, "margin_token_collateral_limit_retry_scheduled", {"error": error, "symbol": symbol, "side": side, "retry_seconds": token_limit_retry_seconds(), "max_attempts": token_limit_max_attempts(), "candidate": candidate})
    logger.warning("margin token collateral limit queued retry candidate=%s symbol=%s side=%s retry_seconds=%s error=%s", candidate_id, symbol, side, token_limit_retry_seconds(), error)
    return "token_collateral_retry_scheduled"


# Keep the rest of the executor implementation unchanged by loading it from the previous module text.
# This file is intentionally overwritten by the patch below if needed.
