import os
import time
from datetime import datetime, timezone

from raspberry_executor.classic_candidate_executor import execute_classic_candidate
from raspberry_executor.config import load_settings
from raspberry_executor.exchange_factory import create_margin_exchange
from raspberry_executor.local_candidate_store import mark_candidate_executed, upsert_remote_candidates
from raspberry_executor.logging_setup import setup_logging
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


def ensure_candidate_visible(candidate: dict) -> None:
    try:
        upsert_remote_candidates([candidate])
    except Exception as exc:
        logger.warning("failed to upsert candidate locally candidate=%s error=%s", candidate.get("candidate_id"), exc)


def set_candidate_executed(candidate_id: str) -> None:
    try:
        mark_candidate_executed(candidate_id)
    except Exception as exc:
        logger.warning("failed to mark local candidate executed candidate=%s error=%s", candidate_id, exc)


def is_margin_unavailable(text: str) -> bool:
    low = str(text or "").lower()
    return any(x in low for x in [
        "not support",
        "not supported",
        "not exist",
        "does not exist",
        "margin account does not exist",
        "invalid symbol",
        "-1121",
        "-11001",
        "-3028",
    ])


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
    entry_payload = result.get("entry_payload") if isinstance(result, dict) else {}
    confirm_payload = result.get("entry_confirm_payload") if isinstance(result, dict) else {}
    borrow_payload = result.get("borrow_payload") if isinstance(result, dict) else {}
    return bool(
        result.get("dry_run")
        or (isinstance(entry_payload, dict) and entry_payload.get("dry_run"))
        or (isinstance(confirm_payload, dict) and confirm_payload.get("confirmed_dry_run"))
        or (isinstance(borrow_payload, dict) and borrow_payload.get("status") == "dry_run")
    )


def margin_unavailable_error(state: StateStore, candidate_id: str, candidate: dict, symbol: str, side: str, error: str) -> str:
    ensure_candidate_visible(candidate)
    state.mark_executed(candidate_id)
    set_candidate_executed(candidate_id)
    remove_pending(candidate_id)
    state.add_event(candidate_id, "margin_unavailable_error", {"error": error, "symbol": symbol, "side": side, "candidate": candidate})
    logger.error("margin unavailable candidate=%s symbol=%s side=%s error=%s", candidate_id, symbol, side, error)
    return "margin_unavailable_error"


def mark_signal_done(state: StateStore, candidate_id: str, fingerprint: str) -> None:
    state.mark_executed(candidate_id)
    set_candidate_executed(candidate_id)
    if signal_fingerprint_enabled():
        state.mark_executed_fingerprint(fingerprint)


def save_dry_run_simulation(state: StateStore, candidate_id: str, fingerprint: str, candidate: dict, symbol: str, side: str, result: dict) -> None:
    ensure_candidate_visible(candidate)
    mark_signal_done(state, candidate_id, fingerprint)
    remove_pending(candidate_id)
    state.add_event(candidate_id, "position_simulated_dry_run", {"symbol": symbol, "side": side, "quantity": result.get("quantity"), "entry_price": result.get("entry_price"), "target_price": candidate.get("target_price"), "stop_price": candidate.get("stop_price"), "entry_order_id": result.get("entry_order_id"), "dry_run": True, "candidate": candidate, "margin_payload": result})


def save_short_position(state: StateStore, candidate_id: str, fingerprint: str, candidate: dict, symbol: str, result: dict) -> None:
    ensure_candidate_visible(candidate)
    if result_is_dry_run(result):
        save_dry_run_simulation(state, candidate_id, fingerprint, candidate, symbol, "short", result)
        return
    mark_signal_done(state, candidate_id, fingerprint)
    remove_pending(candidate_id)
    state.add_open_position(candidate_id, {"candidate_id": candidate_id, "signal_fingerprint": fingerprint, "signal_symbol": candidate["symbol"], "execution_symbol": symbol, "side": "short", "mode": result.get("mode") or "margin", "margin_isolated": result.get("margin_isolated"), "quantity": result.get("quantity"), "entry_price": float(result.get("entry_price") or 0), "stop_price": candidate.get("stop_price"), "target_price": candidate.get("target_price"), "entry_order_id": result.get("entry_order_id"), "borrow_base_amount": result.get("borrow_base_amount"), "base_asset": result.get("base_asset"), "candidate": candidate, "margin_payload": result, "borrow_payload": result.get("borrow_payload") or {}, "entry_payload": result.get("entry_payload") or {}})


def save_long_position(state: StateStore, candidate_id: str, fingerprint: str, candidate: dict, symbol: str, manager: MarginOrderManager, result: dict) -> None:
    ensure_candidate_visible(candidate)
    if result_is_dry_run(result):
        save_dry_run_simulation(state, candidate_id, fingerprint, candidate, symbol, "long", result)
        return
    mark_signal_done(state, candidate_id, fingerprint)
    remove_pending(candidate_id)
    state.add_open_position(candidate_id, {"candidate_id": candidate_id, "signal_fingerprint": fingerprint, "signal_symbol": candidate["symbol"], "execution_symbol": symbol, "side": "long", "mode": "isolated_margin" if manager.margin.isolated else "cross_margin", "margin_isolated": manager.margin.isolated, "quantity": result["quantity"], "entry_price": float(result["entry_price"]), "stop_price": candidate.get("stop_price"), "target_price": float(candidate["target_price"]), "entry_order_id": result.get("entry_order_id"), "oco_order_list_id": None, "tp_order_id": result.get("tp_order_id"), "sl_order_id": None, "exit_strategy": "take_profit_only", "candidate": candidate, "margin_payload": result, "entry_payload": result.get("entry_payload") or {}, "tp_payload": result.get("tp_payload") or {}, "needs_tp_replay": not bool(result.get("tp_order_id")), "tp_error": result.get("tp_error")})


def queue_margin_token_limit(state: StateStore, candidate_id: str, candidate: dict, symbol: str, side: str, error: str, *, from_queue: bool = False) -> str:
    ensure_candidate_visible(candidate)
    if from_queue:
        bump_pending(candidate_id, f"token_collateral_limit_retry:{error}")
    else:
        add_pending(candidate, f"token_collateral_limit:{error}")
    state.add_event(candidate_id, "margin_token_collateral_limit_retry_scheduled", {"error": error, "symbol": symbol, "side": side, "retry_seconds": token_limit_retry_seconds(), "max_attempts": token_limit_max_attempts(), "candidate": candidate})
    logger.warning("margin token collateral limit queued retry candidate=%s symbol=%s side=%s retry_seconds=%s error=%s", candidate_id, symbol, side, token_limit_retry_seconds(), error)
    return "token_collateral_retry_scheduled"


def process_candidate(settings, kraken, rules, manager: MarginOrderManager, spot_manager: SpotOrderManager, state: StateStore, guard: RiskGuard, candidate: dict, *, from_queue: bool = False) -> str:
    """Compatibility wrapper around the unified classic candidate executor.

    Classic long candidates now use one path: validate once, try cross-margin
    BUY MARKET with leverage, fall back to spot BUY MARKET, then place the
    matching take-profit SELL LIMIT for the mode actually opened.
    """
    return execute_classic_candidate(settings, kraken, rules, manager, spot_manager, state, guard, candidate, from_queue=from_queue)

def process_pending(settings, kraken, rules, manager, spot_manager, state, guard) -> dict:
    stats = {"pending_checked": 0, "pending_retried": 0, "pending_waiting_cooldown": 0, "pending_opened": 0, "pending_errors": 0, "pending_dropped": 0}
    cooldown = token_limit_retry_seconds()
    max_attempts = token_limit_max_attempts()
    for item in list_pending(limit=pending_retry_limit()):
        reason = str(item.get("reason") or "")
        if "token_collateral_limit" not in reason:
            continue
        stats["pending_checked"] += 1
        candidate_id = item["candidate_id"]
        attempts = int(item.get("attempts") or 0)
        if attempts >= max_attempts:
            remove_pending(candidate_id)
            state.mark_executed(candidate_id)
            set_candidate_executed(candidate_id)
            state.add_event(candidate_id, "margin_token_collateral_limit_retry_exhausted", {"symbol": item.get("symbol"), "side": item.get("side"), "attempts": attempts, "reason": reason})
            stats["pending_dropped"] += 1
            continue
        age = _age_seconds(item.get("updated_at"))
        if age < cooldown:
            stats["pending_waiting_cooldown"] += 1
            continue
        stats["pending_retried"] += 1
        result = process_candidate(settings, kraken, rules, manager, spot_manager, state, guard, item["candidate"], from_queue=True)
        if result in {"opened", "opened_spot_fallback"}:
            stats["pending_opened"] += 1
        elif result == "error":
            stats["pending_errors"] += 1
    return stats


def main() -> None:
    settings = load_settings()
    if not margin_enabled():
        logger.warning("margin executor started while MARGIN_MODE_ENABLED is false")
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    kraken, margin, rules = create_margin_exchange(settings, isolated=margin_isolated(), dry_run=margin_dry_run())
    manager = MarginOrderManager(kraken, margin, rules)
    spot_manager = SpotOrderManager(kraken, rules)
    state = StateStore()
    guard = RiskGuard(settings.quote_assets, settings.max_candidate_age_seconds)
    limit = candidate_fetch_limit()
    logger.info("Raspberry margin executor started exchange=%s dry_run=%s isolated=%s shorts_enabled=%s signal_fingerprint_dedupe=%s token_retry_seconds=%s token_max_attempts=%s spot_fallback=enabled", getattr(kraken, "exchange_name", "kraken"), margin.dry_run, margin.isolated, shorts_enabled(), signal_fingerprint_enabled(), token_limit_retry_seconds(), token_limit_max_attempts())
    while True:
        try:
            candidates = signalmaker.get_open_candidates(limit=limit)
            stats = {"fetched": len(candidates), "opened": 0, "opened_spot_fallback": 0, "errors": 0, "margin_unavailable": 0, "duplicate_signal": 0, "skipped": 0, "shorts_disabled": 0, "insufficient_balance": 0, "token_collateral_retry_scheduled": 0, "simulated_dry_run": 0}
            for candidate in candidates:
                result = process_candidate(settings, kraken, rules, manager, spot_manager, state, guard, candidate)
                if result == "opened": stats["opened"] += 1
                elif result == "opened_spot_fallback": stats["opened_spot_fallback"] += 1
                elif result == "error": stats["errors"] += 1
                elif result == "margin_unavailable_error": stats["margin_unavailable"] += 1
                elif result == "duplicate_signal": stats["duplicate_signal"] += 1
                elif result == "insufficient_balance": stats["insufficient_balance"] += 1
                elif result == "shorts_disabled": stats["shorts_disabled"] += 1
                elif result == "token_collateral_retry_scheduled": stats["token_collateral_retry_scheduled"] += 1
                elif result == "simulated_dry_run": stats["simulated_dry_run"] += 1
                else: stats["skipped"] += 1
            pending_stats = process_pending(settings, kraken, rules, manager, spot_manager, state, guard)
            logger.info("margin executor summary=%s pending=%s", stats, pending_stats)
        except Exception as exc:
            logger.error("margin executor loop error=%s", str(exc))
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
