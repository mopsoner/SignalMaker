from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from raspberry_executor.local_candidate_store import mark_candidate_executed, upsert_remote_candidates
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import margin_enabled
from raspberry_executor.pending_trade_queue import remove_pending
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("classic-candidate-executor")


def _order_id(payload: dict | None):
    if not payload:
        return None
    return payload.get("orderId") or payload.get("order_id")


def _executed_qty(payload: dict, fallback: float | str) -> float:
    try:
        value = float(payload.get("executedQty") or 0)
        return value if value > 0 else float(fallback)
    except Exception:
        return float(fallback)


def _quantity_from_quote(price: float, quote_amount: float) -> float:
    if price <= 0:
        raise RuntimeError("Invalid current price")
    return round(float(quote_amount) / float(price), 6)


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


def is_insufficient_balance(text: str) -> bool:
    low = str(text or "").lower()
    return any(x in low for x in ["insufficient balance", "insufficient account balance", "balance was too low", "available balance was too low", "margin_insufficient_quote_balance", "margin_long_no_quote_available", "-2010", "-2019"])


def _ensure_candidate_visible(candidate: dict) -> None:
    try:
        upsert_remote_candidates([candidate])
    except Exception as exc:
        logger.warning("failed to upsert candidate locally candidate=%s error=%s", candidate.get("candidate_id"), exc)


def _mark_remote_executed(candidate_id: str) -> None:
    try:
        mark_candidate_executed(candidate_id)
    except Exception as exc:
        logger.warning("failed to mark local candidate executed candidate=%s error=%s", candidate_id, exc)


@dataclass(frozen=True)
class NormalizedCandidate:
    candidate_id: str
    symbol: str
    side: str
    fingerprint: str
    target_price: float


def normalize_and_validate_candidate(state: StateStore, guard: RiskGuard, candidate: dict) -> tuple[NormalizedCandidate | None, str | None]:
    candidate_id = candidate.get("candidate_id")
    if not candidate_id:
        return None, "missing_candidate_id"
    accepted, reason = guard.accept(candidate, already_executed=state.already_executed(candidate_id))
    if not accepted:
        return None, reason
    symbol = guard.execution_symbol(candidate)
    side = guard.normalize_side(str(candidate.get("side", "")))
    if side != "long":
        return None, "classic_executor_long_only"
    return NormalizedCandidate(candidate_id, symbol, side, signal_fingerprint(symbol, side, candidate), float(candidate["target_price"])), None


def _mark_done(state: StateStore, normalized: NormalizedCandidate) -> None:
    state.mark_executed(normalized.candidate_id)
    state.mark_executed_fingerprint(normalized.fingerprint)
    _mark_remote_executed(normalized.candidate_id)
    remove_pending(normalized.candidate_id)


def _save_position(state: StateStore, normalized: NormalizedCandidate, candidate: dict, result: dict, mode: str, exchange: str) -> None:
    _mark_done(state, normalized)
    state.add_open_position(normalized.candidate_id, {
        "candidate_id": normalized.candidate_id,
        "signal_fingerprint": normalized.fingerprint,
        "signal_symbol": candidate["symbol"],
        "execution_symbol": normalized.symbol,
        "side": normalized.side,
        "mode": mode,
        "margin_isolated": False if mode == "cross_margin" else None,
        "quantity": result["quantity"],
        "entry_price": float(result["entry_price"]),
        "stop_price": candidate.get("stop_price"),
        "target_price": normalized.target_price,
        "entry_order_id": result.get("entry_order_id"),
        "tp_order_id": result.get("tp_order_id"),
        "exit_strategy": "take_profit_only",
        "candidate": candidate,
        "entry_payload": result.get("entry_payload") or {},
        "tp_payload": result.get("tp_payload") or {},
        "margin_payload": result if mode == "cross_margin" else {},
        "exchange": exchange,
    })


def _open_margin_long(manager: MarginOrderManager, symbol: str, quote_amount: float, target_price: float) -> dict:
    result = manager.open_long_with_margin_take_profit(symbol=symbol, quote_amount=quote_amount, target_price=target_price)
    if result.get("tp_error"):
        raise RuntimeError(result.get("tp_error"))
    result["mode"] = "cross_margin"
    return result


def _open_spot_long(spot_manager: SpotOrderManager | None, exchange: Any, symbol: str, quote_amount: float, target_price: float) -> dict:
    if spot_manager is not None:
        result = spot_manager.open_long_with_take_profit(symbol=symbol, quote_amount=quote_amount, target_price=target_price)
        result["mode"] = "spot"
        return result
    current_price = exchange.current_price(symbol)
    quantity = _quantity_from_quote(current_price, quote_amount)
    entry = exchange.place_market_entry(symbol, "long", quantity)
    entry_price = exchange.average_fill_price(entry, fallback=current_price) if hasattr(exchange, "average_fill_price") else current_price
    executed_qty = _executed_qty(entry, quantity)
    tp = exchange.place_exit_limit(symbol, "long", executed_qty, target_price)
    return {"symbol": symbol, "side": "long", "mode": "spot", "quantity": executed_qty, "entry_price": entry_price, "entry_order_id": _order_id(entry), "tp_order_id": _order_id(tp), "entry_payload": entry, "tp_payload": tp}


def execute_classic_candidate(settings, exchange: Any, rules: Any, margin_manager: MarginOrderManager | None, spot_manager: SpotOrderManager | None, state: StateStore, guard: RiskGuard, candidate: dict, *, from_queue: bool = False) -> str:
    _ensure_candidate_visible(candidate)
    normalized, skip_reason = normalize_and_validate_candidate(state, guard, candidate)
    if normalized is None:
        if from_queue and skip_reason == "already_executed_locally":
            remove_pending(candidate.get("candidate_id"))
        return skip_reason or "skipped"
    if state.already_executed_fingerprint(normalized.fingerprint):
        _mark_done(state, normalized)
        state.add_event(normalized.candidate_id, "candidate_skipped_duplicate_signal", {"symbol": normalized.symbol, "side": normalized.side, "signal_fingerprint": normalized.fingerprint, "candidate": candidate})
        return "duplicate_signal"
    if state.has_open_position_for(normalized.symbol, normalized.side):
        _mark_done(state, normalized)
        state.add_event(normalized.candidate_id, "candidate_skipped_open_position_exists", {"symbol": normalized.symbol, "side": normalized.side, "signal_fingerprint": normalized.fingerprint, "candidate": candidate})
        return "open_position_exists"

    exchange_name = getattr(exchange, "exchange_name", getattr(settings, "exchange", "kraken"))
    margin_error = None
    if margin_manager is not None and margin_enabled():
        try:
            result = _open_margin_long(margin_manager, normalized.symbol, float(settings.order_quote_amount), normalized.target_price)
            _save_position(state, normalized, candidate, result, "cross_margin", exchange_name)
            logger.info("classic candidate opened on margin candidate=%s symbol=%s qty=%s tp=%s", normalized.candidate_id, normalized.symbol, result.get("quantity"), result.get("tp_order_id"))
            return "opened"
        except Exception as exc:
            margin_error = str(exc)
            state.add_event(normalized.candidate_id, "margin_fallback_to_spot", {"error": margin_error, "symbol": normalized.symbol, "candidate": candidate})
            if not (is_margin_unavailable(margin_error) or is_insufficient_balance(margin_error) or margin_error):
                logger.warning("margin rejected, falling back to spot candidate=%s error=%s", normalized.candidate_id, margin_error)

    try:
        result = _open_spot_long(spot_manager, exchange, normalized.symbol, float(settings.order_quote_amount), normalized.target_price)
        _save_position(state, normalized, candidate, result, "spot", exchange_name)
        logger.info("classic candidate opened on spot candidate=%s symbol=%s qty=%s tp=%s margin_error=%s", normalized.candidate_id, normalized.symbol, result.get("quantity"), result.get("tp_order_id"), margin_error)
        return "opened_spot_fallback" if margin_error else "opened"
    except Exception as exc:
        state.add_event(normalized.candidate_id, "execution_error", {"error": str(exc), "margin_error": margin_error, "candidate": candidate})
        logger.exception("classic execution failed candidate=%s", normalized.candidate_id)
        return "error"
