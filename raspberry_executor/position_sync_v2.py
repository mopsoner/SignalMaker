import os
import time
from datetime import datetime, timezone

from raspberry_executor.exchange_factory import create_margin_exchange
from raspberry_executor.candidate_levels import levels_for_position
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import margin_dry_run
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-position-sync")

TP_REPLAY_SKIP_EVENT_COOLDOWN_SECONDS = 300


def max_tp_replay_attempts() -> int:
    try:
        return max(1, int(os.getenv("MAX_TP_REPLAY_ATTEMPTS", os.getenv("MAX_OCO_REPAIR_ATTEMPTS", "8")) or "8"))
    except Exception:
        return 8


def tp_replay_fractions() -> list[float]:
    raw = os.getenv("TP_REPLAY_FRACTIONS", "1,0.5,0.25,0.125,0.0625") or "1,0.5,0.25,0.125,0.0625"
    values: list[float] = []
    for item in raw.split(","):
        try:
            value = float(item.strip())
        except Exception:
            continue
        if 0 < value <= 1:
            values.append(value)
    return values or [1.0, 0.5, 0.25, 0.125, 0.0625]


def auto_close_ghost_positions() -> bool:
    return str(os.getenv("AUTO_CLOSE_GHOST_POSITIONS", "true") or "true").lower() in {"1", "true", "yes", "on"}


def ghost_base_epsilon() -> float:
    try:
        return max(0.0, float(os.getenv("GHOST_BASE_EPSILON", "0.00000001") or "0.00000001"))
    except Exception:
        return 0.00000001


def tp_fallback_enabled() -> bool:
    return str(os.getenv("TP_FALLBACK_LEVELS_ENABLED", os.getenv("OCO_FALLBACK_LEVELS_ENABLED", "true")) or "true").lower() in {"1", "true", "yes", "on"}


def tp_fallback_pct() -> float:
    try:
        return max(0.001, float(os.getenv("TP_FALLBACK_PCT", os.getenv("OCO_FALLBACK_TP_PCT", "0.10")) or "0.10"))
    except Exception:
        return 0.10


def _status(payload):
    return str((payload or {}).get("status") or "").upper()


def _float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _has_sync_error(payload: dict | None) -> bool:
    return isinstance(payload, dict) and bool(payload.get("sync_error"))


def _is_not_found_error(payload: dict | None) -> bool:
    if not _has_sync_error(payload):
        return False
    text = str(payload.get("sync_error") or "").lower()
    return any(x in text for x in ["-2013", "order does not exist", "unknown order", "not found"])


def _is_momentum_position(candidate_id: str, position: dict) -> bool:
    strategy = str(position.get("strategy") or "").lower()
    return str(candidate_id).startswith("momentum-") or isinstance(position.get("momentum_decision"), dict) or strategy == "momentum_rotation"


LEGACY_MARGIN_MODES = {"cross", "cross_margin", "isolated", "isolated_margin", "margin"}
LEGACY_SPOT_MODES = {"spot", "cash"}


def normalize_position_execution_mode(position: dict) -> dict:
    """Normalize legacy persisted execution modes at the migration boundary."""
    mode = str(position.get("mode") or "").strip().lower()
    normalized = dict(position)
    if mode in LEGACY_MARGIN_MODES:
        normalized["mode"] = "margin"
        normalized["margin_account_mode"] = "cross"
        normalized["margin_isolated"] = False
    elif mode in LEGACY_SPOT_MODES:
        normalized["mode"] = "spot"
        normalized.pop("margin_account_mode", None)
        normalized.pop("margin_isolated", None)
    return normalized


def _is_margin_position(position: dict) -> bool:
    mode = str(position.get("mode") or "").lower()
    if mode == "spot":
        return False
    if mode == "margin":
        return True
    logger.warning("unknown_position_execution_mode mode=%s candidate=%s", mode, position.get("candidate_id"))
    return False


def momentum_balance_missing_grace_seconds() -> float:
    try:
        return max(0.0, float(os.getenv("MOMENTUM_BALANCE_MISSING_GRACE_SECONDS", "120") or "120"))
    except Exception:
        return 120.0


def _parse_opened_at(value) -> float | None:
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _position_age_seconds(position: dict) -> float | None:
    opened_ts = _parse_opened_at(position.get("opened_at"))
    if opened_ts is None:
        return None
    return max(0.0, time.time() - opened_ts)

def momentum_dust_value() -> float:
    try:
        return max(0.0, float(os.getenv("MOMENTUM_POSITION_DUST_VALUE", "5.0") or "5.0"))
    except Exception:
        return 5.0


def _track_momentum_position(candidate_id: str, position: dict, symbol: str, kraken, rules, state, margin=None) -> bool:
    qty = _float(position.get("quantity"))
    entry = _float(position.get("entry_price"))
    side = str(position.get("side") or "long").lower()
    try:
        mark = kraken.current_price(symbol)
    except Exception as exc:
        state.update_open_position(candidate_id, {"position_tracker": "momentum", "mark_price_error": str(exc)})
        logger.warning("momentum mark lookup failed candidate=%s symbol=%s error=%s", candidate_id, symbol, str(exc))
        return False

    pnl = ((mark - entry) * qty) if side != "short" else ((entry - mark) * qty)
    updates = {"position_tracker": "momentum", "strategy": position.get("strategy") or "momentum_rotation", "mark_price": mark, "unrealized_pnl": pnl, "last_position_sync_ts": time.time()}
    if not kraken.dry_run:
        try:
            base = rules.base_asset(symbol)
            use_margin = _is_margin_position(position)
            if use_margin and margin is not None:
                available_base = margin.margin_free_balance(symbol, base)
                balance_source = "margin"
            else:
                available_base = kraken.free_balance(base)
                balance_source = "spot"
            value = available_base * mark
            updates.update({"base_asset": base, "available_base_balance": available_base, "available_base_value": value, "balance_source": balance_source})
            if value < momentum_dust_value():
                age_seconds = _position_age_seconds(position)
                grace_seconds = momentum_balance_missing_grace_seconds()
                if age_seconds is not None and age_seconds < grace_seconds:
                    updates.update({"balance_missing_grace_until_age_seconds": grace_seconds, "balance_missing_age_seconds": age_seconds})
                    state.update_open_position(candidate_id, updates, event_type="momentum_balance_missing_grace")
                    logger.info("deferred momentum balance missing candidate=%s symbol=%s source=%s value=%s age=%s grace=%s", candidate_id, symbol, balance_source, value, age_seconds, grace_seconds)
                    return False
                payload = {"symbol": symbol, "base_asset": base, "available_base_balance": available_base, "available_base_value": value, "dust_value": momentum_dust_value(), "mark_price": mark, "unrealized_pnl": pnl, "balance_source": balance_source, "age_seconds": age_seconds, "grace_seconds": grace_seconds, "position": position}
                state.close_position(candidate_id, "momentum_balance_missing", payload)
                logger.info("closed momentum position candidate=%s symbol=%s reason=balance_missing source=%s value=%s", candidate_id, symbol, balance_source, value)
                return True
        except Exception as exc:
            updates["balance_sync_error"] = str(exc)
    state.update_open_position(candidate_id, updates)
    return False


def _order(kraken, margin, symbol, order_id, *, use_margin: bool):
    if not order_id:
        return None
    try:
        if use_margin:
            return margin.get_margin_order(symbol, order_id)
        return kraken.get_order(symbol, order_id)
    except Exception as exc:
        return {"orderId": order_id, "sync_error": str(exc)}


def _levels(position, symbol):
    if position.get("target_price") is not None:
        return {"target_price": position.get("target_price"), "source": "local_position"}
    matched = levels_for_position(position, symbol)
    if matched and matched.get("target_price") is not None:
        return {"target_price": matched.get("target_price"), "source": matched.get("source") or "candidate_levels", **matched}
    return None


def _fallback_levels(kraken, symbol: str, position: dict) -> dict | None:
    if not tp_fallback_enabled() or kraken is None:
        return None
    current = float(kraken.current_price(symbol))
    tp_pct = tp_fallback_pct()
    side = str(position.get("side") or "long").lower()
    if side == "short":
        return {"target_price": current * (1.0 - tp_pct), "source": "fallback_current_price_tp_short", "current_price": current, "tp_pct": tp_pct}
    return {"target_price": current * (1.0 + tp_pct), "source": "fallback_current_price_tp", "current_price": current, "tp_pct": tp_pct}


def _available_base_balance(kraken, margin, rules, symbol: str, *, use_margin: bool) -> float | None:
    base = rules.base_asset(symbol)
    try:
        if use_margin:
            return float(margin.margin_free_balance(symbol, base))
        return float(kraken.free_balance(base))
    except Exception:
        return None


def _entry_executed_quantity(entry_payload: dict | None, fallback: float) -> float:
    payload = entry_payload or {}
    for key in ("executedQty", "origQty", "quantity"):
        qty = _float(payload.get(key))
        if qty > 0:
            return qty
    return fallback


def _repair_quantity(position: dict, available_base: float | None) -> tuple[bool, float, str]:
    expected_qty = _float(position.get("quantity"))
    if expected_qty <= 0:
        return False, 0.0, "missing_quantity"
    if available_base is None:
        return True, expected_qty, "balance_unchecked"
    if available_base <= 0:
        return False, expected_qty, f"base_balance_empty expected={expected_qty} available={available_base}"
    return True, min(expected_qty, available_base), "ok"


def _order_type(order: dict) -> str:
    return str(order.get("type") or order.get("origType") or "").upper()


def _order_qty(order: dict) -> float:
    return _float(order.get("origQty") or order.get("quantity") or order.get("executedQty"))


def _is_tp_order(order: dict) -> bool:
    order_type = _order_type(order)
    return str(order.get("side") or "").upper() == "SELL" and order_type in {"LIMIT", "LIMIT_MAKER"}


def _list_open_orders(kraken, margin, symbol: str, *, use_margin: bool) -> list[dict]:
    try:
        if use_margin:
            return margin.open_margin_orders(symbol)
        return kraken.open_orders(symbol)
    except Exception as exc:
        logger.warning("open orders lookup failed symbol=%s use_margin=%s error=%s", symbol, use_margin, str(exc))
        return []


def _ghost_check(candidate_id: str, position: dict, symbol: str, kraken, margin, rules, state, *, use_margin: bool) -> bool:
    if not auto_close_ghost_positions() or kraken.dry_run:
        return False
    entry_id = position.get("entry_order_id")
    entry_payload = _order(kraken, margin, symbol, entry_id, use_margin=use_margin) if entry_id else None
    entry_missing = (not entry_id) or _is_not_found_error(entry_payload)
    entry_status = _status(entry_payload)
    entry_live = bool(entry_status and entry_status not in {"CANCELED", "REJECTED", "EXPIRED"} and not _has_sync_error(entry_payload))
    open_orders = _list_open_orders(kraken, margin, symbol, use_margin=use_margin)
    available_base = _available_base_balance(kraken, margin, rules, symbol, use_margin=use_margin)
    has_base = available_base is not None and available_base > ghost_base_epsilon()
    if entry_missing and not entry_live and not open_orders and not has_base:
        payload = {"symbol": symbol, "mode": position.get("mode"), "reason": "no_entry_order_no_open_orders_no_base_balance", "entry_order_id": entry_id, "entry_lookup": entry_payload, "available_base": available_base, "open_orders_count": len(open_orders), "position": position}
        state.close_position(candidate_id, "ghost_position_removed", payload)
        logger.warning("ghost open position removed candidate=%s symbol=%s mode=%s", candidate_id, symbol, position.get("mode"))
        return True
    return False


def _attach_existing_take_profit(candidate_id, position, symbol, kraken, margin, state, *, use_margin: bool) -> bool:
    expected_qty = _float(position.get("quantity"))
    if expected_qty <= 0:
        return False
    open_orders = _list_open_orders(kraken, margin, symbol, use_margin=use_margin)
    if not open_orders:
        return False
    min_required = expected_qty * 0.02
    tp_candidates = []
    for order in open_orders:
        qty = _order_qty(order)
        if qty < min_required:
            continue
        if _is_tp_order(order):
            tp_candidates.append(order)
    if not tp_candidates:
        return False
    target = _float(position.get("target_price"))
    if target > 0:
        tp = sorted(tp_candidates, key=lambda o: abs(_float(o.get("price")) - target))[0]
    else:
        tp = sorted(tp_candidates, key=lambda o: _float(o.get("price")), reverse=True)[0]
    protected_qty = _order_qty(tp)
    updates = {
        "tp_order_id": tp.get("orderId"),
        "sl_order_id": None,
        "oco_order_list_id": None,
        "target_price": _float(tp.get("price"), target),
        "order_monitor_mode": "margin" if use_margin else "spot",
        "exit_strategy": "take_profit_only",
        "attached_existing_tp_order": tp,
        "tp_protected_quantity": protected_qty,
        "tp_unprotected_quantity": max(0.0, expected_qty - protected_qty),
        "needs_tp_replay": False,
        "tp_replay_blocked": False,
        "tp_replay_attempts": 0,
        "last_tp_replay_skip_reason": None,
        "last_tp_replay_skip_ts": None,
    }
    state.update_open_position(candidate_id, updates, event_type="tp_existing_order_attached")
    logger.info("attached existing take-profit order candidate=%s symbol=%s tp=%s qty=%s", candidate_id, symbol, tp.get("orderId"), protected_qty)
    return True


def _should_emit_skip_event(position: dict, reason: str) -> bool:
    last_reason = str(position.get("last_tp_replay_skip_reason") or "")
    last_ts = _float(position.get("last_tp_replay_skip_ts"), 0.0)
    return reason != last_reason or (time.time() - last_ts) >= TP_REPLAY_SKIP_EVENT_COOLDOWN_SECONDS


def _block_replay_if_needed(state, candidate_id: str, position: dict, reason: str, payload: dict) -> bool:
    attempts = _int(position.get("tp_replay_attempts"), 0) + 1
    updates = {"tp_replay_attempts": attempts, "last_tp_replay_attempt_ts": time.time(), "last_tp_replay_reason": reason, "needs_tp_replay": True}
    if attempts >= max_tp_replay_attempts():
        updates.update({"tp_replay_blocked": True, "tp_replay_blocked_reason": reason, "tp_replay_blocked_payload": payload, "tp_replay_blocked_ts": time.time()})
        state.update_open_position(candidate_id, updates, event_type="tp_replay_blocked")
        logger.error("tp replay blocked candidate=%s attempts=%s reason=%s", candidate_id, attempts, reason)
        return True
    state.update_open_position(candidate_id, updates)
    return False


def _mark_replay_skip(state, candidate_id: str, position: dict, event_type: str, payload: dict) -> None:
    reason = str(payload.get("reason") or event_type)
    blocked = _block_replay_if_needed(state, candidate_id, position, reason, payload)
    if blocked:
        return
    updates = {"last_tp_replay_skip_reason": reason, "last_tp_replay_skip_ts": time.time(), "last_tp_replay_skip_event": event_type, "last_tp_replay_skip_payload": payload, "needs_tp_replay": True}
    if _should_emit_skip_event(position, reason):
        state.update_open_position(candidate_id, updates, event_type=event_type)
    else:
        state.update_open_position(candidate_id, updates)


def _is_replay_blocked(position: dict) -> bool:
    return bool(position.get("tp_replay_blocked"))


def _place_take_profit(use_margin: bool, spot_manager, margin_manager, *, symbol: str, quantity: float | str, target_price: float) -> dict:
    if use_margin:
        return margin_manager.create_margin_take_profit_sell(symbol=symbol, quantity=quantity, target_price=target_price)
    return spot_manager.create_exit_take_profit_for_open_long(symbol=symbol, quantity=quantity, target_price=target_price)


def _replay_take_profit(candidate_id, position, symbol, spot_manager, margin_manager, state, kraken=None, rules=None):
    normalized_position = normalize_position_execution_mode(position)
    if normalized_position != position:
        updates = {k: normalized_position.get(k) for k in ("mode", "margin_account_mode", "margin_isolated") if normalized_position.get(k) != position.get(k)}
        state.update_open_position(candidate_id, updates, event_type="position_execution_mode_normalized")
        position = normalized_position
    if _is_replay_blocked(position):
        return "blocked"
    side = str(position.get("side") or "long").lower()
    if side == "short":
        return "unsupported_side"
    levels = _levels(position, symbol)
    if not levels:
        try:
            levels = _fallback_levels(kraken, symbol, position)
        except Exception as exc:
            levels = None
            fallback_error = str(exc)
        else:
            fallback_error = None
        if not levels:
            payload = {"symbol": symbol, "reason": "no_take_profit_level", "fallback_error": fallback_error}
            _mark_replay_skip(state, candidate_id, position, "tp_replay_waiting_levels", payload)
            return "waiting_levels"
        state.update_open_position(candidate_id, {"target_price": float(levels["target_price"]), "tp_replay_level_source": levels}, event_type="tp_fallback_level_created")
        logger.warning("tp fallback level created candidate=%s symbol=%s tp=%s source=%s", candidate_id, symbol, levels.get("target_price"), levels.get("source"))

    quantity = _float(position.get("quantity"))
    if quantity <= 0:
        payload = {"symbol": symbol, "reason": "missing_quantity", "levels": levels}
        _mark_replay_skip(state, candidate_id, position, "tp_replay_failed", payload)
        return "missing_quantity"

    use_margin = _is_margin_position(position)
    if kraken is not None:
        if _attach_existing_take_profit(candidate_id, position, symbol, kraken, margin_manager.margin, state, use_margin=use_margin):
            return "attached_existing_tp"

    entry_payload = position.get("entry_payload") if isinstance(position.get("entry_payload"), dict) else {}
    if kraken is not None and position.get("entry_order_id"):
        fetched = _order(kraken, margin_manager.margin, symbol, position.get("entry_order_id"), use_margin=use_margin)
        if fetched and not _has_sync_error(fetched):
            entry_payload = fetched
    original_qty = _entry_executed_quantity(entry_payload, quantity)

    available = None
    if kraken is not None and rules is not None and not use_margin:
        available = _available_base_balance(
            kraken,
            margin_manager.margin,
            rules,
            symbol,
            use_margin=use_margin,
        )
    qty_ok, base_qty, qty_reason = _repair_quantity(
        {**position, "quantity": original_qty},
        available,
    )
    if not qty_ok:
        payload = {"symbol": symbol, "mode": position.get("mode"), "reason": qty_reason, "entry_order_id": position.get("entry_order_id"), "entry_payload": entry_payload, "available_base": available, "levels": levels}
        _mark_replay_skip(state, candidate_id, position, "tp_replay_skipped_quantity_unavailable", payload)
        logger.warning("tp replay skipped quantity unavailable candidate=%s symbol=%s reason=%s", candidate_id, symbol, qty_reason)
        return "quantity_unavailable"

    attempts = []
    target_price = float(levels["target_price"])
    last_error = None
    for fraction in tp_replay_fractions():
        requested_qty = base_qty * fraction
        try:
            result = _place_take_profit(use_margin, spot_manager, margin_manager, symbol=symbol, quantity=requested_qty, target_price=target_price)
        except Exception as exc:
            last_error = str(exc)
            attempts.append({"fraction": fraction, "quantity": requested_qty, "status": "failed", "error": last_error})
            continue
        protected_qty = _float(result.get("quantity"), requested_qty)
        remaining_qty = max(0.0, quantity - protected_qty)
        updates = {
            "target_price": target_price,
            "quantity": quantity,
            "tp_order_id": result.get("tp_order_id"),
            "sl_order_id": None,
            "oco_order_list_id": None,
            "tp_payload": result.get("tp_payload") or {},
            "exit_strategy": "take_profit_only",
            "tp_replay_level_source": levels,
            "tp_replay_mode": "margin" if use_margin else "spot",
            "tp_replay_source_entry_order_id": position.get("entry_order_id"),
            "tp_replay_source_entry_payload": entry_payload,
            "tp_replay_fraction": fraction,
            "tp_replay_attempt_details": attempts + [{"fraction": fraction, "quantity": protected_qty, "status": "placed"}],
            "tp_protected_quantity": protected_qty,
            "tp_unprotected_quantity": remaining_qty,
            "needs_tp_replay": False,
            "tp_replay_status": "placed" if remaining_qty <= max(quantity * 0.02, 0.0) else "partial_placed",
            "tp_replay_attempts": 0,
            "tp_replay_blocked": False,
            "last_tp_replay_skip_reason": None,
            "last_tp_replay_skip_ts": None,
        }
        state.update_open_position(candidate_id, updates, event_type="tp_replayed")
        logger.info("tp replayed candidate=%s symbol=%s mode=%s qty=%s fraction=%s source=%s", candidate_id, symbol, updates["tp_replay_mode"], protected_qty, fraction, levels.get("source"))
        return "replayed"

    payload = {"symbol": symbol, "mode": position.get("mode"), "reason": last_error or "all_tp_replay_attempts_failed", "quantity": base_qty, "target_price": target_price, "levels": levels, "attempts": attempts}
    _mark_replay_skip(state, candidate_id, position, "tp_replay_failed", payload)
    return "failed"


def _repair(candidate_id, position, symbol, spot_manager, margin_manager, state, kraken=None, rules=None):
    return _replay_take_profit(candidate_id, position, symbol, spot_manager, margin_manager, state, kraken=kraken, rules=rules)


def _handle_filled_take_profit(candidate_id: str, position: dict, tp: dict, state) -> str:
    quantity = _float(position.get("quantity"))
    protected = _float(position.get("tp_protected_quantity"), quantity)
    if quantity <= 0 or protected >= quantity * 0.98:
        state.close_position(candidate_id, "take_profit_filled", tp)
        return "closed"
    remaining = max(0.0, quantity - protected)
    updates = {
        "quantity": remaining,
        "tp_order_id": None,
        "tp_payload": {},
        "tp_protected_quantity": 0.0,
        "tp_unprotected_quantity": remaining,
        "needs_tp_replay": True,
        "last_tp_fill_payload": tp,
    }
    state.update_open_position(candidate_id, updates, event_type="take_profit_partial_filled")
    return "partial"


def sync_open_positions():
    settings = load_settings()
    kraken, default_margin, rules = create_margin_exchange(settings, dry_run=margin_dry_run())
    spot_manager = SpotOrderManager(kraken, rules)
    state = StateStore()
    checked = closed = missing_tp = replayed_tp = attached_existing = replay_skipped = replay_blocked = ghost_removed = momentum_tracked = partial_filled = 0

    for candidate_id, position in list(state.open_positions().items()):
        normalized_position = normalize_position_execution_mode(position)
        if normalized_position != position:
            updates = {k: normalized_position.get(k) for k in ("mode", "margin_account_mode", "margin_isolated") if normalized_position.get(k) != position.get(k)}
            state.update_open_position(candidate_id, updates, event_type="position_execution_mode_normalized")
            position = normalized_position
        checked += 1
        symbol = str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()
        if not symbol:
            continue

        use_margin = _is_margin_position(position)
        margin = default_margin
        margin_manager = MarginOrderManager(kraken, margin, rules)

        if _is_momentum_position(candidate_id, position):
            if _track_momentum_position(candidate_id, position, symbol, kraken, rules, state, margin=margin):
                closed += 1
            else:
                momentum_tracked += 1
            continue

        if _ghost_check(candidate_id, position, symbol, kraken, margin, rules, state, use_margin=use_margin):
            ghost_removed += 1
            continue

        tp_id = position.get("tp_order_id")
        if not tp_id:
            missing_tp += 1
            try:
                replay_result = _replay_take_profit(candidate_id, position, symbol, spot_manager, margin_manager, state, kraken=kraken, rules=rules)
            except Exception as exc:
                replay_skipped += 1
                state.update_open_position(candidate_id, {"needs_tp_replay": True, "last_tp_replay_exception": str(exc)}, event_type="tp_replay_failed")
                logger.warning("tp replay failed candidate=%s symbol=%s error=%s", candidate_id, symbol, str(exc))
                continue
            if replay_result == "replayed":
                replayed_tp += 1
            elif replay_result == "attached_existing_tp":
                attached_existing += 1
            elif replay_result == "blocked":
                replay_blocked += 1
            else:
                replay_skipped += 1
            continue

        tp = _order(kraken, margin, symbol, tp_id, use_margin=use_margin)
        status = _status(tp)
        state.update_open_position(candidate_id, {"kraken_tp_status": tp, "order_monitor_mode": "margin" if use_margin else "spot"})
        if status == "FILLED":
            fill_result = _handle_filled_take_profit(candidate_id, position, tp, state)
            if fill_result == "closed":
                closed += 1
            else:
                partial_filled += 1
            continue

        if status in {"CANCELED", "REJECTED", "EXPIRED"} or _is_not_found_error(tp):
            missing_tp += 1
            state.update_open_position(
                candidate_id,
                {
                    "tp_order_id": None,
                    "tp_payload": {},
                    "needs_tp_replay": True,
                    "invalid_tp_order_id": tp_id,
                    "invalid_tp_status": status,
                    "invalid_tp_payload": tp,
                },
                event_type="tp_invalid_missing_replay",
            )
            position = {**position, "tp_order_id": None, "tp_payload": {}, "needs_tp_replay": True}
            try:
                replay_result = _replay_take_profit(candidate_id, position, symbol, spot_manager, margin_manager, state, kraken=kraken, rules=rules)
            except Exception as exc:
                replay_skipped += 1
                state.update_open_position(candidate_id, {"needs_tp_replay": True, "last_tp_replay_exception": str(exc)}, event_type="tp_replay_failed")
                logger.warning("tp replay failed candidate=%s symbol=%s error=%s", candidate_id, symbol, str(exc))
                continue
            if replay_result == "replayed":
                replayed_tp += 1
            elif replay_result == "attached_existing_tp":
                attached_existing += 1
            elif replay_result == "blocked":
                replay_blocked += 1
            else:
                replay_skipped += 1
            continue

        if _has_sync_error(tp):
            state.update_open_position(
                candidate_id,
                {
                    "kraken_tp_status": tp,
                    "order_monitor_mode": "margin" if use_margin else "spot",
                    "last_tp_sync_error": tp.get("sync_error"),
                },
            )
            logger.warning("tp sync error candidate=%s symbol=%s tp=%s error=%s", candidate_id, symbol, tp_id, tp.get("sync_error"))
            continue

    summary = {"checked": checked, "closed": closed, "partial_filled": partial_filled, "ghost_removed": ghost_removed, "momentum_tracked": momentum_tracked, "missing_tp": missing_tp, "replayed_tp": replayed_tp, "attached_existing_tp": attached_existing, "replay_skipped": replay_skipped, "replay_blocked": replay_blocked}
    if any(summary.values()):
        logger.info("position sync summary=%s", summary)
    return summary

if __name__ == "__main__":
    print(sync_open_positions())
