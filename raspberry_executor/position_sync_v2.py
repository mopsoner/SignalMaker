import time

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.candidate_levels import latest_levels_for_symbol
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import margin_dry_run, margin_isolated
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-position-sync")

OCO_SKIP_EVENT_COOLDOWN_SECONDS = 300


def _status(payload):
    return str((payload or {}).get("status") or "").upper()


def _float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _is_margin_position(position: dict) -> bool:
    mode = str(position.get("mode") or "").lower()
    return "margin" in mode


def _is_isolated_position(position: dict) -> bool:
    if position.get("margin_isolated") is not None:
        return bool(position.get("margin_isolated"))
    mode = str(position.get("mode") or "").lower()
    if "cross" in mode:
        return False
    if "isolated" in mode:
        return True
    return margin_isolated()


def _order(binance, margin, symbol, order_id, *, use_margin: bool):
    if not order_id:
        return None
    try:
        if use_margin:
            return margin.get_margin_order(symbol, order_id)
        return binance.get_order(symbol, order_id)
    except Exception as exc:
        return {"orderId": order_id, "sync_error": str(exc)}


def _levels(position, symbol):
    if position.get("target_price") is not None and position.get("stop_price") is not None:
        return {"target_price": position.get("target_price"), "stop_price": position.get("stop_price"), "source": "local_position"}
    return latest_levels_for_symbol(symbol)


def _available_base_balance(binance, margin, rules, symbol: str, *, use_margin: bool) -> float | None:
    base = rules.base_asset(symbol)
    try:
        if use_margin:
            return float(margin.margin_free_balance(symbol, base))
        return float(binance.free_balance(base))
    except Exception:
        return None


def _repair_quantity(position: dict, available_base: float | None) -> tuple[bool, float, str]:
    expected_qty = _float(position.get("quantity"))
    if expected_qty <= 0:
        return False, 0.0, "missing_quantity"
    if available_base is None:
        return True, expected_qty, "balance_unchecked"
    min_required = expected_qty * 0.98
    if available_base < min_required:
        return False, expected_qty, f"base_balance_mismatch expected={expected_qty} available={available_base} min_required={min_required}"
    return True, min(expected_qty, available_base), "ok"


def _order_type(order: dict) -> str:
    return str(order.get("type") or order.get("origType") or "").upper()


def _order_qty(order: dict) -> float:
    return _float(order.get("origQty") or order.get("quantity") or order.get("executedQty"))


def _is_tp_order(order: dict) -> bool:
    order_type = _order_type(order)
    return str(order.get("side") or "").upper() == "SELL" and order_type in {"LIMIT", "LIMIT_MAKER"}


def _is_sl_order(order: dict) -> bool:
    order_type = _order_type(order)
    return str(order.get("side") or "").upper() == "SELL" and "STOP" in order_type


def _list_open_orders(binance, margin, symbol: str, *, use_margin: bool) -> list[dict]:
    try:
        if use_margin:
            return margin.open_margin_orders(symbol)
        return binance.open_orders(symbol)
    except Exception as exc:
        logger.warning("open orders lookup failed symbol=%s use_margin=%s error=%s", symbol, use_margin, str(exc))
        return []


def _attach_existing_exit_orders(candidate_id, position, symbol, binance, margin, rules, state, *, use_margin: bool) -> bool:
    expected_qty = _float(position.get("quantity"))
    if expected_qty <= 0:
        return False
    open_orders = _list_open_orders(binance, margin, symbol, use_margin=use_margin)
    if not open_orders:
        return False
    min_required = expected_qty * 0.98
    tp_candidates = []
    sl_candidates = []
    for order in open_orders:
        qty = _order_qty(order)
        if qty < min_required:
            continue
        if _is_tp_order(order):
            tp_candidates.append(order)
        elif _is_sl_order(order):
            sl_candidates.append(order)
    if not tp_candidates or not sl_candidates:
        return False
    tp = sorted(tp_candidates, key=lambda o: _float(o.get("price")), reverse=True)[0]
    sl = sorted(sl_candidates, key=lambda o: _float(o.get("stopPrice") or o.get("price")))[0]
    updates = {
        "tp_order_id": tp.get("orderId"),
        "sl_order_id": sl.get("orderId"),
        "oco_order_list_id": tp.get("orderListId") or sl.get("orderListId") or position.get("oco_order_list_id"),
        "target_price": _float(tp.get("price"), _float(position.get("target_price"))),
        "stop_price": _float(sl.get("stopPrice") or sl.get("price"), _float(position.get("stop_price"))),
        "order_monitor_mode": "margin" if use_margin else "spot",
        "oco_repair_mode": "attached_existing_orders",
        "attached_existing_tp_order": tp,
        "attached_existing_sl_order": sl,
    }
    state.update_open_position(candidate_id, updates, event_type="oco_existing_orders_attached")
    logger.info("attached existing exit orders candidate=%s symbol=%s tp=%s sl=%s", candidate_id, symbol, tp.get("orderId"), sl.get("orderId"))
    return True


def _should_emit_skip_event(position: dict, reason: str) -> bool:
    last_reason = str(position.get("last_oco_repair_skip_reason") or "")
    last_ts = _float(position.get("last_oco_repair_skip_ts"), 0.0)
    return reason != last_reason or (time.time() - last_ts) >= OCO_SKIP_EVENT_COOLDOWN_SECONDS


def _mark_repair_skip(state, candidate_id: str, position: dict, event_type: str, payload: dict) -> None:
    reason = str(payload.get("reason") or event_type)
    updates = {
        "last_oco_repair_skip_reason": reason,
        "last_oco_repair_skip_ts": time.time(),
        "last_oco_repair_skip_event": event_type,
        "last_oco_repair_skip_payload": payload,
    }
    if _should_emit_skip_event(position, reason):
        state.update_open_position(candidate_id, updates, event_type=event_type)
    else:
        state.update_open_position(candidate_id, updates)


def _repair(candidate_id, position, symbol, spot_manager, margin_manager, state, binance=None, rules=None):
    levels = _levels(position, symbol)
    if not levels:
        payload = {"symbol": symbol, "reason": "no_recent_candidate_levels"}
        _mark_repair_skip(state, candidate_id, position, "oco_repair_waiting_levels", payload)
        return "waiting_levels"
    quantity = position.get("quantity")
    if not quantity:
        payload = {"symbol": symbol, "reason": "missing_quantity", "levels": levels}
        _mark_repair_skip(state, candidate_id, position, "oco_repair_failed", payload)
        return "missing_quantity"

    use_margin = _is_margin_position(position)
    if binance is not None and rules is not None:
        if _attach_existing_exit_orders(candidate_id, position, symbol, binance, margin_manager.margin, rules, state, use_margin=use_margin):
            return "attached_existing_orders"
        available = _available_base_balance(binance, margin_manager.margin, rules, symbol, use_margin=use_margin)
        qty_ok, repair_qty, qty_reason = _repair_quantity(position, available)
        if not qty_ok:
            payload = {"symbol": symbol, "mode": position.get("mode"), "reason": qty_reason, "expected_quantity": position.get("quantity"), "available_base": available, "levels": levels}
            _mark_repair_skip(state, candidate_id, position, "oco_repair_skipped_quantity_mismatch", payload)
            logger.warning("oco repair skipped quantity mismatch candidate=%s symbol=%s reason=%s", candidate_id, symbol, qty_reason)
            return "quantity_mismatch"
        quantity = repair_qty

    if use_margin:
        result = margin_manager.create_margin_oco_sell(symbol=symbol, quantity=quantity, target_price=float(levels["target_price"]), stop_price=float(levels["stop_price"]))
        repair_mode = "margin"
    else:
        result = spot_manager.create_exit_oco_for_open_long(symbol=symbol, quantity=quantity, target_price=float(levels["target_price"]), stop_price=float(levels["stop_price"]))
        repair_mode = "spot"

    updates = {"target_price": float(levels["target_price"]), "stop_price": float(levels["stop_price"]), "quantity": result.get("quantity") or quantity, "oco_order_list_id": result.get("oco_order_list_id"), "tp_order_id": result.get("tp_order_id"), "sl_order_id": result.get("sl_order_id"), "oco_payload": result.get("oco_payload") or {}, "oco_repair_level_source": levels, "oco_repair_mode": repair_mode, "oco_repair_validated_quantity": quantity, "last_oco_repair_skip_reason": None, "last_oco_repair_skip_ts": None}
    state.update_open_position(candidate_id, updates, event_type="oco_repaired")
    logger.info("oco repaired candidate=%s symbol=%s mode=%s qty=%s source=%s", candidate_id, symbol, repair_mode, quantity, levels.get("source"))
    return "repaired"


def sync_open_positions():
    settings = load_settings()
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run or margin_dry_run())
    rules = BinanceSymbolRules(settings.binance_base_url)
    spot_manager = SpotOrderManager(binance, rules)
    state = StateStore()
    checked = 0
    closed = 0
    missing_oco = 0
    repaired_oco = 0
    attached_existing = 0
    repair_skipped = 0

    for candidate_id, position in list(state.open_positions().items()):
        checked += 1
        symbol = str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()
        if not symbol:
            continue

        use_margin = _is_margin_position(position)
        margin = MarginClient(binance, isolated=_is_isolated_position(position), dry_run=settings.dry_run or margin_dry_run())
        margin_manager = MarginOrderManager(binance, margin, rules)

        tp_id = position.get("tp_order_id")
        sl_id = position.get("sl_order_id")
        if not tp_id or not sl_id:
            missing_oco += 1
            try:
                repair_result = _repair(candidate_id, position, symbol, spot_manager, margin_manager, state, binance=binance, rules=rules)
                if repair_result == "repaired":
                    repaired_oco += 1
                elif repair_result == "attached_existing_orders":
                    attached_existing += 1
                elif repair_result in {"quantity_mismatch", "waiting_levels", "missing_quantity"}:
                    repair_skipped += 1
            except Exception as exc:
                payload = {"symbol": symbol, "mode": position.get("mode"), "error": str(exc), "position": position}
                _mark_repair_skip(state, candidate_id, position, "oco_repair_failed", payload)
                logger.error("oco repair failed candidate=%s symbol=%s mode=%s error=%s", candidate_id, symbol, position.get("mode"), str(exc))
            continue

        tp = _order(binance, margin, symbol, tp_id, use_margin=use_margin)
        sl = _order(binance, margin, symbol, sl_id, use_margin=use_margin)
        state.update_open_position(candidate_id, {"binance_tp_status": tp, "binance_sl_status": sl, "order_monitor_mode": "margin" if use_margin else "spot"})
        if _status(tp) == "FILLED":
            state.close_position(candidate_id, "take_profit_filled", tp)
            closed += 1
        elif _status(sl) == "FILLED":
            state.close_position(candidate_id, "stop_loss_filled", sl)
            closed += 1

    summary = {"checked": checked, "closed": closed, "missing_oco": missing_oco, "repaired_oco": repaired_oco, "attached_existing_orders": attached_existing, "repair_skipped": repair_skipped}
    if closed or repaired_oco or missing_oco or attached_existing or repair_skipped:
        logger.info("position sync summary=%s", summary)
    return summary


if __name__ == "__main__":
    print(sync_open_positions())
