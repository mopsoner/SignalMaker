from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.candidate_levels import latest_levels_for_symbol
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-position-sync")


def _status(payload):
    return str((payload or {}).get("status") or "").upper()


def _order(binance, symbol, order_id):
    if not order_id:
        return None
    try:
        return binance.get_order(symbol, order_id)
    except Exception as exc:
        return {"orderId": order_id, "sync_error": str(exc)}


def _levels(position, symbol):
    if position.get("target_price") is not None and position.get("stop_price") is not None:
        return {
            "target_price": position.get("target_price"),
            "stop_price": position.get("stop_price"),
            "source": "local_position",
        }
    return latest_levels_for_symbol(symbol)


def _repair(candidate_id, position, symbol, manager, state):
    levels = _levels(position, symbol)
    if not levels:
        state.add_event(candidate_id, "oco_repair_waiting_levels", {"symbol": symbol, "reason": "no_recent_candidate_levels"})
        return "waiting_levels"
    quantity = position.get("quantity")
    if not quantity:
        state.add_event(candidate_id, "oco_repair_failed", {"symbol": symbol, "reason": "missing_quantity", "levels": levels})
        return "missing_quantity"
    result = manager.create_exit_oco_for_open_long(
        symbol=symbol,
        quantity=quantity,
        target_price=float(levels["target_price"]),
        stop_price=float(levels["stop_price"]),
    )
    updates = {
        "target_price": float(levels["target_price"]),
        "stop_price": float(levels["stop_price"]),
        "quantity": result.get("quantity") or quantity,
        "oco_order_list_id": result.get("oco_order_list_id"),
        "tp_order_id": result.get("tp_order_id"),
        "sl_order_id": result.get("sl_order_id"),
        "oco_payload": result.get("oco_payload") or {},
        "oco_repair_level_source": levels,
    }
    state.update_open_position(candidate_id, updates, event_type="oco_repaired")
    logger.info("oco repaired candidate=%s symbol=%s source=%s", candidate_id, symbol, levels.get("source"))
    return "repaired"


def sync_open_positions():
    settings = load_settings()
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    rules = BinanceSymbolRules(settings.binance_base_url)
    manager = SpotOrderManager(binance, rules)
    state = StateStore()
    checked = 0
    closed = 0
    missing_oco = 0
    repaired_oco = 0

    for candidate_id, position in list(state.open_positions().items()):
        checked += 1
        symbol = str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()
        if not symbol:
            continue
        tp_id = position.get("tp_order_id")
        sl_id = position.get("sl_order_id")
        if not tp_id or not sl_id:
            missing_oco += 1
            try:
                if _repair(candidate_id, position, symbol, manager, state) == "repaired":
                    repaired_oco += 1
            except Exception as exc:
                state.add_event(candidate_id, "oco_repair_failed", {"symbol": symbol, "error": str(exc), "position": position})
                logger.error("oco repair failed candidate=%s symbol=%s error=%s", candidate_id, symbol, str(exc))
            continue
        tp = _order(binance, symbol, tp_id)
        sl = _order(binance, symbol, sl_id)
        state.update_open_position(candidate_id, {"binance_tp_status": tp, "binance_sl_status": sl})
        if _status(tp) == "FILLED":
            state.close_position(candidate_id, "take_profit_filled", tp)
            closed += 1
        elif _status(sl) == "FILLED":
            state.close_position(candidate_id, "stop_loss_filled", sl)
            closed += 1

    summary = {"checked": checked, "closed": closed, "missing_oco": missing_oco, "repaired_oco": repaired_oco}
    if closed or repaired_oco or missing_oco:
        logger.info("position sync summary=%s", summary)
    return summary


if __name__ == "__main__":
    print(sync_open_positions())
