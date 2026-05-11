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


def _status(payload):
    return str((payload or {}).get("status") or "").upper()


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
        return {
            "target_price": position.get("target_price"),
            "stop_price": position.get("stop_price"),
            "source": "local_position",
        }
    return latest_levels_for_symbol(symbol)


def _repair(candidate_id, position, symbol, spot_manager, margin_manager, state):
    levels = _levels(position, symbol)
    if not levels:
        state.add_event(candidate_id, "oco_repair_waiting_levels", {"symbol": symbol, "reason": "no_recent_candidate_levels"})
        return "waiting_levels"
    quantity = position.get("quantity")
    if not quantity:
        state.add_event(candidate_id, "oco_repair_failed", {"symbol": symbol, "reason": "missing_quantity", "levels": levels})
        return "missing_quantity"

    if _is_margin_position(position):
        result = margin_manager.create_margin_oco_sell(
            symbol=symbol,
            quantity=quantity,
            target_price=float(levels["target_price"]),
            stop_price=float(levels["stop_price"]),
        )
        repair_mode = "margin"
    else:
        result = spot_manager.create_exit_oco_for_open_long(
            symbol=symbol,
            quantity=quantity,
            target_price=float(levels["target_price"]),
            stop_price=float(levels["stop_price"]),
        )
        repair_mode = "spot"

    updates = {
        "target_price": float(levels["target_price"]),
        "stop_price": float(levels["stop_price"]),
        "quantity": result.get("quantity") or quantity,
        "oco_order_list_id": result.get("oco_order_list_id"),
        "tp_order_id": result.get("tp_order_id"),
        "sl_order_id": result.get("sl_order_id"),
        "oco_payload": result.get("oco_payload") or {},
        "oco_repair_level_source": levels,
        "oco_repair_mode": repair_mode,
    }
    state.update_open_position(candidate_id, updates, event_type="oco_repaired")
    logger.info("oco repaired candidate=%s symbol=%s mode=%s source=%s", candidate_id, symbol, repair_mode, levels.get("source"))
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
                if _repair(candidate_id, position, symbol, spot_manager, margin_manager, state) == "repaired":
                    repaired_oco += 1
            except Exception as exc:
                state.add_event(candidate_id, "oco_repair_failed", {"symbol": symbol, "mode": position.get("mode"), "error": str(exc), "position": position})
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

    summary = {"checked": checked, "closed": closed, "missing_oco": missing_oco, "repaired_oco": repaired_oco}
    if closed or repaired_oco or missing_oco:
        logger.info("position sync summary=%s", summary)
    return summary


if __name__ == "__main__":
    print(sync_open_positions())
