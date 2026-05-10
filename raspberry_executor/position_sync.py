from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-position-sync")


def _upper_status(payload):
    return str((payload or {}).get("status") or "").upper()


def _get_order(binance, symbol, order_id):
    if not order_id:
        return None
    try:
        return binance.get_order(symbol, order_id)
    except Exception as exc:
        return {"orderId": order_id, "sync_error": str(exc)}


def sync_open_positions():
    settings = load_settings()
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    state = StateStore()
    checked = 0
    closed = 0
    missing_oco = 0

    for candidate_id, position in list(state.open_positions().items()):
        checked += 1
        symbol = str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()
        if not symbol:
            continue
        tp_id = position.get("tp_order_id")
        sl_id = position.get("sl_order_id")
        if not tp_id or not sl_id:
            missing_oco += 1
            continue
        tp = _get_order(binance, symbol, tp_id)
        sl = _get_order(binance, symbol, sl_id)
        state.update_open_position(candidate_id, {
            "binance_tp_status": tp,
            "binance_sl_status": sl,
        })
        if _upper_status(tp) == "FILLED":
            state.close_position(candidate_id, "take_profit_filled", tp)
            closed += 1
        elif _upper_status(sl) == "FILLED":
            state.close_position(candidate_id, "stop_loss_filled", sl)
            closed += 1

    summary = {"checked": checked, "closed": closed, "missing_oco": missing_oco}
    if checked:
        logger.info("position sync summary=%s", summary)
    return summary


if __name__ == "__main__":
    print(sync_open_positions())
