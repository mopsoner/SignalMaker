import time

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-executor")


def _order_id(payload: dict | None):
    if not payload:
        return None
    return payload.get("orderId") or payload.get("order_id")


def _executed_qty(payload: dict, fallback: float) -> float:
    try:
        value = float(payload.get("executedQty") or 0)
        return value if value > 0 else fallback
    except Exception:
        return fallback


def _quantity_from_quote(price: float, quote_amount: float) -> float:
    if price <= 0:
        raise RuntimeError("Invalid current price")
    return round(float(quote_amount) / float(price), 6)


def report_final_events(binance: BinanceClient, state: StateStore) -> None:
    for candidate_id, position in list(state.open_positions().items()):
        symbol = position["execution_symbol"]
        tp_order_id = position.get("tp_order_id")
        sl_order_id = position.get("sl_order_id")
        try:
            tp_status = binance.get_order(symbol, tp_order_id) if tp_order_id else None
            sl_status = binance.get_order(symbol, sl_order_id) if sl_order_id else None
        except Exception as exc:
            logger.warning("order status failed candidate=%s error=%s", candidate_id, exc)
            continue

        if tp_status and str(tp_status.get("status", "")).upper() == "FILLED":
            state.close_position(candidate_id, "take_profit_filled", tp_status)
            logger.info("local position closed candidate=%s reason=take_profit_filled", candidate_id)
        elif sl_status and str(sl_status.get("status", "")).upper() == "FILLED":
            state.close_position(candidate_id, "stop_loss_filled", sl_status)
            logger.info("local position closed candidate=%s reason=stop_loss_filled", candidate_id)


def execute_candidate(settings, binance: BinanceClient, state: StateStore, guard: RiskGuard, candidate: dict) -> None:
    candidate_id = candidate["candidate_id"]
    accepted, reason = guard.accept(candidate, already_executed=state.already_executed(candidate_id))
    if not accepted:
        logger.info("skip candidate=%s reason=%s", candidate_id, reason)
        return

    execution_symbol = guard.execution_symbol(candidate, settings.execution_quote_asset)
    side = guard.normalize_side(str(candidate["side"]))
    current_price = binance.current_price(execution_symbol)
    quantity = _quantity_from_quote(current_price, settings.order_quote_amount)

    try:
        logger.info("execute candidate=%s signal=%s execution=%s side=%s amount=%s qty=%s", candidate_id, candidate.get("symbol"), execution_symbol, side, settings.order_quote_amount, quantity)
        entry = binance.place_market_entry(execution_symbol, side, quantity)
        entry_price = BinanceClient.average_fill_price(entry, fallback=current_price)
        if entry_price is None:
            raise RuntimeError("Unable to determine entry fill price")
        executed_qty = _executed_qty(entry, quantity)

        tp = binance.place_exit_limit(execution_symbol, side, executed_qty, float(candidate["target_price"]))
        sl = binance.place_stop_loss(execution_symbol, side, executed_qty, float(candidate["stop_price"]))

        state.mark_executed(candidate_id)
        state.add_open_position(candidate_id, {
            "candidate_id": candidate_id,
            "signal_symbol": candidate["symbol"],
            "execution_symbol": execution_symbol,
            "side": side,
            "quantity": executed_qty,
            "entry_price": float(entry_price),
            "stop_price": float(candidate["stop_price"]),
            "target_price": float(candidate["target_price"]),
            "entry_order_id": _order_id(entry),
            "tp_order_id": _order_id(tp),
            "sl_order_id": _order_id(sl),
            "candidate": candidate,
            "entry_payload": entry,
            "tp_payload": tp or {},
            "sl_payload": sl or {},
        })
        logger.info("local position opened candidate=%s execution=%s qty=%s", candidate_id, execution_symbol, executed_qty)
    except Exception as exc:
        logger.exception("execution failed candidate=%s", candidate_id)
        state.add_event(candidate_id, "execution_error", {"error": str(exc), "candidate": candidate})


def main() -> None:
    settings = load_settings()
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    state = StateStore()
    guard = RiskGuard(settings.allowed_symbols, settings.max_candidate_age_seconds)

    logger.info(
        "Raspberry executor started gateway_id=%s dry_run=%s execution_quote_asset=%s order_quote_amount=%s",
        settings.gateway_id,
        settings.dry_run,
        settings.execution_quote_asset,
        settings.order_quote_amount,
    )
    while True:
        try:
            candidates = signalmaker.get_open_candidates(limit=10)
            logger.info("candidates fetched count=%s", len(candidates))
            for candidate in candidates:
                execute_candidate(settings, binance, state, guard, candidate)
            report_final_events(binance, state)
        except Exception:
            logger.exception("main loop error")
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
