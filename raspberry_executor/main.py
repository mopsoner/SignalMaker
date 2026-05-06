import logging
import time
from datetime import datetime, timezone

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.config import load_settings
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.state import StateStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("raspberry-executor")


def _order_id(payload: dict | None):
    if not payload:
        return None
    return payload.get("orderId") or payload.get("order_id")


def _status(payload: dict | None) -> str:
    return str((payload or {}).get("status", "NEW")).lower()


def build_execution_report(settings, candidate: dict, execution_symbol: str, entry: dict, tp: dict | None, sl: dict | None, quantity: float, entry_price: float) -> dict:
    side = RiskGuard.normalize_side(str(candidate["side"]))
    return {
        "gateway_id": settings.gateway_id,
        "candidate_id": candidate["candidate_id"],
        "signal_symbol": candidate["symbol"],
        "execution_symbol": execution_symbol,
        "side": side,
        "quantity": quantity,
        "entry_price": float(candidate["entry_price"]),
        "stop_price": float(candidate["stop_price"]),
        "target_price": float(candidate["target_price"]),
        "exchange": "binance",
        "mode": "dry_run" if settings.dry_run else "live",
        "entry_order": {
            "exchange_order_id": _order_id(entry),
            "status": _status(entry),
            "avg_price": entry_price,
            "executed_qty": quantity,
            "payload": entry,
        },
        "tp_order": {
            "exchange_order_id": _order_id(tp),
            "status": _status(tp),
            "price": float(candidate["target_price"]),
            "payload": tp or {},
        } if tp else None,
        "sl_order": {
            "exchange_order_id": _order_id(sl),
            "status": _status(sl),
            "price": float(candidate["stop_price"]),
            "payload": sl or {},
        } if sl else None,
        "payload": {"candidate": candidate},
    }


def report_final_events(settings, signalmaker: SignalMakerClient, binance: BinanceClient, state: StateStore) -> None:
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

        event_type = None
        exchange_order_id = None
        payload = None
        if tp_status and str(tp_status.get("status", "")).upper() == "FILLED":
            event_type = "take_profit_filled"
            exchange_order_id = tp_order_id
            payload = tp_status
        elif sl_status and str(sl_status.get("status", "")).upper() == "FILLED":
            event_type = "stop_loss_filled"
            exchange_order_id = sl_order_id
            payload = sl_status

        if event_type:
            signalmaker.report_event({
                "gateway_id": settings.gateway_id,
                "candidate_id": candidate_id,
                "event_type": event_type,
                "exchange": "binance",
                "exchange_order_id": exchange_order_id,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "payload": payload or {},
            })
            state.remove_open_position(candidate_id)
            logger.info("reported final event candidate=%s event=%s", candidate_id, event_type)


def execute_candidate(settings, signalmaker: SignalMakerClient, binance: BinanceClient, state: StateStore, guard: RiskGuard, candidate: dict) -> None:
    candidate_id = candidate["candidate_id"]
    accepted, reason = guard.accept(candidate, already_executed=state.already_executed(candidate_id))
    if not accepted:
        logger.info("skip candidate=%s reason=%s", candidate_id, reason)
        return

    execution_symbol = guard.execution_symbol(candidate, settings.symbol_map)
    side = guard.normalize_side(str(candidate["side"]))
    quantity = settings.quantity

    try:
        entry = binance.place_market_entry(execution_symbol, side, quantity)
        entry_price = BinanceClient.average_fill_price(entry, fallback=float(candidate["entry_price"]))
        if entry_price is None:
            raise RuntimeError("Unable to determine entry fill price")

        tp = binance.place_exit_limit(execution_symbol, side, quantity, float(candidate["target_price"]))
        sl = binance.place_stop_loss(execution_symbol, side, quantity, float(candidate["stop_price"]))

        report = build_execution_report(settings, candidate, execution_symbol, entry, tp, sl, quantity, float(entry_price))
        response = signalmaker.report_execution(report)
        logger.info("execution recorded candidate=%s response=%s", candidate_id, response)

        state.mark_executed(candidate_id)
        state.add_open_position(candidate_id, {
            "execution_symbol": execution_symbol,
            "signal_symbol": candidate["symbol"],
            "side": side,
            "quantity": quantity,
            "entry_order_id": _order_id(entry),
            "tp_order_id": _order_id(tp),
            "sl_order_id": _order_id(sl),
        })
    except Exception as exc:
        logger.exception("execution failed candidate=%s", candidate_id)
        try:
            signalmaker.report_event({
                "gateway_id": settings.gateway_id,
                "candidate_id": candidate_id,
                "event_type": "execution_error",
                "exchange": "binance",
                "reason": str(exc),
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "payload": {},
            })
        except Exception:
            logger.exception("failed to report execution error candidate=%s", candidate_id)


def main() -> None:
    settings = load_settings()
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    state = StateStore()
    guard = RiskGuard(settings.allowed_symbols, settings.max_candidate_age_seconds)

    logger.info("Raspberry executor started gateway_id=%s dry_run=%s", settings.gateway_id, settings.dry_run)
    while True:
        try:
            signalmaker.heartbeat(mode="executor", meta={"dry_run": settings.dry_run})
            candidates = signalmaker.get_open_candidates(limit=10)
            for candidate in candidates:
                execute_candidate(settings, signalmaker, binance, state, guard, candidate)
            report_final_events(settings, signalmaker, binance, state)
        except Exception:
            logger.exception("main loop error")
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
