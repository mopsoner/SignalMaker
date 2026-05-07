import time

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-executor")


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


def execute_candidate(settings, order_manager: SpotOrderManager, state: StateStore, guard: RiskGuard, candidate: dict) -> None:
    candidate_id = candidate["candidate_id"]
    accepted, reason = guard.accept(candidate, already_executed=state.already_executed(candidate_id))
    if not accepted:
        logger.info("skip candidate=%s reason=%s", candidate_id, reason)
        return

    execution_symbol = guard.execution_symbol(candidate)
    side = guard.normalize_side(str(candidate["side"]))
    if side != "long":
        logger.info("skip candidate=%s reason=spot_executor_only_supports_long_oco", candidate_id)
        return

    try:
        logger.info("execute candidate=%s symbol=%s side=%s amount=%s exits=oco", candidate_id, execution_symbol, side, settings.order_quote_amount)
        order_result = order_manager.open_long_with_oco(
            symbol=execution_symbol,
            quote_amount=settings.order_quote_amount,
            target_price=float(candidate["target_price"]),
            stop_price=float(candidate["stop_price"]),
        )

        state.mark_executed(candidate_id)
        state.add_open_position(candidate_id, {
            "candidate_id": candidate_id,
            "signal_symbol": candidate["symbol"],
            "execution_symbol": execution_symbol,
            "side": side,
            "quantity": order_result["quantity"],
            "entry_price": float(order_result["entry_price"]),
            "stop_price": float(candidate["stop_price"]),
            "target_price": float(candidate["target_price"]),
            "entry_order_id": order_result.get("entry_order_id"),
            "oco_order_list_id": order_result.get("oco_order_list_id"),
            "tp_order_id": order_result.get("tp_order_id"),
            "sl_order_id": order_result.get("sl_order_id"),
            "candidate": candidate,
            "entry_payload": order_result.get("entry_payload") or {},
            "oco_payload": order_result.get("oco_payload") or {},
        })
        logger.info(
            "local position opened candidate=%s symbol=%s qty=%s oco_order_list_id=%s",
            candidate_id,
            execution_symbol,
            order_result["quantity"],
            order_result.get("oco_order_list_id"),
        )
    except Exception as exc:
        logger.exception("execution failed candidate=%s", candidate_id)
        state.add_event(candidate_id, "execution_error", {"error": str(exc), "candidate": candidate})


def main() -> None:
    settings = load_settings()
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    rules = BinanceSymbolRules(settings.binance_base_url)
    order_manager = SpotOrderManager(binance, rules)
    state = StateStore()
    guard = RiskGuard(settings.quote_assets, settings.max_candidate_age_seconds, allow_shorts=settings.allow_shorts)

    logger.info(
        "Raspberry spot OCO executor started gateway_id=%s dry_run=%s quote_assets=%s allow_shorts=%s order_quote_amount=%s exits=oco",
        settings.gateway_id,
        settings.dry_run,
        settings.quote_assets,
        settings.allow_shorts,
        settings.order_quote_amount,
    )
    while True:
        try:
            candidates = signalmaker.get_open_candidates(limit=10)
            logger.info("candidates fetched count=%s", len(candidates))
            for candidate in candidates:
                execute_candidate(settings, order_manager, state, guard, candidate)
            report_final_events(binance, state)
        except Exception:
            logger.exception("main loop error")
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
