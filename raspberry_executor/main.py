import time

from typing import Any

from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.classic_candidate_executor import execute_classic_candidate
from raspberry_executor.exchange_factory import create_spot_exchange
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore
import raspberry_executor.position_sync_v2 as position_sync_v2

logger = setup_logging("raspberry-executor")


def report_final_events(exchange: Any, state: StateStore) -> None:
    for candidate_id, position in list(state.open_positions().items()):
        symbol = position["execution_symbol"]
        tp_order_id = position.get("tp_order_id")
        try:
            tp_status = exchange.get_order(symbol, tp_order_id) if tp_order_id else None
        except Exception as exc:
            logger.warning("order status failed candidate=%s error=%s", candidate_id, exc)
            continue

        if tp_status and str(tp_status.get("status", "")).upper() == "FILLED":
            state.close_position(candidate_id, "take_profit_filled", tp_status)
            logger.info("local position closed candidate=%s reason=take_profit_filled", candidate_id)


def execute_candidate(settings, exchange: Any, state: StateStore, guard: RiskGuard, candidate: dict, rules: Any | None = None, spot_manager: SpotOrderManager | None = None) -> None:
    """Compatibility wrapper around the unified classic candidate executor."""
    if spot_manager is None and rules is not None:
        spot_manager = SpotOrderManager(exchange, rules)
    execute_classic_candidate(settings, exchange, rules, None, spot_manager, state, guard, candidate)

def main() -> None:
    settings = load_settings()
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    exchange, rules = create_spot_exchange(settings)
    spot_manager = SpotOrderManager(exchange, rules)
    state = StateStore()
    guard = RiskGuard(settings.allowed_symbols, settings.max_candidate_age_seconds)

    logger.info(
        "Raspberry executor started gateway_id=%s dry_run=%s order_quote_amount=%s",
        settings.gateway_id,
        settings.dry_run,
        settings.order_quote_amount,
    )
    while True:
        try:
            candidates = signalmaker.get_open_candidates(limit=10)
            logger.info("candidates fetched count=%s", len(candidates))
            for candidate in candidates:
                execute_candidate(settings, exchange, state, guard, candidate, rules, spot_manager)
            report_final_events(exchange, state)
            position_sync_v2.sync_open_positions()
        except Exception:
            logger.exception("main loop error")
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
