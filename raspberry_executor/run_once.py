from __future__ import annotations

from raspberry_executor.config import load_settings
from raspberry_executor.exchange_factory import create_spot_exchange
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.main import execute_candidate, report_final_events
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-executor-run-once")


def run_once(limit: int = 10) -> dict:
    settings = load_settings()
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    exchange, _rules = create_spot_exchange(settings)
    state = StateStore()
    guard = RiskGuard(settings.allowed_symbols, settings.max_candidate_age_seconds)

    candidates = signalmaker.get_open_candidates(limit=limit)
    for candidate in candidates:
        execute_candidate(settings, exchange, state, guard, candidate)
    report_final_events(exchange, state)
    summary = {
        "exchange": getattr(exchange, "exchange_name", settings.exchange),
        "fetched": len(candidates),
        "open_positions": len(state.open_positions()),
    }
    logger.info("run_once summary=%s", summary)
    return summary


if __name__ == "__main__":
    print(run_once())
