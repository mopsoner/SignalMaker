import os
import time

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import margin_dry_run, margin_enabled, margin_isolated
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-margin-executor")


def candidate_fetch_limit() -> int:
    try:
        return max(10, int(os.getenv("CANDIDATE_FETCH_LIMIT", "100") or "100"))
    except Exception:
        return 100


def process_candidate(settings, manager: MarginOrderManager, state: StateStore, guard: RiskGuard, candidate: dict) -> str:
    candidate_id = candidate.get("candidate_id")
    if not candidate_id:
        return "missing_candidate_id"
    accepted, reason = guard.accept(candidate, already_executed=state.already_executed(candidate_id))
    if not accepted:
        return reason

    symbol = guard.execution_symbol(candidate)
    side = guard.normalize_side(str(candidate.get("side", "")))

    if side == "short":
        result = manager.sell_all_margin_base(symbol=symbol)
        if result.get("status") == "sold":
            state.mark_executed(candidate_id)
            state.add_event(candidate_id, "margin_base_sold_on_short", {"candidate": candidate, "result": result})
            logger.info("margin base sold candidate=%s symbol=%s qty=%s", candidate_id, symbol, result.get("quantity"))
            return "sold"
        return str(result.get("reason") or "margin_short_no_asset")

    try:
        result = manager.open_long_with_margin_oco(
            symbol=symbol,
            quote_amount=float(settings.order_quote_amount),
            target_price=float(candidate["target_price"]),
            stop_price=float(candidate["stop_price"]),
        )
        state.mark_executed(candidate_id)
        state.add_open_position(candidate_id, {
            "candidate_id": candidate_id,
            "signal_symbol": candidate["symbol"],
            "execution_symbol": symbol,
            "side": "long",
            "mode": "margin",
            "margin_isolated": result.get("margin_isolated"),
            "margin_multiplier": result.get("margin_multiplier"),
            "quantity": result["quantity"],
            "entry_price": float(result["entry_price"]),
            "stop_price": float(candidate["stop_price"]),
            "target_price": float(candidate["target_price"]),
            "entry_order_id": result.get("entry_order_id"),
            "oco_order_list_id": result.get("oco_order_list_id"),
            "tp_order_id": result.get("tp_order_id"),
            "sl_order_id": result.get("sl_order_id"),
            "candidate": candidate,
            "margin_payload": result,
            "entry_payload": result.get("entry_payload") or {},
            "oco_payload": result.get("oco_payload") or {},
        })
        logger.info("margin long opened candidate=%s symbol=%s qty=%s oco=%s", candidate_id, symbol, result["quantity"], result.get("oco_order_list_id"))
        return "opened"
    except Exception as exc:
        text = str(exc)
        logger.error("margin long failed candidate=%s error=%s", candidate_id, text)
        state.add_event(candidate_id, "margin_execution_error", {"error": text, "candidate": candidate})
        return "error"


def main() -> None:
    settings = load_settings()
    if not margin_enabled():
        logger.warning("margin executor started while MARGIN_MODE_ENABLED is false")
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    binance = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run or margin_dry_run())
    rules = BinanceSymbolRules(settings.binance_base_url)
    margin = MarginClient(binance, isolated=margin_isolated(), dry_run=settings.dry_run or margin_dry_run())
    manager = MarginOrderManager(binance, margin, rules)
    state = StateStore()
    guard = RiskGuard(settings.quote_assets, settings.max_candidate_age_seconds)
    fetch_limit = candidate_fetch_limit()

    logger.info("Raspberry margin executor started dry_run=%s isolated=%s quote_assets=%s amount=%s limit=%s", margin.dry_run, margin.isolated, settings.quote_assets, settings.order_quote_amount, fetch_limit)
    while True:
        try:
            candidates = signalmaker.get_open_candidates(limit=fetch_limit)
            stats = {"fetched": len(candidates), "opened": 0, "sold": 0, "errors": 0, "skipped": 0}
            for candidate in candidates:
                result = process_candidate(settings, manager, state, guard, candidate)
                if result == "opened":
                    stats["opened"] += 1
                elif result == "sold":
                    stats["sold"] += 1
                elif result == "error":
                    stats["errors"] += 1
                else:
                    stats["skipped"] += 1
            logger.info("margin executor summary=%s", stats)
        except Exception as exc:
            logger.error("margin executor loop error=%s", str(exc))
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
