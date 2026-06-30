import os
import time

from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-executor")


class DustRemaining(RuntimeError):
    def __init__(self, *, symbol: str, base_asset: str, free_qty: float, last_error: Exception | None) -> None:
        self.symbol = symbol
        self.base_asset = base_asset
        self.free_qty = free_qty
        self.last_error = last_error
        super().__init__(f"dust_remaining symbol={symbol} base_asset={base_asset} free_qty={free_qty} last_error={last_error}")


def candidate_fetch_limit() -> int:
    try:
        return max(10, int(os.getenv("CANDIDATE_FETCH_LIMIT", "100") or "100"))
    except Exception:
        return 100


def oco_repair_max_legs() -> int:
    try:
        return max(1, int(os.getenv("OCO_REPAIR_MAX_LEGS", "10") or "10"))
    except Exception:
        return 10


def sell_spot_balance_for_short(kraken: KrakenClient, rules: KrakenSymbolRules, state: StateStore, candidate: dict, execution_symbol: str) -> None:
    candidate_id = candidate["candidate_id"]
    try:
        base_asset = rules.base_asset(execution_symbol)
        free_qty = kraken.free_balance(base_asset)
        if free_qty <= 0:
            reason = f"no_free_balance:{base_asset}"
            logger.info("skip candidate=%s reason=%s", candidate_id, reason)
            state.add_event(candidate_id, "candidate_skipped", {"reason": reason, "candidate": candidate})
            state.mark_executed(candidate_id)
            return
        price = kraken.current_price(execution_symbol)
        qty = rules.normalize_market_quantity(execution_symbol, free_qty)
        rules.ensure_exit_notional(execution_symbol, qty, price, label="spot_sell_on_short")
        logger.info("sell spot balance on short candidate=%s symbol=%s base=%s qty=%s", candidate_id, execution_symbol, base_asset, qty)
        order = kraken.place_market_entry(execution_symbol, "short", qty)
        state.mark_executed(candidate_id)
        state.add_event(candidate_id, "spot_balance_sold_on_short", {
            "symbol": execution_symbol,
            "base_asset": base_asset,
            "quantity": qty,
            "price": price,
            "candidate": candidate,
            "order": order,
        })
        logger.info("spot balance sold candidate=%s symbol=%s qty=%s", candidate_id, execution_symbol, qty)
    except Exception as exc:
        error_text = str(exc)
        logger.error("spot short sell failed candidate=%s error=%s", candidate_id, error_text)
        state.add_event(candidate_id, "spot_short_sell_error", {"error": error_text, "candidate": candidate})


def validate_oco_repair_quantity(
    rules: KrakenSymbolRules,
    *,
    symbol: str,
    raw_qty: float,
    target_price: float,
    stop_price: float,
    label: str,
) -> str:
    qty = rules.normalize_exit_quantity(symbol, raw_qty)
    tp = rules.normalize_exit_price(symbol, target_price)
    stop = rules.normalize_exit_price(symbol, stop_price)
    stop_limit = rules.normalize_exit_price(symbol, float(stop) * 0.999)
    rules.ensure_exit_notional(symbol, qty, tp, label=f"oco_repair_{label}_take_profit")
    rules.ensure_exit_notional(symbol, qty, stop_limit, label=f"oco_repair_{label}_stop_loss")
    return qty


def choose_oco_repair_quantity(
    rules: KrakenSymbolRules,
    *,
    symbol: str,
    base_asset: str,
    free_qty: float,
    target_price: float,
    stop_price: float,
) -> dict:
    if free_qty <= 0:
        raise DustRemaining(symbol=symbol, base_asset=base_asset, free_qty=free_qty, last_error=None)

    half_error = None
    try:
        half_qty = validate_oco_repair_quantity(
            rules,
            symbol=symbol,
            raw_qty=free_qty / 2,
            target_price=target_price,
            stop_price=stop_price,
            label="half_free_asset",
        )
        return {
            "quantity": half_qty,
            "mode": "half_free_asset",
            "base_asset": base_asset,
            "free_qty": free_qty,
            "raw_qty": free_qty / 2,
        }
    except Exception as exc:
        half_error = exc
        logger.warning("oco repair half quantity rejected symbol=%s base=%s free_qty=%s error=%s", symbol, base_asset, free_qty, str(exc))

    try:
        full_qty = validate_oco_repair_quantity(
            rules,
            symbol=symbol,
            raw_qty=free_qty,
            target_price=target_price,
            stop_price=stop_price,
            label="full_residue",
        )
        return {
            "quantity": full_qty,
            "mode": "full_residue",
            "base_asset": base_asset,
            "free_qty": free_qty,
            "raw_qty": free_qty,
            "half_error": str(half_error),
        }
    except Exception as exc:
        logger.warning("oco repair full residue rejected symbol=%s base=%s free_qty=%s error=%s", symbol, base_asset, free_qty, str(exc))
        raise DustRemaining(symbol=symbol, base_asset=base_asset, free_qty=free_qty, last_error=exc) from exc


def repair_missing_oco(kraken: KrakenClient, rules: KrakenSymbolRules, order_manager: SpotOrderManager, state: StateStore) -> None:
    for candidate_id, position in list(state.open_positions().items()):
        side = str(position.get("side") or "").lower()
        if side != "long":
            continue
        if position.get("oco_order_list_id") and position.get("tp_order_id") and position.get("sl_order_id"):
            continue
        symbol = position.get("execution_symbol") or position.get("signal_symbol")
        target_price = position.get("target_price")
        stop_price = position.get("stop_price")
        if not symbol or not target_price or not stop_price:
            reason = "missing_symbol_target_or_stop"
            logger.warning("oco repair skipped candidate=%s reason=%s", candidate_id, reason)
            state.add_event(candidate_id, "oco_repair_skipped", {"reason": reason, "position": position})
            continue

        symbol = str(symbol).upper()
        try:
            base_asset = rules.base_asset(symbol)
        except Exception as exc:
            error_text = str(exc)
            logger.error("oco repair failed candidate=%s error=%s", candidate_id, error_text)
            state.add_event(candidate_id, "oco_repair_failed", {"error": error_text, "position": position})
            continue

        repaired_legs = list(position.get("oco_repair_legs") or [])
        total_repaired_qty = sum(float(leg.get("quantity") or 0) for leg in repaired_legs if isinstance(leg, dict))
        max_legs = oco_repair_max_legs()

        for leg_index in range(len(repaired_legs) + 1, max_legs + 1):
            free_qty = kraken.free_balance(base_asset)
            try:
                quantity_choice = choose_oco_repair_quantity(
                    rules,
                    symbol=symbol,
                    base_asset=base_asset,
                    free_qty=free_qty,
                    target_price=float(target_price),
                    stop_price=float(stop_price),
                )
            except DustRemaining as exc:
                payload = {
                    "symbol": symbol,
                    "base_asset": exc.base_asset,
                    "dust_remaining": exc.free_qty,
                    "last_error": str(exc.last_error) if exc.last_error else None,
                    "position": position,
                    "repaired_legs": repaired_legs,
                }
                logger.info(
                    "oco repair stopped candidate=%s reason=dust_remaining symbol=%s base=%s dust=%s error=%s",
                    candidate_id,
                    symbol,
                    exc.base_asset,
                    exc.free_qty,
                    payload.get("last_error"),
                )
                state.add_event(candidate_id, "dust_remaining", payload)
                break
            except Exception as exc:
                error_text = str(exc)
                logger.error("oco repair failed candidate=%s error=%s", candidate_id, error_text)
                state.add_event(candidate_id, "oco_repair_failed", {"error": error_text, "position": position})
                break

            quantity = quantity_choice["quantity"]
            try:
                logger.warning(
                    "oco missing; repairing candidate=%s leg=%s symbol=%s qty=%s mode=%s free_qty=%s target=%s stop=%s",
                    candidate_id,
                    leg_index,
                    symbol,
                    quantity,
                    quantity_choice.get("mode"),
                    quantity_choice.get("free_qty"),
                    target_price,
                    stop_price,
                )
                result = order_manager.create_exit_oco_for_open_long(
                    symbol=symbol,
                    quantity=quantity,
                    target_price=float(target_price),
                    stop_price=float(stop_price),
                )
                leg = {
                    "leg_index": leg_index,
                    "quantity": result.get("quantity") or quantity,
                    "oco_order_list_id": result.get("oco_order_list_id"),
                    "tp_order_id": result.get("tp_order_id"),
                    "sl_order_id": result.get("sl_order_id"),
                    "mode": quantity_choice.get("mode"),
                    "free_qty_before": quantity_choice.get("free_qty"),
                    "base_asset": quantity_choice.get("base_asset"),
                    "oco_payload": result.get("oco_payload") or {},
                }
                repaired_legs.append(leg)
                total_repaired_qty += float(leg["quantity"])
                updates = {
                    "quantity": str(total_repaired_qty),
                    "oco_order_list_id": leg.get("oco_order_list_id"),
                    "tp_order_id": leg.get("tp_order_id"),
                    "sl_order_id": leg.get("sl_order_id"),
                    "oco_payload": leg.get("oco_payload") or {},
                    "oco_repair_legs": repaired_legs,
                    "oco_repair_last_mode": leg.get("mode"),
                    "oco_repair_base_asset": leg.get("base_asset"),
                    "oco_repair_total_qty": str(total_repaired_qty),
                }
                state.update_open_position(candidate_id, updates, event_type="oco_repaired")
                logger.info(
                    "oco repaired candidate=%s leg=%s oco_order_list_id=%s tp=%s sl=%s mode=%s total_qty=%s",
                    candidate_id,
                    leg_index,
                    leg.get("oco_order_list_id"),
                    leg.get("tp_order_id"),
                    leg.get("sl_order_id"),
                    leg.get("mode"),
                    total_repaired_qty,
                )
                if leg.get("mode") == "full_residue":
                    break
            except Exception as exc:
                error_text = str(exc)
                logger.error("oco repair failed candidate=%s leg=%s error=%s", candidate_id, leg_index, error_text)
                state.add_event(
                    candidate_id,
                    "oco_repair_failed",
                    {
                        "error": error_text,
                        "leg_index": leg_index,
                        "quantity_choice": quantity_choice,
                        "position": position,
                        "repaired_legs": repaired_legs,
                    },
                )
                break
        else:
            logger.warning("oco repair max legs reached candidate=%s symbol=%s max_legs=%s", candidate_id, symbol, max_legs)
            state.add_event(candidate_id, "oco_repair_max_legs_reached", {"symbol": symbol, "max_legs": max_legs, "repaired_legs": repaired_legs})


def report_final_events(kraken: KrakenClient, state: StateStore) -> None:
    for candidate_id, position in list(state.open_positions().items()):
        symbol = position["execution_symbol"]
        tp_order_id = position.get("tp_order_id")
        sl_order_id = position.get("sl_order_id")
        if not tp_order_id or not sl_order_id:
            continue
        try:
            tp_status = kraken.get_order(symbol, tp_order_id) if tp_order_id else None
            sl_status = kraken.get_order(symbol, sl_order_id) if sl_order_id else None
        except Exception as exc:
            logger.warning("order status failed candidate=%s error=%s", candidate_id, str(exc))
            continue

        if tp_status and str(tp_status.get("status", "")).upper() == "FILLED":
            state.close_position(candidate_id, "take_profit_filled", tp_status)
            logger.info("local position closed candidate=%s reason=take_profit_filled", candidate_id)
        elif sl_status and str(sl_status.get("status", "")).upper() == "FILLED":
            state.close_position(candidate_id, "stop_loss_filled", sl_status)
            logger.info("local position closed candidate=%s reason=stop_loss_filled", candidate_id)


def execute_candidate(settings, kraken: KrakenClient, rules: KrakenSymbolRules, order_manager: SpotOrderManager, state: StateStore, guard: RiskGuard, candidate: dict) -> None:
    candidate_id = candidate["candidate_id"]
    accepted, reason = guard.accept(candidate, already_executed=state.already_executed(candidate_id))
    if not accepted:
        logger.info("skip candidate=%s reason=%s", candidate_id, reason)
        state.add_event(candidate_id, "candidate_skipped", {"reason": reason, "candidate": candidate})
        return

    execution_symbol = guard.execution_symbol(candidate)
    side = guard.normalize_side(str(candidate["side"]))
    if side == "short":
        sell_spot_balance_for_short(kraken, rules, state, candidate, execution_symbol)
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
        logger.info("local position opened candidate=%s symbol=%s qty=%s oco_order_list_id=%s", candidate_id, execution_symbol, order_result["quantity"], order_result.get("oco_order_list_id"))
    except Exception as exc:
        error_text = str(exc)
        logger.error("execution failed candidate=%s error=%s", candidate_id, error_text)
        state.add_event(candidate_id, "execution_error", {"error": error_text, "candidate": candidate})


def main() -> None:
    settings = load_settings()
    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    kraken = KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=settings.dry_run)
    rules = KrakenSymbolRules(settings.kraken_base_url)
    order_manager = SpotOrderManager(kraken, rules)
    state = StateStore()
    guard = RiskGuard(settings.quote_assets, settings.max_candidate_age_seconds)
    fetch_limit = candidate_fetch_limit()

    logger.info(
        "Raspberry spot OCO executor started gateway_id=%s dry_run=%s quote_assets=%s order_quote_amount=%s exits=oco candidate_fetch_limit=%s",
        settings.gateway_id,
        settings.dry_run,
        settings.quote_assets,
        settings.order_quote_amount,
        fetch_limit,
    )
    while True:
        try:
            candidates = signalmaker.get_open_candidates(limit=fetch_limit)
            logger.info("candidates fetched count=%s ids=%s", len(candidates), [c.get("candidate_id") for c in candidates])
            for candidate in candidates:
                execute_candidate(settings, kraken, rules, order_manager, state, guard, candidate)
            repair_missing_oco(kraken, rules, order_manager, state)
            report_final_events(kraken, state)
        except Exception as exc:
            logger.error("main loop error=%s", str(exc))
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
