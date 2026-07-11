from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from raspberry_executor.config import load_settings
from raspberry_executor.exchange_factory import create_margin_exchange
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_settings import margin_dry_run
from raspberry_executor.position_order_helpers import is_tp_order, order_price, order_qty
from raspberry_executor.position_sync_v2 import (
    _confirm_open_take_profit_order,
    _order_id,
    _tp_confirmation_timed_out,
    tp_confirmation_max_age_seconds,
)
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-margin-position-reconcile")


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _quantity(position: dict[str, Any]) -> float:
    return max(0.0, _float(position.get("vol")) - _float(position.get("vol_closed")))


def _entry_order_id(key: str, position: dict[str, Any]) -> str:
    return str(position.get("ordertxid") or key)


def _normalize_symbol(pair: str, rules: Any = None) -> str:
    symbol = str(pair or "").upper().replace("/", "")
    if rules is not None:
        try:
            return str(rules.symbol_info(symbol).get("symbol") or symbol).upper()
        except Exception:
            pass
    return symbol.replace("XBT", "BTC")


def _quote_asset(symbol: str, rules: Any = None) -> str | None:
    if rules is not None:
        try:
            return str(rules.symbol_info(symbol).get("quoteAsset") or "").upper() or None
        except Exception:
            pass
    for quote in ("USDC", "USDT", "USD", "EUR", "BTC", "ETH"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return quote
    return None


def _entry_price(position: dict[str, Any], quantity: float) -> float | None:
    cost = _float(position.get("cost"))
    if cost > 0 and quantity > 0:
        return cost / quantity
    return None


def _leverage(position: dict[str, Any]) -> float | str | None:
    value = position.get("leverage")
    if value not in (None, ""):
        return value
    cost = _float(position.get("cost"))
    margin = _float(position.get("margin"))
    if cost > 0 and margin > 0:
        return cost / margin
    return None


def _same_quantity(local_qty: Any, remote_qty: float) -> bool:
    return abs(_float(local_qty) - remote_qty) <= max(remote_qty * 0.001, 1e-8)


def _find_local(local_positions: dict[str, dict[str, Any]], *, entry_order_id: str, symbol: str, quantity: float) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    for candidate_id, local in local_positions.items():
        if str(local.get("entry_order_id") or "") == str(entry_order_id):
            return candidate_id, local
    for candidate_id, local in local_positions.items():
        local_symbol = str(local.get("execution_symbol") or local.get("signal_symbol") or "").upper()
        if local_symbol == symbol and str(local.get("mode") or "").lower() == "margin" and str(local.get("side") or "").lower() == "long" and _same_quantity(local.get("quantity"), quantity):
            return candidate_id, local
    return None, None


def _list_open_orders(margin: Any, symbol: str) -> list[dict[str, Any]]:
    try:
        return margin.open_margin_orders(symbol) or []
    except Exception as exc:
        logger.warning("kraken margin open orders lookup failed symbol=%s error=%s", symbol, str(exc))
        return []


def _find_existing_tp(margin: Any, symbol: str, expected_qty: float) -> dict[str, Any] | None:
    orders = [o for o in _list_open_orders(margin, symbol) if is_tp_order(o) and order_qty(o) >= expected_qty * 0.02]
    if not orders:
        return None
    return sorted(orders, key=lambda o: order_price(o), reverse=True)[0]


def reconcile_kraken_margin_positions() -> dict:
    settings = load_settings()
    _kraken, margin, rules = create_margin_exchange(settings, dry_run=margin_dry_run())
    state = StateStore()
    remote_positions = margin.open_positions() or {}
    local_positions = state.open_positions()
    summary = {"remote_checked": 0, "already_local": 0, "imported": 0, "skipped": 0, "errors": 0}

    for key, remote in remote_positions.items():
        summary["remote_checked"] += 1
        if not isinstance(remote, dict):
            summary["skipped"] += 1
            logger.warning("kraken margin position import skipped reason=invalid_payload payload=%s", remote)
            continue
        side_type = str(remote.get("type") or "").lower()
        if side_type != "buy":
            summary["skipped"] += 1
            logger.info("kraken margin position skipped short/unsupported key=%s type=%s", key, side_type)
            continue
        qty = _quantity(remote)
        if qty <= 0:
            summary["skipped"] += 1
            logger.warning("kraken margin position import skipped reason=empty_quantity payload=%s", remote)
            continue
        symbol = _normalize_symbol(str(remote.get("pair") or ""), rules)
        entry_id = _entry_order_id(str(key), remote)
        logger.info("kraken margin remote position detected symbol=%s entry=%s qty=%s", symbol, entry_id, qty)
        local_id, _local = _find_local(local_positions, entry_order_id=entry_id, symbol=symbol, quantity=qty)
        if local_id:
            updates = {"last_kraken_open_position_sync_ts": time.time(), "kraken_open_position_payload": remote, "imported_from_kraken_open_positions": bool(_local.get("imported_from_kraken_open_positions"))}
            tp_id = _local.get("tp_order_id")
            target_price = _float(_local.get("target_price"))
            confirmed_tp = None
            if tp_id and not _local.get("tp_exchange_confirmed") and target_price > 0:
                confirmed_tp = _confirm_open_take_profit_order(local_id, symbol, qty, target_price, str(tp_id), None, margin, state, use_margin=True, attempts=1, sleep_seconds=0)
            if confirmed_tp:
                updates.update({
                    "tp_order_id": _order_id(confirmed_tp) or tp_id,
                    "tp_payload": confirmed_tp,
                    "kraken_tp_status": confirmed_tp,
                    "tp_exchange_confirmed": True,
                    "tp_exchange_confirmed_at": _now_iso(),
                    "needs_tp_confirmation": False,
                    "needs_tp_replay": False,
                    "order_monitor_mode": "margin",
                })
                state.update_open_position(local_id, updates, event_type="tp_exchange_confirmation_completed")
                logger.info("tp exchange confirmation completed candidate=%s symbol=%s tp_order_id=%s", local_id, symbol, updates.get("tp_order_id"))
            elif tp_id and _local.get("needs_tp_confirmation") and not _tp_confirmation_timed_out(_local):
                updates.update({"tp_exchange_confirmed": False, "needs_tp_confirmation": True, "last_tp_confirmation_miss_ts": _now_iso()})
                state.update_open_position(local_id, updates)
                logger.info("tp confirmation pending candidate=%s symbol=%s tp=%s timeout=%ss", local_id, symbol, tp_id, tp_confirmation_max_age_seconds())
            else:
                existing_tp = _find_existing_tp(margin, symbol, qty)
                if existing_tp:
                    updates.update({
                        "tp_order_id": _order_id(existing_tp),
                        "tp_payload": existing_tp,
                        "kraken_tp_status": existing_tp,
                        "tp_exchange_confirmed": True,
                        "tp_exchange_confirmed_at": _now_iso(),
                        "needs_tp_confirmation": False,
                        "needs_tp_replay": False,
                        "target_price": order_price(existing_tp) or _local.get("target_price"),
                        "tp_protected_quantity": order_qty(existing_tp),
                        "order_monitor_mode": "margin",
                    })
                    state.update_open_position(local_id, updates, event_type="tp_existing_order_attached")
                    logger.info("kraken margin attached existing TP candidate=%s symbol=%s tp=%s", local_id, symbol, updates.get("tp_order_id"))
                elif tp_id and _local.get("needs_tp_confirmation") and _tp_confirmation_timed_out(_local):
                    updates.update({"tp_order_id": None, "tp_payload": {}, "needs_tp_replay": True, "needs_tp_confirmation": False, "tp_exchange_confirmed": False})
                    state.update_open_position(local_id, updates, event_type="tp_exchange_confirmation_timeout")
                else:
                    updates.update({"needs_tp_replay": True})
                    state.update_open_position(local_id, updates)
            summary["already_local"] += 1
            logger.info("kraken margin position already local symbol=%s entry=%s", symbol, entry_id)
            continue
        try:
            tp = _find_existing_tp(margin, symbol, qty)
            tp_id = _order_id(tp) if tp else None
            if tp_id:
                logger.info("kraken margin attached existing TP symbol=%s entry=%s tp=%s", symbol, entry_id, tp_id)
            else:
                logger.info("kraken margin imported without TP, needs replay symbol=%s entry=%s", symbol, entry_id)
            candidate_id = f"kraken-margin-{symbol}-{entry_id}"
            payload = {
                "candidate_id": candidate_id,
                "signal_symbol": symbol,
                "execution_symbol": symbol,
                "side": "long",
                "strategy": "kraken_margin_import",
                "mode": "margin",
                "margin_account_mode": "cross",
                "margin_isolated": False,
                "quantity": str(qty),
                "entry_price": _entry_price(remote, qty),
                "entry_order_id": entry_id,
                "entry_payload": remote,
                "kraken_open_position_payload": remote,
                "imported_from_kraken_open_positions": True,
                "imported_at": _now_iso(),
                "target_price": order_price(tp) if tp else None,
                "tp_order_id": tp_id,
                "tp_payload": tp or {},
                "exit_strategy": "take_profit_only",
                "needs_tp_replay": not bool(tp_id),
                "needs_tp_confirmation": False,
                "tp_exchange_confirmed": bool(tp_id),
                "tp_exchange_confirmed_at": _now_iso() if tp_id else None,
                "leverage": _leverage(remote),
                "quote_asset": _quote_asset(symbol, rules),
            }
            if tp:
                payload["tp_protected_quantity"] = order_qty(tp)
            state.add_open_position(candidate_id, payload)
            state.add_event(candidate_id, "kraken_margin_position_imported", {"remote_position": remote, "tp_order_id": tp_id})
            local_positions[candidate_id] = payload
            summary["imported"] += 1
            logger.info("kraken margin position imported candidate=%s symbol=%s qty=%s entry=%s tp=%s", candidate_id, symbol, qty, payload.get("entry_price"), tp_id)
        except Exception as exc:
            summary["errors"] += 1
            logger.warning("kraken margin position import failed symbol=%s entry=%s error=%s payload=%s", symbol, entry_id, str(exc), remote)
    return summary


if __name__ == "__main__":
    print(reconcile_kraken_margin_positions())
