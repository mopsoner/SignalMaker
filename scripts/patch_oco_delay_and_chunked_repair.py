from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# 1) Wait after BUY before first OCO
# ---------------------------------------------------------------------------
manager_path = ROOT / "raspberry_executor" / "margin_order_manager.py"
manager = manager_path.read_text()

if "def oco_after_buy_delay_seconds" not in manager:
    manager = manager.replace(
        "def amount_str(value: float) -> str:\n    return f\"{float(value):.8f}\".rstrip(\"0\").rstrip(\".\")\n",
        "def amount_str(value: float) -> str:\n    return f\"{float(value):.8f}\".rstrip(\"0\").rstrip(\".\")\n\n\ndef oco_after_buy_delay_seconds() -> float:\n    try:\n        return max(0.0, float(os.getenv(\"MARGIN_OCO_AFTER_BUY_DELAY_SECONDS\", \"20\") or \"20\"))\n    except Exception:\n        return 20.0\n",
    )

if "def _post_buy_oco_delay" not in manager:
    manager = manager.replace(
        "    def _entry_confirm_poll_seconds(self) -> float:\n        try:\n            return max(0.2, float(os.getenv(\"MARGIN_ENTRY_CONFIRM_POLL_SECONDS\", \"0.5\") or \"0.5\"))\n        except Exception:\n            return 0.5\n",
        "    def _entry_confirm_poll_seconds(self) -> float:\n        try:\n            return max(0.2, float(os.getenv(\"MARGIN_ENTRY_CONFIRM_POLL_SECONDS\", \"0.5\") or \"0.5\"))\n        except Exception:\n            return 0.5\n\n    def _post_buy_oco_delay(self) -> float:\n        if self.margin.dry_run:\n            return 0.0\n        return oco_after_buy_delay_seconds()\n",
    )

old_result = '"entry_payload": entry}'
new_result = '"entry_payload": entry, "oco_after_buy_delay_seconds": self._post_buy_oco_delay()}'
if old_result in manager and new_result not in manager:
    manager = manager.replace(old_result, new_result)

old_try = "        try:\n            oco_result = self.create_margin_oco_sell(symbol=symbol, quantity=executed_qty, target_price=target_price, stop_price=stop_price)\n"
new_try = "        delay = self._post_buy_oco_delay()\n        if delay > 0:\n            time.sleep(delay)\n\n        try:\n            oco_result = self.create_margin_oco_sell(symbol=symbol, quantity=executed_qty, target_price=target_price, stop_price=stop_price)\n"
if old_try in manager and new_try not in manager:
    manager = manager.replace(old_try, new_try)

manager_path.write_text(manager)
print("patched OCO after-buy delay", manager_path)

# ---------------------------------------------------------------------------
# 2) Chunked OCO repair: use available base / 2 repeatedly until no usable base
# ---------------------------------------------------------------------------
sync_path = ROOT / "raspberry_executor" / "position_sync_v2.py"
sync = sync_path.read_text()

if "def _oco_repair_max_chunks" not in sync:
    sync = sync.replace(
        "def max_oco_repair_attempts() -> int:\n",
        "def _oco_repair_max_chunks() -> int:\n    try:\n        return max(1, int(os.getenv(\"OCO_REPAIR_MAX_CHUNKS\", \"8\") or \"8\"))\n    except Exception:\n        return 8\n\n\ndef _oco_repair_min_remaining_ratio() -> float:\n    try:\n        return min(0.95, max(0.01, float(os.getenv(\"OCO_REPAIR_MIN_REMAINING_RATIO\", \"0.02\") or \"0.02\")))\n    except Exception:\n        return 0.02\n\n\ndef max_oco_repair_attempts() -> int:\n",
    )

old_block = '''    use_margin = _is_margin_position(position)
    if binance is not None and rules is not None:
        if _attach_existing_exit_orders(candidate_id, position, symbol, binance, margin_manager.margin, rules, state, use_margin=use_margin):
            return "attached_existing_orders"
        available = _available_base_balance(binance, margin_manager.margin, rules, symbol, use_margin=use_margin)
        qty_ok, repair_qty, qty_reason = _repair_quantity(position, available)
        if not qty_ok:
            payload = {"symbol": symbol, "mode": position.get("mode"), "reason": qty_reason, "expected_quantity": position.get("quantity"), "available_base": available, "levels": levels}
            _mark_repair_skip(state, candidate_id, position, "oco_repair_skipped_quantity_mismatch", payload)
            logger.warning("oco repair skipped quantity mismatch candidate=%s symbol=%s reason=%s", candidate_id, symbol, qty_reason)
            return "quantity_mismatch"
        quantity = repair_qty

    try:
        if use_margin:
            result = margin_manager.create_margin_oco_sell(symbol=symbol, quantity=quantity, target_price=float(levels["target_price"]), stop_price=float(levels["stop_price"]))
            repair_mode = "margin"
        else:
            result = spot_manager.create_exit_oco_for_open_long(symbol=symbol, quantity=quantity, target_price=float(levels["target_price"]), stop_price=float(levels["stop_price"]))
            repair_mode = "spot"
    except Exception as exc:
        payload = {"symbol": symbol, "mode": position.get("mode"), "reason": str(exc), "quantity": quantity, "levels": levels}
        _mark_repair_skip(state, candidate_id, position, "oco_repair_failed", payload)
        raise

    updates = {
        "target_price": float(levels["target_price"]), "stop_price": float(levels["stop_price"]), "quantity": result.get("quantity") or quantity,
        "oco_order_list_id": result.get("oco_order_list_id"), "tp_order_id": result.get("tp_order_id"), "sl_order_id": result.get("sl_order_id"),
        "oco_payload": result.get("oco_payload") or {}, "oco_repair_level_source": levels, "oco_repair_mode": repair_mode,
        "oco_repair_validated_quantity": quantity, "oco_repair_attempts": 0, "oco_repair_blocked": False,
        "last_oco_repair_skip_reason": None, "last_oco_repair_skip_ts": None,
    }
    state.update_open_position(candidate_id, updates, event_type="oco_repaired")
    logger.info("oco repaired candidate=%s symbol=%s mode=%s qty=%s source=%s", candidate_id, symbol, repair_mode, quantity, levels.get("source"))
    return "repaired"
'''

new_block = '''    use_margin = _is_margin_position(position)
    if binance is not None and rules is not None:
        if _attach_existing_exit_orders(candidate_id, position, symbol, binance, margin_manager.margin, rules, state, use_margin=use_margin):
            return "attached_existing_orders"

    repair_mode = "margin" if use_margin else "spot"
    chunks = []
    last_error = None
    protected_qty = 0.0
    first_result = None
    first_quantity = None
    expected_qty = _float(position.get("quantity"))
    available = None

    for chunk_index in range(_oco_repair_max_chunks()):
        if binance is not None and rules is not None:
            available = _available_base_balance(binance, margin_manager.margin, rules, symbol, use_margin=use_margin)
        else:
            available = None

        if available is None:
            candidate_qty = _float(quantity)
        else:
            if available <= ghost_base_epsilon():
                break
            # User rule: repair with available asset divided by 2, then loop.
            candidate_qty = available / 2.0
            # If only dust remains, try the rest once instead of looping forever.
            if expected_qty > 0 and available <= expected_qty * _oco_repair_min_remaining_ratio():
                candidate_qty = available

        try:
            if use_margin:
                result = margin_manager.create_margin_oco_sell(symbol=symbol, quantity=candidate_qty, target_price=float(levels["target_price"]), stop_price=float(levels["stop_price"]))
            else:
                result = spot_manager.create_exit_oco_for_open_long(symbol=symbol, quantity=candidate_qty, target_price=float(levels["target_price"]), stop_price=float(levels["stop_price"]))
            result_qty = _float(result.get("quantity") or candidate_qty)
            protected_qty += result_qty
            chunk = {"chunk": chunk_index + 1, "quantity": result.get("quantity") or candidate_qty, "oco_order_list_id": result.get("oco_order_list_id"), "tp_order_id": result.get("tp_order_id"), "sl_order_id": result.get("sl_order_id"), "payload": result.get("oco_payload") or {}}
            chunks.append(chunk)
            if first_result is None:
                first_result = result
                first_quantity = result.get("quantity") or candidate_qty
            logger.info("oco repair chunk placed candidate=%s symbol=%s mode=%s chunk=%s qty=%s protected_total=%s", candidate_id, symbol, repair_mode, chunk_index + 1, result_qty, protected_qty)
        except Exception as exc:
            last_error = str(exc)
            # If half failed because of filters/notional or invalid price order,
            # smaller chunks will not solve this sync pass.
            if "notional" in last_error.lower() or "invalid_margin_oco_price_order" in last_error.lower():
                break
            break

        if binance is None or rules is None or available is None:
            break

    if not chunks:
        payload = {"symbol": symbol, "mode": position.get("mode"), "reason": last_error or "no_available_base_for_oco_repair", "quantity": quantity, "available_base": available, "levels": levels}
        _mark_repair_skip(state, candidate_id, position, "oco_repair_failed", payload)
        raise RuntimeError(payload["reason"])

    updates = {
        "target_price": float(levels["target_price"]), "stop_price": float(levels["stop_price"]), "quantity": protected_qty or first_quantity,
        "oco_order_list_id": first_result.get("oco_order_list_id"), "tp_order_id": first_result.get("tp_order_id"), "sl_order_id": first_result.get("sl_order_id"),
        "oco_payload": first_result.get("oco_payload") or {}, "oco_repair_level_source": levels, "oco_repair_mode": repair_mode,
        "oco_repair_validated_quantity": protected_qty, "oco_repair_chunks": chunks, "oco_repair_chunks_count": len(chunks),
        "oco_repair_attempts": 0, "oco_repair_blocked": False,
        "last_oco_repair_skip_reason": None, "last_oco_repair_skip_ts": None,
    }
    state.update_open_position(candidate_id, updates, event_type="oco_repaired")
    logger.info("oco repaired candidate=%s symbol=%s mode=%s chunks=%s protected_qty=%s source=%s", candidate_id, symbol, repair_mode, len(chunks), protected_qty, levels.get("source"))
    return "repaired"
'''

if old_block in sync and new_block not in sync:
    sync = sync.replace(old_block, new_block)
else:
    print("chunked repair block already patched or exact block not found")

sync_path.write_text(sync)
print("patched chunked OCO repair", sync_path)
