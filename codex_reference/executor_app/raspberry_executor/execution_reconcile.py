def _f(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def has_live_order_id(order_id) -> bool:
    text = str(order_id or "")
    return bool(text) and not text.startswith("dry-") and not text.startswith("dry_")


def ensure_live_order_id(result: dict, label: str = "entry") -> tuple[bool, str]:
    order_id = result.get("entry_order_id") or result.get("order_id") or result.get("orderId")
    if result.get("dry_run") or (result.get("entry_payload") or {}).get("dry_run"):
        return True, "dry_run"
    if has_live_order_id(order_id):
        return True, "ok"
    return False, f"missing_live_{label}_order_id"


def spot_total_balance(kraken, asset: str) -> float:
    wanted = asset.upper()
    for row in kraken.account().get("balances") or []:
        if str(row.get("asset", "")).upper() == wanted:
            return _f(row.get("free")) + _f(row.get("locked"))
    return 0.0


def confirm_spot_long(kraken, rules, symbol: str, result: dict) -> tuple[bool, str]:
    if kraken.dry_run:
        return True, "dry_run"
    ok, reason = ensure_live_order_id(result, "spot_entry")
    if not ok:
        return False, reason
    base = rules.base_asset(symbol)
    qty = _f(result.get("quantity"))
    total = spot_total_balance(kraken, base)
    if qty > 0 and total >= min(qty * 0.25, qty):
        return True, "confirmed_spot_balance"
    if total > 0:
        return True, "confirmed_spot_any_balance"
    return False, f"spot_balance_not_found:{base}"


def _margin_asset_rows(margin, symbol: str):
    data = margin.margin_account(symbol)
    return data.get("userAssets") or data.get("assets") or []


def margin_asset_stats(margin, symbol: str, asset: str) -> dict:
    wanted = asset.upper()
    for row in _margin_asset_rows(margin, symbol):
        if str(row.get("asset") or "").upper() == wanted:
            return {
                "free": _f(row.get("free")),
                "locked": _f(row.get("locked")),
                "borrowed": _f(row.get("borrowed")),
                "interest": _f(row.get("interest")),
                "netAsset": _f(row.get("netAsset")),
            }
    return {"free": 0.0, "locked": 0.0, "borrowed": 0.0, "interest": 0.0, "netAsset": 0.0}


def confirm_margin_long(manager, symbol: str, result: dict) -> tuple[bool, str]:
    if manager.margin.dry_run:
        return True, "dry_run"
    ok, reason = ensure_live_order_id(result, "margin_entry")
    if not ok:
        return False, reason
    base = manager.rules.base_asset(symbol)
    stats = margin_asset_stats(manager.margin, symbol, base)
    total = stats["free"] + stats["locked"]
    qty = _f(result.get("quantity"))
    if qty > 0 and total >= min(qty * 0.25, qty):
        return True, "confirmed_margin_base_balance"
    if total > 0 or stats["netAsset"] > 0:
        return True, "confirmed_margin_base_any_balance"
    return False, f"margin_base_not_found:{base}:{stats}"


def confirm_margin_short(manager, symbol: str, result: dict) -> tuple[bool, str]:
    if manager.margin.dry_run:
        return True, "dry_run"
    ok, reason = ensure_live_order_id(result, "margin_short_entry")
    if not ok:
        return False, reason
    base = result.get("base_asset") or manager.rules.base_asset(symbol)
    stats = margin_asset_stats(manager.margin, symbol, base)
    debt = stats["borrowed"] + stats["interest"]
    qty = _f(result.get("borrow_base_amount") or result.get("quantity"))
    if qty > 0 and debt >= min(qty * 0.25, qty):
        return True, "confirmed_margin_base_debt"
    if debt > 0:
        return True, "confirmed_margin_any_debt"
    return False, f"margin_debt_not_found:{base}:{stats}"
