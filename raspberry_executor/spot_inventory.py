from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.state import StateStore


def _local_positions():
    out = {}
    for cid, pos in StateStore().open_positions().items():
        symbol = str(pos.get("execution_symbol") or pos.get("signal_symbol") or "").upper()
        if symbol:
            out[symbol] = {"candidate_id": cid, "position": pos}
    return out


def scan_inventory(min_notional=1.0):
    settings = load_settings()
    client = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    rules = BinanceSymbolRules(settings.binance_base_url)
    local = _local_positions()
    rows = []
    if settings.dry_run:
        return {"status": "dry_run", "rows": [], "count": 0}

    seen_assets = set()
    for symbol in sorted(local.keys()):
        try:
            base = rules.base_asset(symbol)
            seen_assets.add(base)
            qty = client.free_balance(base)
            if qty <= 0:
                continue
            price = client.current_price(symbol)
            notional = qty * price
            if notional < min_notional:
                continue
            linked = local.get(symbol, {})
            rows.append({"asset": base, "symbol": symbol, "free": qty, "price": price, "notional": notional, "tracked": True, "candidate_id": linked.get("candidate_id"), "local_position": linked.get("position") or {}})
        except Exception:
            continue

    return {"status": "ok", "rows": rows, "count": len(rows), "seen_assets": sorted(seen_assets)}


if __name__ == "__main__":
    print(scan_inventory())
