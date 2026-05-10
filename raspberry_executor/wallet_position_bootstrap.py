from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_settings import execution_mode
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-wallet-bootstrap")


def _float(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def _existing_symbols(state: StateStore):
    symbols = set()
    for _, pos in state.open_positions().items():
        symbol = str(pos.get("execution_symbol") or pos.get("signal_symbol") or "").upper()
        side = str(pos.get("side") or "").lower()
        mode = str(pos.get("mode") or "").lower()
        if symbol:
            symbols.add((symbol, side, mode))
            symbols.add((symbol, side, ""))
    return symbols


def _first_valid_symbol(client: BinanceClient, asset: str, quote_assets: list[str]):
    for quote in quote_assets:
        symbol = f"{asset}{quote.upper()}"
        try:
            price = client.current_price(symbol)
            return symbol, quote.upper(), price
        except Exception:
            continue
    return None, None, None


def _add_position(state, existing, *, symbol, side, mode, quantity, price, source, extra):
    key = (symbol.upper(), side.lower(), mode.lower())
    if key in existing or (symbol.upper(), side.lower(), "") in existing:
        return False
    candidate_id = f"{source}-{side}-{symbol}".replace("_", "-")
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": symbol,
        "execution_symbol": symbol,
        "side": side,
        "mode": mode,
        "quantity": str(quantity),
        "entry_price": price,
        "stop_price": None,
        "target_price": None,
        "entry_order_id": None,
        "oco_order_list_id": None,
        "tp_order_id": None,
        "sl_order_id": None,
        "source": source,
        **extra,
    })
    existing.add(key)
    existing.add((symbol.upper(), side.lower(), ""))
    return True


def _bootstrap_spot(client, state, existing, quote_assets, min_notional):
    created = seen = skipped = 0
    account = client.account()
    for bal in account.get("balances") or []:
        asset = str(bal.get("asset") or "").upper()
        if not asset or asset in quote_assets:
            continue
        free = _float(bal.get("free"))
        locked = _float(bal.get("locked"))
        total = free + locked
        if total <= 0:
            continue
        seen += 1
        symbol, quote, price = _first_valid_symbol(client, asset, quote_assets)
        if not symbol or not price:
            skipped += 1
            continue
        notional = total * price
        if notional < min_notional:
            skipped += 1
            continue
        if _add_position(state, existing, symbol=symbol, side="long", mode="spot", quantity=total, price=price, source="binance_spot_bootstrap", extra={"wallet_asset": asset, "wallet_quote": quote, "wallet_free": free, "wallet_locked": locked, "wallet_notional": notional}):
            created += 1
    return seen, created, skipped


def _asset_values(asset_row):
    free = _float(asset_row.get("free"))
    locked = _float(asset_row.get("locked"))
    borrowed = _float(asset_row.get("borrowed"))
    interest = _float(asset_row.get("interest"))
    net = _float(asset_row.get("netAsset"))
    total = free + locked
    debt = borrowed + interest
    return free, locked, borrowed, interest, net, total, debt


def _bootstrap_cross(client, state, existing, quote_assets, min_notional):
    created = seen = skipped = 0
    account = client._signed("GET", "/sapi/v1/margin/account", {})
    for asset_row in account.get("userAssets") or []:
        asset = str(asset_row.get("asset") or "").upper()
        if not asset or asset in quote_assets:
            continue
        free, locked, borrowed, interest, net, total, debt = _asset_values(asset_row)
        if total <= 0 and debt <= 0 and abs(net) <= 0:
            continue
        seen += 1
        symbol, quote, price = _first_valid_symbol(client, asset, quote_assets)
        if not symbol or not price:
            skipped += 1
            continue
        if debt > 0 and net < 0:
            qty = debt
            side = "short"
            notional = qty * price
        else:
            qty = max(total, net, 0.0)
            side = "long"
            notional = qty * price
        if qty <= 0 or notional < min_notional:
            skipped += 1
            continue
        if _add_position(state, existing, symbol=symbol, side=side, mode="cross_margin", quantity=qty, price=price, source="binance_cross_margin_bootstrap", extra={"margin_asset": asset, "margin_quote": quote, "margin_free": free, "margin_locked": locked, "margin_borrowed": borrowed, "margin_interest": interest, "margin_net_asset": net, "margin_notional": notional}):
            created += 1
    return seen, created, skipped


def _bootstrap_isolated(client, state, existing, quote_assets, min_notional):
    created = seen = skipped = 0
    account = client._signed("GET", "/sapi/v1/margin/isolated/account", {})
    for row in account.get("assets") or []:
        symbol = str(row.get("symbol") or "").upper()
        base_row = row.get("baseAsset") or {}
        quote_row = row.get("quoteAsset") or {}
        base = str(base_row.get("asset") or "").upper()
        quote = str(quote_row.get("asset") or "").upper()
        if not symbol or not base or quote not in quote_assets:
            continue
        free, locked, borrowed, interest, net, total, debt = _asset_values(base_row)
        if total <= 0 and debt <= 0 and abs(net) <= 0:
            continue
        seen += 1
        try:
            price = client.current_price(symbol)
        except Exception:
            skipped += 1
            continue
        if debt > 0 and net < 0:
            qty = debt
            side = "short"
            notional = qty * price
        else:
            qty = max(total, net, 0.0)
            side = "long"
            notional = qty * price
        if qty <= 0 or notional < min_notional:
            skipped += 1
            continue
        if _add_position(state, existing, symbol=symbol, side=side, mode="isolated_margin", quantity=qty, price=price, source="binance_isolated_margin_bootstrap", extra={"margin_asset": base, "margin_quote": quote, "margin_free": free, "margin_locked": locked, "margin_borrowed": borrowed, "margin_interest": interest, "margin_net_asset": net, "margin_notional": notional}):
            created += 1
    return seen, created, skipped


def bootstrap_wallet_positions(min_notional: float = 1.0):
    settings = load_settings()
    state = StateStore()
    client = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    quote_assets = [q.upper() for q in settings.quote_assets]
    existing = _existing_symbols(state)
    mode = execution_mode()

    if not client.is_configured():
        return {"status": "missing_api_credentials", "mode": mode, "created": 0, "seen": 0, "skipped": 0}

    try:
        if mode == "spot":
            seen, created, skipped = _bootstrap_spot(client, state, existing, quote_assets, min_notional)
        elif mode == "isolated":
            seen, created, skipped = _bootstrap_isolated(client, state, existing, quote_assets, min_notional)
        else:
            seen, created, skipped = _bootstrap_cross(client, state, existing, quote_assets, min_notional)
    except Exception as exc:
        logger.error("position bootstrap failed mode=%s error=%s", mode, str(exc))
        return {"status": "error", "mode": mode, "error": str(exc), "created": 0, "seen": 0, "skipped": 0}

    summary = {"status": "ok", "mode": mode, "seen": seen, "created": created, "skipped": skipped}
    if created or seen:
        logger.info("position bootstrap summary=%s", summary)
    return summary


if __name__ == "__main__":
    print(bootstrap_wallet_positions())
