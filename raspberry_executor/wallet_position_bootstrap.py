from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
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
        if symbol:
            symbols.add(symbol)
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


def bootstrap_wallet_positions(min_notional: float = 1.0):
    settings = load_settings()
    state = StateStore()
    if settings.dry_run:
        return {"status": "dry_run", "created": 0, "seen": 0, "skipped": 0}

    client = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run)
    quote_assets = [q.upper() for q in settings.quote_assets]
    existing_symbols = _existing_symbols(state)
    created = 0
    seen = 0
    skipped = 0

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
        if symbol in existing_symbols:
            continue

        candidate_id = f"wallet-{symbol}"
        state.add_open_position(candidate_id, {
            "candidate_id": candidate_id,
            "signal_symbol": symbol,
            "execution_symbol": symbol,
            "side": "long",
            "quantity": str(total),
            "entry_price": price,
            "stop_price": None,
            "target_price": None,
            "entry_order_id": None,
            "oco_order_list_id": None,
            "tp_order_id": None,
            "sl_order_id": None,
            "source": "binance_wallet_bootstrap",
            "wallet_asset": asset,
            "wallet_quote": quote,
            "wallet_free": free,
            "wallet_locked": locked,
            "wallet_notional": notional,
        })
        existing_symbols.add(symbol)
        created += 1

    summary = {"status": "ok", "seen": seen, "created": created, "skipped": skipped}
    if created:
        logger.info("wallet bootstrap summary=%s", summary)
    return summary


if __name__ == "__main__":
    print(bootstrap_wallet_positions())
