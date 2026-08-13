from __future__ import annotations

"""Non-destructive, live preflight for the Kraken calls used by SignalMaker."""

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from typing import Any, Callable

from app.core.config import settings
from app.services.execution.kraken_client import KrakenClient
from app.services.execution.kraken_symbol_rules import KrakenSymbolRules
from app.services.kraken_candle_importer import KrakenPair, fetch_kraken_ohlc


@dataclass(frozen=True)
class Check:
    name: str
    endpoint: str
    ok: bool
    detail: str


def _run(checks: list[Check], name: str, endpoint: str, call: Callable[[], str]) -> Any | None:
    try:
        detail = call()
    except Exception as exc:  # report every independent endpoint in one run
        checks.append(Check(name, endpoint, False, f"{type(exc).__name__}: {exc}"))
        return None
    checks.append(Check(name, endpoint, True, detail))
    return detail


def check_workflow(symbol: str, quote_amount: float, modes: list[str]) -> list[Check]:
    client = KrakenClient(
        settings.kraken_base_url,
        settings.kraken_api_key,
        settings.kraken_secret_key,
        dry_run=True,
    )
    rules = KrakenSymbolRules(
        client,
        [value.strip() for value in settings.kraken_quote_assets.split(",") if value.strip()],
    )
    checks: list[Check] = []
    info: dict[str, Any] | None = None

    def pair_check() -> str:
        nonlocal info
        info = rules.symbol_info(symbol)
        return f"pair={info['pair_key']} base={info['baseAsset']} quote={info['quoteAsset']}"

    _run(checks, "asset_pairs", "GET /0/public/AssetPairs", pair_check)
    price: float | None = None

    def ticker_check() -> str:
        nonlocal price
        price = client.current_price(symbol)
        if price <= 0:
            raise ValueError("ticker price is not positive")
        return f"positive price received for {symbol.upper()}"

    _run(checks, "ticker", "GET /0/public/Ticker", ticker_check)

    def ohlc_check() -> str:
        pair_info = info or rules.symbol_info(symbol)
        pair = KrakenPair(
            pair_key=str(pair_info["pair_key"]),
            altname=str(pair_info.get("altname") or pair_info["pair_key"]),
            wsname=str(pair_info.get("wsname") or ""),
            base=str(pair_info["baseAsset"]),
            quote=str(pair_info["quoteAsset"]),
            symbol=symbol.upper(),
            leverage_buy=[int(value) for value in pair_info.get("leverage_buy", [])],
            leverage_sell=[int(value) for value in pair_info.get("leverage_sell", [])],
        )
        candles = fetch_kraken_ohlc(pair=pair, interval="15m", limit=2, base_url=settings.kraken_base_url)
        if not candles:
            raise ValueError("OHLC response contains no candles")
        return f"received {len(candles)} candle(s)"

    _run(checks, "ohlc", "GET /0/public/OHLC", ohlc_check)

    if not client.is_configured():
        detail = "KRAKEN_API_KEY and KRAKEN_SECRET_KEY are required"
        for name, endpoint in (
            ("balance", "POST /0/private/Balance"),
            ("open_orders", "POST /0/private/OpenOrders"),
            ("open_positions", "POST /0/private/OpenPositions"),
            ("add_order_validate", "POST /0/private/AddOrder validate=true"),
        ):
            checks.append(Check(name, endpoint, False, detail))
        return checks

    _run(checks, "balance", "POST /0/private/Balance", lambda: f"received {len(client.balance())} balance entrie(s)")
    _run(checks, "open_orders", "POST /0/private/OpenOrders", lambda: f"received {len((client._signed('POST', '/0/private/OpenOrders', {'trades': True}) or {}).get('open', {}))} open order(s)")
    _run(checks, "open_positions", "POST /0/private/OpenPositions", lambda: f"received {len(client._signed('POST', '/0/private/OpenPositions', {'docalcs': 'true'}))} open position(s)")

    if price is None:
        checks.append(Check("add_order_validate", "POST /0/private/AddOrder validate=true", False, "ticker check failed; safe quantity cannot be calculated"))
        return checks

    for mode in modes:
        for side in ("buy", "sell"):
            name = f"add_order_validate_{mode}_{side}"

            def validate(mode: str = mode, side: str = side) -> str:
                leverage = None
                notional = quote_amount
                if mode == "margin":
                    leverage = rules.max_supported_leverage(symbol, side, settings.kraken_margin_max_leverage)
                    notional *= leverage
                quantity = rules.quantity_from_quote(symbol, notional, price)
                client.validate_market_entry(symbol, side, quantity, leverage=leverage)
                return f"accepted without submission (mode={mode}, side={side}, leverage={leverage or 1})"

            _run(checks, name, "POST /0/private/AddOrder validate=true", validate)
    return checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Safely verify live Kraken calls used by the trading workflow.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--quote-amount", type=float, default=settings.kraken_order_quote_amount)
    parser.add_argument("--mode", action="append", choices=("spot", "margin"), dest="modes")
    args = parser.parse_args(argv)
    modes = args.modes or [settings.momentum_execution_mode]
    checks = check_workflow(args.symbol, args.quote_amount, modes)
    print(json.dumps({"ok": all(check.ok for check in checks), "non_destructive": True, "checks": [asdict(check) for check in checks]}, indent=2))
    return 0 if all(check.ok for check in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
