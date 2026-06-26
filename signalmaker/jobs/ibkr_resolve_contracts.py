import argparse
import asyncio

from app.db.session import SessionLocal
from signalmaker.data_providers.ibkr.client import IBKRClient
from signalmaker.data_providers.ibkr.config import get_ibkr_config
from signalmaker.data_providers.ibkr.contracts import IBKRContractResolver
from signalmaker.data_providers.ibkr.repository import IBKRRepository


DEFAULT_SYMBOLS = [
    {"symbol": "AAPL", "currency": "USD", "exchange": "SMART"},
    {"symbol": "MSFT", "currency": "USD", "exchange": "SMART"},
    {"symbol": "SPY", "currency": "USD", "exchange": "SMART"},
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", type=str, default=None)
    parser.add_argument("--currency", type=str, default="USD")
    parser.add_argument("--exchange", type=str, default="SMART")
    parser.add_argument("--primary-exchange", type=str, default=None)
    return parser.parse_args()


async def main():
    args = parse_args()
    config = get_ibkr_config()

    if not config.enabled:
        print("IBKR is disabled. Set IBKR_ENABLED=true.")
        return

    if args.symbols:
        assets = [
            {
                "symbol": symbol.strip().upper(),
                "currency": args.currency,
                "exchange": args.exchange,
                "primary_exchange": args.primary_exchange,
            }
            for symbol in args.symbols.split(",")
            if symbol.strip()
        ]
    else:
        assets = DEFAULT_SYMBOLS

    client = IBKRClient(config)
    await client.connect()

    resolver = IBKRContractResolver(client.ib)
    db = SessionLocal()
    repo = IBKRRepository(db)
    run_id = await repo.create_import_run("resolve_contracts", total_assets=len(assets), metadata={"symbols": [a["symbol"] for a in assets]})
    success = 0
    failed = 0

    try:
        for asset in assets:
            try:
                resolved = await resolver.resolve_stock_or_etf(
                    symbol=asset["symbol"],
                    currency=asset["currency"],
                    exchange=asset.get("exchange", "SMART"),
                    primary_exchange=asset.get("primary_exchange"),
                )

                print(
                    f"RESOLVED {resolved.symbol} "
                    f"conid={resolved.conid} "
                    f"secType={resolved.sec_type} "
                    f"exchange={resolved.exchange} "
                    f"primaryExchange={resolved.primary_exchange} "
                    f"currency={resolved.currency} "
                    f"ambiguous={resolved.ambiguous}"
                )

                await repo.upsert_contract(
                    asset_id=None,
                    symbol=resolved.symbol,
                    sec_type=resolved.sec_type,
                    exchange=asset.get("exchange", "SMART"),
                    primary_exchange=resolved.primary_exchange,
                    currency=resolved.currency,
                    conid=resolved.conid,
                    local_symbol=resolved.local_symbol,
                    trading_class=resolved.trading_class,
                    resolved=True,
                    ambiguous=resolved.ambiguous,
                    last_error=None,
                )
                success += 1

            except Exception as exc:
                failed += 1
                print(f"FAILED {asset['symbol']} error={exc}")
                try:
                    await repo.upsert_contract(
                        asset_id=None,
                        symbol=asset["symbol"],
                        sec_type="STK",
                        exchange=asset.get("exchange", "SMART"),
                        primary_exchange=asset.get("primary_exchange"),
                        currency=asset["currency"],
                        conid=None,
                        local_symbol=None,
                        trading_class=None,
                        resolved=False,
                        ambiguous=False,
                        last_error=str(exc),
                    )
                except Exception as db_exc:
                    print(f"FAILED_TO_SAVE_ERROR {asset['symbol']} error={db_exc}")

        await repo.finish_import_run(run_id, "finished", success, failed)
    except Exception as exc:
        await repo.finish_import_run(run_id, "failed", success, failed, error_message=str(exc))
        raise
    finally:
        db.close()
        client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
