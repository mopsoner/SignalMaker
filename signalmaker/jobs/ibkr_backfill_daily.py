import argparse
import asyncio

from ib_async import Contract

from app.db.session import SessionLocal
from signalmaker.data_providers.ibkr.client import IBKRClient
from signalmaker.data_providers.ibkr.config import get_ibkr_config
from signalmaker.data_providers.ibkr.historical import IBKRHistoricalDataService
from signalmaker.data_providers.ibkr.repository import IBKRRepository


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--symbols", type=str, default=None)
    return parser.parse_args()


def build_contract(row):
    contract = Contract()
    contract.conId = int(row["conid"])
    contract.symbol = row["symbol"]
    contract.secType = row["sec_type"]
    contract.exchange = row.get("exchange") or "SMART"
    contract.currency = row["currency"]

    if row.get("primary_exchange"):
        contract.primaryExchange = row["primary_exchange"]

    return contract


async def backfill_one(row, historical_service, repo, config):
    contract = build_contract(row)

    candles = await historical_service.fetch_daily_candles(contract)

    count = await repo.upsert_ibkr_candles(
        symbol=row["symbol"],
        conid=row["conid"],
        asset_id=row.get("asset_id"),
        timeframe="1d",
        candles=candles,
    )

    await asyncio.sleep(config.sleep_seconds)

    return count


async def main():
    args = parse_args()
    config = get_ibkr_config()

    if not config.enabled:
        print("IBKR is disabled. Set IBKR_ENABLED=true.")
        return

    client = IBKRClient(config)
    await client.connect()

    historical_service = IBKRHistoricalDataService(client.ib, config)
    db = SessionLocal()
    repo = IBKRRepository(db)

    contract_rows = await repo.list_active_resolved_contracts(limit=args.limit)

    if args.symbols:
        wanted_symbols = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}
        contract_rows = [
            row for row in contract_rows
            if row["symbol"].upper() in wanted_symbols
        ]

    if args.limit:
        contract_rows = contract_rows[:args.limit]

    total = len(contract_rows)
    success = 0
    failed = 0
    run_id = await repo.create_import_run("backfill_daily", total_assets=total, metadata={"symbols": args.symbols, "limit": args.limit})

    semaphore = asyncio.Semaphore(config.max_concurrent)

    async def guarded(row):
        nonlocal success, failed

        async with semaphore:
            try:
                count = await backfill_one(row, historical_service, repo, config)
                success += 1
                print(f"OK {row['symbol']} candles={count}")
            except Exception as exc:
                failed += 1
                print(f"FAILED {row.get('symbol')} error={exc}")

    try:
        await asyncio.gather(*(guarded(row) for row in contract_rows))
        await repo.finish_import_run(run_id, "finished", success, failed)
    except Exception as exc:
        await repo.finish_import_run(run_id, "failed", success, failed, error_message=str(exc))
        raise
    finally:
        db.close()
        client.disconnect()

    print(f"IBKR BACKFILL FINISHED total={total} success={success} failed={failed}")


if __name__ == "__main__":
    asyncio.run(main())
