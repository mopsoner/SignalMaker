import argparse, asyncio
from app.db.session import SessionLocal
from signalmaker.data_providers.ibkr.config import get_ibkr_config
from signalmaker.data_providers.ibkr.client import IBKRClient
from signalmaker.data_providers.ibkr.historical import IBKRHistoricalService
from signalmaker.data_providers.eodhd.repository import EODHDRepository

async def main():
    p=argparse.ArgumentParser(); p.add_argument('--limit', type=int); p.add_argument('--universe'); p.add_argument('--asset-type'); p.add_argument('--symbols')
    args=p.parse_args(); config=get_ibkr_config(); client=IBKRClient(config)
    ok=fail=0
    try:
      with SessionLocal() as db:
        repo=EODHDRepository(db); repo.ensure_schema(); hist=IBKRHistoricalService(client, config)
        symbols=[s.strip() for s in args.symbols.split(',')] if args.symbols else None
        assets=await repo.list_enabled_market_assets(asset_type=args.asset_type, universe_name=args.universe, limit=args.limit, symbols=symbols)
        run_id=await repo.create_import_run('IBKR','daily_backfill', metadata={'universe':args.universe,'asset_type':args.asset_type,'symbols':symbols})
        sem=asyncio.Semaphore(max(1, config.max_concurrent))
        async def one(asset):
          nonlocal ok, fail
          async with sem:
            try:
              candles=await hist.fetch_daily_candles(asset['provider_symbol'])
              n=await repo.upsert_market_candles(asset['id'],'IBKR',asset['provider_symbol'],'1d',candles)
              ok+=1; print(f"OK {asset['provider_symbol']} candles={n}")
            except Exception as e:
              fail+=1; print(f"FAILED {asset['provider_symbol']}: {e}")
            await asyncio.sleep(config.request_sleep_seconds)
        await asyncio.gather(*(one(a) for a in assets)); await repo.finish_import_run(run_id,'SUCCESS' if fail==0 else 'PARTIAL',len(assets),ok,fail); db.commit()
    finally:
      await client.close()

if __name__ == "__main__": asyncio.run(main())
