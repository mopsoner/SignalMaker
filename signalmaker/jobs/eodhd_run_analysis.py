import argparse, asyncio
from app.db.session import SessionLocal
from signalmaker.data_providers.eodhd.repository import EODHDRepository
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter

async def main():
 p=argparse.ArgumentParser(); p.add_argument('--engine', choices=['momentum','wyckoff_smc','both'], default='both'); p.add_argument('--universe'); p.add_argument('--asset-type'); p.add_argument('--limit', type=int, default=50); p.add_argument('--timeframe', default='1d')
 args=p.parse_args(); ok=fail=0
 with SessionLocal() as db:
  repo=EODHDRepository(db); repo.ensure_schema(); adapter=MarketAnalysisAdapter(repo)
  assets=await repo.list_enabled_market_assets(asset_type=args.asset_type, universe_name=args.universe, limit=args.limit)
  engines=['momentum','wyckoff_smc'] if args.engine=='both' else [args.engine]
  run_id=await repo.create_analysis_run(args.engine, timeframe=args.timeframe, metadata={'universe':args.universe,'asset_type':args.asset_type})
  for asset in assets:
   for engine in engines:
    try:
     result = await (adapter.run_momentum_analysis(asset['id'], args.timeframe) if engine=='momentum' else adapter.run_wyckoff_smc_analysis(asset['id'], args.timeframe))
     await repo.insert_analysis_result(run_id, asset['id'], result['engine_name'], args.timeframe, result)
     ok+=1; print(f"OK {asset['provider_symbol']} {engine} {result['signal']}")
    except Exception as e:
     fail+=1; print(f"FAILED {asset['provider_symbol']} {engine}: {e}")
  await repo.finish_analysis_run(run_id, 'SUCCESS' if fail==0 else 'PARTIAL', len(assets)*len(engines), ok, fail); db.commit()
if __name__ == '__main__': asyncio.run(main())
