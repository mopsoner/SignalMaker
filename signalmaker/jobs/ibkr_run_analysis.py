import argparse, asyncio
from app.db.session import SessionLocal
from signalmaker.market_data.repository import MarketDataRepository
from signalmaker.market_data.analysis_service import MarketAnalysisService

def build_parser():
 p=argparse.ArgumentParser(); p.add_argument('--engine', choices=['momentum','wyckoff_smc','both'], default='both'); p.add_argument('--universe'); p.add_argument('--asset-type'); p.add_argument('--limit', type=int, default=50); p.add_argument('--timeframe', default='15m')
 return p

async def main(argv=None):
 args=build_parser().parse_args(argv)
 with SessionLocal() as db:
  repo=MarketDataRepository(db); repo.ensure_schema()
  report=await MarketAnalysisService(repo, market_scope='stock_etf').run(
   engine=args.engine, universe=args.universe, asset_type=args.asset_type,
   limit=args.limit, timeframe=args.timeframe)
  print(f"run_id={report['run_id']} run_identifier={report['run_identifier']} status={report['status']}")
  print("summary " + " ".join(f"{key}={value}" for key, value in report['summary'].items()))
  return report
if __name__ == '__main__': asyncio.run(main())
