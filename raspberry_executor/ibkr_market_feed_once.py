import argparse, json
from raspberry_executor.ibkr_market_feed import run_once

parser = argparse.ArgumentParser()
parser.add_argument('--symbols', default='')
parser.add_argument('--limit', type=int, default=3)
args = parser.parse_args()
symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()] or None
print(json.dumps(run_once(symbols=symbols, limit=args.limit), indent=2, default=str))
