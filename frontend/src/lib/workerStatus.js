export const MANAGED_WORKERS = [
  'pipeline',
  'wyckoff_paper',
  'kraken_candle_feed',
  'momentum_paper',
  'momentum_live',
  'wyckoff_live',
  'momentum_backtest',
  'ibkr_ingestion',
  'stock_etf_analysis',
  'scheduler',
]

// process_state is canonical; running supports older APIs during rolling deploys.
export function isWorkerRunning(info) {
  return info?.process_state ? info.process_state === 'running' : Boolean(info?.running)
}
