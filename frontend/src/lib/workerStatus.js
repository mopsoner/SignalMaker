export const WORKER_CATEGORIES = {
  live: { label: 'LIVE — appels réels', order: 0 },
  paper: { label: 'PAPER — simulation', order: 1 },
  infrastructure: { label: 'Infrastructure', order: 2 },
}

export const WORKER_METADATA = [
  { id: 'wyckoff_paper', label: 'Wyckoff / SMC — Paper', type: 'paper' },
  { id: 'wyckoff_live', label: 'Wyckoff / SMC — LIVE (Kraken)', type: 'live', logFile: 'wyckoff_live.log' },
  { id: 'stock_etf_analysis', label: 'Stocks & ETF — Analysis', type: 'paper' },
  { id: 'scheduler', label: 'Scheduler', type: 'infrastructure' },
  { id: 'pipeline', label: 'Pipeline', type: 'infrastructure' },
  { id: 'momentum_paper', label: 'Momentum — Paper', type: 'paper' },
  { id: 'momentum_live', label: 'Momentum — LIVE (Kraken)', type: 'live', logFile: 'momentum_live.log' },
  { id: 'kraken_candle_feed', label: 'Kraken — Candle feed', type: 'infrastructure' },
  { id: 'ibkr_ingestion', label: 'IBKR — Ingestion', type: 'infrastructure' },
]

export const MANAGED_WORKERS = WORKER_METADATA.map(({ id }) => id)
export const WORKER_BY_ID = Object.fromEntries(WORKER_METADATA.map((worker) => [worker.id, worker]))

export const LEGACY_WORKER_IDS = {
  executor: 'wyckoff_paper',
  momentum_engine: 'momentum_paper',
}

export function canonicalWorkerId(id) {
  return LEGACY_WORKER_IDS[id] || id
}

// Normalize status payloads during rolling deployments, where an older API may
// still return the worker IDs that predate the paper/live split. A canonical
// entry always wins if both names are present.
export function normalizeWorkerStatuses(statuses) {
  const entries = Object.entries(statuses || {})
  const normalized = Object.fromEntries(entries.map(([id, info]) => [canonicalWorkerId(id), info]))
  for (const [id, info] of entries) {
    if (!LEGACY_WORKER_IDS[id]) normalized[id] = info
  }
  return normalized
}

export function getWorkerMetadata(id) {
  const canonicalId = canonicalWorkerId(id)
  return WORKER_BY_ID[canonicalId] || { id: canonicalId, label: canonicalId, type: 'infrastructure' }
}

export const WORKERS_BY_CATEGORY = Object.keys(WORKER_CATEGORIES).map((type) => ({
  type,
  ...WORKER_CATEGORIES[type],
  workers: WORKER_METADATA.filter((worker) => worker.type === type),
})).sort((a, b) => a.order - b.order)

// process_state is canonical; running supports older APIs during rolling deploys.
export function isWorkerRunning(info) {
  return info?.process_state ? info.process_state === 'running' : Boolean(info?.running)
}

export function failedStartMessage(worker, error) {
  const detail = error?.message || String(error)
  return `${detail} Select the canonical "${worker.id}" log tab for startup diagnostics.`
}
