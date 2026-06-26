const API_BASE = import.meta.env.VITE_API_BASE || ''

function getOperatorKey() {
  try {
    return window.localStorage.getItem('signalmaker_operator_key') || ''
  } catch {
    return ''
  }
}

async function request(path, options = {}) {
  const operatorKey = getOperatorKey()
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) }
  if (operatorKey) headers['x-operator-key'] = operatorKey

  const res = await fetch(`${API_BASE}${path}`, {
    headers,
    ...options,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res.json()
}

export const api = {
  base: API_BASE,
  setOperatorKey: (value) => {
    try { window.localStorage.setItem('signalmaker_operator_key', value || '') } catch {}
  },
  getOperatorKey,
  health: () => request('/api/v1/health'),
  services: () => request('/api/v1/services'),
  assets: (params = '') => request(`/api/v1/assets${params}`),
  asset: (symbol) => request(`/api/v1/assets/${encodeURIComponent(symbol)}`),
  candidates: (params = '') => request(`/api/v1/trade-candidates${params}`),
  momentumCandidates: (params = '') => request(`/api/v1/momentum-candidates${params}`),
  clearCandidates: (status = '') => request(`/api/v1/trade-candidates${status ? `?status=${encodeURIComponent(status)}` : ''}`, { method: 'DELETE' }),
  clearOpenCandidates: () => request('/api/v1/trade-candidates/open', { method: 'DELETE' }),
  positions: (params = '') => request(`/api/v1/positions${params}`),
  positionsSummary: (params = '') => request(`/api/v1/positions/summary${params}`),
  clearPositions: (status = '') => request(`/api/v1/positions${status ? `?status=${encodeURIComponent(status)}` : ''}`, { method: 'DELETE' }),
  clearOpenPositions: () => request('/api/v1/positions/open', { method: 'DELETE' }),
  liveRuns: (params = '') => request(`/api/v1/live-runs${params}`),
  orders: (params = '') => request(`/api/v1/orders${params}`),
  clearOrders: (status = '') => request(`/api/v1/orders${status ? `?status=${encodeURIComponent(status)}` : ''}`, { method: 'DELETE' }),
  clearOpenOrders: () => request('/api/v1/orders/open', { method: 'DELETE' }),
  fills: (params = '') => request(`/api/v1/fills${params}`),
  candles: (params = '') => request(`/api/v1/market-data/candles${params}`),
  candleSummary: (params = '') => request(`/api/v1/market-data/candles/summary${params}`),
  runPipeline: (limit = 5) => request(`/api/v1/pipeline/run-once?limit=${limit}`, { method: 'POST' }),
  runExecutor: (limit = 10, quantity = 1) => request(`/api/v1/executor/run-once?limit=${limit}&quantity=${quantity}`, { method: 'POST' }),
  momentumBacktestLatest: () => request('/api/v1/momentum-backtest/runs/latest'),
  momentumBacktestCreate: (settings = {}) => request('/api/v1/momentum-backtest/runs', { method: 'POST', body: JSON.stringify({ settings }) }),
  momentumBacktestTrades: (runId, limit = 300) => request('/api/v1/momentum-backtest/runs/' + runId + '/trades?limit=' + limit),
  momentumBacktestEquity: (runId, limit = 1000) => request('/api/v1/momentum-backtest/runs/' + runId + '/equity?limit=' + limit),
  adminSettings: () => request('/api/v1/admin/settings'),
  updateAdminSettings: (payload) => request('/api/v1/admin/settings', { method: 'PUT', body: JSON.stringify(payload) }),
  workerStatus: () => request('/api/v1/admin/workers'),
  startWorker: (name) => request(`/api/v1/admin/workers/${name}/start`, { method: 'POST' }),
  stopWorker: (name) => request(`/api/v1/admin/workers/${name}/stop`, { method: 'POST' }),
  testBinance: () => request('/api/v1/admin/test/binance', { method: 'POST' }),
  testNotifications: () => request('/api/v1/admin/test/notifications', { method: 'POST' }),

  stockEtfDashboard: (params = '') => request(`/api/v1/stocks-etfs/dashboard${params}`),
  stockEtfAssets: (params = '') => request(`/api/v1/stocks-etfs/assets${params}`),
  stockEtfResults: (params = '') => request(`/api/v1/stocks-etfs/results${params}`),
  stockEtfCandidates: (params = '') => request(`/api/v1/stocks-etfs/candidates${params}`),
  stockEtfPositions: (params = '') => request(`/api/v1/stocks-etfs/positions${params}`),
  stockEtfQuality: (params = '') => request(`/api/v1/stocks-etfs/data-quality${params}`),
  stockEtfFreshness: (params = '') => request(`/api/v1/stocks-etfs/freshness${params}`),
  stockEtfConfluence: (params = '') => request(`/api/v1/stocks-etfs/confluence${params}`),
  stockEtfExportUrl: (params = '') => `${API_BASE}/api/v1/stocks-etfs/export.csv${params}`,
  marketDataSettings: () => request('/admin/market-data'),
  envSettings: () => request('/admin/env'),
  testEodhd: () => request('/admin/market-data/test-eodhd', { method: 'POST' }),
  syncMarketAssets: () => request('/admin/market-data/sync-assets', { method: 'POST' }),
  runMarketAnalysis: (payload = {}) => request('/admin/market-data/analyze', { method: 'POST', body: JSON.stringify(payload) }),
  previewMarketAction: (payload = {}) => request('/admin/market-data/preview', { method: 'POST', body: JSON.stringify(payload) }),
  queueMarketJob: (payload = {}) => request('/admin/market-data/queue-job', { method: 'POST', body: JSON.stringify(payload) }),
  updateMarketAsset: (assetId, payload = {}) => request(`/admin/market-data/assets/${encodeURIComponent(assetId)}`, { method: 'PATCH', body: JSON.stringify(payload) }),
  updateMarketUniverse: (universeId, payload = {}) => request(`/admin/market-data/universes/${encodeURIComponent(universeId)}`, { method: 'PATCH', body: JSON.stringify(payload) }),
  workerLogs: (worker, lines = 300) => request(`/api/v1/admin/logs/${worker}?lines=${lines}`),
}
