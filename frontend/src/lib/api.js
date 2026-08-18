// An unset base deliberately uses same-origin requests.  This is the production
// default because the frontend server proxies /api and /admin to FastAPI.
const configuredBase = String(import.meta.env.VITE_API_BASE || '').trim()
const API_BASE = configuredBase.replace(/\/+$/, '')

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

  const url = `${API_BASE}${path}`
  let res
  try {
    res = await fetch(url, { ...options, headers })
  } catch (error) {
    const requestError = new Error(`API request failed for ${url}: ${error.message || 'network error'}`)
    console.error('[SignalMaker] API loading error', { method: options.method || 'GET', url, error: requestError.message })
    throw requestError
  }
  if (!res.ok) {
    const text = await res.text()
    let detail = text
    try {
      const payload = JSON.parse(text)
      detail = payload.detail || payload.message || text
    } catch {
      // Keep a plain-text response as-is.
    }
    const requestId = res.headers.get('x-request-id')
    const requestError = new Error(`${res.status} ${res.statusText} from ${url}${detail ? `: ${detail}` : ''}${requestId ? ` (request ${requestId})` : ''}`)
    console.error('[SignalMaker] API response error', { method: options.method || 'GET', url, status: res.status, requestId, error: requestError.message })
    throw requestError
  }
  if (res.status === 204) return null
  try {
    return await res.json()
  } catch {
    const requestError = new Error(`API returned invalid JSON from ${url}`)
    console.error('[SignalMaker] API parsing error', { method: options.method || 'GET', url, error: requestError.message })
    throw requestError
  }
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
  momentumDecision: () => request('/api/v1/momentum-engine/decision'),
  momentumStatus: () => request('/api/v1/momentum-engine/status'),
  momentumPositions: (params = '') => request(`/api/v1/momentum-engine/positions${params}`),
  momentumTrades: (params = '') => request(`/api/v1/momentum-engine/trades${params}`),
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
  clearMomentumAnalysis: () => request('/api/v1/momentum/cleanup', { method: 'DELETE' }),
  clearMomentumEngine: () => request('/api/v1/momentum-engine/cleanup', { method: 'DELETE' }),
  clearApplicationData: () => request('/api/v1/admin/cleanup/app-data', { method: 'DELETE' }),
  fills: (params = '') => request(`/api/v1/fills${params}`),
  candles: (params = '') => request(`/api/v1/market-data/candles${params}`),
  candleSummary: (params = '') => request(`/api/v1/market-data/candles/summary${params}`),
  runPipeline: (limit = 5) => request(`/api/v1/pipeline/run-once?limit=${limit}`, { method: 'POST' }),
  runExecutor: (limit = 10, quantity = 1) => request(`/api/v1/executor/run-once?limit=${limit}&quantity=${quantity}`, { method: 'POST' }),
  adminSettings: () => request('/api/v1/admin/settings'),
  updateAdminSettings: (payload) => request('/api/v1/admin/settings', { method: 'PUT', body: JSON.stringify(payload) }),
  deleteAdminSettingOverride: (category, key) => request(`/api/v1/admin/settings/${encodeURIComponent(category)}/${encodeURIComponent(key)}`, { method: 'DELETE' }),
  workerStatus: () => request('/api/v1/admin/workers'),
  startWorker: (name) => request(`/api/v1/admin/workers/${name}/start`, { method: 'POST' }),
  stopWorker: (name) => request(`/api/v1/admin/workers/${name}/stop`, { method: 'POST' }),
  testNotifications: () => request('/api/v1/admin/test/notifications', { method: 'POST' }),

  stockEtfDashboard: (params = '') => request(`/api/v1/stocks-etfs/dashboard${params}`),
  stockEtfAssets: (params = '') => request(`/api/v1/stocks-etfs/assets${params}`),
  stockEtfResults: (params = '') => request(`/api/v1/stocks-etfs/results${params}`),
  stockEtfCandidates: (params = '') => request(`/api/v1/stocks-etfs/candidates${params}`),
  stockEtfPositions: (params = '') => request(`/api/v1/stocks-etfs/positions${params}`),
  stockEtfQuality: (params = '') => request(`/api/v1/stocks-etfs/data-quality${params}`),
  stockEtfFreshness: (params = '') => request(`/api/v1/stocks-etfs/freshness${params}`),
  stockEtfExportUrl: (params = '') => `${API_BASE}/api/v1/stocks-etfs/export.csv${params}`,
  clearStockEtfGeneratedData: () => request('/api/v1/stocks-etfs/cleanup', { method: 'DELETE' }),
  clearAllStockEtfData: () => request('/admin/market-data', { method: 'DELETE' }),
  marketDataSettings: () => request('/admin/market-data'),
  marketDataRuns: (params = '') => request(`/admin/market-data/runs${params}`),
  marketDataRun: (kind, runId) => request(`/admin/market-data/runs/${encodeURIComponent(kind)}/${encodeURIComponent(runId)}`),
  cancelMarketDataRun: (kind, runId) => request(`/admin/market-data/runs/${encodeURIComponent(kind)}/${encodeURIComponent(runId)}/cancel`, { method: 'POST' }),
  retryMarketDataRun: (kind, runId) => request(`/admin/market-data/runs/${encodeURIComponent(kind)}/${encodeURIComponent(runId)}/retry`, { method: 'POST' }),
  envSettings: () => request('/admin/env'),
  syncMarketAssets: () => request('/admin/market-data/sync-assets', { method: 'POST' }),
  runMarketAnalysis: (payload = {}) => request('/admin/market-data/analyze', { method: 'POST', body: JSON.stringify(payload) }),
  previewMarketAction: (payload = {}) => request('/admin/market-data/preview', { method: 'POST', body: JSON.stringify(payload) }),
  queueMarketJob: (payload = {}) => request('/admin/market-data/queue-job', { method: 'POST', body: JSON.stringify(payload) }),
  updateMarketAsset: (assetId, payload = {}) => request(`/admin/market-data/assets/${encodeURIComponent(assetId)}`, { method: 'PATCH', body: JSON.stringify(payload) }),
  updateMarketUniverse: (universeId, payload = {}) => request(`/admin/market-data/universes/${encodeURIComponent(universeId)}`, { method: 'PATCH', body: JSON.stringify(payload) }),
  workerLogs: (worker, lines = 300) => request(`/api/v1/admin/logs/${worker}?lines=${lines}`),
}
