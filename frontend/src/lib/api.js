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
  candidates: (params = '') => request(`/api/v1/trade-candidates${params}`),
  positions: (params = '') => request(`/api/v1/positions${params}`),
  liveRuns: (params = '') => request(`/api/v1/live-runs${params}`),
  orders: (params = '') => request(`/api/v1/orders${params}`),
  fills: (params = '') => request(`/api/v1/fills${params}`),
  candles: (params = '') => request(`/api/v1/market-data/candles${params}`),
  candleSummary: (params = '') => request(`/api/v1/market-data/candles/summary${params}`),
  runPipeline: (limit = 5) => request(`/api/v1/pipeline/run-once?limit=${limit}`, { method: 'POST' }),
  runExecutor: (limit = 10, quantity = 1) => request(`/api/v1/executor/run-once?limit=${limit}&quantity=${quantity}`, { method: 'POST' }),
  adminSettings: () => request('/api/v1/admin/settings'),
  updateAdminSettings: (payload) => request('/api/v1/admin/settings', { method: 'PUT', body: JSON.stringify(payload) }),
  workerStatus: () => request('/api/v1/admin/workers'),
  startWorker: (name) => request(`/api/v1/admin/workers/${name}/start`, { method: 'POST' }),
  stopWorker: (name) => request(`/api/v1/admin/workers/${name}/stop`, { method: 'POST' }),
  testBinance: () => request('/api/v1/admin/test/binance', { method: 'POST' }),
  testNotifications: () => request('/api/v1/admin/test/notifications', { method: 'POST' }),
  workerLogs: (worker, lines = 300) => request(`/api/v1/admin/logs/${worker}?lines=${lines}`),
}
