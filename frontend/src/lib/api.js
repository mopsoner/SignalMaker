const API_BASE = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8080'

async function request(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
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
  health: () => request('/api/v1/health'),
  services: () => request('/api/v1/services'),
  assets: (params = '') => request(`/api/v1/assets${params}`),
  candidates: (params = '') => request(`/api/v1/trade-candidates${params}`),
  positions: (params = '') => request(`/api/v1/positions${params}`),
  liveRuns: (params = '') => request(`/api/v1/live-runs${params}`),
  orders: (params = '') => request(`/api/v1/orders${params}`),
  fills: (params = '') => request(`/api/v1/fills${params}`),
  candles: (params = '') => request(`/api/v1/market-data/candles${params}`),
  runPipeline: (limit = 5) => request(`/api/v1/pipeline/run-once?limit=${limit}`, { method: 'POST' }),
  runExecutor: (limit = 10, quantity = 1) => request(`/api/v1/executor/run-once?limit=${limit}&quantity=${quantity}`, { method: 'POST' }),
}
