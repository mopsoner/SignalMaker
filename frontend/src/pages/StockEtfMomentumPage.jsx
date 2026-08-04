import { useMemo } from 'react'
import { MomentumView } from './MomentumPage'
import { tradingViewUrl } from '../lib/tradingview'

const API_BASE = import.meta.env.VITE_API_BASE || ''

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, { headers: { 'Content-Type': 'application/json' }, ...options })
  if (!response.ok) throw new Error(await response.text() || `HTTP ${response.status}`)
  return response.json()
}

function scopeQuery(scope = {}, extra = {}) {
  const params = new URLSearchParams(extra)
  if (scope.universe) params.set('universe', scope.universe)
  if (scope.asset_type) params.set('asset_type', scope.asset_type)
  const query = params.toString()
  return query ? `?${query}` : ''
}

export function createStockEtfMomentumApi() {
  return {
    loadRanking: (limit, scope) => request(`/api/v1/stocks-etfs/momentum/ranking${scopeQuery(scope, { limit })}`),
    loadStatus: (_cadence, scope) => request(`/api/v1/stocks-etfs/momentum/status${scopeQuery(scope)}`),
    run: (cadenceHours, force, scope) => request('/api/v1/stocks-etfs/momentum/run-once', {
      method: 'POST', body: JSON.stringify({ cadence_hours: cadenceHours, force, ...scope }),
    }),
    loadCadence: async () => (await request('/api/v1/stocks-etfs/momentum/cadence')).cadence_hours,
    saveCadence: (cadenceHours) => request('/api/v1/stocks-etfs/momentum/cadence', {
      method: 'PUT', body: JSON.stringify({ cadence_hours: cadenceHours }),
    }),
    tradingViewLink: (row) => tradingViewUrl(row.provider_symbol || row.symbol, { market: 'stock-etf' }),
  }
}

export default function StockEtfMomentumPage() {
  const momentumApi = useMemo(() => createStockEtfMomentumApi(), [])
  return <MomentumView
    momentumApi={momentumApi}
    title="ETF & Stocks · Momentum Ranking"
    subtitle="Shared Momentum workflow over isolated Stock/ETF results. Intraday fields remain unavailable until their exact feeder candles exist."
    marketFilters={{ universes: ['Europe Stocks', 'Europe ETF'], assetTypes: ['ETF', 'STOCK', 'INDEX'] }}
  />
}
