import { useCallback } from 'react'
import WyckoffSmcDashboard from '../components/WyckoffSmcDashboard'
import { api } from '../lib/api'
import { tradingViewUrl } from '../lib/tradingview'

export default function DashboardPage() {
  const loadAssets = useCallback(() => api.assets('?sort_by=updated_at'), [])
  return <WyckoffSmcDashboard loadAssets={loadAssets} detailUrl={(row) => `/assets/${encodeURIComponent(row.symbol)}`} tradingViewLink={(row) => tradingViewUrl(row.symbol)} />
}
