import { useCallback, useMemo } from 'react'
import { Link } from 'react-router-dom'
import DataTable from '../components/DataTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber, stageBadgeClass } from '../lib/format'

function summarizeContext(value) {
  if (!value || typeof value !== 'object') return '—'
  const type = value.type || '—'
  const level = value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'
  return `${type} @ ${level}`
}

function stateContext(row, key) {
  return row?.state_payload?.[key] || null
}

function summarizeScore(row) {
  const breakdown = row?.state_payload?.score_breakdown
  if (!breakdown) return '—'
  return `L${breakdown.liquidity || 0} · S${breakdown.structure || 0} · C${breakdown.confirmation || 0} · Se${breakdown.session || 0} · Q${breakdown.quality || 0} · V${breakdown.volume || 0}`
}

export default function DashboardPage() {
  const settingsLoader = useCallback(() => api.adminSettings(), [])
  const { data: adminSettings } = usePollingQuery(settingsLoader, 30000)
  const assetLimit = Number(adminSettings?.binance?.binance_max_symbols || 50)

  const loadAssets = useCallback(() => api.assets(`?limit=${assetLimit}`), [assetLimit])
  const { data: assets = [], loading, error } = usePollingQuery(loadAssets, 15000)

  const tradeCount = assets.filter((item) => item.stage === 'trade').length
  const confirmCount = assets.filter((item) => item.stage === 'confirm').length
  const zoneCount = assets.filter((item) => item.stage === 'zone').length
  const avgScore = assets.length ? (assets.reduce((sum, item) => sum + (item.score || 0), 0) / assets.length).toFixed(2) : '0.00'

  const sessionCounts = useMemo(() => assets.reduce((acc, item) => {
    const key = item.session || 'unknown'
    acc[key] = (acc[key] || 0) + 1
    return acc
  }, {}), [assets])

  const strongestAssets = useMemo(() => [...assets].sort((a, b) => (b.score || 0) - (a.score || 0)).slice(0, 6), [assets])

  const columns = [
    {
      key: 'symbol',
      title: 'Symbol',
      render: (row) => (
        <div style={{ display: 'grid', gap: 6 }}>
          <Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link>
          <a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">Open on TradingView</a>
        </div>
      ),
      sortValue: (row) => row.symbol,
    },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{row.stage}</span>, sortValue: (row) => row.stage },
    { key: 'bias', title: 'Bias', sortValue: (row) => row.bias },
    { key: 'session_phase', title: 'Session', render: (row) => row?.state_payload?.session_phase || row.session, sortValue: (row) => row?.state_payload?.session_phase || row.session },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score, 2), sortValue: (row) => Number(row.score || 0) },
    { key: 'zone_quality', title: 'Zone quality', render: (row) => row?.state_payload?.zone_quality || '—', sortValue: (row) => row?.state_payload?.zone_quality || '' },
    { key: 'price', title: 'Price', render: (row) => fmtNumber(row.price, 4), sortValue: (row) => Number(row.price || 0) },
    { key: 'rsi_1h', title: 'RSI 1H', render: (row) => fmtNumber(row.rsi_1h, 2), sortValue: (row) => Number(row.rsi_1h ?? -1) },
    { key: 'macro_liquidity_context', title: 'Macro liquidity', render: (row) => summarizeContext(stateContext(row, 'macro_liquidity_context') || row.liquidity_context), sortValue: (row) => (stateContext(row, 'macro_liquidity_context') || row.liquidity_context)?.level ?? -1 },
    { key: 'entry_liquidity_context', title: 'Entry liquidity', render: (row) => summarizeContext(stateContext(row, 'entry_liquidity_context')), sortValue: (row) => stateContext(row, 'entry_liquidity_context')?.level ?? -1 },
    { key: 'projected_target', title: 'Projected', render: (row) => summarizeContext(stateContext(row, 'projected_target')), sortValue: (row) => stateContext(row, 'projected_target')?.level ?? -1 },
    { key: 'execution_target', title: 'Target', render: (row) => summarizeContext(row.execution_target), sortValue: (row) => row.execution_target?.level ?? -1 },
    { key: 'updated_at', title: 'Updated', render: (row) => fmtDate(row.updated_at), sortValue: (row) => row.updated_at },
  ]

  const strongestColumns = [
    { key: 'symbol', title: 'Symbol', render: (row) => <Link to={`/assets/${encodeURIComponent(row.symbol)}`}>{row.symbol}</Link>, sortValue: (row) => row.symbol },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{row.stage}</span>, sortValue: (row) => row.stage },
    { key: 'bias', title: 'Bias', sortValue: (row) => row.bias },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score, 2), sortValue: (row) => Number(row.score || 0) },
    { key: 'score_breakdown', title: 'Breakdown', render: (row) => summarizeScore(row), sortValue: (row) => JSON.stringify(row?.state_payload?.score_breakdown || {}) },
  ]

  return (
    <div className="page-stack">
      <PageHeader title="Dashboard 360" subtitle="Market overview with debug links, projected targets, and asset drill-down." />
      <div className="stats-grid">
        <StatCard label="Tracked assets" value={assets.length} hint={`limit ${assetLimit}`} />
        <StatCard label="Trade stage" value={tradeCount} />
        <StatCard label="Confirm stage" value={confirmCount} />
        <StatCard label="Zone stage" value={zoneCount} />
      </div>
      <div className="stats-grid">
        <StatCard label="Average score" value={avgScore} />
        <StatCard label="London total" value={(sessionCounts.london || 0) + (sessionCounts.london_open || 0)} hint={`Open: ${sessionCounts.london_open || 0} · Core: ${sessionCounts.london || 0}`} />
        <StatCard label="New York" value={sessionCounts.new_york || 0} />
        <StatCard label="Asia / off" value={(sessionCounts.asia || 0) + (sessionCounts.off_session || 0)} hint={`Asia: ${sessionCounts.asia || 0} · Off: ${sessionCounts.off_session || 0}`} />
      </div>
      {loading ? <div className="panel">Loading assets…</div> : null}
      {error ? <div className="panel error">{error}</div> : null}
      <details className="panel collapsible-panel" open>
        <summary>
          <h2>Highest score assets</h2>
          <span className="collapse-indicator">⌄</span>
        </summary>
        <DataTable columns={strongestColumns} rows={strongestAssets} empty="No asset state available" />
      </details>
      <details className="panel collapsible-panel" open>
        <summary>
          <h2>Market view 360</h2>
          <span className="collapse-indicator">⌄</span>
        </summary>
        <DataTable columns={columns} rows={assets} empty="No asset state available" />
      </details>
    </div>
  )
}
