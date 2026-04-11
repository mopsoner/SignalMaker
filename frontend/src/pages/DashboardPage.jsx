import { useCallback } from 'react'
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

export default function DashboardPage() {
  const loadAssets = useCallback(() => api.assets('?limit=25'), [])
  const loadRuns = useCallback(() => api.liveRuns('?limit=10'), [])
  const { data: assets = [], loading, error } = usePollingQuery(loadAssets, 15000)
  const { data: runs = [] } = usePollingQuery(loadRuns, 15000)

  const tradeCount = assets.filter((item) => item.stage === 'trade').length
  const confirmCount = assets.filter((item) => item.stage === 'confirm').length
  const avgScore = assets.length ? (assets.reduce((sum, item) => sum + (item.score || 0), 0) / assets.length).toFixed(2) : '0.00'

  const columns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{row.stage}</span> },
    { key: 'bias', title: 'Bias' },
    { key: 'session', title: 'Session' },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score, 2) },
    { key: 'price', title: 'Price', render: (row) => fmtNumber(row.price, 4) },
    { key: 'rsi_5m', title: 'RSI 5M', render: (row) => fmtNumber(row.rsi_5m, 2) },
    { key: 'rsi_1h', title: 'RSI 1H', render: (row) => fmtNumber(row.rsi_1h, 2) },
    { key: 'macro_liquidity_context', title: 'Macro liquidity', render: (row) => summarizeContext(stateContext(row, 'macro_liquidity_context') || row.liquidity_context) },
    { key: 'entry_liquidity_context', title: 'Entry liquidity', render: (row) => summarizeContext(stateContext(row, 'entry_liquidity_context')) },
    { key: 'execution_target', title: 'Target', render: (row) => summarizeContext(row.execution_target) },
    { key: 'updated_at', title: 'Updated', render: (row) => fmtDate(row.updated_at) },
  ]

  const runColumns = [
    { key: 'run_id', title: 'Run ID' },
    { key: 'status', title: 'Status' },
    { key: 'symbols_total', title: 'Total' },
    { key: 'symbols_scanned', title: 'Scanned' },
    { key: 'started_at', title: 'Started', render: (row) => fmtDate(row.started_at) },
  ]

  return (
    <div className="page-stack">
      <PageHeader title="Dashboard" subtitle="Live state overview with separated macro and entry liquidity contexts" />
      <div className="stats-grid">
        <StatCard label="Tracked assets" value={assets.length} />
        <StatCard label="Trade stage" value={tradeCount} />
        <StatCard label="Confirm stage" value={confirmCount} />
        <StatCard label="Average score" value={avgScore} />
      </div>
      {loading ? <div className="panel">Loading assets…</div> : null}
      {error ? <div className="panel error">{error}</div> : null}
      <section className="panel">
        <h2>Top assets</h2>
        <DataTable columns={columns} rows={assets} empty="No asset state available" />
      </section>
      <section className="panel">
        <h2>Recent runs</h2>
        <DataTable columns={runColumns} rows={runs} empty="No live runs yet" />
      </section>
    </div>
  )
}
