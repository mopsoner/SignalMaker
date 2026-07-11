import { useCallback } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'

function safeText(value) {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  if (typeof value === 'number') return String(value)
  return String(value)
}

function decisionRows(decision) {
  if (!decision || typeof decision !== 'object') return []
  return [decision]
}

export default function MomentumDecisionsPage() {
  const loadDecision = useCallback(() => api.momentumDecision(), [])
  const { data: decision, loading, error } = usePollingQuery(loadDecision, 60 * 1000)
  const rows = decisionRows(decision)

  const columns = [
    { key: 'produced_at', title: 'Produced at', render: (row) => fmtDate(row.produced_at), sortValue: (row) => row.produced_at || '' },
    { key: 'action', title: 'Action', render: (row) => safeText(row.action) },
    { key: 'decision_action', title: 'Decision action', render: (row) => safeText(row.decision_action) },
    { key: 'symbol', title: 'Symbol', render: (row) => safeText(row.symbol) },
    { key: 'target_symbol', title: 'Target symbol', render: (row) => safeText(row.target_symbol) },
    { key: 'buy_symbol', title: 'Buy symbol', render: (row) => safeText(row.buy_symbol) },
    { key: 'sell_symbol', title: 'Sell symbol', render: (row) => safeText(row.sell_symbol) },
    { key: 'should_trade', title: 'Should trade', render: (row) => safeText(row.should_trade), sortValue: (row) => row.should_trade ? 1 : 0 },
    { key: 'status', title: 'Status', render: (row) => safeText(row.status) },
    { key: 'reason', title: 'Reason', render: (row) => safeText(row.reason) },
    { key: 'recommendation', title: 'Recommendation', render: (row) => safeText(row.recommendation) },
    { key: 'cash', title: 'Cash', render: (row) => fmtNumber(row.cash, 2), sortValue: (row) => Number(row.cash ?? 0) },
    { key: 'equity', title: 'Equity', render: (row) => fmtNumber(row.equity, 2), sortValue: (row) => Number(row.equity ?? 0) },
    { key: 'total_pnl', title: 'Total PnL', render: (row) => fmtNumber(row.total_pnl, 2), sortValue: (row) => Number(row.total_pnl ?? 0) },
    { key: 'total_pnl_pct', title: 'Total PnL %', render: (row) => fmtNumber(row.total_pnl_pct, 2), sortValue: (row) => Number(row.total_pnl_pct ?? 0) },
    { key: 'strategy', title: 'Strategy', render: (row) => safeText(row.strategy) },
    { key: 'mode', title: 'Mode', render: (row) => safeText(row.mode) },
  ]

  return <div className="page-stack">
    <PageHeader title="Momentum Decisions" subtitle="Current persisted momentum-engine decision from /api/v1/momentum-engine/decision. Historical decision storage should use a separate backend history table in a distinct task." />
    <div className="stats-grid">
      <StatCard label="Current action" value={safeText(decision?.action)} hint={decision?.produced_at ? `Produced ${fmtDate(decision.produced_at)}` : 'No produced_at persisted'} />
      <StatCard label="Should trade" value={safeText(decision?.should_trade)} hint={safeText(decision?.status)} />
      <StatCard label="Equity" value={fmtNumber(decision?.equity, 2)} hint={`Cash ${fmtNumber(decision?.cash, 2)}`} />
      <StatCard label="Total PnL" value={fmtNumber(decision?.total_pnl, 2)} hint={`${fmtNumber(decision?.total_pnl_pct, 2)}%`} />
    </div>
    {loading ? <div className="panel">Loading momentum decision…</div> : null}
    {error ? <div className="panel error">{error}</div> : null}
    <FoldableTable title="Current persisted momentum decision" columns={columns} rows={rows} empty="No persisted momentum decision yet" paginated={false} />
  </div>
}
