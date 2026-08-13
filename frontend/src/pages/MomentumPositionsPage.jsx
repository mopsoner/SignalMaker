import { useCallback } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'

function pnlTone(value) {
  const number = Number(value)
  if (number > 0) return { color: 'var(--green)', fontWeight: 700 }
  if (number < 0) return { color: 'var(--red)', fontWeight: 700 }
  return { fontWeight: 700 }
}

function money(value) {
  return Number.isFinite(Number(value)) ? fmtNumber(value, 2) : '—'
}

function StatCard({ label, value, hint, tone }) {
  return <div className="stat-card">
    <div className="stat-label">{label}</div>
    <div className="stat-value" style={tone === undefined ? undefined : pnlTone(tone)}>{value}</div>
    <div className="stat-hint">{hint}</div>
  </div>
}

function actionKind(action) {
  const normalized = String(action || '').toUpperCase()
  if (normalized.startsWith('BUY')) return 'BUY'
  if (normalized.startsWith('SELL')) return 'SELL'
  return normalized
}

export default function MomentumPositionsPage() {
  const { data: status, loading: statusLoading, error: statusError } = usePollingQuery(useCallback(() => api.momentumStatus(), []), 15000)
  const { data: positionPage, loading: positionsLoading, error: positionsError } = usePollingQuery(useCallback(() => api.momentumPositions('?limit=1000'), []), 15000)
  const { data: tradePage, loading: tradesLoading, error: tradesError } = usePollingQuery(useCallback(() => api.momentumTrades('?limit=1000'), []), 15000)

  const positions = positionPage?.items || []
  const trades = tradePage?.items || []
  const openPositions = positions.filter((row) => row.status === 'open')
  const closedPositions = positions.filter((row) => row.status === 'closed')
  const sellTrades = trades.filter((row) => actionKind(row.action) === 'SELL')
  const realizedPnl = sellTrades.reduce((sum, row) => sum + (Number(row.pnl) || 0), 0)
  const unrealizedPnl = openPositions.reduce((sum, row) => sum + (Number(row.unrealized_pnl) || 0), 0)
  const winners = sellTrades.filter((row) => Number(row.pnl) > 0).length
  const winRate = sellTrades.length ? (winners / sellTrades.length) * 100 : 0
  const portfolioValue = status?.equity ?? openPositions.reduce((sum, row) => sum + ((Number(row.mark_price) || Number(row.entry_price) || 0) * (Number(row.quantity) || 0)), 0)
  const loading = statusLoading || positionsLoading || tradesLoading
  const errors = [statusError, positionsError, tradesError].filter(Boolean)

  const positionColumns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'status', title: 'Status' },
    { key: 'quantity', title: 'Qty', render: (row) => fmtNumber(row.quantity, 6) },
    { key: 'entry_price', title: 'Entry', render: (row) => fmtNumber(row.entry_price, 4) },
    { key: 'mark_price', title: 'Mark', render: (row) => fmtNumber(row.mark_price, 4) },
    { key: 'entry_value', title: 'Entry value', render: (row) => money(row.entry_value) },
    { key: 'entry_score', title: 'Entry score', render: (row) => fmtNumber(row.entry_score, 2) },
    { key: 'entry_rank', title: 'Entry rank' },
    { key: 'unrealized_pnl', title: 'PnL', render: (row) => <span style={pnlTone(row.unrealized_pnl)}>{money(row.unrealized_pnl)}</span> },
    { key: 'opened_at', title: 'Opened', render: (row) => fmtDate(row.opened_at) },
    { key: 'closed_at', title: 'Closed', render: (row) => fmtDate(row.closed_at) },
  ]
  const tradeColumns = [
    { key: 'action', title: 'Action', render: (row) => {
      const kind = actionKind(row.action)
      const color = kind === 'BUY' ? 'var(--green)' : kind === 'SELL' ? 'var(--red)' : 'var(--muted)'
      return <strong style={{ color }}>{kind}</strong>
    } },
    { key: 'symbol', title: 'Symbol' },
    { key: 'price', title: 'Price', render: (row) => fmtNumber(row.price, 4) },
    { key: 'quantity', title: 'Qty', render: (row) => fmtNumber(row.quantity, 6) },
    { key: 'value', title: 'Value', render: (row) => money(row.value) },
    { key: 'pnl', title: 'PnL', render: (row) => <span style={pnlTone(row.pnl)}>{money(row.pnl)}</span> },
    { key: 'reason', title: 'Reason' },
    { key: 'created_at', title: 'Executed', render: (row) => fmtDate(row.created_at) },
  ]

  return <div className="page-stack">
    <PageHeader title="Momentum Positions" subtitle="Crypto momentum portfolio, positions and executed BUY / SELL decisions" />
    {loading ? <div className="panel">Loading Momentum positions and trades…</div> : null}
    {errors.map((error, index) => <div className="panel error" key={index}>{error.message || String(error)}</div>)}

    <div className="stats-grid">
      <StatCard label="Portfolio value" value={money(portfolioValue)} hint="Current Momentum equity" />
      <StatCard label="Realized PnL" value={money(realizedPnl)} tone={realizedPnl} hint={`${sellTrades.length} completed sales`} />
      <StatCard label="Unrealized PnL" value={money(unrealizedPnl)} tone={unrealizedPnl} hint={`${openPositions.length} open positions`} />
      <StatCard label="Win rate" value={`${fmtNumber(winRate, 1)}%`} tone={winRate} hint={`${winners}/${sellTrades.length} winning sales`} />
      <StatCard label="Positions" value={String(positions.length)} hint={`${openPositions.length} open · ${closedPositions.length} closed`} />
    </div>

    <FoldableTable title="Open Momentum positions" columns={positionColumns} rows={openPositions} empty="No open Momentum positions" paginated initialPageSize={25} />
    <FoldableTable title="Closed Momentum positions" columns={positionColumns} rows={closedPositions} empty="No closed Momentum positions" paginated initialPageSize={25} />
    <FoldableTable title="Executed Momentum decisions" columns={tradeColumns} rows={trades} empty="No executed Momentum decisions" paginated initialPageSize={50} />
  </div>
}
