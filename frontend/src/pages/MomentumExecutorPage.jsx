import { useCallback, useState } from 'react'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import FoldableTable from '../components/FoldableTable'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { fmtDate, fmtNumber } from '../lib/format'

const API_BASE = import.meta.env.VITE_API_BASE || ''

async function request(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, { headers: { 'Content-Type': 'application/json' }, ...options })
  if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`)
  return res.json()
}

function ActionBadge({ action }) {
  const normalized = String(action || 'WAIT').toUpperCase()
  const className = normalized === 'ERROR' ? 'badge red' : normalized === 'BUY' || normalized === 'ROTATE' ? 'badge green' : normalized === 'SELL' ? 'badge orange' : 'badge blue'
  return <span className={className}>{normalized}</span>
}

function ProfitCurve({ decision }) {
  const points = (() => {
    const raw = decision?.equity_curve || decision?.equity || decision?.profit_curve || []
    if (Array.isArray(raw) && raw.length) {
      return raw.map((row, index) => ({
        x: index,
        value: Number(row?.equity ?? row?.profit ?? row?.pnl ?? row?.value ?? row ?? 0),
      })).filter((row) => Number.isFinite(row.value))
    }
    let cumulative = 0
    return (decision?.trades || []).slice().reverse().map((trade, index) => {
      cumulative += Number(trade?.pnl || 0)
      return { x: index, value: cumulative }
    }).filter((row) => Number.isFinite(row.value))
  })()

  if (points.length < 2) {
    return <p className="stat-hint">No profit curve yet. The graph appears when the remote decision exposes an equity/profit curve or trade PnL history.</p>
  }

  const values = points.map((row) => row.value)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const spread = max - min || 1
  const polyline = points.map((row, index) => {
    const x = points.length < 2 ? 0 : (index / (points.length - 1)) * 100
    const y = 100 - ((row.value - min) / spread) * 100
    return `${x},${y}`
  }).join(' ')
  const last = values[values.length - 1]

  return (
    <div>
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: '100%', height: 220, background: 'rgba(255,255,255,.03)', border: '1px solid var(--line)', borderRadius: 12 }}>
        <line x1="0" x2="100" y1={100 - ((0 - min) / spread) * 100} y2={100 - ((0 - min) / spread) * 100} stroke="currentColor" opacity="0.18" strokeWidth="1" vectorEffect="non-scaling-stroke" />
        <polyline points={polyline} fill="none" stroke="currentColor" strokeWidth="1.8" vectorEffect="non-scaling-stroke" />
      </svg>
      <div className="market-toolbar-hint" style={{ marginTop: 8 }}>Points: {points.length} · Last: {fmtNumber(last, 2)} · Min: {fmtNumber(min, 2)} · Max: {fmtNumber(max, 2)}</div>
    </div>
  )
}

export default function MomentumExecutorPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const { data, error, refresh } = usePollingQuery(useCallback(() => request('/api/v1/momentum-executor/status'), []), 5000)
  const decision = data?.decision || {}
  const local = data?.local_position

  async function runOnce() {
    setBusy(true)
    setMessage('')
    try {
      const result = await request('/api/v1/momentum-executor/run-once?force=true', { method: 'POST' })
      setMessage(`${result.action || 'OK'} · ${result.decision?.symbol || result.decision?.action || ''}`)
      refresh()
    } catch (e) {
      setMessage(e.message || String(e))
    } finally {
      setBusy(false)
    }
  }

  const rows = [decision?.target_asset, decision?.top_watch_asset].filter(Boolean).map((row, i) => ({ id: i, ...row }))
  const cols = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'rank', title: 'Rank' },
    { key: 'price', title: 'Price', render: (r) => fmtNumber(r.price, 6) },
    { key: 'momentum_score', title: 'Score', render: (r) => fmtNumber(r.momentum_score, 2) },
    { key: 'rsi_1h', title: 'RSI 1h', render: (r) => fmtNumber(r.rsi_1h, 2) },
    { key: 'entry_status', title: 'Entry' },
    { key: 'structure_15m_status', title: 'Structure 15m' },
  ]

  return (
    <div className="page-stack">
      <PageHeader
        title="Momentum Executor"
        subtitle="Raspberry executor bridge for remote momentum rotation decisions."
        actions={<button className="button primary" disabled={busy} onClick={runOnce}>{busy ? 'Running…' : 'Apply decision once'}</button>}
      />
      {message ? <div className="panel"><strong>{message}</strong></div> : null}
      {error ? <div className="panel error">{error}</div> : null}

      <section className="stats-grid">
        <StatCard label="Mode" value={data?.mode || '—'} hint={data?.enabled ? 'enabled' : 'disabled'} />
        <StatCard label="Decision" value={<ActionBadge action={decision?.action} />} hint={decision?.symbol || decision?.reason || '—'} />
        <StatCard label="Local position" value={local?.symbol || 'Cash'} hint={local ? `Entry ${fmtNumber(local.entry_price, 6)} · ${local.status}` : 'No momentum position'} />
        <StatCard label="Remote PnL" value={fmtNumber(decision?.total_pnl, 2)} hint={`${fmtNumber(decision?.total_pnl_pct, 2)}%`} />
      </section>

      <section className="panel two-col">
        <div>
          <h2>Recommendation</h2>
          <p>{decision?.reason || '—'}</p>
          <p style={{ color: 'var(--muted)' }}>Next check: {fmtDate(decision?.next_check_at)}</p>
          <p className="market-toolbar-hint">Updated: {fmtDate(data?.updated_at)} · {data?.api_base || '—'}</p>
        </div>
        <div>
          <h2>Local follow-up</h2>
          <pre style={{ whiteSpace: 'pre-wrap', fontSize: 12 }}>{JSON.stringify(local || {}, null, 2)}</pre>
        </div>
      </section>

      <section className="panel">
        <h2>Profit graph</h2>
        <ProfitCurve decision={decision} />
      </section>

      <FoldableTable title="Momentum target / watch" columns={cols} rows={rows} empty="No momentum target yet" />
    </div>
  )
}
