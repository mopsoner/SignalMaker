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
        <StatCard label="Enabled" value={data?.enabled ? 'Yes' : 'No'} hint={data?.mode || '—'} />
        <StatCard label="Remote action" value={<ActionBadge action={decision?.action} />} hint={decision?.symbol || decision?.buy_symbol || decision?.sell_symbol || '—'} />
        <StatCard label="Local position" value={local?.symbol || 'Cash'} hint={local ? `${fmtNumber(local.quantity, 6)} @ ${fmtNumber(local.entry_price, 6)}` : 'No local momentum position'} />
        <StatCard label="Updated" value={fmtDate(data?.updated_at)} hint={data?.api_base || '—'} />
      </section>

      <section className="panel">
        <h2>Recommendation</h2>
        <p className="stat-hint">{decision?.reason || '—'}</p>
        <div className="market-toolbar-hint" style={{ marginTop: 12 }}>Next check: {fmtDate(decision?.next_check_at)}</div>
      </section>

      <FoldableTable title="Remote momentum assets" columns={cols} rows={rows} empty="No remote asset payload" />

      <section className="panel">
        <h2>Local follow-up</h2>
        <pre style={{ whiteSpace: 'pre-wrap', margin: 0 }}>{JSON.stringify(local || {}, null, 2)}</pre>
      </section>
    </div>
  )
}
