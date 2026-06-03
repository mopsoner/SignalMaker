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

export default function MomentumExecutorPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const { data, error, refresh } = usePollingQuery(useCallback(() => request('/api/v1/momentum-executor/status'), []), 5000)
  const decision = data?.decision || {}
  const local = data?.local_position

  async function runOnce() {
    setBusy(true); setMessage('')
    try {
      const result = await request('/api/v1/momentum-executor/run-once?force=true', { method: 'POST' })
      setMessage(`${result.action || 'OK'} · ${result.decision?.symbol || result.decision?.action || ''}`)
      refresh()
    } catch (e) { setMessage(e.message || String(e)) }
    finally { setBusy(false) }
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

  return <div className="page-stack">
    <PageHeader title="Momentum Executor" subtitle="Raspberry bridge for SignalMaker momentum rotation decisions." actions={<button className="button" disabled={busy} onClick={runOnce}>{busy ? 'Running…' : 'Apply decision once'}</button>} />
    {message ? <div className="panel info">{message}</div> : null}
    {error ? <div className="panel error">{error}</div> : null}
    <div className="stats-grid">
      <StatCard label="Mode" value={data?.mode || '—'} hint={data?.enabled ? 'enabled' : 'disabled'} />
      <StatCard label="Decision" value={decision?.action || '—'} hint={decision?.symbol || decision?.reason || '—'} />
      <StatCard label="Local position" value={local?.symbol || 'Cash'} hint={local ? `Entry ${fmtNumber(local.entry_price, 6)} · ${local.status}` : 'No momentum position'} />
      <StatCard label="Remote PnL" value={fmtNumber(decision?.total_pnl, 2)} hint={`${fmtNumber(decision?.total_pnl_pct, 2)}%`} />
    </div>
    <section className="panel two-col">
      <div><h2>Recommendation</h2><p>{decision?.reason || '—'}</p><p style={{ color: 'var(--muted)' }}>Next check: {fmtDate(decision?.next_check_at)}</p></div>
      <div><h2>Local follow-up</h2><pre style={{ whiteSpace: 'pre-wrap', fontSize: 12 }}>{JSON.stringify(local || {}, null, 2)}</pre></div>
    </section>
    <FoldableTable title="Momentum target / watch" columns={cols} rows={rows} empty="No momentum target yet" />
  </div>
}
