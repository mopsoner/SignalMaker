import { useCallback, useState } from 'react'
import DataTable from '../components/DataTable'
import PageHeader from '../components/PageHeader'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber, stageBadgeClass } from '../lib/format'

function summarizeContext(value) {
  if (!value || typeof value !== 'object') return '—'
  const type = value.type || '—'
  const level = value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'
  return `${type} @ ${level}`
}

function setupState(row) {
  const state = row?.payload?.state
  if (state === 'spring_watch') return 'spring_watch'
  if (state === 'utad_watch') return 'utad_watch'
  return row?.payload?.bias || '—'
}

function confirmLabel(row) {
  return row?.payload?.trigger || row?.notes || '—'
}

function stopSource(row) {
  return row?.payload?.trade?.stop_source || '—'
}

export default function CandidatesPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const loadCandidates = useCallback(() => api.candidates('?limit=100'), [])
  const { data: rows = [], loading, error } = usePollingQuery(loadCandidates, 10000)

  async function runPipeline() {
    setBusy(true)
    setMessage('')
    try {
      const result = await api.runPipeline(5)
      setMessage(`Pipeline OK · scanned ${result.symbols_scanned} · candidates ${result.candidates_created}`)
    } catch (err) {
      setMessage(err.message || String(err))
    } finally {
      setBusy(false)
    }
  }

  const columns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'side', title: 'Side' },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{row.stage}</span> },
    { key: 'status', title: 'Status' },
    { key: 'setup', title: 'Setup', render: (row) => setupState(row), sortValue: (row) => setupState(row) },
    { key: 'confirm', title: 'Confirm', render: (row) => confirmLabel(row), sortValue: (row) => confirmLabel(row) },
    { key: 'macro', title: 'Macro', render: (row) => summarizeContext(row?.payload?.macro_liquidity_context || row?.liquidity_context), sortValue: (row) => (row?.payload?.macro_liquidity_context || row?.liquidity_context)?.level ?? -1 },
    { key: 'entry', title: 'Entry context', render: (row) => summarizeContext(row?.payload?.entry_liquidity_context), sortValue: (row) => row?.payload?.entry_liquidity_context?.level ?? -1 },
    { key: 'stop_source', title: 'Stop source', render: (row) => stopSource(row), sortValue: (row) => stopSource(row) },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score, 2) },
    { key: 'entry_price', title: 'Entry', render: (row) => fmtNumber(row.entry_price, 4) },
    { key: 'stop_price', title: 'Stop', render: (row) => fmtNumber(row.stop_price, 4) },
    { key: 'target_price', title: 'Target', render: (row) => fmtNumber(row.target_price, 4) },
    { key: 'rr_ratio', title: 'RR', render: (row) => fmtNumber(row.rr_ratio, 2) },
    { key: 'created_at', title: 'Created', render: (row) => fmtDate(row.created_at) },
  ]

  return (
    <div className="page-stack">
      <PageHeader
        title="Trade Candidates"
        subtitle="Planner outputs with confirmed setup, liquidity context and stop source."
        actions={<button className="button" disabled={busy} onClick={runPipeline}>{busy ? 'Running…' : 'Run pipeline'}</button>}
      />
      {message ? <div className="panel info">{message}</div> : null}
      {loading ? <div className="panel">Loading candidates…</div> : null}
      {error ? <div className="panel error">{error}</div> : null}
      <section className="panel">
        <h2>Open and executed candidates</h2>
        <DataTable columns={columns} rows={rows} empty="No trade candidates yet" />
      </section>
    </div>
  )
}
