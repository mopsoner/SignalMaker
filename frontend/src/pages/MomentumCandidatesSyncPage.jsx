import { useCallback, useState } from 'react'
import DataTable from '../components/DataTable'
import PageHeader from '../components/PageHeader'
import { api } from '../lib/api'
import { fmtDate, fmtNumber, stageBadgeClass } from '../lib/format'
import { usePollingQuery } from '../hooks/usePollingQuery'

function sourceLabel(row) {
  return row?.payload?.source || row?.payload?.remote_candidate?.source || 'momentum_rankings'
}

function rankLabel(row) {
  return row?.payload?.remote_candidate?.rank ?? row?.payload?.momentum_trade_candidate?.rank ?? '—'
}

function classificationLabel(row) {
  return row?.payload?.remote_candidate?.classification || row?.payload?.momentum_trade_candidate?.classification || '—'
}

function rsi1h(row) {
  return row?.payload?.remote_candidate?.rsi_1h ?? row?.payload?.momentum_trade_candidate?.rsi_1h ?? row?.rsi_1h
}

function buyableLabel(row) {
  const value = Number(rsi1h(row))
  if (!Number.isFinite(value)) return 'No RSI'
  return value >= 45 && value <= 55 ? 'Buyable' : 'Blocked'
}

export default function MomentumCandidatesSyncPage() {
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')
  const loadMomentumCandidates = useCallback(() => api.candidates('?limit=100&stage=momentum'), [])
  const { data: rows = [], loading, error: tableError, refresh } = usePollingQuery(loadMomentumCandidates, 10000)

  async function syncNow() {
    setBusy(true)
    setError('')
    try {
      setResult(await api.syncMomentumCandidates())
      refresh?.()
    } catch (err) {
      setError(err.message || 'Momentum candidate sync failed')
    } finally {
      setBusy(false)
    }
  }

  const columns = [
    { key: 'rank', title: 'Rank', render: rankLabel, sortValue: (row) => Number(rankLabel(row)) || 9999 },
    { key: 'symbol', title: 'Symbol' },
    { key: 'status', title: 'Status' },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{row.stage}</span> },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score, 2) },
    { key: 'rsi_1h', title: 'RSI 1H', render: (row) => fmtNumber(rsi1h(row), 2), sortValue: rsi1h },
    { key: 'buyable', title: 'Buyable', render: buyableLabel, sortValue: buyableLabel },
    { key: 'entry_price', title: 'Entry', render: (row) => fmtNumber(row.entry_price, 6) },
    { key: 'target_price', title: 'Target', render: (row) => fmtNumber(row.target_price, 6) },
    { key: 'rr_ratio', title: 'RR', render: (row) => fmtNumber(row.rr_ratio, 2) },
    { key: 'classification', title: 'Class', render: classificationLabel, sortValue: classificationLabel },
    { key: 'source', title: 'Source', render: sourceLabel, sortValue: sourceLabel },
    { key: 'created_at', title: 'Created', render: (row) => fmtDate(row.created_at) },
  ]

  return (
    <div className="page-stack">
      <PageHeader
        title="Momentum Trade Candidates"
        subtitle="Sync central momentum asset rankings and store them as normal trade candidates for the executor."
        actions={<button className="button" disabled={busy} onClick={syncNow}>{busy ? 'Syncing…' : 'Sync now'}</button>}
      />
      {error ? <section className="panel"><p className="stat-hint">{error}</p></section> : null}
      <section className="stats-grid">
        <div className="stat-card"><div className="stat-label">Fetched assets</div><div className="stat-value">{result?.fetched ?? '—'}</div></div>
        <div className="stat-card"><div className="stat-label">Trade candidates</div><div className="stat-value">{result?.upserted ?? rows.length ?? '—'}</div></div>
        <div className="stat-card"><div className="stat-label">Skipped</div><div className="stat-value">{result?.skipped?.length ?? '—'}</div></div>
        <div className="stat-card"><div className="stat-label">Errors</div><div className="stat-value">{result?.errors?.length ?? '—'}</div></div>
      </section>
      {loading ? <div className="panel">Loading momentum trade candidates…</div> : null}
      {tableError ? <div className="panel error">{tableError}</div> : null}
      <section className="panel">
        <h2>Momentum candidates in the normal executor backlog</h2>
        <p className="stat-hint">These rows come from the momentum ranking feed, but execution now only buys candidates whose 1H RSI is between 45 and 55 and then sees them through the same trade-candidate API as Wyckoff/SMC candidates.</p>
        <DataTable columns={columns} rows={rows} empty="No momentum trade candidates yet" defaultSortKey="rank" defaultSortDir="asc" />
      </section>
      {result ? (
        <section className="panel">
          <h2>Last sync summary</h2>
          <pre style={{ whiteSpace: 'pre-wrap', overflowX: 'auto' }}>{JSON.stringify(result, null, 2)}</pre>
        </section>
      ) : null}
    </div>
  )
}
