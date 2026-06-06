import { useState } from 'react'
import PageHeader from '../components/PageHeader'
import { api } from '../lib/api'

export default function MomentumCandidatesSyncPage() {
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')

  async function syncNow() {
    setBusy(true)
    setError('')
    try {
      setResult(await api.syncMomentumCandidates())
    } catch (err) {
      setError(err.message || 'Momentum candidate sync failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="page-stack">
      <PageHeader
        title="Momentum Candidates"
        subtitle="Sync central momentum candidates into the local Raspberry executor backlog."
        actions={<button className="button" disabled={busy} onClick={syncNow}>{busy ? 'Syncing…' : 'Sync now'}</button>}
      />
      {error ? <section className="panel"><p className="stat-hint">{error}</p></section> : null}
      <section className="stats-grid">
        <div className="stat-card"><div className="stat-label">Fetched</div><div className="stat-value">{result?.fetched ?? '—'}</div></div>
        <div className="stat-card"><div className="stat-label">Upserted</div><div className="stat-value">{result?.upserted ?? '—'}</div></div>
        <div className="stat-card"><div className="stat-label">Skipped</div><div className="stat-value">{result?.skipped?.length ?? '—'}</div></div>
        <div className="stat-card"><div className="stat-label">Errors</div><div className="stat-value">{result?.errors?.length ?? '—'}</div></div>
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
