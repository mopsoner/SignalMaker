import { useCallback, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate } from '../lib/format'

export default function MarketDataAdminPage() {
  const [message, setMessage] = useState('')
  const [universe, setUniverse] = useState('ETF PEA')
  const [preview, setPreview] = useState(null)
  const { data, loading, error, refresh } = usePollingQuery(useCallback(() => api.marketDataSettings(), []), 30000)
  const { data: env } = usePollingQuery(useCallback(() => api.envSettings(), []), 30000)
  const { data: assets = [] } = usePollingQuery(useCallback(() => api.stockEtfAssets('?limit=500'), []), 30000)

  async function action(label, fn) {
    setMessage(`${label}…`)
    try { const result = await fn(); setMessage(`${label}: ${JSON.stringify(result)}`); refresh?.() } catch (e) { setMessage(`${label} failed: ${e.message}`) }
  }

  async function toggleAsset(row) {
    await action(`Update ${row.provider_symbol}`, () => api.updateMarketAsset(row.id, { enabled: !row.enabled }))
  }

  const columns = [
    { key: 'symbol', title: 'Symbol', render: (r) => <strong>{r.provider_symbol}</strong>, sortValue: (r) => r.provider_symbol },
    { key: 'name', title: 'Name', render: (r) => r.name || '—', sortValue: (r) => r.name || '' },
    { key: 'universe', title: 'Universe', render: (r) => r.universe_name || '—', sortValue: (r) => r.universe_name || '' },
    { key: 'type', title: 'Type', render: (r) => r.asset_type, sortValue: (r) => r.asset_type },
    { key: 'enabled', title: 'Enabled', render: (r) => <button className="button" onClick={() => toggleAsset(r)}>{r.enabled ? 'Enabled' : 'Disabled'}</button>, sortValue: (r) => Number(r.enabled) },
    { key: 'priority', title: 'Priority', render: (r) => r.priority, sortValue: (r) => Number(r.priority || 0) },
  ]

  return <div className="page-stack">
    <PageHeader title="Admin · ETF & Stock Market Data" subtitle="EODHD configuration, PEA/Europe universes and stock/ETF analysis controls. Secrets are never displayed in full." />
    {loading ? <div className="panel">Loading…</div> : null}{error ? <div className="panel error">{error.message}</div> : null}
    <div className="stats-grid"><StatCard label="Primary provider" value={data?.primary_provider || '—'} /><StatCard label="EODHD enabled" value={data?.eodhd_enabled ? 'Yes' : 'No'} /><StatCard label="API key" value={data?.eodhd_api_key_configured ? 'Configured' : 'Missing'} /><StatCard label="Assets / candles" value={`${data?.total_assets || 0} / ${data?.total_candles || 0}`} /></div>
    <section className="panel"><h2>Actions</h2><div className="page-actions" style={{ flexWrap: 'wrap', marginTop: 12 }}>
      <select value={universe} onChange={(e) => setUniverse(e.target.value)}><option>ETF PEA</option><option>ETF Europe UCITS</option><option>Stocks Euronext Paris</option><option>Stocks Europe</option><option>Benchmark Indices</option><option>US Benchmarks</option></select>
      <button className="button" onClick={() => action('Test EODHD', api.testEodhd)}>Test EODHD connection</button>
      <button className="button" onClick={() => action('Sync assets', api.syncMarketAssets)}>Run asset sync</button>
      <button className="button" onClick={() => action('Run momentum', () => api.runMarketAnalysis({ engine: 'momentum', universe, limit: 50 }))}>Run Momentum</button>
      <button className="button" onClick={() => action('Run Wyckoff SMC', () => api.runMarketAnalysis({ engine: 'wyckoff_smc', universe, limit: 50 }))}>Run Wyckoff SMC</button>
      <button className="button" onClick={() => action('Preview backfill', async () => { const r = await api.previewMarketAction({ action: 'backfill', universe, limit: 50 }); setPreview(r); return r })}>Preview backfill</button>
      <button className="button" onClick={() => action('Queue backfill', () => api.queueMarketJob({ job_type: 'backfill', universe, limit: 50 }))}>Queue backfill</button>
      <button className="button" onClick={() => action('Run both engines', () => api.runMarketAnalysis({ engine: 'both', universe, limit: 50 }))}>Run both</button>
    </div>{message ? <p className="stat-hint" style={{ marginTop: 12 }}>{message}</p> : null}{preview ? <pre className="stat-hint" style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(preview, null, 2)}</pre> : null}</section>
    <section className="panel"><h2>Configuration</h2><div className="stats-grid" style={{ marginTop: 12 }}><StatCard label="Timeframe" value={data?.default_timeframe || '1d'} /><StatCard label="Exchange" value={data?.default_exchange || 'PA'} /><StatCard label="Concurrency" value={data?.max_concurrent ?? '—'} /><StatCard label="Sleep seconds" value={data?.request_sleep_seconds ?? '—'} /></div><p className="stat-hint">Start date: {data?.start_date || '—'} · Adjusted data: {data?.adjusted_data ? 'yes' : 'no'} · Last import: {fmtDate(data?.last_import_run?.started_at)} · Last analysis: {fmtDate(data?.last_analysis_run?.started_at)}</p></section>
    <section className="panel"><h2>Environment variables</h2>{env?.warnings?.length ? <ul>{env.warnings.map((w) => <li key={w}>{w}</li>)}</ul> : <p className="stat-hint">No warnings.</p>}<p className="stat-hint">{env?.instructions}</p></section>

    <section className="panel"><h2>Run history & queued automation</h2><div className="stats-grid" style={{ marginTop: 12 }}><StatCard label="Import runs" value={data?.import_runs?.length || 0} /><StatCard label="Analysis runs" value={data?.analysis_runs?.length || 0} /><StatCard label="Queued jobs" value={data?.job_requests?.length || 0} /><StatCard label="Scheduler" value="CLI / worker safe" /></div><p className="stat-hint">Long backfills are queued or run from CLI to avoid blocking HTTP requests on Replit.</p></section>
    <section className="panel"><h2>ETF & Stock assets</h2><FoldableTable rows={assets} columns={columns} initialSortKey="symbol" /></section>
  </div>
}
