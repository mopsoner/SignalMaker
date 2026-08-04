import { useCallback, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { confirmAndClearAllStockEtfData } from '../lib/clearAllStockEtfData'
import { fmtDate } from '../lib/format'

export default function MarketDataAdminPage() {
  const [message, setMessage] = useState('')
  const [clearingAll, setClearingAll] = useState(false)
  const [universe, setUniverse] = useState('Europe Stocks')
  const [preview, setPreview] = useState(null)
  const { data, loading, error, refresh } = usePollingQuery(useCallback(() => api.marketDataSettings(), []), 30000)
  const { data: env, error: envError } = usePollingQuery(useCallback(() => api.envSettings(), []), 30000)
  const { data: assetData, error: assetsError, refresh: refreshAssets } = usePollingQuery(useCallback(() => api.stockEtfAssets('?limit=500'), []), 30000)
  const assets = Array.isArray(assetData) ? assetData : Array.isArray(assetData?.assets) ? assetData.assets : []
  const errorText = (value) => value?.message || String(value || '')

  async function action(label, fn) {
    setMessage(`${label}…`)
    try { const result = await fn(); setMessage(`${label}: ${JSON.stringify(result)}`); refresh?.() } catch (e) { setMessage(`${label} failed: ${e.message}`) }
  }

  async function toggleAsset(row) {
    await action(`Update ${row.provider_symbol}`, () => api.updateMarketAsset(row.id, { enabled: !row.enabled }))
  }

  async function clearAllData() {
    try {
      const result = await confirmAndClearAllStockEtfData({
        confirm: window.confirm,
        request: api.clearAllStockEtfData,
        refreshSettings: () => refresh?.(),
        refreshAssets: () => refreshAssets?.(),
        onConfirmed: () => {
          setClearingAll(true)
          setMessage('Clear all ETF/stock data…')
        },
      })
      if (result === null) return
      setMessage(`Clear all ETF/stock data: ${JSON.stringify(result)}`)
    } catch (e) {
      setMessage(`Clear all ETF/stock data failed: ${e.message}`)
    } finally {
      setClearingAll(false)
    }
  }

  const columns = [
    { key: 'symbol', title: 'Symbol', render: (r) => <strong>{r.provider_symbol}</strong>, sortValue: (r) => r.provider_symbol },
    { key: 'name', title: 'Name', render: (r) => r.name || '—', sortValue: (r) => r.name || '' },
    { key: 'universe', title: 'Universe', render: (r) => r.universe_name || '—', sortValue: (r) => r.universe_name || '' },
    { key: 'type', title: 'Type', render: (r) => r.asset_type, sortValue: (r) => r.asset_type },
    { key: 'pea', title: 'PEA', render: (r) => r.pea_eligible == null ? 'Unknown' : r.pea_eligible ? 'Yes' : 'No', sortValue: (r) => r.pea_eligible == null ? -1 : Number(r.pea_eligible) },
    { key: 'ucits', title: 'UCITS', render: (r) => r.ucits == null ? 'Unknown' : r.ucits ? 'Yes' : 'No', sortValue: (r) => r.ucits == null ? -1 : Number(r.ucits) },
    { key: 'enabled', title: 'Enabled', render: (r) => <button className="button" onClick={() => toggleAsset(r)}>{r.enabled ? 'Enabled' : 'Disabled'}</button>, sortValue: (r) => Number(r.enabled) },
    { key: 'priority', title: 'Priority', render: (r) => r.priority, sortValue: (r) => Number(r.priority || 0) },
  ]

  return <div className="page-stack">
    <PageHeader title="Admin · ETF & Stock Market Data" subtitle="IBKR configuration and dynamically discovered universes and stock/ETF analysis controls. Secrets are never displayed in full." />
    {loading ? <div className="panel">Loading…</div> : null}{error ? <div className="panel error" role="alert"><strong>Unable to load market-data settings.</strong> {errorText(error)}</div> : null}
    {envError ? <div className="panel error" role="alert"><strong>Unable to load environment status.</strong> {errorText(envError)}</div> : null}
    {assetsError ? <div className="panel error" role="alert"><strong>Unable to load stock/ETF assets.</strong> {errorText(assetsError)}</div> : null}
    <div className="stats-grid"><StatCard label="Primary provider" value={data?.primary_provider || '—'} /><StatCard label="IBKR enabled" value={data?.ibkr_enabled ? 'Yes' : 'No'} /><StatCard label="IBKR auth" value={data?.ibkr_auth_method || '—'} /><StatCard label="Assets / candles" value={`${data?.total_assets || 0} / ${data?.total_candles || 0}`} /></div>
    <section className="panel"><h2>Actions</h2><div className="page-actions" style={{ flexWrap: 'wrap', marginTop: 12 }}>
      <select value={universe} onChange={(e) => setUniverse(e.target.value)}><option>Europe Stocks</option><option>Europe ETF</option></select>
      <button className="button" onClick={() => action('Sync assets', api.syncMarketAssets)}>Run asset sync</button>
      <button className="button" onClick={() => action('Run momentum', () => api.runMarketAnalysis({ engine: 'momentum', universe, limit: 50 }))}>Run Momentum</button>
      <button className="button" onClick={() => action('Run Wyckoff SMC', () => api.runMarketAnalysis({ engine: 'wyckoff_smc', universe, limit: 50 }))}>Run Wyckoff SMC</button>
      <button className="button" onClick={() => action('Preview backfill', async () => { const r = await api.previewMarketAction({ action: 'backfill', universe, limit: 50 }); setPreview(r); return r })}>Preview backfill</button>
      <button className="button" onClick={() => action('Queue analysis', () => api.queueMarketJob({ job_type: 'analysis', market_scope: 'stock_etf', engine: 'both', universe, limit: 50 }))}>Queue analysis</button>
      <button className="button" onClick={() => action('Run both engines', () => api.runMarketAnalysis({ engine: 'both', universe, limit: 50 }))}>Run both</button>
      <button className="button" style={{ borderColor: 'rgba(239, 68, 68, 0.45)', color: 'var(--red)' }} disabled={clearingAll} onClick={clearAllData}>{clearingAll ? 'Clearing all ETF/stock data…' : 'Clear all ETF/stock data'}</button>
    </div>{message ? <p className="stat-hint" style={{ marginTop: 12 }}>{message}</p> : null}{preview ? <pre className="stat-hint" style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(preview, null, 2)}</pre> : null}</section>
    <section className="panel"><h2>Configuration</h2><div className="stats-grid" style={{ marginTop: 12 }}><StatCard label="Timeframe" value={data?.default_timeframe || '1d'} /><StatCard label="Exchange" value={data?.default_exchange || 'PA'} /><StatCard label="Concurrency" value={data?.max_concurrent ?? '—'} /><StatCard label="Sleep seconds" value={data?.request_sleep_seconds ?? '—'} /></div><p className="stat-hint">Start date: {data?.start_date || '—'} · Adjusted data: {data?.adjusted_data ? 'yes' : 'no'} · Last import: {fmtDate(data?.last_import_run?.started_at)} · Last analysis: {fmtDate(data?.last_analysis_run?.started_at)}</p></section>
    <section className="panel"><h2>Environment variables</h2>{env?.warnings?.length ? <ul>{env.warnings.map((w) => <li key={w}>{w}</li>)}</ul> : <p className="stat-hint">No warnings.</p>}<p className="stat-hint">{env?.instructions}</p></section>

    <section className="panel"><h2>Run history & queued automation</h2><div className="stats-grid" style={{ marginTop: 12 }}><StatCard label="Import runs" value={data?.import_runs?.length || 0} /><StatCard label="Analysis runs" value={data?.analysis_runs?.length || 0} /><StatCard label="Queued jobs" value={data?.job_requests?.length || 0} /><StatCard label="Scheduler" value="CLI / worker safe" /></div><p className="stat-hint">Long backfills are queued or run from CLI to avoid blocking HTTP requests on Replit.</p></section>
    <section className="panel"><h2>ETF & Stock assets</h2><FoldableTable rows={assets} columns={columns} defaultSortKey="symbol" empty="No stock/ETF assets yet. Run asset sync first." /></section>
  </div>
}
