import { useCallback, useState } from 'react'
import { api } from '../lib/api'
import { usePollingQuery } from '../hooks/usePollingQuery'

export default function MarketDataAdminPage() {
  const [provider, setProvider] = useState('IBKR')
  const { data: ibkr, error, loading } = usePollingQuery(useCallback(() => api.ibkrFeedStatus(), []), 30000)
  const [result, setResult] = useState(null)
  const act = async (fn) => setResult(await fn())
  return (
    <div className="page">
      <h1>Market Data Admin</h1>
      <p className="warning">IBKR Client Portal Gateway runs on Raspberry. SignalMaker does not connect to the gateway directly. The Raspberry must push candles to this backend.</p>
      <section className="card">
        <h2>IBKR Raspberry feed status</h2>
        {loading && <p>Loading…</p>}
        {error && <p className="error">{String(error.message || error)}</p>}
        <dl>
          <dt>Enabled</dt><dd>{String(ibkr?.enabled ?? false)}</dd>
          <dt>Ingest endpoint</dt><dd>{ibkr?.ingest_endpoint}</dd>
          <dt>IBKR candles</dt><dd>{ibkr?.total_ibkr_candles ?? 0}</dd>
          <dt>IBKR assets</dt><dd>{ibkr?.total_ibkr_assets ?? 0}</dd>
          <dt>Last import</dt><dd>{ibkr?.last_ibkr_import_run?.started_at || 'n/a'}</dd>
        </dl>
        <button onClick={() => act(api.testIbkrFeedIngest)}>Test IBKR ingest endpoint</button>
        <button onClick={() => act(api.ibkrCandleSummary)}>Load IBKR candle summary</button>
      </section>
      <section className="card">
        <h2>Provider-specific analysis</h2>
        <select value={provider} onChange={(event) => setProvider(event.target.value)}>
          <option>EODHD</option><option>IBKR</option><option>AUTO</option>
        </select>
        <button onClick={() => act(() => api.runMarketAnalysis({ provider, engine: 'momentum' }))}>Run Momentum</button>
        <button onClick={() => act(() => api.runMarketAnalysis({ provider, engine: 'wyckoff_smc' }))}>Run Wyckoff</button>
        <button onClick={() => act(() => api.runMarketAnalysis({ provider, engine: 'both' }))}>Run both engines</button>
      </section>
      {result && <pre>{JSON.stringify(result, null, 2)}</pre>}
    </div>
  )
}
