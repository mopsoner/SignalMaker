import { useCallback, useMemo, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import WyckoffSmcDashboard from '../components/WyckoffSmcDashboard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'
import { tradingViewUrl } from '../lib/tradingview'

const signalClass = (signal) => signal === 'BUY' ? 'badge green' : signal === 'SELL' ? 'badge orange' : 'badge gray'
const asRows = (value, key) => Array.isArray(value) ? value : Array.isArray(value?.[key]) ? value[key] : []
const errorText = (error) => error?.message || String(error || '')

function UniverseFilter({ universe, setUniverse, assetType, setAssetType }) {
  return <div className="page-actions" style={{ flexWrap: 'wrap' }}>
    <select value={universe} onChange={(e) => setUniverse(e.target.value)}>
      <option value="">All universes</option><option>Europe Stocks</option><option>Europe ETF</option>
    </select>
    <select value={assetType} onChange={(e) => setAssetType(e.target.value)}>
      <option value="">All asset types</option><option value="ETF">ETF</option><option value="STOCK">Stock</option><option value="INDEX">Index</option>
    </select>
  </div>
}

function query(universe, assetType, extra = '') {
  const params = new URLSearchParams()
  if (universe) params.set('universe', universe)
  if (assetType) params.set('asset_type', assetType)
  if (extra) for (const [k, v] of new URLSearchParams(extra)) params.set(k, v)
  const str = params.toString()
  return str ? `?${str}` : ''
}

function ResultsTable({ rows, engine, empty }) {
  const columns = [
    { key: 'symbol', title: 'Symbol', render: (row) => <div style={{ display: 'grid', gap: 4 }}><strong>{row.provider_symbol || row.symbol}</strong><span className="stat-hint">{row.name || '—'} · {row.universe_name || '—'}</span><a href={tradingViewUrl(row.provider_symbol || row.symbol, { market: 'stock-etf' })} target="_blank" rel="noreferrer">TradingView</a></div>, sortValue: (row) => row.provider_symbol || row.symbol },
    { key: 'signal', title: 'Signal', render: (row) => <span className={signalClass(row.signal)}>{row.signal || 'NO_SIGNAL'}</span>, sortValue: (row) => row.signal || '' },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score, 2), sortValue: (row) => Number(row.score || 0) },
    { key: 'trend', title: 'Trend', render: (row) => row.trend || '—', sortValue: (row) => row.trend || '' },
    { key: 'confidence', title: 'Confidence', render: (row) => fmtNumber(row.confidence, 2), sortValue: (row) => Number(row.confidence || 0) },
    { key: 'engine', title: 'Engine', render: (row) => row.engine_name || engine, sortValue: (row) => row.engine_name || engine },
    { key: 'updated', title: 'Updated', render: (row) => fmtDate(row.created_at), sortValue: (row) => row.created_at },
  ]
  return <FoldableTable rows={rows} columns={columns} defaultSortKey="score" defaultSortDir="desc" empty={empty || 'No analysis results yet. Load complete 15m/1h/4h history, then run the shared workflow from Admin Market Data.'} />
}

function Dashboard({ engine, title, subtitle, candidatesOnly = false, positionsOnly = false }) {
  const [universe, setUniverse] = useState('')
  const [assetType, setAssetType] = useState('')
  const fetcher = useCallback(() => {
    const q = query(universe, assetType, engine ? `engine=${engine}&limit=300` : 'limit=300')
    // Dashboard pages consume the dashboard object; candidates and positions
    // retain their purpose-built endpoints.
    return candidatesOnly ? api.stockEtfCandidates(q) : positionsOnly ? api.stockEtfPositions(q) : api.stockEtfDashboard(q)
  }, [universe, assetType, engine, candidatesOnly, positionsOnly])
  const { data, loading, error } = usePollingQuery(fetcher, 30000)
  const rows = candidatesOnly ? asRows(data, 'candidates') : positionsOnly ? asRows(data, 'positions') : engine === 'momentum' ? asRows(data, 'momentum') : engine === 'wyckoff_smc' ? asRows(data, 'wyckoff_smc') : [...asRows(data, 'momentum'), ...asRows(data, 'wyckoff_smc')]
  const empty = candidatesOnly ? 'No stock/ETF candidates yet.' : positionsOnly ? 'No stock/ETF positions yet.' : engine === 'momentum' ? 'No momentum results yet.' : engine === 'wyckoff_smc' ? 'No Wyckoff SMC results yet.' : 'No analysis results yet.'
  const counts = useMemo(() => ({ total: rows.length, buy: rows.filter((r) => r.signal === 'BUY').length, sell: rows.filter((r) => r.signal === 'SELL').length, hold: rows.filter((r) => ['HOLD', 'NO_SIGNAL'].includes(r.signal)).length }), [rows])
  return <div className="page-stack">
    <PageHeader title={title} subtitle={subtitle} />
    <div className="panel"><strong>Shared Stock/ETF workflow:</strong> canonical Momentum and Wyckoff/SMC engines on closed 15m, 1h and 4h candles · no broker execution.</div>
    <UniverseFilter universe={universe} setUniverse={setUniverse} assetType={assetType} setAssetType={setAssetType} />
    <div className="stats-grid"><StatCard label="Results" value={counts.total} /><StatCard label="Buy" value={counts.buy} /><StatCard label="Sell" value={counts.sell} /><StatCard label="Hold / No signal" value={counts.hold} /></div>
    {loading ? <div className="panel">Loading…</div> : null}
    {error ? <div className="panel error" role="alert"><strong>Unable to load stock/ETF data.</strong> {errorText(error)}</div> : null}
    <section className="panel"><ResultsTable rows={rows} engine={engine} empty={empty} /></section>
  </div>
}

const stockEtfWyckoffFilters = [
  { key: 'universe', label: 'Universe', allLabel: 'All universes', options: ['Europe Stocks', 'Europe ETF'] },
  { key: 'asset_type', label: 'Asset type', allLabel: 'All asset types', options: [{ value: 'ETF', label: 'ETF' }, { value: 'STOCK', label: 'Stock' }, { value: 'INDEX', label: 'Index' }] },
]

export function StockEtfWyckoffDashboardPage() {
  const loadAssets = useCallback(async ({ universe, asset_type: assetType }) => {
    const dashboard = await api.stockEtfDashboard(query(universe, assetType, 'engine=wyckoff_smc&limit=500'))
    return asRows(dashboard, 'wyckoff_smc').map((row) => ({ ...row, updated_at: row.updated_at || row.created_at }))
  }, [])
  return <WyckoffSmcDashboard
    loadAssets={loadAssets}
    tradingViewLink={(row) => tradingViewUrl(row.provider_symbol || row.symbol, { market: 'stock-etf' })}
    symbolFor={(row) => row.provider_symbol || row.symbol}
    assetMeta={(row) => [row.name, row.asset_type, row.universe_name].filter(Boolean).join(' · ')}
    title="ETF & Stocks · Wyckoff SMC Dashboard"
    subtitle="4H context/target, 1H Wyckoff-SMC setup and 15m alignment for the ETF/Stock universe."
    labels={{ symbol: 'ETF / Stock' }}
    marketFilters={stockEtfWyckoffFilters}
    pollingInterval={30000}
  />
}
export function StockEtfTradeCandidatesPage() { return <Dashboard engine="wyckoff_smc" candidatesOnly title="ETF & Stocks · Trade Candidates" subtitle="BUY/SELL candidates produced by the shared multi-timeframe analytical workflow." /> }
export function StockEtfPositionsPage() { return <Dashboard engine="wyckoff_smc" positionsOnly title="ETF & Stocks · Positions" subtitle="Paper/watch positions inferred from shared workflow results; no broker execution." /> }
export function StockEtfMomentumDashboardPage() { return <Dashboard engine="momentum" title="ETF & Stocks · Momentum Dashboard" subtitle="Canonical momentum analysis using closed 15m, 1h and 4h Stock/ETF candles." /> }


function QualityTable({ rows }) {
  const columns = [
    { key: 'symbol', title: 'Symbol', render: (r) => <strong>{r.provider_symbol}</strong>, sortValue: (r) => r.provider_symbol },
    { key: 'status', title: 'Data status', render: (r) => <span className={r.data_status === 'OK' ? 'badge green' : r.data_status === 'STALE' ? 'badge orange' : 'badge gray'}>{r.data_status}</span>, sortValue: (r) => r.data_status },
    { key: 'candles', title: 'Candles', render: (r) => fmtNumber(r.candles_count, 0), sortValue: (r) => Number(r.candles_count || 0) },
    { key: 'first', title: 'First candle', render: (r) => fmtDate(r.first_candle_at), sortValue: (r) => r.first_candle_at },
    { key: 'last', title: 'Last candle', render: (r) => fmtDate(r.last_candle_at), sortValue: (r) => r.last_candle_at },
    { key: 'analysis', title: 'Analysis status', render: (r) => <span className={r.analysis_status === 'OK' ? 'badge green' : 'badge orange'}>{r.analysis_status || '—'}</span>, sortValue: (r) => r.analysis_status || '' },
    { key: 'lastAnalysis', title: 'Last analysis', render: (r) => r.last_analysis_at ? fmtDate(r.last_analysis_at) : 'Missing analysis', sortValue: (r) => r.last_analysis_at || '' },
    { key: 'lastError', title: 'Last error', render: (r) => r.last_error || '—', sortValue: (r) => r.last_error || '' },
  ]
  return <FoldableTable rows={rows} columns={columns} initialSortKey="status" emptyMessage="No stock/ETF data-quality rows yet." />
}

export function StockEtfDataQualityPage() {
  const [universe, setUniverse] = useState('')
  const [assetType, setAssetType] = useState('')
  const q = query(universe, assetType, 'limit=500')
  const { data, loading, error } = usePollingQuery(useCallback(() => api.stockEtfFreshness(q), [q]), 30000)
  const rows = asRows(data, 'freshness')
  return <div className="page-stack">
    <PageHeader title="ETF & Stocks · Data Quality" subtitle="Freshness, 15m/1h/4h candle coverage and stale shared-workflow checks." />
    <UniverseFilter universe={universe} setUniverse={setUniverse} assetType={assetType} setAssetType={setAssetType} />
    <div className="page-actions"><a className="button" href={api.stockEtfExportUrl(query(universe, assetType, 'kind=quality&limit=500'))}>Export CSV</a></div>
    {loading ? <div className="panel">Loading…</div> : null}{error ? <div className="panel error" role="alert"><strong>Unable to load data quality.</strong> {errorText(error)}</div> : null}
    <section className="panel"><QualityTable rows={rows} /></section>
  </div>
}
