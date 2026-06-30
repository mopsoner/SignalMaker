import { useCallback, useMemo, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'
import { tradingViewUrl } from '../lib/tradingview'

const signalClass = (signal) => signal === 'BUY' ? 'badge green' : signal === 'SELL' ? 'badge orange' : 'badge gray'

function UniverseFilter({ universe, setUniverse, assetType, setAssetType }) {
  return <div className="page-actions" style={{ flexWrap: 'wrap' }}>
    <select value={universe} onChange={(e) => setUniverse(e.target.value)}>
      <option value="">All universes</option><option>ETF PEA</option><option>ETF Europe UCITS</option><option>Stocks Euronext Paris</option><option>Stocks Europe</option><option>Benchmark Indices</option><option>US Benchmarks</option>
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

function ResultsTable({ rows, engine }) {
  const columns = [
    { key: 'symbol', title: 'Symbol', render: (row) => <div style={{ display: 'grid', gap: 4 }}><strong>{row.provider_symbol || row.symbol}</strong><span className="stat-hint">{row.name || '—'} · {row.universe_name || '—'}</span><a href={tradingViewUrl(row.provider_symbol || row.symbol, { market: 'stock-etf' })} target="_blank" rel="noreferrer">TradingView</a></div>, sortValue: (row) => row.provider_symbol || row.symbol },
    { key: 'signal', title: 'Signal', render: (row) => <span className={signalClass(row.signal)}>{row.signal || 'NO_SIGNAL'}</span>, sortValue: (row) => row.signal || '' },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score, 2), sortValue: (row) => Number(row.score || 0) },
    { key: 'trend', title: 'Trend', render: (row) => row.trend || '—', sortValue: (row) => row.trend || '' },
    { key: 'confidence', title: 'Confidence', render: (row) => fmtNumber(row.confidence, 2), sortValue: (row) => Number(row.confidence || 0) },
    { key: 'engine', title: 'Engine', render: (row) => row.engine_name || engine, sortValue: (row) => row.engine_name || engine },
    { key: 'updated', title: 'Updated', render: (row) => fmtDate(row.created_at), sortValue: (row) => row.created_at },
  ]
  return <FoldableTable rows={rows} columns={columns} initialSortKey="score" initialSortDirection="desc" emptyMessage="No EODHD analysis results yet. Sync assets, backfill daily candles, then run analysis from Admin Market Data." />
}

function Dashboard({ engine, title, subtitle, candidatesOnly = false, positionsOnly = false }) {
  const [universe, setUniverse] = useState('')
  const [assetType, setAssetType] = useState('')
  const fetcher = useCallback(() => {
    const q = query(universe, assetType, engine ? `engine=${engine}&limit=300` : 'limit=300')
    if (candidatesOnly) return api.stockEtfCandidates(q)
    if (positionsOnly) return api.stockEtfPositions(q)
    return engine ? api.stockEtfResults(q) : api.stockEtfDashboard(q)
  }, [universe, assetType, engine, candidatesOnly, positionsOnly])
  const { data, loading, error } = usePollingQuery(fetcher, 30000)
  const rows = Array.isArray(data) ? data : engine === 'momentum' ? data?.momentum || [] : engine === 'wyckoff_smc' ? data?.wyckoff_smc || [] : [...(data?.momentum || []), ...(data?.wyckoff_smc || [])]
  const counts = useMemo(() => ({ total: rows.length, buy: rows.filter((r) => r.signal === 'BUY').length, sell: rows.filter((r) => r.signal === 'SELL').length, hold: rows.filter((r) => ['HOLD', 'NO_SIGNAL'].includes(r.signal)).length }), [rows])
  return <div className="page-stack">
    <PageHeader title={title} subtitle={subtitle} />
    <div className="panel"><strong>Phase 1 Stock/ETF mode:</strong> daily EODHD data only · no realtime stream · no broker execution · isolated from crypto decision flows.</div>
    <UniverseFilter universe={universe} setUniverse={setUniverse} assetType={assetType} setAssetType={setAssetType} />
    <div className="stats-grid"><StatCard label="Results" value={counts.total} /><StatCard label="Buy" value={counts.buy} /><StatCard label="Sell" value={counts.sell} /><StatCard label="Hold / No signal" value={counts.hold} /></div>
    {loading ? <div className="panel">Loading…</div> : null}
    {error ? <div className="panel error">{error.message}</div> : null}
    <section className="panel"><ResultsTable rows={rows} engine={engine} /></section>
  </div>
}

export function StockEtfWyckoffDashboardPage() { return <Dashboard engine="wyckoff_smc" title="ETF & Stocks · Wyckoff SMC Dashboard" subtitle="Daily EODHD stock/ETF candles adapted into the existing Wyckoff-SMC workflow without touching the crypto process." /> }
export function StockEtfTradeCandidatesPage() { return <Dashboard engine="wyckoff_smc" candidatesOnly title="ETF & Stocks · Trade Candidates" subtitle="BUY/SELL candidates generated from stock/ETF EODHD daily analysis results." /> }
export function StockEtfPositionsPage() { return <Dashboard engine="wyckoff_smc" positionsOnly title="ETF & Stocks · Positions" subtitle="Phase-1 paper/watch positions inferred from BUY analysis results; no broker execution or IBKR dependency." /> }
export function StockEtfMomentumDashboardPage() { return <Dashboard engine="momentum" title="ETF & Stocks · Momentum Dashboard" subtitle="Daily EODHD stock/ETF candles adapted for the momentum dashboard layer." /> }


function QualityTable({ rows }) {
  const columns = [
    { key: 'symbol', title: 'Symbol', render: (r) => <strong>{r.provider_symbol}</strong>, sortValue: (r) => r.provider_symbol },
    { key: 'status', title: 'Data status', render: (r) => <span className={r.data_status === 'OK' ? 'badge green' : r.data_status === 'STALE' ? 'badge orange' : 'badge gray'}>{r.data_status}</span>, sortValue: (r) => r.data_status },
    { key: 'candles', title: 'Candles', render: (r) => fmtNumber(r.candles_count, 0), sortValue: (r) => Number(r.candles_count || 0) },
    { key: 'first', title: 'First candle', render: (r) => fmtDate(r.first_candle_at), sortValue: (r) => r.first_candle_at },
    { key: 'last', title: 'Last candle', render: (r) => fmtDate(r.last_candle_at), sortValue: (r) => r.last_candle_at },
    { key: 'analysis', title: 'Analysis status', render: (r) => <span className={r.analysis_status === 'OK' ? 'badge green' : 'badge orange'}>{r.analysis_status || '—'}</span>, sortValue: (r) => r.analysis_status || '' },
    { key: 'lastAnalysis', title: 'Last analysis', render: (r) => fmtDate(r.last_analysis_at), sortValue: (r) => r.last_analysis_at },
  ]
  return <FoldableTable rows={rows} columns={columns} initialSortKey="status" emptyMessage="No stock/ETF data-quality rows yet." />
}

export function StockEtfDataQualityPage() {
  const [universe, setUniverse] = useState('')
  const [assetType, setAssetType] = useState('')
  const q = query(universe, assetType, 'limit=500')
  const { data = [], loading, error } = usePollingQuery(useCallback(() => api.stockEtfFreshness(q), [q]), 30000)
  return <div className="page-stack">
    <PageHeader title="ETF & Stocks · Data Quality" subtitle="Freshness, candle coverage and stale-analysis checks for isolated EODHD daily candles." />
    <UniverseFilter universe={universe} setUniverse={setUniverse} assetType={assetType} setAssetType={setAssetType} />
    <div className="page-actions"><a className="button" href={api.stockEtfExportUrl(query(universe, assetType, 'kind=quality&limit=500'))}>Export CSV</a></div>
    {loading ? <div className="panel">Loading…</div> : null}{error ? <div className="panel error">{error.message}</div> : null}
    <section className="panel"><QualityTable rows={data} /></section>
  </div>
}

export function StockEtfConfluencePage() {
  const [universe, setUniverse] = useState('')
  const [assetType, setAssetType] = useState('')
  const q = query(universe, assetType, 'limit=500')
  const { data = [], loading, error } = usePollingQuery(useCallback(() => api.stockEtfConfluence(q), [q]), 30000)
  const columns = [
    { key: 'symbol', title: 'Symbol', render: (r) => <strong>{r.provider_symbol}</strong>, sortValue: (r) => r.provider_symbol },
    { key: 'confluence', title: 'Confluence', render: (r) => <span className={r.confluence === 'STRONG_BUY' ? 'badge green' : r.confluence === 'AVOID' ? 'badge orange' : 'badge gray'}>{r.confluence}</span>, sortValue: (r) => r.confluence_rank },
    { key: 'momentum', title: 'Momentum', render: (r) => `${r.momentum_signal || '—'} (${fmtNumber(r.momentum_score, 2)})`, sortValue: (r) => Number(r.momentum_score || 0) },
    { key: 'wyckoff', title: 'Wyckoff SMC', render: (r) => `${r.wyckoff_signal || '—'} (${fmtNumber(r.wyckoff_score, 2)})`, sortValue: (r) => Number(r.wyckoff_score || 0) },
    { key: 'universe', title: 'Universe', render: (r) => r.universe_name || '—', sortValue: (r) => r.universe_name || '' },
  ]
  return <div className="page-stack">
    <PageHeader title="ETF & Stocks · Confluence" subtitle="Momentum + Wyckoff SMC agreement layer; engines remain unchanged and results stay stock/ETF-scoped." />
    <UniverseFilter universe={universe} setUniverse={setUniverse} assetType={assetType} setAssetType={setAssetType} />
    <div className="page-actions"><a className="button" href={api.stockEtfExportUrl(query(universe, assetType, 'kind=confluence&limit=500'))}>Export CSV</a></div>
    {loading ? <div className="panel">Loading…</div> : null}{error ? <div className="panel error">{error.message}</div> : null}
    <section className="panel"><FoldableTable rows={data} columns={columns} initialSortKey="confluence" emptyMessage="No confluence rows yet. Run both engines first." /></section>
  </div>
}
