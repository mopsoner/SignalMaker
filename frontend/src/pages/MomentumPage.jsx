import { useCallback, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { fmtDate, fmtNumber } from '../lib/format'

const API_BASE = import.meta.env.VITE_API_BASE || ''

function getOperatorKey() {
  try {
    return window.localStorage.getItem('signalmaker_operator_key') || ''
  } catch {
    return ''
  }
}

async function fetchMomentum(limit = 300) {
  const operatorKey = getOperatorKey()
  const headers = { 'Content-Type': 'application/json' }
  if (operatorKey) headers['x-operator-key'] = operatorKey
  const res = await fetch(`${API_BASE}/api/v1/momentum?limit=${limit}`, { headers })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res.json()
}

function classLabel(value) {
  const labels = {
    strong_bull: '🔥 Strong Bull',
    bull: '🟢 Bull',
    neutral_bull: '🟡 Neutral Bull',
    neutral_bear: '🟠 Neutral Bear',
    bear: '🔴 Bear',
  }
  return labels[value] || value || '—'
}

function scoreBadgeClass(value) {
  if (value === 'strong_bull' || value === 'bull') return 'badge green'
  if (value === 'neutral_bull') return 'badge blue'
  if (value === 'neutral_bear') return 'badge orange'
  return 'badge gray'
}

function trendLabel(value) {
  if (value === 'above_ema') return 'Above EMA'
  if (value === 'below_ema') return 'Below EMA'
  if (value === 'at_ema') return 'At EMA'
  if (value === 'insufficient_data') return 'No data'
  return value || '—'
}

export default function MomentumPage() {
  const [filter, setFilter] = useState('all')
  const { data: rows = [], loading, error } = usePollingQuery(useCallback(() => fetchMomentum(300), []), 30000)

  const counts = useMemo(() => ({
    all: rows.length,
    strong_bull: rows.filter((row) => row.classification === 'strong_bull').length,
    bull: rows.filter((row) => row.classification === 'bull').length,
    neutral: rows.filter((row) => ['neutral_bull', 'neutral_bear'].includes(row.classification)).length,
    bear: rows.filter((row) => row.classification === 'bear').length,
    complete: rows.filter((row) => row.data_quality === 'complete').length,
  }), [rows])

  const filteredRows = useMemo(() => {
    if (filter === 'all') return rows
    if (filter === 'neutral') return rows.filter((row) => ['neutral_bull', 'neutral_bear'].includes(row.classification))
    return rows.filter((row) => row.classification === filter)
  }, [filter, rows])

  const strongest = useMemo(() => rows.slice(0, 6), [rows])
  const avgScore = rows.length ? rows.reduce((sum, row) => sum + Number(row.momentum_score || 0), 0) / rows.length : 0

  const filters = [
    ['all', `All (${counts.all})`],
    ['strong_bull', `Strong Bull (${counts.strong_bull})`],
    ['bull', `Bull (${counts.bull})`],
    ['neutral', `Neutral (${counts.neutral})`],
    ['bear', `Bear (${counts.bear})`],
  ]

  const columns = [
    { key: 'rank', title: 'Rank', render: (row) => row.rank, sortValue: (row) => row.rank, defaultSortDir: 'asc' },
    { key: 'symbol', title: 'Symbol', render: (row) => <div style={{ display: 'grid', gap: 6 }}><Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link><a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">TradingView</a></div>, sortValue: (row) => row.symbol },
    { key: 'price', title: 'Price', render: (row) => fmtNumber(row.price, 6), sortValue: (row) => Number(row.price ?? -1) },
    { key: 'score', title: 'Momentum score', render: (row) => <strong>{fmtNumber(row.momentum_score, 2)}</strong>, sortValue: (row) => Number(row.momentum_score ?? -999) },
    { key: 'classification', title: 'Class', render: (row) => <span className={scoreBadgeClass(row.classification)}>{classLabel(row.classification)}</span>, sortValue: (row) => row.classification || '' },
    { key: 'm15', title: '15m', render: (row) => fmtNumber(row.momentum_15m, 2), sortValue: (row) => Number(row.momentum_15m ?? -999) },
    { key: 'm1h', title: '1h', render: (row) => fmtNumber(row.momentum_1h, 2), sortValue: (row) => Number(row.momentum_1h ?? -999) },
    { key: 'm4h', title: '4h', render: (row) => fmtNumber(row.momentum_4h, 2), sortValue: (row) => Number(row.momentum_4h ?? -999) },
    { key: 'rsi15', title: 'RSI 15m', render: (row) => fmtNumber(row.rsi_15m, 2), sortValue: (row) => Number(row.rsi_15m ?? -1) },
    { key: 'rsi1h', title: 'RSI 1h', render: (row) => fmtNumber(row.rsi_1h, 2), sortValue: (row) => Number(row.rsi_1h ?? -1) },
    { key: 'rsi4h', title: 'RSI 4h', render: (row) => fmtNumber(row.rsi_4h, 2), sortValue: (row) => Number(row.rsi_4h ?? -1) },
    { key: 'change15', title: 'Change 15m %', render: (row) => fmtNumber(row.change_15m, 2), sortValue: (row) => Number(row.change_15m ?? -999) },
    { key: 'change1h', title: 'Change 1h %', render: (row) => fmtNumber(row.change_1h, 2), sortValue: (row) => Number(row.change_1h ?? -999) },
    { key: 'change4h', title: 'Change 4h %', render: (row) => fmtNumber(row.change_4h, 2), sortValue: (row) => Number(row.change_4h ?? -999) },
    { key: 'ema', title: 'EMA trend', render: (row) => `${trendLabel(row.ema_trend_15m)} / ${trendLabel(row.ema_trend_1h)} / ${trendLabel(row.ema_trend_4h)}`, sortValue: (row) => `${row.ema_trend_15m}-${row.ema_trend_1h}-${row.ema_trend_4h}` },
    { key: 'quality', title: 'Data', render: (row) => row.data_quality || '—', sortValue: (row) => row.data_quality || '' },
    { key: 'updated', title: 'Updated', render: (row) => fmtDate(row.updated_at), sortValue: (row) => row.updated_at || '' },
  ]

  return <div className="page-stack">
    <PageHeader title="Momentum Ranking" subtitle="Read-only crypto ranking from 15m, 1h and 4h momentum. This page does not trigger trades or modify SignalMaker signals." />
    <div className="stats-grid">
      <StatCard label="Tracked assets" value={counts.all} hint={`${counts.complete} complete data sets`} />
      <StatCard label="Strong Bull" value={counts.strong_bull} />
      <StatCard label="Bull" value={counts.bull} />
      <StatCard label="Average momentum" value={fmtNumber(avgScore, 2)} />
    </div>
    {loading ? <div className="panel">Loading momentum ranking…</div> : null}
    {error ? <div className="panel error">{error}</div> : null}
    <FoldableTable title="Top momentum assets" columns={columns.slice(0, 8)} rows={strongest} empty="No momentum data available" defaultSortKey="score" defaultSortDir="desc" />
    <details className="panel collapsible-panel" open>
      <summary><h2>Momentum scanner</h2><span className="collapse-indicator">⌄</span></summary>
      <div className="market-toolbar">
        <div className="filter-chips" role="tablist" aria-label="Momentum filters">
          {filters.map(([key, label]) => <button key={key} type="button" className={`filter-chip ${filter === key ? 'active' : ''}`} onClick={() => setFilter(key)}>{label}</button>)}
        </div>
        <div className="market-toolbar-hint">Showing {filteredRows.length} / {rows.length}</div>
      </div>
      <FoldableTable title="All momentum rankings" columns={columns} rows={filteredRows} empty="No momentum data available" defaultSortKey="score" defaultSortDir="desc" paginated initialPageSize={50} />
    </details>
  </div>
}
