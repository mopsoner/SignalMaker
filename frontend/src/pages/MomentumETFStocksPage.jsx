import { useCallback, useMemo, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'

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

function tradingViewSymbol(row) {
  const symbol = encodeURIComponent(row.symbol || '')
  if (row.currency === 'EUR') return `EURONEXT%3A${symbol}`
  return row.primary_exchange ? `${encodeURIComponent(row.primary_exchange)}%3A${symbol}` : symbol
}

function MomentumBarChart({ rows }) {
  const points = rows.slice(0, 12)
  if (!points.length) return <div className="panel">No IBKR stock/ETF momentum rows yet.</div>
  const maxAbs = Math.max(...points.map((row) => Math.abs(Number(row.momentum_score || 0))), 1)
  return <div style={{ display: 'grid', gap: 10 }}>
    {points.map((row) => {
      const score = Number(row.momentum_score || 0)
      const width = Math.max((Math.abs(score) / maxAbs) * 100, 3)
      const positive = score >= 0
      return <div key={`${row.symbol}-${row.conid || ''}`} style={{ display: 'grid', gridTemplateColumns: '90px 1fr 90px', gap: 10, alignItems: 'center' }}>
        <strong>{row.symbol}</strong>
        <div style={{ height: 18, borderRadius: 999, background: 'rgba(148, 163, 184, 0.18)', overflow: 'hidden' }}>
          <div style={{ width: `${width}%`, height: '100%', borderRadius: 999, background: positive ? '#22c55e' : '#f97316' }} />
        </div>
        <span style={{ textAlign: 'right' }}>{fmtNumber(score, 2)}</span>
      </div>
    })}
    <div className="market-toolbar-hint">Daily IBKR candles are adapted into the existing momentum math with 21/63/126 daily-bar windows, preserving the crypto momentum engine unchanged.</div>
  </div>
}

export default function MomentumETFStocksPage() {
  const [filter, setFilter] = useState('all')
  const { data: rows = [], loading, error } = usePollingQuery(useCallback(() => api.ibkrMomentum('?limit=300'), []), 30000)

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

  const strongest = useMemo(() => rows.slice(0, 10), [rows])
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
    { key: 'symbol', title: 'Ticker', render: (row) => <div style={{ display: 'grid', gap: 6 }}><strong>{row.symbol}</strong><a href={`https://www.tradingview.com/chart/?symbol=${tradingViewSymbol(row)}`} target="_blank" rel="noreferrer">TradingView</a></div>, sortValue: (row) => row.symbol },
    { key: 'currency', title: 'Currency' },
    { key: 'route', title: 'IBKR route', render: (row) => `${row.exchange || 'SMART'}${row.primary_exchange ? ` / ${row.primary_exchange}` : ''}`, sortValue: (row) => `${row.exchange || ''}-${row.primary_exchange || ''}` },
    { key: 'conid', title: 'conId', render: (row) => row.conid || '—', sortValue: (row) => Number(row.conid || 0) },
    { key: 'price', title: 'Last close', render: (row) => fmtNumber(row.price, 4), sortValue: (row) => Number(row.price ?? -1) },
    { key: 'score', title: 'Momentum score', render: (row) => <strong>{fmtNumber(row.momentum_score, 2)}</strong>, sortValue: (row) => Number(row.momentum_score ?? -999) },
    { key: 'classification', title: 'Class', render: (row) => <span className={scoreBadgeClass(row.classification)}>{classLabel(row.classification)}</span>, sortValue: (row) => row.classification || '' },
    { key: 'm1d', title: '21D', render: (row) => fmtNumber(row.momentum_1d, 2), sortValue: (row) => Number(row.momentum_1d ?? -999) },
    { key: 'm1m', title: '63D', render: (row) => fmtNumber(row.momentum_1m, 2), sortValue: (row) => Number(row.momentum_1m ?? -999) },
    { key: 'm6m', title: '126D', render: (row) => fmtNumber(row.momentum_6m, 2), sortValue: (row) => Number(row.momentum_6m ?? -999) },
    { key: 'rsi1d', title: 'RSI 21D', render: (row) => fmtNumber(row.rsi_1d, 2), sortValue: (row) => Number(row.rsi_1d ?? -1) },
    { key: 'rsi1m', title: 'RSI 63D', render: (row) => fmtNumber(row.rsi_1m, 2), sortValue: (row) => Number(row.rsi_1m ?? -1) },
    { key: 'rsi6m', title: 'RSI 126D', render: (row) => fmtNumber(row.rsi_6m, 2), sortValue: (row) => Number(row.rsi_6m ?? -1) },
    { key: 'change1d', title: 'Change 21D %', render: (row) => fmtNumber(row.change_1d, 2), sortValue: (row) => Number(row.change_1d ?? -999) },
    { key: 'change1m', title: 'Change 63D %', render: (row) => fmtNumber(row.change_1m, 2), sortValue: (row) => Number(row.change_1m ?? -999) },
    { key: 'change6m', title: 'Change 126D %', render: (row) => fmtNumber(row.change_6m, 2), sortValue: (row) => Number(row.change_6m ?? -999) },
    { key: 'ema', title: 'EMA trend', render: (row) => `${trendLabel(row.ema_trend_1d)} / ${trendLabel(row.ema_trend_1m)} / ${trendLabel(row.ema_trend_6m)}`, sortValue: (row) => `${row.ema_trend_1d}-${row.ema_trend_1m}-${row.ema_trend_6m}` },
    { key: 'quality', title: 'Data', render: (row) => `${row.data_quality || '—'} · ${row.candle_count || 0} bars`, sortValue: (row) => row.data_quality || '' },
    { key: 'updated', title: 'Last daily bar', render: (row) => fmtDate(row.updated_at), sortValue: (row) => row.updated_at || '' },
  ]

  return <div className="page-stack">
    <PageHeader title="Momentum · ETF & Stocks" subtitle="IBKR daily stock/ETF momentum ranking using the existing momentum calculation helpers adapted to daily bars only." />
    <div className="stats-grid">
      <StatCard label="Tracked IBKR assets" value={counts.all} hint={`${counts.complete} complete daily data sets`} />
      <StatCard label="Strong Bull" value={counts.strong_bull} />
      <StatCard label="Bull" value={counts.bull} />
      <StatCard label="Average momentum" value={fmtNumber(avgScore, 2)} />
    </div>
    {loading ? <div className="panel">Loading IBKR stock/ETF momentum…</div> : null}
    {error ? <div className="panel error">{error}</div> : null}

    <section className="panel" style={{ display: 'grid', gap: 12 }}>
      <h2 style={{ margin: 0 }}>IBKR momentum strength chart</h2>
      <MomentumBarChart rows={strongest} />
    </section>

    <FoldableTable title="Top 10 ETF & stock momentum" columns={columns.slice(0, 11)} rows={strongest} empty="No IBKR momentum data available" defaultSortKey="score" defaultSortDir="desc" />

    <details className="panel collapsible-panel" open>
      <summary><h2>IBKR momentum scanner</h2><span className="collapse-indicator">⌄</span></summary>
      <div className="market-toolbar">
        <div className="filter-chips" role="tablist" aria-label="IBKR momentum filters">
          {filters.map(([key, label]) => <button key={key} type="button" className={`filter-chip ${filter === key ? 'active' : ''}`} onClick={() => setFilter(key)}>{label}</button>)}
        </div>
        <div className="market-toolbar-hint">Showing {filteredRows.length} / {rows.length}</div>
      </div>
      <FoldableTable title="All IBKR ETF & stock momentum rankings" columns={columns} rows={filteredRows} empty="No IBKR daily momentum rows available" defaultSortKey="score" defaultSortDir="desc" paginated initialPageSize={50} />
    </details>
  </div>
}
