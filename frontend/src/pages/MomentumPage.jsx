import { useCallback, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { fmtDate, fmtNumber } from '../lib/format'

const API_BASE = import.meta.env.VITE_API_BASE || ''
const DEFAULT_CADENCE_HOURS = 4
const STARTING_CAPITAL = 1000
const MIN_MOMENTUM_SCORE = 0

function getOperatorKey() {
  try {
    return window.localStorage.getItem('signalmaker_operator_key') || ''
  } catch {
    return ''
  }
}

function headers() {
  const operatorKey = getOperatorKey()
  const out = { 'Content-Type': 'application/json' }
  if (operatorKey) out['x-operator-key'] = operatorKey
  return out
}

async function fetchJson(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, { headers: headers(), ...options })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res.json()
}

function fetchMomentum(limit = 300) {
  return fetchJson(`/api/v1/momentum?limit=${limit}`)
}

function fetchMomentumEngine(cadenceHours = DEFAULT_CADENCE_HOURS) {
  const params = new URLSearchParams({
    cadence_hours: String(cadenceHours),
    starting_capital: String(STARTING_CAPITAL),
    min_momentum_score: String(MIN_MOMENTUM_SCORE),
  })
  return fetchJson(`/api/v1/momentum-engine/status?${params.toString()}`)
}

function runMomentumEngine(cadenceHours = DEFAULT_CADENCE_HOURS, force = true) {
  return fetchJson('/api/v1/momentum-engine/run-once', {
    method: 'POST',
    body: JSON.stringify({
      force,
      cadence_hours: Number(cadenceHours),
      starting_capital: STARTING_CAPITAL,
      min_momentum_score: MIN_MOMENTUM_SCORE,
    }),
  })
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
  const [cadenceHours, setCadenceHours] = useState(DEFAULT_CADENCE_HOURS)
  const [engineOverride, setEngineOverride] = useState(null)
  const [engineActionError, setEngineActionError] = useState(null)
  const { data: rows = [], loading, error } = usePollingQuery(useCallback(() => fetchMomentum(300), []), 30000)
  const { data: engineData, loading: engineLoading, error: engineError, refresh: refreshEngine } = usePollingQuery(useCallback(() => fetchMomentumEngine(cadenceHours), [cadenceHours]), 30000)
  const engine = engineOverride || engineData

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

  async function onRunEngine(force = true) {
    setEngineActionError(null)
    try {
      const result = await runMomentumEngine(cadenceHours, force)
      setEngineOverride(result)
      refreshEngine()
    } catch (err) {
      setEngineActionError(err.message || String(err))
    }
  }

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

  const tradeColumns = [
    { key: 'created_at', title: 'Time', render: (row) => fmtDate(row.created_at), sortValue: (row) => row.created_at || '' },
    { key: 'action', title: 'Action', render: (row) => row.action, sortValue: (row) => row.action || '' },
    { key: 'symbol', title: 'Symbol', render: (row) => row.symbol, sortValue: (row) => row.symbol || '' },
    { key: 'price', title: 'Price', render: (row) => fmtNumber(row.price, 6), sortValue: (row) => Number(row.price || 0) },
    { key: 'quantity', title: 'Qty', render: (row) => fmtNumber(row.quantity, 6), sortValue: (row) => Number(row.quantity || 0) },
    { key: 'value', title: 'Value', render: (row) => fmtNumber(row.value, 2), sortValue: (row) => Number(row.value || 0) },
    { key: 'pnl', title: 'PnL', render: (row) => fmtNumber(row.pnl, 2), sortValue: (row) => Number(row.pnl || 0) },
    { key: 'reason', title: 'Reason', render: (row) => row.reason || '—', sortValue: (row) => row.reason || '' },
  ]

  return <div className="page-stack">
    <PageHeader title="Momentum Ranking" subtitle="Read-only ranking + dedicated backend paper engine for 4H momentum rotation. No real Binance order is sent." />
    <div className="stats-grid">
      <StatCard label="Tracked assets" value={counts.all} hint={`${counts.complete} complete data sets`} />
      <StatCard label="Strong Bull" value={counts.strong_bull} />
      <StatCard label="Bull" value={counts.bull} />
      <StatCard label="Average momentum" value={fmtNumber(avgScore, 2)} />
    </div>
    {loading ? <div className="panel">Loading momentum ranking…</div> : null}
    {error ? <div className="panel error">{error}</div> : null}

    <FoldableTable title="Top 10 momentum fort" columns={columns.slice(0, 8)} rows={strongest} empty="No momentum data available" defaultSortKey="score" defaultSortDir="desc" />

    <details className="panel collapsible-panel" open>
      <summary><h2>Positions momentum · backend paper engine</h2><span className="collapse-indicator">⌄</span></summary>
      {engineLoading ? <div className="panel">Loading momentum engine…</div> : null}
      {engineError ? <div className="panel error">{engineError}</div> : null}
      {engineActionError ? <div className="panel error">{engineActionError}</div> : null}
      <div className="stats-grid">
        <StatCard label="Equity paper" value={fmtNumber(engine?.equity, 2)} hint={`Start: ${fmtNumber(engine?.starting_capital || STARTING_CAPITAL, 2)} USDC`} />
        <StatCard label="Total PnL" value={`${fmtNumber(engine?.total_pnl, 2)} USDC`} hint={`${fmtNumber(engine?.total_pnl_pct, 2)}%`} />
        <StatCard label="Cash" value={fmtNumber(engine?.cash, 2)} />
        <StatCard label="Next check" value={engine?.next_check_at ? fmtDate(engine.next_check_at) : 'Now'} hint={`Cadence: ${cadenceHours}h`} />
      </div>
      <div className="market-toolbar">
        <div className="filter-chips">
          <button className="filter-chip active" type="button" onClick={() => onRunEngine(true)}>Run engine now</button>
          <button className="filter-chip" type="button" onClick={() => onRunEngine(false)}>Run only if due</button>
          <label className="market-toolbar-hint">Cadence{' '}
            <select value={cadenceHours} onChange={(event) => { setCadenceHours(Number(event.target.value)); setEngineOverride(null) }}>
              <option value={4}>4h · default</option>
              <option value={8}>8h · calmer rotation</option>
              <option value={24}>24h · swing mode</option>
            </select>
          </label>
        </div>
        <div className="market-toolbar-hint">Default 4h is aligned with the macro 4H momentum and avoids noisy 15m over-rotation.</div>
      </div>
      <div className="stats-grid">
        <StatCard label="Current paper position" value={engine?.open_position?.symbol || 'Cash'} hint={engine?.open_position ? `Entry ${fmtNumber(engine.open_position.entry_price, 6)} · rank #${engine.open_position.entry_rank}` : 'No open paper position'} />
        <StatCard label="Open PnL" value={`${fmtNumber(engine?.open_position?.unrealized_pnl, 2)} USDC`} hint={engine?.open_position?.mark_price ? `Mark ${fmtNumber(engine.open_position.mark_price, 6)}` : 'No mark'} />
        <StatCard label="Best eligible now" value={engine?.best_asset?.symbol || '—'} hint={engine?.best_asset ? `Score ${fmtNumber(engine.best_asset.momentum_score, 2)} · rank #${engine.best_asset.rank}` : `Needs score > ${MIN_MOMENTUM_SCORE}`} />
        <StatCard label="Recommendation" value={engine?.due_now ? 'Due now' : 'Waiting'} hint={engine?.recommendation || '—'} />
      </div>
      <FoldableTable title="Backend momentum trade log" columns={tradeColumns} rows={engine?.trades || []} empty="No backend paper trades yet" defaultSortKey="created_at" defaultSortDir="desc" paginated initialPageSize={10} />
    </details>

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
