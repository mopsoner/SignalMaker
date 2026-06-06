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

function actionTone(action) {
  const normalized = String(action || '').toUpperCase()
  if (normalized.startsWith('BUY')) return '#22c55e'
  if (normalized.startsWith('SELL')) return '#f97316'
  if (normalized.includes('ROTATE')) return '#38bdf8'
  if (normalized.includes('HOLD') || normalized.includes('WAIT')) return '#94a3b8'
  return '#a78bfa'
}

function shortActionLabel(action) {
  const normalized = String(action || '').toUpperCase()
  if (normalized.startsWith('BUY_AFTER')) return 'BUY ↻'
  if (normalized.startsWith('BUY')) return 'BUY'
  if (normalized.startsWith('SELL')) return 'SELL'
  if (normalized.includes('ROTATE')) return 'ROTATE'
  if (normalized.includes('HOLD')) return 'HOLD'
  if (normalized.includes('WAIT')) return 'WAIT'
  return normalized || 'EVENT'
}

function isPerformedMomentumAction(action) {
  const normalized = String(action || '').toUpperCase()
  return normalized.startsWith('BUY') || normalized.startsWith('SELL') || normalized.includes('ROTATE') || normalized.includes('HOLD') || normalized.includes('WAIT')
}

function buildMomentumTimeline(engine) {
  if (!engine) return []

  const trades = [...(engine?.trades || [])]
    .filter((trade) => isPerformedMomentumAction(trade.action))
    .sort((a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0))
  let realizedPnl = 0
  const points = [{ id: 'start', label: 'Start', created_at: null, profit: 0, delta: 0, action: 'START' }]

  trades.forEach((trade, index) => {
    realizedPnl += Number(trade.pnl || 0)
    points.push({
      id: trade.trade_id || `${trade.created_at || 'trade'}-${index}`,
      label: shortActionLabel(trade.action),
      created_at: trade.created_at,
      profit: realizedPnl,
      delta: Number(trade.pnl || 0),
      pnl_pct: trade.pnl_pct,
      action: trade.action,
      symbol: trade.symbol,
      price: trade.price,
      price_source: trade.price_source,
    })
  })

  const currentPnl = Number(engine.total_pnl ?? realizedPnl)
  const lastProfit = points[points.length - 1]?.profit
  if (Math.abs(currentPnl - Number(lastProfit || 0)) > 0.00000001 || engine.open_position) {
    points.push({
      id: 'mark-to-market',
      label: engine.open_position ? 'MARK' : 'PNL',
      created_at: engine.last_check_at || new Date().toISOString(),
      profit: currentPnl,
      delta: currentPnl - realizedPnl,
      action: engine.open_position ? 'MARK_TO_MARKET' : 'PNL_UPDATE',
      symbol: engine.open_position?.symbol,
      price: engine.open_position?.mark_price,
      price_source: engine.open_position?.mark_price_source,
    })
  }

  return points
}

function MomentumTradeChart({ points }) {
  if (!points.length) return <div className="panel">Loading momentum profit chart…</div>

  const width = 720
  const height = 260
  const pad = 34
  const profits = points.map((point) => Number(point.profit || 0))
  const minProfit = Math.min(...profits, 0)
  const maxProfit = Math.max(...profits, 0)
  const span = Math.max(maxProfit - minProfit, 1)
  const xFor = (index) => pad + (points.length === 1 ? 0 : (index / (points.length - 1)) * (width - pad * 2))
  const yFor = (profit) => height - pad - ((Number(profit || 0) - minProfit) / span) * (height - pad * 2)
  const path = points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${xFor(index).toFixed(2)} ${yFor(point.profit).toFixed(2)}`).join(' ')
  const zeroY = yFor(0)
  const last = points[points.length - 1]
  const performedCount = points.filter((point) => point.action !== 'START' && point.action !== 'MARK_TO_MARKET' && point.action !== 'PNL_UPDATE').length

  return <div style={{ display: 'grid', gap: 12 }}>
    <div className="stats-grid">
      <StatCard label="Latest PnL" value={`${fmtNumber(last.profit, 2)} USDC`} hint={last.symbol ? `${last.label} ${last.symbol}` : last.label} />
      <StatCard label="Profit range" value={`${fmtNumber(minProfit, 2)} → ${fmtNumber(maxProfit, 2)}`} hint="USDC cumulative profit" />
      <StatCard label="Actions" value={performedCount} hint="BUY / SELL / ROTATE / HOLD only" />
    </div>
    <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Momentum profit evolution with buy sell rotate actions" style={{ width: '100%', minHeight: 260, background: 'rgba(15, 23, 42, 0.35)', borderRadius: 16 }}>
      <line x1={pad} y1={pad} x2={pad} y2={height - pad} stroke="rgba(148, 163, 184, 0.35)" />
      <line x1={pad} y1={height - pad} x2={width - pad} y2={height - pad} stroke="rgba(148, 163, 184, 0.35)" />
      <line x1={pad} y1={zeroY} x2={width - pad} y2={zeroY} stroke="rgba(148, 163, 184, 0.25)" strokeDasharray="5 5" />
      <text x={pad} y={20} fill="var(--muted)" fontSize="12">{fmtNumber(maxProfit, 2)} USDC</text>
      <text x={pad} y={height - 8} fill="var(--muted)" fontSize="12">{fmtNumber(minProfit, 2)} USDC</text>
      <path d={path} fill="none" stroke="#38bdf8" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
      {points.map((point, index) => {
        const x = xFor(index)
        const y = yFor(point.profit)
        const tone = actionTone(point.action)
        return <g key={point.id || index}>
          <circle cx={x} cy={y} r={index === points.length - 1 ? 6 : 4} fill={tone} stroke="rgba(15, 23, 42, 0.9)" strokeWidth="2" />
          {index > 0 ? <text x={Math.min(x + 8, width - 100)} y={Math.max(y - 8, 14)} fill={tone} fontSize="11" fontWeight="700">{point.label}{point.symbol ? ` ${point.symbol}` : ''}</text> : null}
          {index > 0 && point.delta ? <text x={Math.min(x + 8, width - 100)} y={Math.min(y + 18, height - 8)} fill="var(--muted)" fontSize="10">PnL {fmtNumber(point.delta, 2)}</text> : null}
        </g>
      })}
    </svg>
    <div className="market-toolbar-hint">Profit chart uses performed momentum actions only; passive checks without trades are excluded.</div>
  </div>
}

export default function MomentumPage() {
  const [filter, setFilter] = useState('all')
  const [cadenceHours, setCadenceHours] = useState(DEFAULT_CADENCE_HOURS)
  const [engineOverride, setEngineOverride] = useState(null)
  const [engineActionError, setEngineActionError] = useState(null)
  const { data: rows = [], loading, error } = usePollingQuery(useCallback(() => fetchMomentum(300), []), 30000)
  const { data: engineData, loading: engineLoading, error: engineError, refresh: refreshEngine } = usePollingQuery(useCallback(() => fetchMomentumEngine(cadenceHours), [cadenceHours]), 30000)
  const engine = engineOverride || engineData
  const engineTimeline = useMemo(() => buildMomentumTimeline(engine), [engine])

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
    { key: 'pnl_pct', title: 'PnL %', render: (row) => row.pnl_pct == null ? '—' : `${fmtNumber(row.pnl_pct, 2)}%`, sortValue: (row) => Number(row.pnl_pct || 0) },
    { key: 'price_source', title: 'Price source', render: (row) => row.price_source || '—', sortValue: (row) => row.price_source || '' },
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

    <section className="panel" style={{ display: 'grid', gap: 12 }}>
      <h2 style={{ margin: 0 }}>Momentum profit evolution</h2>
      {engineLoading ? <div className="market-toolbar-hint">Loading momentum engine…</div> : null}
      <MomentumTradeChart points={engineTimeline} />
    </section>

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
        <StatCard label="Open PnL" value={`${fmtNumber(engine?.open_position?.unrealized_pnl, 2)} USDC`} hint={engine?.open_position?.mark_price ? `Mark ${fmtNumber(engine.open_position.mark_price, 6)} · ${engine.open_position.mark_price_source || 'price source unknown'}` : 'No mark'} />
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
