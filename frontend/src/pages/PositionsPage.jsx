import { useCallback, useMemo, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'

const SHORT_SIDES = new Set(['short', 'sell', 'bear', 'bear_watch'])
const LONG_SIDES = new Set(['long', 'buy', 'bull', 'bull_watch'])
const KNOWN_QUOTE_ASSETS = [
  'USDC', 'USDT', 'FDUSD', 'TUSD', 'BUSD', 'DAI', 'USD',
  'BTC', 'ETH', 'BNB', 'EUR', 'GBP', 'TRY', 'BRL', 'AUD', 'JPY',
]

function normalizedSide(side) {
  const value = String(side || '').toLowerCase()
  if (SHORT_SIDES.has(value)) return 'short'
  if (LONG_SIDES.has(value)) return 'long'
  return value
}
function isShort(row) { return normalizedSide(row?.side) === 'short' }
function isPnlSide(row) { return ['long', 'short'].includes(normalizedSide(row?.side)) }
function numericValue(value) {
  const number = Number(value)
  return Number.isFinite(number) ? number : null
}
function quoteAsset(row) {
  const explicitQuote = row?.quote_asset || row?.quoteAsset || row?.meta?.quote_asset || row?.meta?.quoteAsset
  if (explicitQuote) return String(explicitQuote).toUpperCase()

  const symbol = String(row?.symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '')
  const knownQuote = KNOWN_QUOTE_ASSETS.find((asset) => symbol.endsWith(asset))
  return knownQuote || 'QUOTE'
}
function hasTriggeredStop(row) {
  const mark = numericValue(row?.mark_price), stop = numericValue(row?.stop_price)
  if (mark === null || stop === null || !isPnlSide(row)) return false
  return isShort(row) ? mark >= stop : mark <= stop
}
function effectivePnlPrice(row) {
  const mark = numericValue(row?.mark_price), stop = numericValue(row?.stop_price)
  if (mark === null) return null
  return hasTriggeredStop(row) && stop !== null ? stop : mark
}
function pnlValue(row) {
  const entry = numericValue(row?.entry_price), effectivePrice = effectivePnlPrice(row), qty = numericValue(row?.quantity)
  if (entry === null || effectivePrice === null || qty === null || !isPnlSide(row)) return null
  return isShort(row) ? (entry - effectivePrice) * qty : (effectivePrice - entry) * qty
}
function entryExposureQuote(row) {
  const entry = numericValue(row?.entry_price), qty = numericValue(row?.quantity)
  if (entry === null || qty === null || entry === 0 || !isPnlSide(row)) return null
  return Math.abs(entry * qty)
}
function stopRiskQuote(row) {
  const entry = numericValue(row?.entry_price), stop = numericValue(row?.stop_price), qty = numericValue(row?.quantity)
  if (entry === null || stop === null || qty === null || entry === 0 || stop === entry || !isPnlSide(row)) return null
  return Math.abs(entry - stop) * Math.abs(qty)
}
function pnlRiskRatio(row) {
  const pnl = pnlValue(row), risk = stopRiskQuote(row)
  if (pnl === null || risk === null || risk === 0) return null
  return pnl / risk
}
function pnlPct(row) {
  const entry = numericValue(row?.entry_price), effectivePrice = effectivePnlPrice(row)
  if (entry === null || effectivePrice === null || entry === 0 || !isPnlSide(row)) return null
  return isShort(row) ? ((entry - effectivePrice) / entry) * 100 : ((effectivePrice - entry) / entry) * 100
}
function rawPnlPct(row) {
  const entry = numericValue(row?.entry_price), mark = numericValue(row?.mark_price)
  if (entry === null || mark === null || entry === 0 || !isPnlSide(row)) return null
  return isShort(row) ? ((entry - mark) / entry) * 100 : ((mark - entry) / entry) * 100
}
function distanceToStopPct(row) {
  const entry = Number(row?.entry_price), stop = Number(row?.stop_price)
  if (!Number.isFinite(entry) || !Number.isFinite(stop) || entry === 0) return null
  return isShort(row) ? ((stop - entry) / entry) * 100 : ((entry - stop) / entry) * 100
}
function distanceToTargetPct(row) {
  const entry = Number(row?.entry_price), target = Number(row?.target_price)
  if (!Number.isFinite(entry) || !Number.isFinite(target) || entry === 0) return null
  return isShort(row) ? ((entry - target) / entry) * 100 : ((target - entry) / entry) * 100
}
function pnlTone(value) {
  if (value === null || value === undefined) return {}
  if (value > 0) return { color: 'var(--green)', fontWeight: 700 }
  if (value < 0) return { color: 'var(--red)', fontWeight: 700 }
  return { fontWeight: 700 }
}
function formatSignedNumber(value, decimals = 2) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—'
  const number = Number(value)
  const sign = number > 0 ? '+' : ''
  return `${sign}${fmtNumber(number, decimals)}`
}
function calculatePnlSummaryByQuoteAsset(positions) {
  const summaryByQuote = new Map()

  for (const row of positions || []) {
    const pnl = pnlValue(row)
    const exposure = entryExposureQuote(row)
    if (pnl === null || exposure === null) continue

    const risk = stopRiskQuote(row)
    const quote = quoteAsset(row)
    const current = summaryByQuote.get(quote) || {
      quoteAsset: quote,
      totalPnlQuote: 0,
      totalEntryValueQuote: 0,
      totalStopRiskQuote: 0,
      globalPnlPercent: 0,
      globalRiskRatio: null,
      count: 0,
      riskCount: 0,
      stoppedCount: 0,
    }

    current.totalPnlQuote += pnl
    current.totalEntryValueQuote += exposure
    current.count += 1
    if (risk !== null) {
      current.totalStopRiskQuote += risk
      current.riskCount += 1
    }
    if (hasTriggeredStop(row)) current.stoppedCount += 1
    summaryByQuote.set(quote, current)
  }

  return Array.from(summaryByQuote.values())
    .map((item) => ({
      ...item,
      globalPnlPercent: item.totalEntryValueQuote > 0
        ? (item.totalPnlQuote / item.totalEntryValueQuote) * 100
        : 0,
      globalRiskRatio: item.totalStopRiskQuote > 0
        ? item.totalPnlQuote / item.totalStopRiskQuote
        : null,
    }))
    .sort((a, b) => Math.abs(b.totalPnlQuote) - Math.abs(a.totalPnlQuote))
}

export default function PositionsPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const { data: positions = [], loading, error } = usePollingQuery(useCallback(() => api.positions('?limit=100'), []), 10000)
  const { data: orders = [] } = usePollingQuery(useCallback(() => api.orders('?limit=50'), []), 10000)
  const pnlSummary = useMemo(() => calculatePnlSummaryByQuoteAsset(positions), [positions])

  async function runExecutor() {
    setBusy(true); setMessage('')
    try {
      const result = await api.runExecutor(10, 1)
      setMessage(`Executor OK · executed ${result.executed.length} · skipped ${result.skipped.length}`)
    } catch (err) { setMessage(err.message || String(err)) }
    finally { setBusy(false) }
  }

  const positionColumns = [
    { key: 'symbol', title: 'Symbol' }, { key: 'side', title: 'Side' }, { key: 'status', title: 'Status' },
    { key: 'quantity', title: 'Qty', render: (row) => fmtNumber(row.quantity, 2) },
    { key: 'quote_asset', title: 'Quote', render: (row) => quoteAsset(row) },
    { key: 'entry_price', title: 'Entry', render: (row) => fmtNumber(row.entry_price, 4) },
    { key: 'mark_price', title: 'Now', render: (row) => fmtNumber(row.mark_price, 4) },
    { key: 'effective_price', title: 'PnL price', render: (row) => {
      const effectivePrice = effectivePnlPrice(row)
      return <span style={hasTriggeredStop(row) ? { color: 'var(--orange)', fontWeight: 700 } : {}}>{effectivePrice === null ? '—' : fmtNumber(effectivePrice, 4)}{hasTriggeredStop(row) ? ' stop' : ''}</span>
    }, sortValue: (row) => effectivePnlPrice(row) ?? -999999 },
    { key: 'pnl', title: 'PnL quote', render: (row) => {
      const pnl = pnlValue(row)
      return <span style={pnlTone(pnl)}>{pnl === null ? '—' : `${formatSignedNumber(pnl, 4)} ${quoteAsset(row)}`}</span>
    }, sortValue: (row) => pnlValue(row) ?? -999999 },
    { key: 'pnl_pct', title: 'PnL %', render: (row) => {
      const pct = pnlPct(row)
      return <span style={pnlTone(pct)}>{pct === null ? '—' : `${formatSignedNumber(pct, 2)}%`}</span>
    }, sortValue: (row) => pnlPct(row) ?? -999999 },
    { key: 'raw_pnl_pct', title: 'Raw %', render: (row) => {
      const pct = rawPnlPct(row)
      return <span style={pnlTone(pct)}>{pct === null ? '—' : `${formatSignedNumber(pct, 2)}%`}</span>
    }, sortValue: (row) => rawPnlPct(row) ?? -999999 },
    { key: 'stop_price', title: 'Stop', render: (row) => fmtNumber(row.stop_price, 4) },
    { key: 'stop_risk', title: 'Risk @ stop', render: (row) => {
      const risk = stopRiskQuote(row)
      return risk === null ? '—' : `${fmtNumber(risk, 4)} ${quoteAsset(row)}`
    }, sortValue: (row) => stopRiskQuote(row) ?? -999999 },
    { key: 'pnl_r', title: 'PnL / Risk', render: (row) => {
      const ratio = pnlRiskRatio(row)
      return <span style={pnlTone(ratio)}>{ratio === null ? '—' : `${formatSignedNumber(ratio, 2)}R`}</span>
    }, sortValue: (row) => pnlRiskRatio(row) ?? -999999 },
    { key: 'dist_stop', title: 'Dist stop %', render: (row) => fmtNumber(distanceToStopPct(row), 2), sortValue: (row) => distanceToStopPct(row) ?? -999999 },
    { key: 'target_price', title: 'Target', render: (row) => fmtNumber(row.target_price, 4) },
    { key: 'dist_target', title: 'Dist target %', render: (row) => fmtNumber(distanceToTargetPct(row), 2), sortValue: (row) => distanceToTargetPct(row) ?? -999999 },
    { key: 'opened_at', title: 'Opened', render: (row) => fmtDate(row.opened_at) },
  ]
  const orderColumns = [
    { key: 'symbol', title: 'Symbol' }, { key: 'side', title: 'Side' }, { key: 'order_type', title: 'Type' }, { key: 'status', title: 'Status' },
    { key: 'quantity', title: 'Qty', render: (row) => fmtNumber(row.quantity, 2) },
    { key: 'filled_price', title: 'Filled', render: (row) => fmtNumber(row.filled_price, 4) },
    { key: 'created_at', title: 'Created', render: (row) => fmtDate(row.created_at) },
  ]

  return <div className="page-stack">
    <PageHeader title="Positions" subtitle="Paper execution state, orders, fills, PnL and stop/target distances" actions={<button className="button" disabled={busy} onClick={runExecutor}>{busy ? 'Executing…' : 'Run executor'}</button>} />
    {message ? <div className="panel info">{message}</div> : null}
    {loading ? <div className="panel">Loading positions…</div> : null}
    {error ? <div className="panel error">{error}</div> : null}
    <div className="stats-grid">
      {pnlSummary.length ? pnlSummary.map((item) => (
        <div className="stat-card" key={item.quoteAsset}>
          <div className="stat-label">Global PnL · {item.quoteAsset}</div>
          <div className="stat-value" style={pnlTone(item.totalPnlQuote)}>
            {formatSignedNumber(item.totalPnlQuote, 4)} {item.quoteAsset}
          </div>
          <div className="stat-hint">
            {formatSignedNumber(item.globalPnlPercent, 2)}% · {item.globalRiskRatio === null ? 'risk ratio —' : `${formatSignedNumber(item.globalRiskRatio, 2)}R`} · stopped {item.stoppedCount}/{item.count} · risk {fmtNumber(item.totalStopRiskQuote, 2)} {item.quoteAsset} · exposure {fmtNumber(item.totalEntryValueQuote, 2)} {item.quoteAsset} · {item.riskCount}/{item.count} stop{item.count > 1 ? 's' : ''}
          </div>
        </div>
      )) : (
        <div className="stat-card">
          <div className="stat-label">Global PnL</div>
          <div className="stat-value">—</div>
          <div className="stat-hint">No valid entry/now positions yet</div>
        </div>
      )}
    </div>
    <FoldableTable title="Open positions" columns={positionColumns} rows={positions} empty="No positions yet" />
    <FoldableTable title="Recent orders" columns={orderColumns} rows={orders} empty="No orders yet" />
  </div>
}
