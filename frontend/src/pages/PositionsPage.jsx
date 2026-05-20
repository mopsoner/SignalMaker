import { useCallback, useMemo, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'

const SHORT_SIDES = new Set(['short', 'sell', 'bear', 'bear_watch'])
const LONG_SIDES = new Set(['long', 'buy', 'bull', 'bull_watch'])

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
function hasTriggeredStop(row) {
  const mark = numericValue(row?.mark_price), stop = numericValue(row?.stop_price)
  if (mark === null || stop === null || !isPnlSide(row)) return false
  return isShort(row) ? mark >= stop : mark <= stop
}
function hasTriggeredTarget(row) {
  const mark = numericValue(row?.mark_price), target = numericValue(row?.target_price)
  if (mark === null || target === null || !isPnlSide(row)) return false
  return isShort(row) ? mark <= target : mark >= target
}
function effectivePnlPrice(row) {
  const mark = numericValue(row?.mark_price), stop = numericValue(row?.stop_price)
  if (mark === null) return null
  return hasTriggeredStop(row) && stop !== null ? stop : mark
}
function effectiveSlTpPnlPrice(row) {
  const mark = numericValue(row?.mark_price)
  const stop = numericValue(row?.stop_price)
  const target = numericValue(row?.target_price)
  if (mark === null) return null
  if (hasTriggeredStop(row) && stop !== null) return stop
  if (hasTriggeredTarget(row) && target !== null) return target
  return mark
}
function pnlFromPrice(row, price) {
  const entry = numericValue(row?.entry_price), qty = numericValue(row?.quantity)
  if (entry === null || price === null || qty === null || !isPnlSide(row)) return null
  return isShort(row) ? (entry - price) * qty : (price - entry) * qty
}
function pnlPctFromPrice(row, price) {
  const entry = numericValue(row?.entry_price)
  if (entry === null || price === null || entry === 0 || !isPnlSide(row)) return null
  return isShort(row) ? ((entry - price) / entry) * 100 : ((price - entry) / entry) * 100
}
function pnlValue(row) {
  return pnlFromPrice(row, effectivePnlPrice(row))
}
function pnlPct(row) {
  return pnlPctFromPrice(row, effectivePnlPrice(row))
}
function slTpPnlValue(row) {
  return pnlFromPrice(row, effectiveSlTpPnlPrice(row))
}
function slTpPnlPct(row) {
  return pnlPctFromPrice(row, effectiveSlTpPnlPrice(row))
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
function calculatePnlSummary(positions) {
  let totalPnlPercent = 0
  let totalPnlValue = 0
  let count = 0
  let stoppedCount = 0
  let winners = 0
  let losers = 0
  let slTpTotalPnlPercent = 0
  let slTpTotalPnlValue = 0
  let slTpCount = 0
  let targetedCount = 0
  let slTpWinners = 0
  let slTpLosers = 0

  for (const row of positions || []) {
    const pct = pnlPct(row)
    const pnl = pnlValue(row)
    if (pct === null || pnl === null) continue

    totalPnlPercent += pct
    totalPnlValue += pnl
    count += 1
    if (pct > 0) winners += 1
    if (pct < 0) losers += 1
    if (hasTriggeredStop(row)) stoppedCount += 1

    const slTpPct = slTpPnlPct(row)
    const slTpPnl = slTpPnlValue(row)
    if (slTpPct === null || slTpPnl === null) continue
    slTpTotalPnlPercent += slTpPct
    slTpTotalPnlValue += slTpPnl
    slTpCount += 1
    if (slTpPct > 0) slTpWinners += 1
    if (slTpPct < 0) slTpLosers += 1
    if (!hasTriggeredStop(row) && hasTriggeredTarget(row)) targetedCount += 1
  }

  return {
    totalPnlPercent,
    averagePnlPercent: count > 0 ? totalPnlPercent / count : 0,
    totalPnlValue,
    count,
    stoppedCount,
    winners,
    losers,
    slTpTotalPnlPercent,
    slTpAveragePnlPercent: slTpCount > 0 ? slTpTotalPnlPercent / slTpCount : 0,
    slTpTotalPnlValue,
    slTpCount,
    targetedCount,
    slTpWinners,
    slTpLosers,
  }
}

export default function PositionsPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const { data: positions = [], loading, error } = usePollingQuery(useCallback(() => api.positions('?limit=1000'), []), 10000)
  const { data: orders = [] } = usePollingQuery(useCallback(() => api.orders('?limit=50'), []), 10000)
  const pnlSummary = useMemo(() => calculatePnlSummary(positions), [positions])

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
    { key: 'entry_price', title: 'Entry', render: (row) => fmtNumber(row.entry_price, 4) },
    { key: 'mark_price', title: 'Mark', render: (row) => fmtNumber(row.mark_price, 4) },
    { key: 'pnl', title: 'PnL', render: (row) => <span style={pnlTone(pnlValue(row))}>{fmtNumber(pnlValue(row), 4)}</span>, sortValue: (row) => pnlValue(row) ?? -999999 },
    { key: 'pnl_pct', title: 'PnL %', render: (row) => <span style={pnlTone(pnlPct(row))}>{fmtNumber(pnlPct(row), 2)}</span>, sortValue: (row) => pnlPct(row) ?? -999999 },
    { key: 'stop_price', title: 'Stop', render: (row) => fmtNumber(row.stop_price, 4) },
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
      <div className="stat-card">
        <div className="stat-label">Total PnL % · all loaded positions</div>
        <div className="stat-value" style={pnlTone(pnlSummary.totalPnlPercent)}>
          {formatSignedNumber(pnlSummary.totalPnlPercent, 2)}%
        </div>
        <div className="stat-hint">
          Sum of capped position PnL % · {pnlSummary.count} positions · stopped {pnlSummary.stoppedCount}/{pnlSummary.count}
        </div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Avg PnL % · all loaded positions</div>
        <div className="stat-value" style={pnlTone(pnlSummary.averagePnlPercent)}>
          {formatSignedNumber(pnlSummary.averagePnlPercent, 2)}%
        </div>
        <div className="stat-hint">
          Total PnL % / positions · wins {pnlSummary.winners}/{pnlSummary.count} · losses {pnlSummary.losers}/{pnlSummary.count}
        </div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Total PnL % · SL/TP model</div>
        <div className="stat-value" style={pnlTone(pnlSummary.slTpTotalPnlPercent)}>
          {formatSignedNumber(pnlSummary.slTpTotalPnlPercent, 2)}%
        </div>
        <div className="stat-hint">
          Avg {formatSignedNumber(pnlSummary.slTpAveragePnlPercent, 2)}% · TP {pnlSummary.targetedCount}/{pnlSummary.slTpCount} · SL {pnlSummary.stoppedCount}/{pnlSummary.slTpCount} · wins {pnlSummary.slTpWinners}/{pnlSummary.slTpCount}
        </div>
      </div>
    </div>
    <FoldableTable title="Open positions" columns={positionColumns} rows={positions} empty="No positions yet" paginated initialPageSize={25} pageSizeOptions={[25, 50, 100, 250]} />
    <FoldableTable title="Recent orders" columns={orderColumns} rows={orders} empty="No orders yet" />
  </div>
}
