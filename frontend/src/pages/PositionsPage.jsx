import { useCallback, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'

const SHORT_SIDES = new Set(['short', 'sell', 'bear', 'bear_watch'])
const LONG_SIDES = new Set(['long', 'buy', 'bull', 'bull_watch'])
const EMPTY_PNL_SUMMARY = {
  totalPnlPercent: 0,
  averagePnlPercent: 0,
  totalPnlValue: 0,
  count: 0,
  stoppedCount: 0,
  targetedCount: 0,
  winners: 0,
  losers: 0,
  winRatePercent: 0,
  lossRatePercent: 0,
  averageWinPercent: 0,
  averageLossPercent: 0,
  averageWinValue: 0,
  averageLossValue: 0,
  profitFactor: 0,
  expectancyPercent: 0,
  expectancyValue: 0,
  bestTradePercent: 0,
  worstTradePercent: 0,
  slTpCount: 0,
  slTpWinRatePercent: 0,
  slTpLossRatePercent: 0,
  tpHitRatePercent: 0,
  slHitRatePercent: 0,
  slTpProfitFactor: 0,
  slTpExpectancyPercent: 0,
}

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
function pnlPct(row) {
  const entry = numericValue(row?.entry_price), effectivePrice = effectivePnlPrice(row)
  if (entry === null || effectivePrice === null || entry === 0 || !isPnlSide(row)) return null
  return isShort(row) ? ((entry - effectivePrice) / entry) * 100 : ((effectivePrice - entry) / entry) * 100
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
function formatPercent(value, decimals = 2) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—'
  return `${fmtNumber(value, decimals)}%`
}
function formatProfitFactor(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—'
  return fmtNumber(value, 2)
}
function PnlMetricCard({ label, value, hint, toneValue, suffix = '', decimals = 2 }) {
  const display = typeof value === 'string' ? value : `${formatSignedNumber(value, decimals)}${suffix}`
  return <div className="stat-card">
    <div className="stat-label">{label}</div>
    <div className="stat-value" style={pnlTone(toneValue ?? value)}>{display}</div>
    <div className="stat-hint">{hint}</div>
  </div>
}

export default function PositionsPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const { data: positions = [], loading, error } = usePollingQuery(useCallback(() => api.positions('?limit=1000'), []), 10000)
  const { data: pnlSummary = EMPTY_PNL_SUMMARY, loading: summaryLoading, error: summaryError } = usePollingQuery(useCallback(() => api.positionsSummary(), []), 10000)
  const { data: orders = [] } = usePollingQuery(useCallback(() => api.orders('?limit=50'), []), 10000)

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
    <PageHeader title="Positions" subtitle="Paper execution state, orders, fills, PnL and strategy quality metrics" actions={<button className="button" disabled={busy} onClick={runExecutor}>{busy ? 'Executing…' : 'Run executor'}</button>} />
    {message ? <div className="panel info">{message}</div> : null}
    {loading ? <div className="panel">Loading positions…</div> : null}
    {summaryLoading ? <div className="panel">Loading PnL summary…</div> : null}
    {error ? <div className="panel error">{error}</div> : null}
    {summaryError ? <div className="panel error">{summaryError}</div> : null}

    <div className="stats-grid">
      <PnlMetricCard label="Net PnL" value={pnlSummary.totalPnlValue} toneValue={pnlSummary.totalPnlValue} decimals={4} hint={`Total ${formatSignedNumber(pnlSummary.totalPnlPercent, 2)}% · ${pnlSummary.count} positions`} />
      <PnlMetricCard label="Win Rate" value={formatPercent(pnlSummary.winRatePercent, 2)} toneValue={pnlSummary.winRatePercent - pnlSummary.lossRatePercent} hint={`Wins ${pnlSummary.winners}/${pnlSummary.count}`} />
      <PnlMetricCard label="Loss Rate" value={formatPercent(pnlSummary.lossRatePercent, 2)} toneValue={-(pnlSummary.lossRatePercent || 0)} hint={`Losses ${pnlSummary.losers}/${pnlSummary.count}`} />
      <PnlMetricCard label="Profit Factor" value={formatProfitFactor(pnlSummary.profitFactor)} toneValue={(pnlSummary.profitFactor || 0) - 1} hint="Gross profit / gross loss" />
    </div>

    <div className="stats-grid">
      <PnlMetricCard label="Avg Win" value={pnlSummary.averageWinPercent} toneValue={pnlSummary.averageWinPercent} suffix="%" hint={`${formatSignedNumber(pnlSummary.averageWinValue, 4)} avg value`} />
      <PnlMetricCard label="Avg Loss" value={pnlSummary.averageLossPercent} toneValue={pnlSummary.averageLossPercent} suffix="%" hint={`${formatSignedNumber(pnlSummary.averageLossValue, 4)} avg value`} />
      <PnlMetricCard label="Expectancy" value={pnlSummary.expectancyPercent} toneValue={pnlSummary.expectancyPercent} suffix="%" hint={`${formatSignedNumber(pnlSummary.expectancyValue, 4)} per trade`} />
      <PnlMetricCard label="Best / Worst" value={`${formatSignedNumber(pnlSummary.bestTradePercent, 2)}% / ${formatSignedNumber(pnlSummary.worstTradePercent, 2)}%`} toneValue={pnlSummary.totalPnlPercent} hint="Best and worst capped position PnL" />
    </div>

    <div className="stats-grid">
      <PnlMetricCard label="TP Hit Rate" value={formatPercent(pnlSummary.tpHitRatePercent, 2)} toneValue={pnlSummary.tpHitRatePercent} hint={`TP ${pnlSummary.targetedCount}/${pnlSummary.slTpCount}`} />
      <PnlMetricCard label="SL Hit Rate" value={formatPercent(pnlSummary.slHitRatePercent, 2)} toneValue={-(pnlSummary.slHitRatePercent || 0)} hint={`SL ${pnlSummary.stoppedCount}/${pnlSummary.slTpCount}`} />
      <PnlMetricCard label="SL/TP Win Rate" value={formatPercent(pnlSummary.slTpWinRatePercent, 2)} toneValue={pnlSummary.slTpWinRatePercent - pnlSummary.slTpLossRatePercent} hint={`Wins ${pnlSummary.slTpWinners}/${pnlSummary.slTpCount}`} />
      <PnlMetricCard label="SL/TP Model" value={pnlSummary.slTpExpectancyPercent} toneValue={pnlSummary.slTpExpectancyPercent} suffix="%" hint={`PF ${formatProfitFactor(pnlSummary.slTpProfitFactor)} · total ${formatSignedNumber(pnlSummary.slTpTotalPnlPercent, 2)}%`} />
    </div>

    <FoldableTable title="Open positions" columns={positionColumns} rows={positions} empty="No positions yet" paginated initialPageSize={25} pageSizeOptions={[25, 50, 100, 250]} />
    <FoldableTable title="Recent orders" columns={orderColumns} rows={orders} empty="No orders yet" />
  </div>
}
