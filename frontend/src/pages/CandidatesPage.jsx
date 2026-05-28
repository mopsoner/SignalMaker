import { useCallback, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber, stageBadgeClass } from '../lib/format'

function safeText(value) {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value)
  if (typeof value === 'object') {
    const type = value.type || value.source || value.name || value.reason
    const level = value.level !== null && value.level !== undefined ? ` @ ${fmtNumber(value.level, 4)}` : ''
    if (type) return `${type}${level}`
    try { return JSON.stringify(value) } catch (_) { return 'object' }
  }
  return String(value)
}
function boolText(value) { return value === true ? 'true' : value === false ? 'false' : '' }
function payload(row) { return row?.payload || {} }
function gate(row) { return payload(row)?.hierarchy_gate || {} }
function confirmation(row) { return payload(row)?.confirmation_model || {} }
function execution(row) { return payload(row)?.execution_trigger || payload(row)?.execution_trigger_5m || {} }
function oneHourDecision(row) { return payload(row)?.one_hour_decision || {} }
function plannerReason(row) { return payload(row)?.planner_candidate_reason || payload(row)?.confirm_block_reason || row?.notes || '—' }
function summarizeContext(value) {
  if (!value || typeof value !== 'object') return '—'
  const type = value.type || value.source || '—'
  const level = value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'
  return `${type} @ ${level}`
}
function setupState(row) {
  const p = payload(row)
  const state = p.state
  if (state === 'spring_watch') return 'spring_watch'
  if (state === 'utad_watch') return 'utad_watch'
  return safeText(p.bias)
}
function confirmLabel(row) {
  const p = payload(row)
  const c = confirmation(row)
  const e = execution(row)
  const g = gate(row)
  const parts = [
    c.confirmed_by_15m ? '15m_confirmed' : '',
    e.mss_bull || p.mss_bull ? 'mss_bull' : '',
    e.bos_bull || p.bos_bull ? 'bos_bull' : '',
    e.mss_bear || p.mss_bear ? 'mss_bear' : '',
    e.bos_bear || p.bos_bear ? 'bos_bear' : '',
    e.aligned || g.execution_15m_aligned ? '15m_aligned' : '',
    e.confirm_source || p.confirm_source || c.confirmation_source || p.trigger || row.notes,
  ].filter(Boolean)
  return safeText(parts.join(' · ') || 'wait')
}
function stopSource(row) {
  const p = payload(row)
  return safeText(p?.trade?.stop_source || p?.trade?.stop || p?.stop_source || p?.planner?.stop_source || p?.planner_candidate?.stop_source)
}
function targetSource(row) {
  const p = payload(row)
  return safeText(p?.trade?.target_source || p?.target_source || p?.projected_target || row?.execution_target)
}
function csvValue(value) {
  const text = safeText(value).replaceAll('—', '')
  return `"${text.replaceAll('"', '""')}"`
}
function exportCandidatesCsv(rows) {
  const headers = [
    'symbol', 'side', 'stage', 'status', 'setup', 'confirm',
    'confirmation_path', 'confirmed_by_1h', 'confirmed_by_15m', 'confirmation_source',
    'execution_15m_alignment', 'execution_15m_aligned', 'confirm_15m_seen',
    'mss_bull', 'bos_bull', 'mss_bear', 'bos_bear',
    'execution_mss_bull', 'execution_bos_bull', 'execution_mss_bear', 'execution_bos_bear',
    'execution_trigger', 'execution_seen', 'execution_valid', 'execution_confirm_source',
    'one_hour_valid', 'one_hour_source', 'one_hour_sweep_seen', 'one_hour_reclaim_seen', 'one_hour_mss_seen', 'one_hour_bos_seen',
    'planner_candidate_reason', 'trade_status', 'pipeline_trade', 'macro', 'entry_context', 'stop_source', 'target_source',
    'score', 'entry_price', 'stop_price', 'target_price', 'rr_ratio', 'created_at',
  ]
  const csvRows = rows.map((row) => {
    const p = payload(row), c = confirmation(row), e = execution(row), g = gate(row), oh = oneHourDecision(row)
    return [
      row.symbol, row.side, row.stage, row.status, setupState(row), confirmLabel(row),
      g.confirmation_path, c.confirmed_by_1h, c.confirmed_by_15m, c.confirmation_source,
      g.execution_15m_alignment || c.fifteen_min_alignment || e.alignment_status, g.execution_15m_aligned, g.confirm_15m_seen,
      p.mss_bull, p.bos_bull, p.mss_bear, p.bos_bear,
      e.mss_bull, e.bos_bull, e.mss_bear, e.bos_bear,
      e.trigger, e.seen, e.valid, e.confirm_source,
      oh.valid, oh.source, oh.sweep_seen, oh.reclaim_seen, oh.mss_seen, oh.bos_seen,
      plannerReason(row), p?.trade?.status, p?.pipeline?.trade,
      summarizeContext(p?.macro_liquidity_context || row?.liquidity_context), summarizeContext(p?.entry_liquidity_context),
      stopSource(row), targetSource(row), row.score, row.entry_price, row.stop_price, row.target_price, row.rr_ratio, row.created_at,
    ].map(csvValue).join(',')
  })
  const csv = [headers.join(','), ...csvRows].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = `signalmaker-candidates-${new Date().toISOString().slice(0, 10)}.csv`
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}
export default function CandidatesPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const loadCandidates = useCallback(() => api.candidates('?limit=1000'), [])
  const { data: rows = [], loading, error } = usePollingQuery(loadCandidates, 10000)
  async function runPipeline() {
    setBusy(true); setMessage('')
    try {
      const result = await api.runPipeline(5)
      setMessage(`Pipeline OK · scanned ${result.symbols_scanned} · candidates ${result.candidates_created}`)
    } catch (err) { setMessage(err.message || String(err)) }
    finally { setBusy(false) }
  }
  const columns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'side', title: 'Side' },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{safeText(row.stage)}</span> },
    { key: 'status', title: 'Status' },
    { key: 'setup', title: 'Setup', render: setupState, sortValue: setupState },
    { key: 'confirm', title: 'Confirm', render: confirmLabel, sortValue: confirmLabel },
    { key: 'confirmation_path', title: 'Path', render: (row) => safeText(gate(row).confirmation_path), sortValue: (row) => safeText(gate(row).confirmation_path) },
    { key: 'confirmed_by_15m', title: '15m?', render: (row) => boolText(confirmation(row).confirmed_by_15m || gate(row).execution_15m_aligned || execution(row).aligned), sortValue: (row) => confirmation(row).confirmed_by_15m ? 1 : 0 },
    { key: 'bos_mss', title: 'BOS/MSS', render: (row) => [payload(row).mss_bull || execution(row).mss_bull ? 'mss_bull' : '', payload(row).bos_bull || execution(row).bos_bull ? 'bos_bull' : '', payload(row).mss_bear || execution(row).mss_bear ? 'mss_bear' : '', payload(row).bos_bear || execution(row).bos_bear ? 'bos_bear' : ''].filter(Boolean).join(' · ') || '—' },
    { key: 'reason', title: 'Reason', render: plannerReason, sortValue: plannerReason },
    { key: 'macro', title: 'Macro', render: (row) => summarizeContext(payload(row)?.macro_liquidity_context || row?.liquidity_context), sortValue: (row) => (payload(row)?.macro_liquidity_context || row?.liquidity_context)?.level ?? -1 },
    { key: 'entry', title: 'Entry context', render: (row) => summarizeContext(payload(row)?.entry_liquidity_context), sortValue: (row) => payload(row)?.entry_liquidity_context?.level ?? -1 },
    { key: 'stop_source', title: 'Stop source', render: stopSource, sortValue: stopSource },
    { key: 'target_source', title: 'Target source', render: targetSource, sortValue: targetSource },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score, 2) },
    { key: 'entry_price', title: 'Entry', render: (row) => fmtNumber(row.entry_price, 4) },
    { key: 'stop_price', title: 'Stop', render: (row) => fmtNumber(row.stop_price, 4) },
    { key: 'target_price', title: 'Target', render: (row) => fmtNumber(row.target_price, 4) },
    { key: 'rr_ratio', title: 'RR', render: (row) => fmtNumber(row.rr_ratio, 2) },
    { key: 'created_at', title: 'Created', render: (row) => fmtDate(row.created_at) },
  ]
  return <div className="page-stack">
    <PageHeader title="Trade Candidates" subtitle="Planner outputs with explicit 15m confirmation diagnostics." actions={<div className="button-row"><button className="button secondary" disabled={!rows.length} onClick={() => exportCandidatesCsv(rows)}>Export CSV</button><button className="button" disabled={busy} onClick={runPipeline}>{busy ? 'Running…' : 'Run pipeline'}</button></div>} />
    {message ? <div className="panel info">{message}</div> : null}
    {loading ? <div className="panel">Loading candidates…</div> : null}
    {error ? <div className="panel error">{error}</div> : null}
    <FoldableTable title="Open and executed candidates" columns={columns} rows={rows} empty="No trade candidates yet" paginated initialPageSize={25} pageSizeOptions={[25, 50, 100, 250]} />
  </div>
}
