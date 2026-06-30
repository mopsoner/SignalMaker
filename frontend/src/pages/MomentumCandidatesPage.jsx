import { useCallback } from 'react'
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

function boolText(value) { return value === true ? 'true' : value === false ? 'false' : '—' }
function payload(row) { return row?.payload || {} }
function momentumAsset(row) { return payload(row)?.momentum_asset || {} }
function tradePlan(row) { return payload(row)?.planner_trade_plan || payload(row)?.trade || {} }

function summarizeContext(value) {
  if (!value || typeof value !== 'object') return '—'
  const type = value.type || value.source || '—'
  const level = value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'
  return `${type} @ ${level}`
}

function momentumLabel(row) {
  const asset = momentumAsset(row)
  const parts = [
    asset.entry_status || 'ready',
    asset.classification || payload(row).classification,
    asset.structure_15m_status ? `15m ${asset.structure_15m_status}` : '',
  ].filter(Boolean)
  return safeText(parts.join(' · '))
}

function stopSource(row) {
  const p = payload(row)
  return safeText(p?.trade?.stop_source || p?.planner_trade_plan?.stop_source || p?.stop_source)
}

function targetSource(row) {
  const p = payload(row)
  return safeText(p?.trade?.target_source || p?.planner_trade_plan?.target_source || p?.target_source || row?.execution_target)
}

function csvValue(value) {
  const text = safeText(value).replaceAll('—', '')
  return `"${text.replaceAll('"', '""')}"`
}

function exportMomentumCandidatesCsv(rows) {
  const headers = [
    'symbol', 'side', 'stage', 'status', 'momentum_label', 'rank', 'classification',
    'momentum_score', 'rsi_1h', 'rsi_15m', 'structure_15m_status', 'structure_15m_bias',
    'wyckoff_context_available', 'entry_context', 'target_context', 'stop_source', 'target_source',
    'entry_price', 'stop_price', 'target_price', 'rr_ratio', 'notes', 'created_at',
  ]
  const csvRows = rows.map((row) => {
    const p = payload(row), asset = momentumAsset(row)
    return [
      row.symbol, row.side, row.stage, row.status, momentumLabel(row), asset.rank || p.momentum_rank,
      asset.classification || p.classification, row.score ?? p.momentum_score, asset.rsi_1h ?? p.rsi_1h,
      asset.rsi_15m ?? p.rsi_15m, asset.structure_15m_status, asset.structure_15m_bias,
      p.wyckoff_context_available, summarizeContext(p.entry_liquidity_context || row.liquidity_context),
      summarizeContext(row.execution_target || p.execution_target), stopSource(row), targetSource(row),
      row.entry_price, row.stop_price, row.target_price, row.rr_ratio, row.notes, row.created_at,
    ].map(csvValue).join(',')
  })
  const csv = [headers.join(','), ...csvRows].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = `signalmaker-momentum-candidates-${new Date().toISOString().slice(0, 10)}.csv`
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

export default function MomentumCandidatesPage() {
  const loadCandidates = useCallback(() => api.momentumCandidates('?limit=1000'), [])
  const { data: rows = [], loading, error } = usePollingQuery(loadCandidates, 5 * 60 * 1000)

  const columns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'side', title: 'Side' },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{safeText(row.stage)}</span> },
    { key: 'status', title: 'Status' },
    { key: 'momentum', title: 'Momentum setup', render: momentumLabel, sortValue: momentumLabel },
    { key: 'rank', title: 'Rank', render: (row) => safeText(momentumAsset(row).rank || payload(row).momentum_rank), sortValue: (row) => momentumAsset(row).rank || payload(row).momentum_rank || 9999 },
    { key: 'classification', title: 'Class', render: (row) => safeText(momentumAsset(row).classification || payload(row).classification), sortValue: (row) => momentumAsset(row).classification || payload(row).classification || '' },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(row.score ?? payload(row).momentum_score, 2) },
    { key: 'rsi_1h', title: 'RSI 1h', render: (row) => fmtNumber(momentumAsset(row).rsi_1h ?? payload(row).rsi_1h, 2) },
    { key: 'structure', title: '15m structure', render: (row) => safeText(momentumAsset(row).structure_reason || momentumAsset(row).structure_15m_status), sortValue: (row) => momentumAsset(row).structure_15m_status || '' },
    { key: 'wyckoff', title: 'Wyckoff context?', render: (row) => boolText(payload(row).wyckoff_context_available), sortValue: (row) => payload(row).wyckoff_context_available ? 1 : 0 },
    { key: 'entry_context', title: 'Entry context', render: (row) => summarizeContext(payload(row).entry_liquidity_context || row.liquidity_context), sortValue: (row) => (payload(row).entry_liquidity_context || row.liquidity_context)?.level ?? -1 },
    { key: 'target_context', title: 'Target context', render: (row) => summarizeContext(row.execution_target || payload(row).execution_target), sortValue: (row) => (row.execution_target || payload(row).execution_target)?.level ?? -1 },
    { key: 'stop_source', title: 'Stop source', render: stopSource, sortValue: stopSource },
    { key: 'target_source', title: 'Target source', render: targetSource, sortValue: targetSource },
    { key: 'entry_price', title: 'Entry', render: (row) => fmtNumber(row.entry_price ?? tradePlan(row).entry, 4) },
    { key: 'stop_price', title: 'Stop', render: (row) => fmtNumber(row.stop_price ?? tradePlan(row).stop, 4) },
    { key: 'target_price', title: 'Target', render: (row) => fmtNumber(row.target_price ?? tradePlan(row).target, 4) },
    { key: 'rr_ratio', title: 'RR', render: (row) => fmtNumber(row.rr_ratio ?? tradePlan(row).rr_ratio, 2) },
    { key: 'notes', title: 'Notes', render: (row) => safeText(row.notes) },
    { key: 'created_at', title: 'Created', render: (row) => fmtDate(row.created_at) },
  ]

  return <div className="page-stack">
    <PageHeader title="Momentum Trade Candidates" subtitle="Momentum-ready assets converted into trade candidates with Wyckoff/SMC stop, target and RR context." actions={<button className="button secondary" disabled={!rows.length} onClick={() => exportMomentumCandidatesCsv(rows)}>Export CSV</button>} />
    {loading ? <div className="panel">Loading momentum candidates…</div> : null}
    {error ? <div className="panel error">{error}</div> : null}
    <FoldableTable title="Momentum-ready trade candidates" columns={columns} rows={rows} empty="No momentum trade candidates yet" paginated initialPageSize={25} pageSizeOptions={[25, 50, 100, 250]} />
  </div>
}
