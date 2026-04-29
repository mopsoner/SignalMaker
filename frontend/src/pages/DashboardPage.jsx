import { useCallback, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import DataTable from '../components/DataTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber, stageBadgeClass } from '../lib/format'

function summarizeContext(value) {
  if (!value || typeof value !== 'object') return '—'
  const type = value.type || '—'
  const level = value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'
  return `${type} @ ${level}`
}

function stateContext(row, key) {
  return row?.state_payload?.[key] || null
}

function displayScore(row) {
  const finalScore = stateContext(row, 'final_score')
  if (finalScore !== null && finalScore !== undefined) return Number(finalScore)
  return Number(row.score || 0)
}

function plannerReason(row) {
  return row?.state_payload?.planner_candidate_reason || row?.planner_candidate_reason || row?.state_payload?.hierarchy_block_reason || '—'
}

function isInvalidData(row) {
  const reason = plannerReason(row)
  const volume = stateContext(row, 'volume_debug') || {}
  const market = stateContext(row, 'market_quality_debug') || {}
  return String(reason).startsWith('invalid_market_data') || (
    Number(volume.last || 0) <= 0 &&
    Number(volume.average || 0) <= 0 &&
    Number(market.avg_range_pct || 0) <= 0
  )
}

function isValidData(row) {
  return !isInvalidData(row)
}

function cycleStepClass(done, active, blocked) {
  if (blocked) return '#7f1d1d'
  if (active) return '#f59e0b'
  if (done) return '#166534'
  return '#374151'
}

function summarizeCycle(row) {
  const macro = stateContext(row, 'macro_window_4h') || {}
  const wyckoff = stateContext(row, 'wyckoff_requirement') || {}
  const trigger = stateContext(row, 'execution_trigger_5m') || {}
  const zone = stateContext(row, 'zone_validity') || {}
  const trade = row?.state_payload?.trade || {}
  const side = macro.side || row.bias || 'neutral'
  const status = wyckoff.status || 'waiting'
  const macroDone = Boolean(macro.valid)
  const swept = ['swept_waiting_rejection', 'swept_waiting_reclaim', 'rejected_waiting_5m_confirm', 'reclaimed_waiting_5m_confirm', 'execution_ready'].includes(status)
  const eventDone = ['rejected_waiting_5m_confirm', 'reclaimed_waiting_5m_confirm', 'execution_ready'].includes(status)
  const triggerDone = Boolean(trigger.valid)
  const tradeDone = trade.status && trade.status !== 'watch' && trade.side && trade.side !== 'none'
  const blocked = !macroDone || String(plannerReason(row)).includes('blocked')
  const steps = [
    { key: '4H', label: side === 'neutral' ? '4H' : `${side} 4H`, done: macroDone, active: !macroDone, blocked: !macroDone },
    { key: 'SWP', label: swept ? 'sweep' : 'wait sweep', done: swept, active: macroDone && !swept, blocked: false },
    { key: 'WY', label: eventDone ? (wyckoff.expected || 'event') : `wait ${wyckoff.expected || 'event'}`, done: eventDone, active: swept && !eventDone, blocked: false },
    { key: '5M', label: triggerDone ? '5m confirm' : 'wait 5m', done: triggerDone, active: eventDone && !triggerDone, blocked: false },
    { key: 'TRD', label: tradeDone ? 'trade' : 'no trade', done: tradeDone, active: triggerDone && !tradeDone, blocked: false },
  ]
  return { steps, zoneOk: Boolean(zone.valid), status }
}

function CycleView({ row }) {
  const cycle = summarizeCycle(row)
  return (
    <div style={{ display: 'grid', gap: 4, minWidth: 230 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>
        {cycle.steps.map((step, index) => (
          <span key={step.key} style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
            <span
              title={step.label}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: 28,
                height: 22,
                borderRadius: 999,
                background: cycleStepClass(step.done, step.active, step.blocked),
                color: 'white',
                fontSize: 10,
                fontWeight: 700,
              }}
            >
              {step.done ? '✓' : step.active ? '…' : '·'}
            </span>
            {index < cycle.steps.length - 1 ? <span style={{ opacity: 0.45 }}>›</span> : null}
          </span>
        ))}
      </div>
      <div style={{ fontSize: 11, opacity: 0.85 }}>{cycle.status} · {cycle.zoneOk ? 'zone ok' : 'zone weak'}</div>
    </div>
  )
}

function summarizeScore(row) {
  const breakdown = row?.state_payload?.score_breakdown
  const finalBreakdown = row?.state_payload?.final_score_breakdown
  if (!breakdown) return '—'
  const parts = [
    `L${breakdown.liquidity || 0}`,
    `S${breakdown.structure || 0}`,
    `C${breakdown.confirmation || 0}`,
    `Se${breakdown.session || 0}`,
    `Q${breakdown.quality || 0}`,
    `V${breakdown.volume || 0}`,
    `H${breakdown.htf_alignment || 0}`,
    `M${breakdown.market_quality || 0}`,
    `T${breakdown.target_quality || 0}`,
  ]
  const mssBos = []
  if (row?.state_payload?.mss_bull) mssBos.push('MSS↑')
  if (row?.state_payload?.mss_bear) mssBos.push('MSS↓')
  if (row?.state_payload?.bos_bull) mssBos.push('BOS↑')
  if (row?.state_payload?.bos_bear) mssBos.push('BOS↓')
  const finalInfo = finalBreakdown ? ` · F${fmtNumber(displayScore(row), 1)}` : ''
  return `${parts.join(' · ')}${mssBos.length ? ` · ${mssBos.join(' / ')}` : ''}${finalInfo}`
}

function summarizeWindow(row) {
  const window4h = stateContext(row, 'macro_window_4h')
  if (!window4h) return '—'
  const side = window4h.side || 'neutral'
  const pos = window4h.range_position !== null && window4h.range_position !== undefined ? fmtNumber(window4h.range_position, 2) : '—'
  const status = window4h.valid ? 'valid' : 'blocked'
  return `${side} · ${status} · rp ${pos}`
}

function summarizeWyckoff(row) {
  const wyckoff = stateContext(row, 'wyckoff_requirement')
  if (!wyckoff) return '—'
  const expected = wyckoff.expected || 'none'
  const status = wyckoff.status || 'waiting'
  return `${expected} · ${status}`
}

function summarizeZoneValidity(row) {
  const zoneValidity = stateContext(row, 'zone_validity')
  if (!zoneValidity) return '—'
  return `${zoneValidity.valid ? 'ok' : 'weak'} · ${fmtNumber(zoneValidity.score, 0)}`
}

function MobileAssetCards({ rows }) {
  if (!rows.length) return <div className="empty-cell">No asset state available</div>

  return (
    <div className="mobile-card-grid market-mobile-cards">
      {rows.map((row) => {
        const reason = plannerReason(row)
        const target = summarizeContext(row.execution_target || stateContext(row, 'projected_target'))
        const dataStatus = isInvalidData(row) ? 'Invalid data' : 'Valid data'
        return (
          <article className="mobile-asset-card" key={row.id || row.symbol}>
            <div className="mobile-asset-top">
              <div>
                <Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link>
                <div className="mobile-asset-meta">{row?.state_payload?.state || '—'} · {row.bias || '—'}</div>
              </div>
              <span className={stageBadgeClass(row.stage)}>{row.stage}</span>
            </div>
            <CycleView row={row} />
            <div className="mobile-kpi-grid">
              <div><span>Score</span><strong>{fmtNumber(displayScore(row), 2)}</strong></div>
              <div><span>Price</span><strong>{fmtNumber(row.price, 4)}</strong></div>
              <div><span>Wyckoff</span><strong>{summarizeWyckoff(row)}</strong></div>
              <div><span>Target</span><strong>{target}</strong></div>
            </div>
            <div className={`mobile-data-status ${isInvalidData(row) ? 'invalid' : 'valid'}`}>{dataStatus}</div>
            <div className="mobile-reason"><span>Reason</span><strong>{reason}</strong></div>
            <div className="mobile-asset-actions">
              <Link to={`/assets/${encodeURIComponent(row.symbol)}`}>Debug view</Link>
              <a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">TradingView</a>
            </div>
          </article>
        )
      })}
    </div>
  )
}

export default function DashboardPage() {
  const [marketFilter, setMarketFilter] = useState('all')
  const settingsLoader = useCallback(() => api.adminSettings(), [])
  const { data: adminSettings } = usePollingQuery(settingsLoader, 30000)
  const assetLimit = Number(adminSettings?.binance?.binance_max_symbols || 50)

  const loadAssets = useCallback(() => api.assets(`?limit=${assetLimit}&sort_by=updated_at`), [assetLimit])
  const { data: assets = [], loading, error } = usePollingQuery(loadAssets, 15000)

  const tradeCount = assets.filter((item) => item.stage === 'trade').length
  const confirmCount = assets.filter((item) => item.stage === 'confirm').length
  const zoneCount = assets.filter((item) => item.stage === 'zone').length
  const invalidDataCount = assets.filter(isInvalidData).length
  const avgScore = assets.length ? (assets.reduce((sum, item) => sum + displayScore(item), 0) / assets.length).toFixed(2) : '0.00'

  const sessionCounts = useMemo(() => assets.reduce((acc, item) => {
    const key = item.session || 'unknown'
    acc[key] = (acc[key] || 0) + 1
    return acc
  }, {}), [assets])

  const strongestAssets = useMemo(() => [...assets].sort((a, b) => displayScore(b) - displayScore(a)).slice(0, 6), [assets])
  const strongZoneCount = useMemo(() => assets.filter((item) => stateContext(item, 'zone_validity')?.valid).length, [assets])

  const filteredAssets = useMemo(() => {
    if (marketFilter === 'zone') return assets.filter((item) => item.stage === 'zone')
    if (marketFilter === 'valid') return assets.filter(isValidData)
    if (marketFilter === 'invalid') return assets.filter(isInvalidData)
    return assets
  }, [assets, marketFilter])

  const columns = [
    {
      key: 'symbol',
      title: 'Symbol',
      render: (row) => (
        <div style={{ display: 'grid', gap: 6 }}>
          <Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link>
          <a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">Open on TradingView</a>
        </div>
      ),
      sortValue: (row) => row.symbol,
    },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{row.stage}</span>, sortValue: (row) => row.stage },
    { key: 'cycle', title: 'Cycle', render: (row) => <CycleView row={row} />, sortValue: (row) => summarizeCycle(row).status },
    { key: 'state', title: 'State', render: (row) => row?.state_payload?.state || '—', sortValue: (row) => row?.state_payload?.state || '' },
    { key: 'bias', title: 'Bias', render: (row) => row.bias || '—', sortValue: (row) => row.bias || '' },
    { key: 'session_phase', title: 'Session', render: (row) => row?.state_payload?.session_phase || row.session, sortValue: (row) => row?.state_payload?.session_phase || row.session },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(displayScore(row), 2), sortValue: (row) => displayScore(row) },
    { key: 'planner_reason', title: 'Planner reason', render: (row) => <span className={isInvalidData(row) ? 'reason-invalid' : ''}>{plannerReason(row)}</span>, sortValue: (row) => plannerReason(row) },
    { key: 'zone_validity', title: 'Zone validity', render: (row) => summarizeZoneValidity(row), sortValue: (row) => Number(stateContext(row, 'zone_validity')?.score ?? -1) },
    { key: 'wyckoff_requirement', title: 'Wyckoff wait', render: (row) => summarizeWyckoff(row), sortValue: (row) => stateContext(row, 'wyckoff_requirement')?.status || '' },
    { key: 'price', title: 'Price', render: (row) => fmtNumber(row.price, 4), sortValue: (row) => Number(row.price || 0) },
    { key: 'rsi_1h', title: 'RSI 1H', render: (row) => fmtNumber(row.rsi_1h, 2), sortValue: (row) => Number(row.rsi_1h ?? -1) },
    { key: 'macro_window_4h', title: '4H window', render: (row) => summarizeWindow(row), sortValue: (row) => Number(stateContext(row, 'macro_window_4h')?.range_position ?? -1) },
    { key: 'macro_liquidity_context', title: 'Macro context', render: (row) => summarizeContext(stateContext(row, 'macro_liquidity_context') || row.liquidity_context), sortValue: (row) => (stateContext(row, 'macro_liquidity_context') || row.liquidity_context)?.level ?? -1 },
    { key: 'wyckoff_event_level', title: 'Wyckoff level', render: (row) => summarizeContext(stateContext(row, 'wyckoff_event_level')), sortValue: (row) => stateContext(row, 'wyckoff_event_level')?.level ?? -1 },
    { key: 'entry_liquidity_context', title: 'Entry context', render: (row) => summarizeContext(stateContext(row, 'entry_liquidity_context')), sortValue: (row) => stateContext(row, 'entry_liquidity_context')?.level ?? -1 },
    { key: 'execution_target', title: 'Execution target', render: (row) => summarizeContext(row.execution_target), sortValue: (row) => row.execution_target?.level ?? -1 },
    { key: 'projected_target', title: 'Projected', render: (row) => summarizeContext(stateContext(row, 'projected_target')), sortValue: (row) => stateContext(row, 'projected_target')?.level ?? -1 },
    { key: 'updated_at', title: 'Updated', render: (row) => fmtDate(row.updated_at), sortValue: (row) => row.updated_at },
  ]

  const strongestColumns = [
    { key: 'symbol', title: 'Symbol', render: (row) => <Link to={`/assets/${encodeURIComponent(row.symbol)}`}>{row.symbol}</Link>, sortValue: (row) => row.symbol },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{row.stage}</span>, sortValue: (row) => row.stage },
    { key: 'cycle', title: 'Cycle', render: (row) => <CycleView row={row} />, sortValue: (row) => summarizeCycle(row).status },
    { key: 'state', title: 'State', render: (row) => row?.state_payload?.state || '—', sortValue: (row) => row?.state_payload?.state || '' },
    { key: 'bias', title: 'Bias', render: (row) => row.bias || '—', sortValue: (row) => row.bias || '' },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(displayScore(row), 2), sortValue: (row) => displayScore(row) },
    { key: 'planner_reason', title: 'Reason', render: (row) => plannerReason(row), sortValue: (row) => plannerReason(row) },
    { key: 'score_breakdown', title: 'Breakdown', render: (row) => summarizeScore(row), sortValue: (row) => JSON.stringify(row?.state_payload?.score_breakdown || {}) },
  ]

  const filterOptions = [
    { key: 'all', label: `All (${assets.length})` },
    { key: 'zone', label: `Zone only (${zoneCount})` },
    { key: 'valid', label: `Valid data (${assets.length - invalidDataCount})` },
    { key: 'invalid', label: `Invalid data (${invalidDataCount})` },
  ]

  return (
    <div className="page-stack">
      <PageHeader title="Dashboard 360" subtitle="Market overview with debug links, projected targets, and asset drill-down." />
      <div className="stats-grid">
        <StatCard label="Tracked assets" value={assets.length} hint={`latest updated · limit ${assetLimit}`} />
        <StatCard label="Trade stage" value={tradeCount} />
        <StatCard label="Confirm stage" value={confirmCount} />
        <StatCard label="Zone stage" value={zoneCount} />
      </div>
      <div className="stats-grid">
        <StatCard label="Average score" value={avgScore} />
        <StatCard label="Strong zones" value={strongZoneCount} />
        <StatCard label="Invalid data" value={invalidDataCount} hint="zero volume/range or invalid_market_data" />
        <StatCard label="London total" value={(sessionCounts.london || 0) + (sessionCounts.london_open || 0)} hint={`Open: ${sessionCounts.london_open || 0} · Core: ${sessionCounts.london || 0}`} />
      </div>
      <div className="stats-grid">
        <StatCard label="New York" value={sessionCounts.new_york || 0} />
        <StatCard label="Asia / off" value={(sessionCounts.asia || 0) + (sessionCounts.off_session || 0)} hint={`Asia: ${sessionCounts.asia || 0} · Off: ${sessionCounts.off_session || 0}`} />
      </div>
      {loading ? <div className="panel">Loading assets…</div> : null}
      {error ? <div className="panel error">{error}</div> : null}
      <details className="panel collapsible-panel" open>
        <summary>
          <h2>Highest score assets</h2>
          <span className="collapse-indicator">⌄</span>
        </summary>
        <DataTable columns={strongestColumns} rows={strongestAssets} empty="No asset state available" />
      </details>
      <details className="panel collapsible-panel" open>
        <summary>
          <h2>Market view 360</h2>
          <span className="collapse-indicator">⌄</span>
        </summary>
        <div className="market-toolbar">
          <div className="filter-chips" role="tablist" aria-label="Market filters">
            {filterOptions.map((option) => (
              <button
                key={option.key}
                type="button"
                className={`filter-chip ${marketFilter === option.key ? 'active' : ''}`}
                onClick={() => setMarketFilter(option.key)}
              >
                {option.label}
              </button>
            ))}
          </div>
          <div className="market-toolbar-hint">Showing {filteredAssets.length} / {assets.length}</div>
        </div>
        <div className="desktop-market-table">
          <DataTable columns={columns} rows={filteredAssets} empty="No asset state available" defaultSortKey="updated_at" defaultSortDir="desc" />
        </div>
        <MobileAssetCards rows={filteredAssets} />
      </details>
    </div>
  )
}
