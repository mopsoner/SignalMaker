import { useCallback, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import DataTable from '../components/DataTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber, stageBadgeClass } from '../lib/format'

const get = (row, key) => row?.state_payload?.[key] || null
const score = (row) => Number(get(row, 'final_score') ?? row.score ?? 0)
const reason = (row) => row?.state_payload?.planner_candidate_reason || row?.planner_candidate_reason || row?.state_payload?.hierarchy_block_reason || '—'
const context = (value) => !value || typeof value !== 'object' ? '—' : `${value.type || '—'} @ ${value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'}`
const starts = (value, prefix) => String(value || '').startsWith(prefix)
const executionReady = (row) => row.stage === 'trade' || row.stage === 'confirm' || get(row, 'wyckoff_requirement')?.status === 'execution_ready' || Boolean(get(row, 'execution_trigger_5m')?.valid)

function CycleView({ row }) {
  const macro = get(row, 'macro_window_4h') || {}
  const wyckoff = get(row, 'wyckoff_requirement') || {}
  const trigger = get(row, 'execution_trigger_5m') || {}
  const trade = row?.state_payload?.trade || {}
  const status = wyckoff.status || 'waiting'
  const swept = ['swept_waiting_rejection', 'swept_waiting_reclaim', 'rejected_waiting_5m_confirm', 'reclaimed_waiting_5m_confirm', 'execution_ready'].includes(status)
  const eventDone = ['rejected_waiting_5m_confirm', 'reclaimed_waiting_5m_confirm', 'execution_ready'].includes(status)
  const triggerDone = Boolean(trigger.valid)
  const tradeDone = trade.status && trade.status !== 'watch' && trade.side && trade.side !== 'none'
  const steps = [
    ['4H', Boolean(macro.valid), !macro.valid],
    ['SWP', swept, false],
    ['WY', eventDone, false],
    ['5M', triggerDone, false],
    ['TRD', tradeDone, false],
  ]
  return <div style={{ display: 'grid', gap: 4, minWidth: 190 }}>
    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>{steps.map(([key, done, blocked]) => <span key={key} style={{ borderRadius: 999, padding: '3px 8px', background: blocked ? '#7f1d1d' : done ? '#166534' : '#374151', color: 'white', fontSize: 10, fontWeight: 700 }}>{done ? '✓ ' : '· '}{key}</span>)}</div>
    <div style={{ fontSize: 11, opacity: 0.85 }}>{status}</div>
  </div>
}

function MobileAssetCards({ rows }) {
  if (!rows.length) return <div className="empty-cell">No asset state available</div>
  return <div className="mobile-card-grid market-mobile-cards">{rows.map((row) => <article className="mobile-asset-card" key={row.id || row.symbol}>
    <div className="mobile-asset-top"><div><Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link><div className="mobile-asset-meta">{get(row, 'state') || '—'} · {row.bias || '—'}</div></div><span className={stageBadgeClass(row.stage)}>{row.stage}</span></div>
    <CycleView row={row} />
    <div className="mobile-kpi-grid"><div><span>Score</span><strong>{fmtNumber(score(row), 2)}</strong></div><div><span>Price</span><strong>{fmtNumber(row.price, 4)}</strong></div><div><span>Wyckoff</span><strong>{get(row, 'wyckoff_requirement')?.status || '—'}</strong></div><div><span>Target</span><strong>{context(row.execution_target || get(row, 'projected_target'))}</strong></div></div>
    <div className="mobile-reason"><span>Reason</span><strong>{reason(row)}</strong></div>
    <div className="mobile-asset-actions"><Link to={`/assets/${encodeURIComponent(row.symbol)}`}>Debug view</Link><a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">TradingView</a></div>
  </article>)}</div>
}

export default function DashboardPage() {
  const [marketFilter, setMarketFilter] = useState('all')
  const { data: adminSettings } = usePollingQuery(useCallback(() => api.adminSettings(), []), 30000)
  const assetLimit = Number(adminSettings?.binance?.binance_max_symbols || 50)
  const { data: assets = [], loading, error } = usePollingQuery(useCallback(() => api.assets(`?limit=${assetLimit}&sort_by=updated_at`), [assetLimit]), 15000)

  const counts = useMemo(() => ({
    trade: assets.filter((x) => x.stage === 'trade').length,
    confirm: assets.filter((x) => x.stage === 'confirm').length,
    zone: assets.filter((x) => x.stage === 'zone').length,
    bull: assets.filter((x) => starts(x.bias, 'bull')).length,
    bear: assets.filter((x) => starts(x.bias, 'bear')).length,
    ready: assets.filter(executionReady).length,
    strongZones: assets.filter((x) => get(x, 'zone_validity')?.valid).length,
  }), [assets])
  const avgScore = assets.length ? (assets.reduce((sum, row) => sum + score(row), 0) / assets.length).toFixed(2) : '0.00'
  const strongestAssets = useMemo(() => [...assets].sort((a, b) => score(b) - score(a)).slice(0, 6), [assets])
  const filteredAssets = useMemo(() => {
    if (marketFilter === 'execution_ready') return assets.filter(executionReady)
    if (marketFilter === 'trade') return assets.filter((x) => x.stage === 'trade')
    if (marketFilter === 'confirm') return assets.filter((x) => x.stage === 'confirm')
    if (marketFilter === 'zone') return assets.filter((x) => x.stage === 'zone')
    if (marketFilter === 'bull') return assets.filter((x) => starts(x.bias, 'bull'))
    if (marketFilter === 'bear') return assets.filter((x) => starts(x.bias, 'bear'))
    return assets
  }, [assets, marketFilter])

  const columns = [
    { key: 'symbol', title: 'Symbol', render: (row) => <div style={{ display: 'grid', gap: 6 }}><Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link><a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">TradingView</a></div>, sortValue: (row) => row.symbol },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(row.stage)}>{row.stage}</span>, sortValue: (row) => row.stage },
    { key: 'cycle', title: 'Cycle', render: (row) => <CycleView row={row} />, sortValue: (row) => get(row, 'wyckoff_requirement')?.status || '' },
    { key: 'state', title: 'State', render: (row) => get(row, 'state') || '—', sortValue: (row) => get(row, 'state') || '' },
    { key: 'bias', title: 'Bias', render: (row) => row.bias || '—', sortValue: (row) => row.bias || '' },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(score(row), 2), sortValue: score },
    { key: 'reason', title: 'Planner reason', render: reason, sortValue: reason },
    { key: 'wyckoff', title: 'Wyckoff wait', render: (row) => get(row, 'wyckoff_requirement')?.status || '—', sortValue: (row) => get(row, 'wyckoff_requirement')?.status || '' },
    { key: 'macro', title: 'Macro context', render: (row) => context(get(row, 'macro_liquidity_context') || row.liquidity_context), sortValue: (row) => (get(row, 'macro_liquidity_context') || row.liquidity_context)?.level ?? -1 },
    { key: 'entry', title: 'Entry context', render: (row) => context(get(row, 'entry_liquidity_context')), sortValue: (row) => get(row, 'entry_liquidity_context')?.level ?? -1 },
    { key: 'target', title: 'Target', render: (row) => context(row.execution_target || get(row, 'projected_target')), sortValue: (row) => (row.execution_target || get(row, 'projected_target'))?.level ?? -1 },
    { key: 'updated_at', title: 'Updated', render: (row) => fmtDate(row.updated_at), sortValue: (row) => row.updated_at },
  ]
  const filterOptions = [
    ['all', `All (${assets.length})`], ['execution_ready', `Execution ready (${counts.ready})`], ['trade', `Trade (${counts.trade})`], ['confirm', `Confirm (${counts.confirm})`], ['zone', `Zone (${counts.zone})`], ['bull', `Bull (${counts.bull})`], ['bear', `Bear (${counts.bear})`],
  ]

  return <div className="page-stack">
    <PageHeader title="Dashboard 360" subtitle="Market overview with debug links, projected targets, and asset drill-down." />
    <div className="stats-grid"><StatCard label="Tracked assets" value={assets.length} hint={`latest updated · limit ${assetLimit}`} /><StatCard label="Trade stage" value={counts.trade} /><StatCard label="Confirm stage" value={counts.confirm} /><StatCard label="Zone stage" value={counts.zone} /></div>
    <div className="stats-grid"><StatCard label="Average score" value={avgScore} /><StatCard label="Strong zones" value={counts.strongZones} /><StatCard label="Bull / Bear" value={`${counts.bull} / ${counts.bear}`} /><StatCard label="Execution ready" value={counts.ready} /></div>
    {loading ? <div className="panel">Loading assets…</div> : null}{error ? <div className="panel error">{error}</div> : null}
    <details className="panel collapsible-panel" open><summary><h2>Highest score assets</h2><span className="collapse-indicator">⌄</span></summary><DataTable columns={columns.slice(0, 7)} rows={strongestAssets} empty="No asset state available" /></details>
    <details className="panel collapsible-panel" open><summary><h2>Market view 360</h2><span className="collapse-indicator">⌄</span></summary><div className="market-toolbar"><div className="filter-chips" role="tablist" aria-label="Market filters">{filterOptions.map(([key, label]) => <button key={key} type="button" className={`filter-chip ${marketFilter === key ? 'active' : ''}`} onClick={() => setMarketFilter(key)}>{label}</button>)}</div><div className="market-toolbar-hint">Showing {filteredAssets.length} / {assets.length}</div></div><div className="desktop-market-table"><DataTable columns={columns} rows={filteredAssets} empty="No asset state available" defaultSortKey="updated_at" defaultSortDir="desc" /></div><MobileAssetCards rows={filteredAssets} /></details>
  </div>
}
