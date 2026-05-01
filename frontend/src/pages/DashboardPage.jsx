import { useCallback, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber, stageBadgeClass } from '../lib/format'

const get = (row, key) => row?.state_payload?.[key] || null
const score = (row) => Number(get(row, 'final_score') ?? row.score ?? 0)
const reason = (row) => row?.state_payload?.planner_candidate_reason || row?.planner_candidate_reason || row?.state_payload?.hierarchy_block_reason || '—'
const starts = (value, prefix) => String(value || '').startsWith(prefix)
const context = (value) => !value || typeof value !== 'object' ? '—' : `${value.type || '—'} @ ${value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'}`
const trigger = (row) => get(row, 'execution_trigger') || null
const wyckoffStatus = (row) => get(row, 'wyckoff_requirement')?.status || 'waiting'
const confirmationModel = (row) => get(row, 'confirmation_model') || {}
const oneHourConfirmation = (row) => get(row, 'one_hour_confirmation_debug') || {}
const confirmationLabel = (row) => {
  const model = confirmationModel(row)
  if (model.entry_mode === '15m_confirmed') return `15m confirmed${model.confirmed_by_1h ? ' + 1H' : ''}`
  if (model.entry_mode === '1h_confirm_15m_optional') return '1H confirmed · 15m optional'
  if (model.confirmed_by_1h) return '1H confirmed'
  if (model.confirmed_by_15m) return '15m confirmed'
  return model.entry_mode || 'wait'
}
const stage = (row) => row?.stage || get(row, 'stage') || get(row, 'hierarchy_gate')?.stage || 'collect'
const stageIs = (name) => (row) => stage(row) === name
const tradeCandidate = (row) => stage(row) === 'trade_candidate' || get(row, 'planner_candidate_status') === 'candidate_watch' || get(row, 'trade')?.status === 'candidate'
const tradeReady = (row) => stage(row) === 'trade_ready' || stage(row) === 'trade' || get(row, 'trade')?.status === 'ready'
const oneHourConfirmed = (row) => Boolean(confirmationModel(row)?.confirmed_by_1h || oneHourConfirmation(row)?.valid)
const oneHourOptional = (row) => confirmationModel(row)?.entry_mode === '1h_confirm_15m_optional'
const fifteenMinConfirmed = (row) => Boolean(confirmationModel(row)?.confirmed_by_15m || trigger(row)?.valid)
const oneHourBear = (row) => oneHourConfirmed(row) && oneHourConfirmation(row)?.side === 'bear'
const oneHourBull = (row) => oneHourConfirmed(row) && oneHourConfirmation(row)?.side === 'bull'
const executionReady = (row) => tradeReady(row) || tradeCandidate(row) || stage(row) === 'confirm' || wyckoffStatus(row) === 'execution_ready' || Boolean(trigger(row)?.valid)
const swept = (row) => ['swept_waiting_rejection', 'swept_waiting_reclaim', 'rejected_waiting_15m_confirm', 'reclaimed_waiting_15m_confirm', 'execution_ready'].includes(wyckoffStatus(row))
const waitingSweep = (row) => wyckoffStatus(row) === 'waiting_sweep'
const blocked = (row) => Boolean(get(row, 'confirm_blocked_by_hierarchy')) || wyckoffStatus(row) === 'blocked' || String(reason(row)).includes('blocked')
const lateCycle = (row) => ['late_bear_cycle', 'late_bull_cycle'].includes(stage(row)) || get(row, 'cycle_position_4h')?.is_late_cycle
const macroBlocked = (row) => stage(row) === 'macro_watch' || String(reason(row)).includes('missing_4h_') || get(row, 'hierarchy_gate')?.blocked_at === 'macro_4h'
const actionableWatch = (row) => (tradeCandidate(row) || tradeReady(row) || ['trade', 'confirm', 'confirm_watch', 'wyckoff_watch', 'zone_watch'].includes(stage(row)) || oneHourConfirmed(row)) && !macroBlocked(row) && !lateCycle(row)
const hasTarget = (row) => Boolean(row.execution_target?.level || get(row, 'projected_target')?.level)
const strongZone = (row) => Boolean(get(row, 'zone_validity')?.valid)
const mss = (row) => Boolean(get(row, 'mss_bull') || get(row, 'mss_bear'))
const bos = (row) => Boolean(get(row, 'bos_bull') || get(row, 'bos_bear'))

function CycleView({ row }) {
  const pipeline = row?.state_payload?.pipeline || {}
  const currentStage = stage(row)
  const trade = row?.state_payload?.trade || {}
  const liquidityDone = Boolean(pipeline.liquidity) || ['liquidity', 'zone', 'zone_watch', 'wyckoff_watch', 'confirm_watch', 'confirm', 'trade_candidate', 'trade_ready', 'trade'].includes(currentStage)
  const zoneDone = Boolean(pipeline.zone) || ['zone', 'zone_watch', 'wyckoff_watch', 'confirm_watch', 'confirm', 'trade_candidate', 'trade_ready', 'trade'].includes(currentStage)
  const confirmDone = Boolean(pipeline.confirm) || ['confirm', 'trade_candidate', 'trade_ready', 'trade'].includes(currentStage) || executionReady(row)
  const tradeDone = Boolean(pipeline.trade) || currentStage === 'trade' || currentStage === 'trade_ready' || Boolean(trade.status && trade.status !== 'watch' && trade.side && trade.side !== 'none')
  const cycle = get(row, 'cycle_position_4h')
  const steps = [
    ['Liquidity', liquidityDone],
    ['Zone', zoneDone],
    ['Confirm', confirmDone],
    ['Trade', tradeDone],
  ]
  return <div style={{ display: 'grid', gap: 4, minWidth: 250 }}><div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>{steps.map(([key, done], index) => <span key={key} style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}><span title={key} style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', minWidth: 58, height: 22, padding: '0 8px', borderRadius: 999, background: done ? '#166534' : '#374151', color: 'white', fontSize: 10, fontWeight: 700 }}>{done ? '✓ ' : '· '}{key}</span>{index < steps.length - 1 ? <span style={{ opacity: 0.45 }}>›</span> : null}</span>)}</div><div style={{ fontSize: 11, opacity: 0.85 }}>{cycle?.stage || wyckoffStatus(row)} · {confirmationLabel(row)} · {strongZone(row) ? 'zone ok' : 'zone weak'}</div></div>
}

function MobileAssetCards({ rows }) {
  if (!rows.length) return <div className="empty-cell">No asset state available</div>
  return <div className="mobile-card-grid market-mobile-cards">{rows.map((row) => <article className="mobile-asset-card" key={row.id || row.symbol}><div className="mobile-asset-top"><div><Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link><div className="mobile-asset-meta">{get(row, 'state') || '—'} · {row.bias || '—'}</div></div><span className={stageBadgeClass(stage(row))}>{stage(row)}</span></div><CycleView row={row} /><div className="mobile-kpi-grid"><div><span>Score</span><strong>{fmtNumber(score(row), 2)}</strong></div><div><span>RSI</span><strong>{fmtNumber(row.rsi_15m ?? get(row, 'rsi_15m') ?? get(row, 'rsi_main'), 2)}</strong></div><div><span>Confirm</span><strong>{confirmationLabel(row)}</strong></div><div><span>Target</span><strong>{context(row.execution_target || get(row, 'projected_target'))}</strong></div></div><div className="mobile-reason"><span>Reason</span><strong>{reason(row)}</strong></div><div className="mobile-asset-actions"><Link to={`/assets/${encodeURIComponent(row.symbol)}`}>Debug view</Link><a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">TradingView</a></div></article>)}</div>
}

export default function DashboardPage() {
  const [marketFilter, setMarketFilter] = useState('actionable')
  const { data: adminSettings } = usePollingQuery(useCallback(() => api.adminSettings(), []), 30000)
  const assetLimit = Number(adminSettings?.binance?.binance_max_symbols || 50)
  const { data: assets = [], loading, error } = usePollingQuery(useCallback(() => api.assets(`?limit=${assetLimit}&sort_by=updated_at`), [assetLimit]), 15000)

  const counts = useMemo(() => ({
    actionable: assets.filter(actionableWatch).length,
    tradeCandidate: assets.filter(tradeCandidate).length,
    tradeReady: assets.filter(tradeReady).length,
    oneHourConfirmed: assets.filter(oneHourConfirmed).length,
    oneHourOptional: assets.filter(oneHourOptional).length,
    fifteenMinConfirmed: assets.filter(fifteenMinConfirmed).length,
    oneHourBear: assets.filter(oneHourBear).length,
    oneHourBull: assets.filter(oneHourBull).length,
    lateCycle: assets.filter(lateCycle).length,
    lateBear: assets.filter(stageIs('late_bear_cycle')).length,
    lateBull: assets.filter(stageIs('late_bull_cycle')).length,
    midCycle: assets.filter(stageIs('mid_cycle_watch')).length,
    macroBlocked: assets.filter(macroBlocked).length,
    trade: assets.filter(stageIs('trade')).length,
    confirm: assets.filter(stageIs('confirm')).length,
    confirmWatch: assets.filter(stageIs('confirm_watch')).length,
    wyckoffWatch: assets.filter(stageIs('wyckoff_watch')).length,
    zone: assets.filter(stageIs('zone')).length,
    zoneWatch: assets.filter(stageIs('zone_watch')).length,
    macroWatch: assets.filter(stageIs('macro_watch')).length,
    liquidity: assets.filter(stageIs('liquidity')).length,
    liquidityWatch: assets.filter(stageIs('liquidity_watch')).length,
    collect: assets.filter(stageIs('collect')).length,
    bull: assets.filter((x) => starts(x.bias, 'bull')).length,
    bear: assets.filter((x) => starts(x.bias, 'bear')).length,
    ready: assets.filter(executionReady).length,
    swept: assets.filter(swept).length,
    waiting: assets.filter(waitingSweep).length,
    blocked: assets.filter(blocked).length,
    strongZones: assets.filter(strongZone).length,
    withTarget: assets.filter(hasTarget).length,
    mss: assets.filter(mss).length,
    bos: assets.filter(bos).length,
  }), [assets])
  const avgScore = assets.length ? (assets.reduce((sum, row) => sum + score(row), 0) / assets.length).toFixed(2) : '0.00'
  const strongestAssets = useMemo(() => [...assets].sort((a, b) => score(b) - score(a)).slice(0, 6), [assets])
  const filteredAssets = useMemo(() => {
    if (marketFilter === 'actionable') return assets.filter(actionableWatch)
    if (marketFilter === 'trade_candidate') return assets.filter(tradeCandidate)
    if (marketFilter === 'trade_ready') return assets.filter(tradeReady)
    if (marketFilter === 'one_hour_confirmed') return assets.filter(oneHourConfirmed)
    if (marketFilter === 'one_hour_optional') return assets.filter(oneHourOptional)
    if (marketFilter === 'fifteen_min_confirmed') return assets.filter(fifteenMinConfirmed)
    if (marketFilter === 'one_hour_bear') return assets.filter(oneHourBear)
    if (marketFilter === 'one_hour_bull') return assets.filter(oneHourBull)
    if (marketFilter === 'late_cycle') return assets.filter(lateCycle)
    if (marketFilter === 'late_bear') return assets.filter(stageIs('late_bear_cycle'))
    if (marketFilter === 'late_bull') return assets.filter(stageIs('late_bull_cycle'))
    if (marketFilter === 'mid_cycle') return assets.filter(stageIs('mid_cycle_watch'))
    if (marketFilter === 'macro_blocked') return assets.filter(macroBlocked)
    if (marketFilter === 'execution_ready') return assets.filter(executionReady)
    if (marketFilter === 'trade') return assets.filter(stageIs('trade'))
    if (marketFilter === 'confirm') return assets.filter(stageIs('confirm'))
    if (marketFilter === 'confirm_watch') return assets.filter(stageIs('confirm_watch'))
    if (marketFilter === 'wyckoff_watch') return assets.filter(stageIs('wyckoff_watch'))
    if (marketFilter === 'zone') return assets.filter(stageIs('zone'))
    if (marketFilter === 'zone_watch') return assets.filter(stageIs('zone_watch'))
    if (marketFilter === 'macro_watch') return assets.filter(stageIs('macro_watch'))
    if (marketFilter === 'liquidity') return assets.filter(stageIs('liquidity'))
    if (marketFilter === 'liquidity_watch') return assets.filter(stageIs('liquidity_watch'))
    if (marketFilter === 'collect') return assets.filter(stageIs('collect'))
    if (marketFilter === 'bull') return assets.filter((x) => starts(x.bias, 'bull'))
    if (marketFilter === 'bear') return assets.filter((x) => starts(x.bias, 'bear'))
    if (marketFilter === 'swept') return assets.filter(swept)
    if (marketFilter === 'waiting_sweep') return assets.filter(waitingSweep)
    if (marketFilter === 'blocked') return assets.filter(blocked)
    if (marketFilter === 'strong_zone') return assets.filter(strongZone)
    if (marketFilter === 'with_target') return assets.filter(hasTarget)
    if (marketFilter === 'mss') return assets.filter(mss)
    if (marketFilter === 'bos') return assets.filter(bos)
    return assets
  }, [assets, marketFilter])

  const columns = [
    { key: 'symbol', title: 'Symbol', render: (row) => <div style={{ display: 'grid', gap: 6 }}><Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link><a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">TradingView</a></div>, sortValue: (row) => row.symbol },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(stage(row))}>{stage(row)}</span>, sortValue: stage },
    { key: 'cycle', title: 'Cycle', render: (row) => <CycleView row={row} />, sortValue: (row) => get(row, 'cycle_position_4h')?.stage || stage(row) },
    { key: 'state', title: 'State', render: (row) => get(row, 'state') || '—', sortValue: (row) => get(row, 'state') || '' },
    { key: 'bias', title: 'Bias', render: (row) => row.bias || '—', sortValue: (row) => row.bias || '' },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(score(row), 2), sortValue: score },
    { key: 'confirm_model', title: 'Confirm model', render: confirmationLabel, sortValue: confirmationLabel },
    { key: 'rsi', title: 'RSI', render: (row) => fmtNumber(row.rsi_15m ?? get(row, 'rsi_15m') ?? get(row, 'rsi_main'), 2), sortValue: (row) => Number(row.rsi_15m ?? get(row, 'rsi_main') ?? -1) },
    { key: 'reason', title: 'Planner reason', render: reason, sortValue: reason },
    { key: 'trigger', title: 'Execution trigger', render: (row) => trigger(row)?.trigger || confirmationModel(row)?.confirmation_source || '—', sortValue: (row) => trigger(row)?.trigger || confirmationModel(row)?.confirmation_source || '' },
    { key: 'wyckoff', title: 'Wyckoff wait', render: wyckoffStatus, sortValue: wyckoffStatus },
    { key: 'macro', title: 'Macro context', render: (row) => context(get(row, 'macro_liquidity_context') || row.liquidity_context), sortValue: (row) => (get(row, 'macro_liquidity_context') || row.liquidity_context)?.level ?? -1 },
    { key: 'entry', title: 'Entry context', render: (row) => context(get(row, 'entry_liquidity_context')), sortValue: (row) => get(row, 'entry_liquidity_context')?.level ?? -1 },
    { key: 'target', title: 'Target', render: (row) => context(row.execution_target || get(row, 'projected_target')), sortValue: (row) => (row.execution_target || get(row, 'projected_target'))?.level ?? -1 },
    { key: 'updated_at', title: 'Updated', render: (row) => fmtDate(row.updated_at), sortValue: (row) => row.updated_at },
  ]
  const filters = [
    ['actionable', `Actionable watch (${counts.actionable})`],
    ['trade_candidate', `Trade candidate 1H (${counts.tradeCandidate})`],
    ['trade_ready', `Trade ready (${counts.tradeReady})`],
    ['one_hour_confirmed', `1H confirmed (${counts.oneHourConfirmed})`],
    ['one_hour_optional', `1H confirmed · 15m optional (${counts.oneHourOptional})`],
    ['fifteen_min_confirmed', `15m confirmed (${counts.fifteenMinConfirmed})`],
    ['one_hour_bear', `1H bear UTAD/MSS (${counts.oneHourBear})`],
    ['one_hour_bull', `1H bull Spring/MSS (${counts.oneHourBull})`],
    ['all', `All (${assets.length})`],
    ['late_cycle', `Late cycle (${counts.lateCycle})`],
    ['late_bear', `Late bear (${counts.lateBear})`],
    ['late_bull', `Late bull (${counts.lateBull})`],
    ['mid_cycle', `Mid cycle (${counts.midCycle})`],
    ['macro_blocked', `Macro blocked (${counts.macroBlocked})`],
    ['execution_ready', `Execution ready (${counts.ready})`],
    ['trade', `Trade legacy (${counts.trade})`],
    ['confirm', `Confirm legacy (${counts.confirm})`],
    ['confirm_watch', `Confirm watch (${counts.confirmWatch})`],
    ['wyckoff_watch', `Wyckoff watch (${counts.wyckoffWatch})`],
    ['zone_watch', `Zone watch (${counts.zoneWatch})`],
    ['macro_watch', `Macro watch (${counts.macroWatch})`],
    ['zone', `Zone legacy (${counts.zone})`],
    ['liquidity', `Liquidity legacy (${counts.liquidity})`],
    ['liquidity_watch', `Liquidity watch (${counts.liquidityWatch})`],
    ['collect', `Collect (${counts.collect})`],
    ['bull', `Bull (${counts.bull})`],
    ['bear', `Bear (${counts.bear})`],
    ['swept', `Swept (${counts.swept})`],
    ['waiting_sweep', `Waiting sweep (${counts.waiting})`],
    ['strong_zone', `Strong zone (${counts.strongZones})`],
    ['with_target', `With target (${counts.withTarget})`],
    ['mss', `MSS (${counts.mss})`],
    ['bos', `BOS (${counts.bos})`],
    ['blocked', `Blocked (${counts.blocked})`],
  ]

  return <div className="page-stack"><PageHeader title="Dashboard 360" subtitle="Market overview with debug links, projected targets, and asset drill-down." /><div className="stats-grid"><StatCard label="Tracked assets" value={assets.length} hint={`latest updated · limit ${assetLimit}`} /><StatCard label="Trade candidate" value={counts.tradeCandidate} /><StatCard label="1H confirmed" value={counts.oneHourConfirmed} /><StatCard label="15m confirmed" value={counts.fifteenMinConfirmed} /></div><div className="stats-grid"><StatCard label="Average score" value={avgScore} /><StatCard label="Actionable watch" value={counts.actionable} /><StatCard label="Bull / Bear 1H" value={`${counts.oneHourBull} / ${counts.oneHourBear}`} /><StatCard label="Execution ready" value={counts.ready} /></div>{loading ? <div className="panel">Loading assets…</div> : null}{error ? <div className="panel error">{error}</div> : null}<FoldableTable title="Highest score assets" columns={columns.slice(0, 9)} rows={strongestAssets} empty="No asset state available" defaultSortKey="score" defaultSortDir="desc" /><details className="panel collapsible-panel" open><summary><h2>Market view 360</h2><span className="collapse-indicator">⌄</span></summary><div className="market-toolbar"><div className="filter-chips" role="tablist" aria-label="Market filters">{filters.map(([key, label]) => <button key={key} type="button" className={`filter-chip ${marketFilter === key ? 'active' : ''}`} onClick={() => setMarketFilter(key)}>{label}</button>)}</div><div className="market-toolbar-hint">Showing {filteredAssets.length} / {assets.length}</div></div><div className="desktop-market-table"><FoldableTable title="Assets" columns={columns} rows={filteredAssets} empty="No asset state available" defaultSortKey="score" defaultSortDir="desc" /></div><MobileAssetCards rows={[...filteredAssets].sort((a, b) => score(b) - score(a))} /></details></div>
}
