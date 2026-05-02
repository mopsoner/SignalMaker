import { useCallback, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber, stageBadgeClass } from '../lib/format'

const get = (row, key) => row?.state_payload?.[key] ?? null
const score = (row) => Number(get(row, 'gated_score') ?? get(row, 'score') ?? row.score ?? get(row, 'final_score') ?? 0)
const stage = (row) => row?.stage || get(row, 'stage') || get(row, 'hierarchy_gate')?.stage || 'collect'
const starts = (value, prefix) => String(value || '').startsWith(prefix)
const trigger = (row) => get(row, 'execution_trigger') || null
const gate = (row) => get(row, 'hierarchy_gate') || {}
const blockedAt = (row) => gate(row)?.blocked_at || trigger(row)?.blocked_by || ''
const blockReason = (row) => gate(row)?.block_reason || get(row, 'confirm_block_reason') || get(row, 'hierarchy_block_reason') || ''
const plannerReason = (row) => row?.state_payload?.planner_candidate_reason || row?.planner_candidate_reason || blockReason(row) || '—'
const wyckoff = (row) => get(row, 'wyckoff_requirement') || {}
const wyckoffStatus = (row) => wyckoff(row)?.status || 'waiting'
const confirmationModel = (row) => get(row, 'confirmation_model') || {}
const oneHourDecision = (row) => get(row, 'one_hour_decision') || {}
const rsiOneHour = (row) => row.rsi_1h ?? get(row, 'rsi_1h') ?? get(row, 'rsi_htf') ?? null
const context = (value) => !value || typeof value !== 'object' ? '—' : `${value.type || '—'} @ ${value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'}`

const confirmationLabel = (row) => {
  const model = confirmationModel(row)
  if (model.entry_mode === '15m_confirmed') return `15m confirmed${model.confirmed_by_1h ? ' + 1H' : ''}`
  if (model.entry_mode === '1h_confirm_15m_optional') return '1H decision valid · 15m optional'
  if (model.confirmed_by_1h || oneHourDecision(row)?.valid) return '1H decision valid'
  if (model.confirmed_by_15m || trigger(row)?.valid) return '15m confirmed'
  if (stage(row) === 'waiting_1h_event' || blockedAt(row) === 'decision_1h') return 'waiting 1H decision'
  return model.entry_mode || 'wait'
}

const oneHourConfirmed = (row) => Boolean(confirmationModel(row)?.confirmed_by_1h || oneHourDecision(row)?.valid || wyckoffStatus(row) === '1h_confirmed_15m_optional')
const fifteenMinConfirmed = (row) => Boolean(confirmationModel(row)?.confirmed_by_15m || trigger(row)?.valid)
const tradeCandidate = (row) => stage(row) === 'trade_candidate' || get(row, 'planner_candidate_status') === 'candidate_watch' || get(row, 'trade')?.status === 'candidate'
const tradeReady = (row) => ['trade_ready', 'trade'].includes(stage(row)) || get(row, 'trade')?.status === 'ready'
const waitingOneHourEvent = (row) => !tradeCandidate(row) && !tradeReady(row) && (stage(row) === 'waiting_1h_event' || blockedAt(row) === 'decision_1h' || wyckoffStatus(row) === 'waiting_1h_event' || String(plannerReason(row)).includes('waiting_1h'))
const macroBlocked = (row) => !tradeCandidate(row) && !tradeReady(row) && ['macro_4h', 'context_4h'].includes(blockedAt(row))
const targetBlocked = (row) => !tradeCandidate(row) && !tradeReady(row) && ['target'].includes(blockedAt(row))
const liquidityWaiting = (row) => !tradeCandidate(row) && !tradeReady(row) && blockedAt(row) === 'liquidity_1h'
const executionReady = (row) => tradeReady(row) || tradeCandidate(row) || stage(row) === 'confirm' || wyckoffStatus(row) === 'execution_ready' || oneHourConfirmed(row)
const oneHourBear = (row) => oneHourConfirmed(row) && (oneHourDecision(row)?.side === 'bear' || starts(row.bias, 'bear'))
const oneHourBull = (row) => oneHourConfirmed(row) && (oneHourDecision(row)?.side === 'bull' || starts(row.bias, 'bull'))
const actionableWatch = (row) => tradeCandidate(row) || tradeReady(row) || oneHourConfirmed(row) || ['confirm', 'confirm_watch'].includes(stage(row))
const hasTarget = (row) => Boolean(row.execution_target?.level || get(row, 'projected_target')?.level)
const strongZone = (row) => Boolean(get(row, 'zone_validity')?.valid)
const mss = (row) => Boolean(get(row, 'mss_bull') || get(row, 'mss_bear') || oneHourDecision(row)?.mss_seen)
const bos = (row) => Boolean(get(row, 'bos_bull') || get(row, 'bos_bear') || oneHourDecision(row)?.bos_seen)
const swept = (row) => Boolean(wyckoff(row)?.swept || get(row, 'wyckoff_event_level')?.swept)

function DecisionPath({ row }) {
  const pipeline = get(row, 'pipeline') || {}
  const currentStage = stage(row)
  const oneHourOk = oneHourConfirmed(row)
  const confirmOk = fifteenMinConfirmed(row)
  const tradeOk = tradeCandidate(row) || tradeReady(row)
  const liquidityDone = Boolean(pipeline.liquidity) || !['collect', 'macro_watch', 'context_invalid'].includes(currentStage)
  const targetDone = hasTarget(row)
  const steps = [
    ['Context', liquidityDone],
    ['Target', targetDone],
    ['1H decision', oneHourOk],
    ['15m opt.', confirmOk],
    ['Trade', tradeOk],
  ]
  const detail = oneHourDecision(row)?.source || oneHourDecision(row)?.reason || confirmationLabel(row)
  return <div style={{ display: 'grid', gap: 4, minWidth: 280 }}>
    <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>{steps.map(([key, done], index) => <span key={key} style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}><span title={key} style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', minWidth: 74, height: 22, padding: '0 8px', borderRadius: 999, background: done ? '#166534' : '#374151', color: 'white', fontSize: 10, fontWeight: 700 }}>{done ? '✓ ' : '· '}{key}</span>{index < steps.length - 1 ? <span style={{ opacity: 0.45 }}>›</span> : null}</span>)}</div>
    <div style={{ fontSize: 11, opacity: 0.85 }}>{currentStage} · {detail || 'wait'} · {strongZone(row) ? 'zone ok' : 'diagnostic zone'}</div>
  </div>
}

function MobileAssetCards({ rows }) {
  if (!rows.length) return <div className="empty-cell">No asset state available</div>
  return <div className="mobile-card-grid market-mobile-cards">{rows.map((row) => <article className="mobile-asset-card" key={row.id || row.symbol}>
    <div className="mobile-asset-top"><div><Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link><div className="mobile-asset-meta">{get(row, 'state') || '—'} · {row.bias || '—'}</div></div><span className={stageBadgeClass(stage(row))}>{stage(row)}</span></div>
    <DecisionPath row={row} />
    <div className="mobile-kpi-grid"><div><span>Score</span><strong>{fmtNumber(score(row), 2)}</strong></div><div><span>RSI 1H</span><strong>{fmtNumber(rsiOneHour(row), 2)}</strong></div><div><span>Confirm</span><strong>{confirmationLabel(row)}</strong></div><div><span>Target</span><strong>{context(row.execution_target || get(row, 'projected_target'))}</strong></div></div>
    <div className="mobile-reason"><span>Reason</span><strong>{plannerReason(row)}</strong></div>
    <div className="mobile-asset-actions"><Link to={`/assets/${encodeURIComponent(row.symbol)}`}>Debug view</Link><a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">TradingView</a></div>
  </article>)}</div>
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
    fifteenMinConfirmed: assets.filter(fifteenMinConfirmed).length,
    oneHourBear: assets.filter(oneHourBear).length,
    oneHourBull: assets.filter(oneHourBull).length,
    waitingOneHourEvent: assets.filter(waitingOneHourEvent).length,
    macroBlocked: assets.filter(macroBlocked).length,
    targetBlocked: assets.filter(targetBlocked).length,
    liquidityWaiting: assets.filter(liquidityWaiting).length,
    bull: assets.filter((x) => starts(x.bias, 'bull')).length,
    bear: assets.filter((x) => starts(x.bias, 'bear')).length,
    ready: assets.filter(executionReady).length,
    swept: assets.filter(swept).length,
    strongZones: assets.filter(strongZone).length,
    withTarget: assets.filter(hasTarget).length,
    mss: assets.filter(mss).length,
    bos: assets.filter(bos).length,
  }), [assets])
  const avgScore = assets.length ? (assets.reduce((sum, row) => sum + score(row), 0) / assets.length).toFixed(2) : '0.00'

  const filters = [
    ['actionable', `Actionable (${counts.actionable})`],
    ['trade_candidate', `Trade candidate 1H (${counts.tradeCandidate})`],
    ['trade_ready', `Trade ready (${counts.tradeReady})`],
    ['one_hour_confirmed', `1H decision valid (${counts.oneHourConfirmed})`],
    ['fifteen_min_confirmed', `15m confirmed (${counts.fifteenMinConfirmed})`],
    ['one_hour_bear', `1H bear UTAD/MSS (${counts.oneHourBear})`],
    ['one_hour_bull', `1H bull Spring/MSS (${counts.oneHourBull})`],
    ['waiting_1h_event', `Waiting 1H decision (${counts.waitingOneHourEvent})`],
    ['macro_blocked', `4H context blocked (${counts.macroBlocked})`],
    ['target_blocked', `Target blocked (${counts.targetBlocked})`],
    ['liquidity_waiting', `Liquidity waiting (${counts.liquidityWaiting})`],
    ['bull', `Bull (${counts.bull})`],
    ['bear', `Bear (${counts.bear})`],
    ['swept', `Swept (${counts.swept})`],
    ['strong_zone', `Zone ok diagnostic (${counts.strongZones})`],
    ['with_target', `With target (${counts.withTarget})`],
    ['mss', `MSS (${counts.mss})`],
    ['bos', `BOS (${counts.bos})`],
    ['all', `All (${assets.length})`],
  ]

  const filteredAssets = useMemo(() => {
    if (marketFilter === 'actionable') return assets.filter(actionableWatch)
    if (marketFilter === 'trade_candidate') return assets.filter(tradeCandidate)
    if (marketFilter === 'trade_ready') return assets.filter(tradeReady)
    if (marketFilter === 'one_hour_confirmed') return assets.filter(oneHourConfirmed)
    if (marketFilter === 'fifteen_min_confirmed') return assets.filter(fifteenMinConfirmed)
    if (marketFilter === 'one_hour_bear') return assets.filter(oneHourBear)
    if (marketFilter === 'one_hour_bull') return assets.filter(oneHourBull)
    if (marketFilter === 'waiting_1h_event') return assets.filter(waitingOneHourEvent)
    if (marketFilter === 'macro_blocked') return assets.filter(macroBlocked)
    if (marketFilter === 'target_blocked') return assets.filter(targetBlocked)
    if (marketFilter === 'liquidity_waiting') return assets.filter(liquidityWaiting)
    if (marketFilter === 'bull') return assets.filter((x) => starts(x.bias, 'bull'))
    if (marketFilter === 'bear') return assets.filter((x) => starts(x.bias, 'bear'))
    if (marketFilter === 'swept') return assets.filter(swept)
    if (marketFilter === 'strong_zone') return assets.filter(strongZone)
    if (marketFilter === 'with_target') return assets.filter(hasTarget)
    if (marketFilter === 'mss') return assets.filter(mss)
    if (marketFilter === 'bos') return assets.filter(bos)
    return assets
  }, [assets, marketFilter])

  const sortedFilteredAssets = useMemo(() => [...filteredAssets].sort((a, b) => score(b) - score(a)), [filteredAssets])
  const strongestAssets = useMemo(() => [...assets].sort((a, b) => score(b) - score(a)).slice(0, 6), [assets])

  const columns = [
    { key: 'symbol', title: 'Symbol', render: (row) => <div style={{ display: 'grid', gap: 6 }}><Link to={`/assets/${encodeURIComponent(row.symbol)}`}><strong>{row.symbol}</strong></Link><a href={`https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(row.symbol || '')}`} target="_blank" rel="noreferrer">TradingView</a></div>, sortValue: (row) => row.symbol },
    { key: 'stage', title: 'Stage', render: (row) => <span className={stageBadgeClass(stage(row))}>{stage(row)}</span>, sortValue: stage },
    { key: 'decision', title: '1H decision path', render: (row) => <DecisionPath row={row} />, sortValue: confirmationLabel },
    { key: 'state', title: 'State', render: (row) => get(row, 'state') || '—', sortValue: (row) => get(row, 'state') || '' },
    { key: 'bias', title: 'Bias', render: (row) => row.bias || '—', sortValue: (row) => row.bias || '' },
    { key: 'score', title: 'Score', render: (row) => fmtNumber(score(row), 2), sortValue: score },
    { key: 'confirm_model', title: 'Decision model', render: confirmationLabel, sortValue: confirmationLabel },
    { key: 'rsi', title: 'RSI 1H', render: (row) => fmtNumber(rsiOneHour(row), 2), sortValue: (row) => Number(rsiOneHour(row) ?? -1) },
    { key: 'reason', title: 'Reason', render: plannerReason, sortValue: plannerReason },
    { key: 'block', title: 'Gate', render: (row) => blockedAt(row) || '—', sortValue: blockedAt },
    { key: 'wyckoff', title: '1H/Wyckoff status', render: wyckoffStatus, sortValue: wyckoffStatus },
    { key: 'macro', title: '4H context', render: (row) => context(get(row, 'macro_liquidity_context') || row.liquidity_context), sortValue: (row) => (get(row, 'macro_liquidity_context') || row.liquidity_context)?.level ?? -1 },
    { key: 'entry', title: '1H entry context', render: (row) => context(get(row, 'entry_liquidity_context')), sortValue: (row) => get(row, 'entry_liquidity_context')?.level ?? -1 },
    { key: 'target', title: '4H target', render: (row) => context(row.execution_target || get(row, 'projected_target')), sortValue: (row) => (row.execution_target || get(row, 'projected_target'))?.level ?? -1 },
    { key: 'updated_at', title: 'Updated', render: (row) => fmtDate(row.updated_at), sortValue: (row) => row.updated_at },
  ]

  return <div className="page-stack">
    <PageHeader title="Dashboard 360" subtitle="Market overview: 4H context/target, 1H Wyckoff-SMC decision, optional 15m timing." />
    <div className="stats-grid"><StatCard label="Tracked assets" value={assets.length} hint={`latest updated · limit ${assetLimit}`} /><StatCard label="Trade candidate 1H" value={counts.tradeCandidate} /><StatCard label="1H decision valid" value={counts.oneHourConfirmed} /><StatCard label="Waiting 1H decision" value={counts.waitingOneHourEvent} /></div>
    <div className="stats-grid"><StatCard label="Average score" value={avgScore} /><StatCard label="Actionable" value={counts.actionable} /><StatCard label="Bull / Bear 1H" value={`${counts.oneHourBull} / ${counts.oneHourBear}`} /><StatCard label="4H / Target blocked" value={`${counts.macroBlocked} / ${counts.targetBlocked}`} /></div>
    {loading ? <div className="panel">Loading assets…</div> : null}{error ? <div className="panel error">{error}</div> : null}
    <FoldableTable title="Highest score assets" columns={columns.slice(0, 9)} rows={strongestAssets} empty="No asset state available" defaultSortKey="score" defaultSortDir="desc" />
    <details className="panel collapsible-panel" open><summary><h2>Market view 360</h2><span className="collapse-indicator">⌄</span></summary><div className="market-toolbar"><div className="filter-chips" role="tablist" aria-label="Market filters">{filters.map(([key, label]) => <button key={key} type="button" className={`filter-chip ${marketFilter === key ? 'active' : ''}`} onClick={() => setMarketFilter(key)}>{label}</button>)}</div><div className="market-toolbar-hint">Showing {sortedFilteredAssets.length} / {assets.length}</div></div><div className="desktop-market-table"><FoldableTable title="Assets" columns={columns} rows={sortedFilteredAssets} empty="No asset state available" defaultSortKey="score" defaultSortDir="desc" /></div><MobileAssetCards rows={sortedFilteredAssets} /></details>
  </div>
}
