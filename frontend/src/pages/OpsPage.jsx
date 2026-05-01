import { useCallback, useMemo } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate } from '../lib/format'

function fmtSpan(hours) {
  if (hours == null) return '—'
  if (hours >= 48) return `${(hours / 24).toFixed(1)} d`
  return `${hours.toFixed(1)} h`
}

function objectToRows(obj) {
  if (!obj || typeof obj !== 'object') return []
  return Object.entries(obj).map(([key, value]) => ({ id: key, metric: key, value })).sort((a, b) => Number(b.value || 0) - Number(a.value || 0))
}

export default function OpsPage() {
  const { data: health, error: healthError } = usePollingQuery(useCallback(() => api.health(), []), 10000)
  const { data: services } = usePollingQuery(useCallback(() => api.services(), []), 10000)
  const { data: fills = [] } = usePollingQuery(useCallback(() => api.fills('?limit=50'), []), 10000)
  const { data: candles = [] } = usePollingQuery(useCallback(() => api.candles('?latest=true&limit=200'), []), 10000)
  const { data: summary = [] } = usePollingQuery(useCallback(() => api.candleSummary(), []), 30000)
  const { data: liveRuns = [] } = usePollingQuery(useCallback(() => api.liveRuns('?limit=10'), []), 15000)

  const serviceRows = services ? Object.entries(services).map(([name, meta]) => ({ id: name, name, ...meta })) : []
  const serviceColumns = [
    { key: 'name', title: 'Service' },
    { key: 'status', title: 'Status' },
    { key: 'last_tick_at', title: 'Last tick', render: (row) => fmtDate(row.last_tick_at) },
    { key: 'note', title: 'Note' },
  ]
  const fillColumns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'side', title: 'Side' },
    { key: 'quantity', title: 'Qty' },
    { key: 'price', title: 'Price' },
    { key: 'filled_at', title: 'Filled', render: (row) => fmtDate(row.filled_at) },
  ]
  const candleColumns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'interval', title: 'TF' },
    { key: 'close', title: 'Close' },
    { key: 'volume', title: 'Volume' },
    { key: 'ingested_at', title: 'Ingested', render: (row) => fmtDate(row.ingested_at) },
  ]
  const summaryColumns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'interval', title: 'TF' },
    { key: 'candle_count', title: 'Bougies' },
    { key: 'span_hours', title: 'Étendue', render: (row) => fmtSpan(row.span_hours) },
    { key: 'first_open', title: 'Depuis', render: (row) => fmtDate(row.first_open) },
    { key: 'last_close', title: "Jusqu'à", render: (row) => fmtDate(row.last_close) },
    { key: 'last_ingested', title: 'Ingéré', render: (row) => fmtDate(row.last_ingested) },
  ]
  const metricColumns = [{ key: 'metric', title: 'Metric' }, { key: 'value', title: 'Value' }]

  const symbols = [...new Set(summary.map((r) => r.symbol))].length
  const totalCandles = summary.reduce((s, r) => s + (r.candle_count || 0), 0)
  const latestRun = useMemo(() => (Array.isArray(liveRuns) && liveRuns.length ? liveRuns[0] : null), [liveRuns])
  const latestRunWithStats = useMemo(() => Array.isArray(liveRuns) ? liveRuns.find((run) => run?.stats && Object.keys(run.stats || {}).length > 0) || null : null, [liveRuns])
  const displayRun = latestRunWithStats || latestRun
  const latestStats = displayRun?.stats || {}
  const pipelineMetricRows = objectToRows(latestStats.pipeline_counts)
  const plannerMetricRows = objectToRows(latestStats.planner_reason_counts)
  const dataQualityMetricRows = objectToRows(latestStats.data_quality_counts)
  const structureMetricRows = objectToRows(latestStats.structure_counts)
  const hasAnyMetrics = Boolean(pipelineMetricRows.length || plannerMetricRows.length || dataQualityMetricRows.length || structureMetricRows.length)

  return <div className="page-stack">
    <PageHeader title="Ops" subtitle="Service health, fills and market ingestion state" />
    <div className="stats-grid"><StatCard label="API status" value={health?.status || '—'} hint={healthError || ''} /><StatCard label="Environment" value={health?.env || '—'} /><StatCard label="Database" value={health?.database || '—'} /><StatCard label="Symbols tracked" value={symbols || '—'} hint={`${totalCandles.toLocaleString()} candles total`} /></div>
    <div className="stats-grid"><StatCard label="Last pipeline run" value={displayRun ? fmtDate(displayRun.started_at) : '—'} hint={displayRun?.run_id || ''} /><StatCard label="Scanned" value={latestStats.symbols_scanned ?? displayRun?.symbols_scanned ?? '—'} hint={`Collected: ${latestStats.symbols_collected ?? '—'} / Requested: ${latestStats.symbols_requested ?? displayRun?.symbols_total ?? '—'}`} /><StatCard label="Candidates" value={latestStats.candidates_created ?? '—'} hint={`Workers: ${latestStats.collect_workers ?? '—'}`} /><StatCard label="Candles written" value={latestStats.candles_written ?? '—'} /></div>
    <section className="panel"><h2>Pipeline audit metrics</h2>{!displayRun ? <div>No live run data yet.</div> : null}{displayRun && !hasAnyMetrics ? <div>Latest runs found, but no saved stats yet. Run the pipeline once after backend restart.</div> : null}{displayRun && hasAnyMetrics ? <div className="two-col"><FoldableTable title="Pipeline counts" columns={metricColumns} rows={pipelineMetricRows} empty="No pipeline metrics" /><FoldableTable title="Planner rejection reasons" columns={metricColumns} rows={plannerMetricRows} empty="No planner metrics" /><FoldableTable title="Data quality alerts" columns={metricColumns} rows={dataQualityMetricRows} empty="No data quality alerts" defaultOpen={false} /><FoldableTable title="Structure counters" columns={metricColumns} rows={structureMetricRows} empty="No structure counters" /></div> : null}</section>
    <FoldableTable title="Service health" columns={serviceColumns} rows={serviceRows} empty="No service data" />
    <FoldableTable title="Étendue des candles par symbole / timeframe" columns={summaryColumns} rows={summary.map((r) => ({ ...r, id: `${r.symbol}-${r.interval}` }))} empty="Aucune donnée" />
    <section className="panel two-col"><FoldableTable title="Recent fills" columns={fillColumns} rows={fills} empty="No fills yet" /><FoldableTable title="Recent candles" columns={candleColumns} rows={candles} empty="No candles stored yet" /></section>
  </div>
}
