import { useCallback, useMemo } from 'react'
import DataTable from '../components/DataTable'
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
  return Object.entries(obj)
    .map(([key, value]) => ({ id: key, metric: key, value }))
    .sort((a, b) => Number(b.value || 0) - Number(a.value || 0))
}

export default function OpsPage() {
  const healthLoader = useCallback(() => api.health(), [])
  const servicesLoader = useCallback(() => api.services(), [])
  const fillsLoader = useCallback(() => api.fills('?limit=50'), [])
  const candlesLoader = useCallback(() => api.candles('?latest=true&limit=200'), [])
  const summaryLoader = useCallback(() => api.candleSummary(), [])
  const liveRunsLoader = useCallback(() => api.liveRuns('?limit=10'), [])

  const { data: health, error: healthError } = usePollingQuery(healthLoader, 10000)
  const { data: services, error: servicesError } = usePollingQuery(servicesLoader, 10000)
  const { data: fills = [] } = usePollingQuery(fillsLoader, 10000)
  const { data: candles = [] } = usePollingQuery(candlesLoader, 10000)
  const { data: summary = [] } = usePollingQuery(summaryLoader, 30000)
  const { data: liveRuns = [] } = usePollingQuery(liveRunsLoader, 15000)

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
  const metricColumns = [
    { key: 'metric', title: 'Metric' },
    { key: 'value', title: 'Value' },
  ]

  const symbols = [...new Set(summary.map((r) => r.symbol))].length
  const totalCandles = summary.reduce((s, r) => s + (r.candle_count || 0), 0)

  const latestRun = useMemo(() => (Array.isArray(liveRuns) && liveRuns.length ? liveRuns[0] : null), [liveRuns])
  const latestStats = latestRun?.stats || {}
  const pipelineMetricRows = objectToRows(latestStats.pipeline_counts)
  const plannerMetricRows = objectToRows(latestStats.planner_reason_counts)
  const dataQualityMetricRows = objectToRows(latestStats.data_quality_counts)
  const structureMetricRows = objectToRows(latestStats.structure_counts)

  return (
    <div className="page-stack">
      <PageHeader title="Ops" subtitle="Service health, fills and market ingestion state" />
      <div className="stats-grid">
        <StatCard label="API status" value={health?.status || '—'} hint={healthError || ''} />
        <StatCard label="Environment" value={health?.env || '—'} />
        <StatCard label="Database" value={health?.database || '—'} />
        <StatCard label="Symbols tracked" value={symbols || '—'} hint={`${totalCandles.toLocaleString()} candles total`} />
      </div>
      <div className="stats-grid">
        <StatCard label="Last pipeline run" value={latestRun ? fmtDate(latestRun.started_at) : '—'} hint={latestRun?.run_id || ''} />
        <StatCard label="Scanned" value={latestStats.symbols_scanned ?? latestRun?.symbols_scanned ?? '—'} hint={`Collected: ${latestStats.symbols_collected ?? '—'} / Requested: ${latestStats.symbols_requested ?? latestRun?.symbols_total ?? '—'}`} />
        <StatCard label="Candidates" value={latestStats.candidates_created ?? '—'} hint={`Workers: ${latestStats.collect_workers ?? '—'}`} />
        <StatCard label="Candles written" value={latestStats.candles_written ?? '—'} />
      </div>
      <section className="panel">
        <h2>Pipeline audit metrics</h2>
        {!latestRun ? <div>No live run data yet.</div> : null}
        {latestRun ? (
          <div className="two-col">
            <div>
              <h3>Pipeline counts</h3>
              <DataTable columns={metricColumns} rows={pipelineMetricRows} empty="No pipeline metrics" />
            </div>
            <div>
              <h3>Planner rejection reasons</h3>
              <DataTable columns={metricColumns} rows={plannerMetricRows} empty="No planner metrics" />
            </div>
            <div>
              <h3>Data quality alerts</h3>
              <DataTable columns={metricColumns} rows={dataQualityMetricRows} empty="No data quality alerts" />
            </div>
            <div>
              <h3>Structure counters</h3>
              <DataTable columns={metricColumns} rows={structureMetricRows} empty="No structure counters" />
            </div>
          </div>
        ) : null}
      </section>
      <section className="panel">
        <h2>Service health</h2>
        <DataTable columns={serviceColumns} rows={serviceRows} empty="No service data" />
      </section>
      <section className="panel">
        <h2>Étendue des candles par symbole / timeframe</h2>
        <DataTable
          columns={summaryColumns}
          rows={summary.map((r) => ({ ...r, id: `${r.symbol}-${r.interval}` }))}
          empty="Aucune donnée"
        />
      </section>
      <section className="panel two-col">
        <div>
          <h2>Recent fills</h2>
          <DataTable columns={fillColumns} rows={fills} empty="No fills yet" />
        </div>
        <div>
          <h2>Recent candles</h2>
          <DataTable columns={candleColumns} rows={candles} empty="No candles stored yet" />
        </div>
      </section>
    </div>
  )
}
