import { useCallback } from 'react'
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

export default function OpsPage() {
  const healthLoader = useCallback(() => api.health(), [])
  const servicesLoader = useCallback(() => api.services(), [])
  const fillsLoader = useCallback(() => api.fills('?limit=50'), [])
  const candlesLoader = useCallback(() => api.candles('?latest=true&limit=200'), [])
  const summaryLoader = useCallback(() => api.candleSummary(), [])

  const { data: health, error: healthError } = usePollingQuery(healthLoader, 10000)
  const { data: services, error: servicesError } = usePollingQuery(servicesLoader, 10000)
  const { data: fills = [] } = usePollingQuery(fillsLoader, 10000)
  const { data: candles = [] } = usePollingQuery(candlesLoader, 10000)
  const { data: summary = [] } = usePollingQuery(summaryLoader, 30000)

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

  const symbols = [...new Set(summary.map((r) => r.symbol))].length
  const totalCandles = summary.reduce((s, r) => s + (r.candle_count || 0), 0)

  return (
    <div className="page-stack">
      <PageHeader title="Ops" subtitle="Service health, fills and market ingestion state" />
      <div className="stats-grid">
        <StatCard label="API status" value={health?.status || '—'} hint={healthError || ''} />
        <StatCard label="Environment" value={health?.env || '—'} />
        <StatCard label="Database" value={health?.database || '—'} />
        <StatCard label="Symbols tracked" value={symbols || '—'} hint={`${totalCandles.toLocaleString()} candles total`} />
      </div>
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
