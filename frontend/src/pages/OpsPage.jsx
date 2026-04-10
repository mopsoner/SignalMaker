import { useCallback } from 'react'
import DataTable from '../components/DataTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate } from '../lib/format'

export default function OpsPage() {
  const healthLoader = useCallback(() => api.health(), [])
  const servicesLoader = useCallback(() => api.services(), [])
  const fillsLoader = useCallback(() => api.fills('?limit=50'), [])
  const candlesLoader = useCallback(() => api.candles('?limit=25'), [])

  const { data: health, error: healthError } = usePollingQuery(healthLoader, 10000)
  const { data: services, error: servicesError } = usePollingQuery(servicesLoader, 10000)
  const { data: fills = [] } = usePollingQuery(fillsLoader, 10000)
  const { data: candles = [] } = usePollingQuery(candlesLoader, 10000)

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

  return (
    <div className="page-stack">
      <PageHeader title="Ops" subtitle="Service health, fills and market ingestion state" />
      <div className="stats-grid">
        <StatCard label="API status" value={health?.status || '—'} hint={healthError || ''} />
        <StatCard label="Environment" value={health?.env || '—'} />
        <StatCard label="Database" value={health?.database || '—'} />
        <StatCard label="Services" value={serviceRows.length} hint={servicesError || ''} />
      </div>
      <section className="panel">
        <h2>Service health</h2>
        <DataTable columns={serviceColumns} rows={serviceRows} empty="No service data" />
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
