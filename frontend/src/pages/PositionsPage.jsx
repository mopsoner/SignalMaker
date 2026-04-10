import { useCallback, useState } from 'react'
import DataTable from '../components/DataTable'
import PageHeader from '../components/PageHeader'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'

export default function PositionsPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const loadPositions = useCallback(() => api.positions('?limit=100'), [])
  const loadOrders = useCallback(() => api.orders('?limit=50'), [])
  const { data: positions = [], loading, error } = usePollingQuery(loadPositions, 10000)
  const { data: orders = [] } = usePollingQuery(loadOrders, 10000)

  async function runExecutor() {
    setBusy(true)
    setMessage('')
    try {
      const result = await api.runExecutor(10, 1)
      setMessage(`Executor OK · executed ${result.executed.length} · skipped ${result.skipped.length}`)
    } catch (err) {
      setMessage(err.message || String(err))
    } finally {
      setBusy(false)
    }
  }

  const positionColumns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'side', title: 'Side' },
    { key: 'status', title: 'Status' },
    { key: 'quantity', title: 'Qty', render: (row) => fmtNumber(row.quantity, 2) },
    { key: 'entry_price', title: 'Entry', render: (row) => fmtNumber(row.entry_price, 4) },
    { key: 'mark_price', title: 'Mark', render: (row) => fmtNumber(row.mark_price, 4) },
    { key: 'stop_price', title: 'Stop', render: (row) => fmtNumber(row.stop_price, 4) },
    { key: 'target_price', title: 'Target', render: (row) => fmtNumber(row.target_price, 4) },
    { key: 'opened_at', title: 'Opened', render: (row) => fmtDate(row.opened_at) },
  ]

  const orderColumns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'side', title: 'Side' },
    { key: 'order_type', title: 'Type' },
    { key: 'status', title: 'Status' },
    { key: 'quantity', title: 'Qty', render: (row) => fmtNumber(row.quantity, 2) },
    { key: 'filled_price', title: 'Filled', render: (row) => fmtNumber(row.filled_price, 4) },
    { key: 'created_at', title: 'Created', render: (row) => fmtDate(row.created_at) },
  ]

  return (
    <div className="page-stack">
      <PageHeader
        title="Positions"
        subtitle="Paper execution state, orders and fills"
        actions={<button className="button" disabled={busy} onClick={runExecutor}>{busy ? 'Executing…' : 'Run executor'}</button>}
      />
      {message ? <div className="panel info">{message}</div> : null}
      {loading ? <div className="panel">Loading positions…</div> : null}
      {error ? <div className="panel error">{error}</div> : null}
      <section className="panel">
        <h2>Open positions</h2>
        <DataTable columns={positionColumns} rows={positions} empty="No positions yet" />
      </section>
      <section className="panel">
        <h2>Recent orders</h2>
        <DataTable columns={orderColumns} rows={orders} empty="No orders yet" />
      </section>
    </div>
  )
}
