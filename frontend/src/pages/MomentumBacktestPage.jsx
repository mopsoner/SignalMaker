import { useCallback, useEffect, useMemo, useState } from 'react'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import FoldableTable from '../components/FoldableTable'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate } from '../lib/format'

function fmtNum(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  return Number(value).toFixed(digits)
}

function EquityCurve({ rows }) {
  const points = useMemo(() => {
    if (!rows?.length) return []
    const values = rows.map((r) => Number(r.equity || 0))
    const min = Math.min(...values)
    const max = Math.max(...values)
    const spread = max - min || 1
    return rows.map((row, index) => {
      const x = rows.length <= 1 ? 0 : (index / (rows.length - 1)) * 100
      const y = 100 - ((Number(row.equity || 0) - min) / spread) * 100
      return `${x},${y}`
    }).join(' ')
  }, [rows])

  if (!rows?.length) return <div style={{ color: 'var(--muted)' }}>No equity data yet.</div>

  const first = rows[0]
  const last = rows[rows.length - 1]
  return (
    <div>
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: '100%', height: 260, background: 'rgba(255,255,255,0.03)', border: '1px solid var(--line)', borderRadius: 12 }}>
        <polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
      </svg>
      <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--muted)', fontSize: 12, marginTop: 8 }}>
        <span>{fmtDate(first.timestamp)}</span>
        <span>{rows.length} points</span>
        <span>{fmtDate(last.timestamp)}</span>
      </div>
    </div>
  )
}

export default function MomentumBacktestPage() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const latestLoader = useCallback(() => api.momentumBacktestLatest(), [])
  const { data: run, error, refresh } = usePollingQuery(latestLoader, 5000)
  const runId = run?.run_id
  const tradesLoader = useCallback(() => runId ? api.momentumBacktestTrades(runId, 300) : Promise.resolve([]), [runId])
  const equityLoader = useCallback(() => runId ? api.momentumBacktestEquity(runId, 1000) : Promise.resolve([]), [runId])
  const { data: trades = [], refresh: refreshTrades } = usePollingQuery(tradesLoader, 8000)
  const { data: equity = [], refresh: refreshEquity } = usePollingQuery(equityLoader, 8000)
  const { data: workers = {}, refresh: refreshWorkers } = usePollingQuery(useCallback(() => api.workerStatus(), []), 6000)

  useEffect(() => {
    if (runId) {
      refreshTrades()
      refreshEquity()
    }
  }, [runId])

  async function startWorker() {
    setBusy(true)
    setMessage('')
    try {
      await api.startWorker('momentum_backtest')
      await refreshWorkers()
      setMessage('Momentum backtest worker started.')
    } catch (e) {
      setMessage(e.message || String(e))
    } finally {
      setBusy(false)
    }
  }

  async function createRun() {
    setBusy(true)
    setMessage('')
    try {
      await api.momentumBacktestCreate({})
      await refresh()
      setMessage('Backtest queued. The background worker will process it once.')
    } catch (e) {
      setMessage(e.message || String(e))
    } finally {
      setBusy(false)
    }
  }

  const active = run?.status === 'queued' || run?.status === 'running'
  const workerRunning = workers?.momentum_backtest?.running

  const tradeColumns = [
    { key: 'symbol', title: 'Symbol' },
    { key: 'entry_time', title: 'Entry', render: (row) => fmtDate(row.entry_time) },
    { key: 'entry_price', title: 'Entry price', render: (row) => fmtNum(row.entry_price, 6) },
    { key: 'exit_time', title: 'Exit', render: (row) => fmtDate(row.exit_time) },
    { key: 'exit_price', title: 'Exit price', render: (row) => fmtNum(row.exit_price, 6) },
    { key: 'pnl', title: 'PnL', render: (row) => fmtNum(row.pnl, 2) },
    { key: 'pnl_pct', title: 'PnL %', render: (row) => fmtNum(row.pnl_pct, 2) + '%' },
    { key: 'entry_rsi_1h', title: 'RSI 1h', render: (row) => fmtNum(row.entry_rsi_1h, 1) },
    { key: 'reason', title: 'Reason' },
  ]

  return (
    <div className="page-stack">
      <PageHeader title="Momentum Backtesting" subtitle="Run a one-off background backtest on stored crypto candles, with trades and equity curve." />

      <div className="stats-grid">
        <StatCard label="Status" value={run?.status || '—'} hint={run?.run_id || error || ''} />
        <StatCard label="Worker" value={workerRunning ? 'Running' : 'Stopped'} hint={workers?.momentum_backtest?.pid ? `PID ${workers.momentum_backtest.pid}` : 'momentum_backtest'} />
        <StatCard label="Final equity" value={run?.final_equity ? fmtNum(run.final_equity, 2) : '—'} hint={`Initial ${fmtNum(run?.initial_capital, 2)}`} />
        <StatCard label="PnL" value={run?.total_pnl_pct != null ? `${fmtNum(run.total_pnl_pct, 2)}%` : '—'} hint={`PnL ${fmtNum(run?.total_pnl, 2)}`} />
      </div>

      <div className="stats-grid">
        <StatCard label="Max drawdown" value={run?.max_drawdown_pct != null ? `${fmtNum(run.max_drawdown_pct, 2)}%` : '—'} />
        <StatCard label="Trades" value={run?.trade_count ?? '—'} hint={`Winrate ${fmtNum(run?.winrate, 1)}%`} />
        <StatCard label="Profit factor" value={fmtNum(run?.profit_factor, 2)} />
        <StatCard label="Symbols" value={`${run?.symbols_processed ?? 0} / ${run?.symbols_total ?? 0}`} hint={run?.completed_at ? `Done ${fmtDate(run.completed_at)}` : run?.started_at ? `Started ${fmtDate(run.started_at)}` : ''} />
      </div>

      <section className="panel">
        <h2>Controls</h2>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center' }}>
          <button className="button" onClick={startWorker} disabled={busy || workerRunning}>Start backtest worker</button>
          <button className="button" onClick={createRun} disabled={busy || active}>Run backtest once</button>
          <button className="button" onClick={() => { refresh(); refreshTrades(); refreshEquity(); refreshWorkers() }} disabled={busy}>Refresh</button>
          {message ? <span style={{ color: 'var(--muted)' }}>{message}</span> : null}
        </div>
        <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 12 }}>
          The run is queued once and processed by the background worker. Refreshes only read stored results.
        </p>
      </section>

      <section className="panel">
        <h2>Equity curve</h2>
        <EquityCurve rows={equity} />
      </section>

      <section className="panel two-col">
        <div>
          <h2>Settings</h2>
          <pre style={{ whiteSpace: 'pre-wrap', fontSize: 12, color: 'var(--muted)' }}>{JSON.stringify(run?.settings || {}, null, 2)}</pre>
        </div>
        <div>
          <h2>Run details</h2>
          <p>Status: <strong>{run?.status || '—'}</strong></p>
          <p>Created: {fmtDate(run?.created_at)}</p>
          <p>Started: {fmtDate(run?.started_at)}</p>
          <p>Completed: {fmtDate(run?.completed_at)}</p>
          {run?.error ? <p style={{ color: 'var(--red)' }}>{run.error}</p> : null}
        </div>
      </section>

      <FoldableTable title="Backtest trades" columns={tradeColumns} rows={trades || []} empty="No trades yet" />
    </div>
  )
}
