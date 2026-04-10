import { useCallback, useEffect, useRef, useState } from 'react'
import PageHeader from '../components/PageHeader'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate } from '../lib/format'

const WORKERS = ['pipeline', 'executor', 'scheduler']

function dot(running) {
  return (
    <span style={{
      display: 'inline-block',
      width: 10, height: 10,
      borderRadius: '50%',
      background: running ? 'var(--green)' : 'var(--red)',
      marginRight: 8,
      flexShrink: 0,
      boxShadow: running ? '0 0 6px var(--green)' : 'none',
    }} />
  )
}

function WorkerCard({ name, info, onAction }) {
  const running = info?.running ?? false
  const [busy, setBusy] = useState(false)

  async function act(fn) {
    setBusy(true)
    try { await fn() } finally { setBusy(false) }
  }

  return (
    <div className="stat-card" style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        {dot(running)}
        <span style={{ fontWeight: 700, textTransform: 'capitalize', fontSize: 15 }}>{name}</span>
        <span style={{ marginLeft: 'auto', fontSize: 12, color: running ? 'var(--green)' : 'var(--red)', fontWeight: 700 }}>
          {running ? 'Running' : 'Stopped'}
        </span>
      </div>
      {info?.pid ? <div className="stat-hint">PID {info.pid}</div> : null}
      <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
        <button
          className="button"
          style={{ flex: 1, padding: '8px 10px', fontSize: 13, background: running ? 'var(--line)' : 'var(--green)', color: 'white' }}
          disabled={busy || running}
          onClick={() => act(() => api.startWorker(name)).then(onAction)}
        >Start</button>
        <button
          className="button"
          style={{ flex: 1, padding: '8px 10px', fontSize: 13, background: running ? 'var(--red)' : 'var(--line)', color: 'white' }}
          disabled={busy || !running}
          onClick={() => act(() => api.stopWorker(name)).then(onAction)}
        >Stop</button>
      </div>
    </div>
  )
}

function RunsTable({ runs }) {
  if (!runs.length) return <p style={{ color: 'var(--muted)', fontSize: 14 }}>No pipeline runs recorded yet.</p>

  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            <th>Run ID</th>
            <th>Status</th>
            <th>Symbols</th>
            <th>Candles</th>
            <th>Candidates</th>
            <th>Errors</th>
            <th>Duration</th>
            <th>Started</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((r) => {
            const duration = r.started_at && r.completed_at
              ? ((new Date(r.completed_at) - new Date(r.started_at)) / 1000).toFixed(1) + 's'
              : '—'
            const errCount = (r.stats?.errors || []).length
            return (
              <tr key={r.run_id}>
                <td style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--muted)' }}>{r.run_id}</td>
                <td>
                  <span className={`badge ${r.status === 'completed' ? 'green' : r.status === 'running' ? 'blue' : 'orange'}`}>
                    {r.status}
                  </span>
                </td>
                <td>{r.symbols_scanned ?? '—'} / {r.symbols_total ?? '—'}</td>
                <td>{r.stats?.candles_written ?? '—'}</td>
                <td>{r.stats?.candidates_created ?? '—'}</td>
                <td style={{ color: errCount > 0 ? 'var(--red)' : 'inherit' }}>{errCount}</td>
                <td>{duration}</td>
                <td style={{ fontSize: 12 }}>{fmtDate(r.started_at)}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function colorLine(line) {
  const l = line.toLowerCase()
  if (l.includes('error') || l.includes('exception') || l.includes('traceback') || l.includes('critical')) {
    return 'var(--red)'
  }
  if (l.includes('warning') || l.includes('warn')) return 'var(--orange)'
  if (l.includes('started') || l.includes('complete') || l.includes('success') || l.includes('ok')) return 'var(--green)'
  if (l.match(/^\d{4}-\d{2}-\d{2}/) || l.match(/^info/i)) return 'var(--muted)'
  return 'var(--text)'
}

function LogViewer({ worker }) {
  const termRef = useRef(null)
  const [autoScroll, setAutoScroll] = useState(true)
  const loader = useCallback(() => api.workerLogs(worker, 300), [worker])
  const { data, loading, error, refresh } = usePollingQuery(loader, 5000)

  const lines = data?.lines || []
  const sizeKb = data?.size_bytes ? (data.size_bytes / 1024).toFixed(1) : null

  useEffect(() => {
    if (autoScroll && termRef.current) {
      termRef.current.scrollTop = termRef.current.scrollHeight
    }
  }, [lines, autoScroll])

  function handleScroll() {
    const el = termRef.current
    if (!el) return
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40
    setAutoScroll(atBottom)
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <span style={{ fontSize: 13, color: 'var(--muted)' }}>
          {data?.path || 'No log file found'}
          {sizeKb ? ` — ${sizeKb} KB` : ''}
          {lines.length ? ` — ${lines.length} lines` : ''}
        </span>
        <button
          className="button"
          style={{ marginLeft: 'auto', padding: '6px 12px', fontSize: 12 }}
          onClick={refresh}
          disabled={loading}
        >{loading ? 'Loading…' : 'Refresh'}</button>
        <label style={{ fontSize: 12, color: 'var(--muted)', display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' }}>
          <input type="checkbox" checked={autoScroll} onChange={(e) => setAutoScroll(e.target.checked)} />
          Auto-scroll
        </label>
      </div>

      {error && <p style={{ color: 'var(--red)', fontSize: 13 }}>{error}</p>}

      <div
        ref={termRef}
        onScroll={handleScroll}
        style={{
          background: '#07111f',
          border: '1px solid var(--line)',
          borderRadius: 12,
          padding: '14px 16px',
          height: 420,
          overflowY: 'auto',
          fontFamily: '"JetBrains Mono", "Fira Code", "Cascadia Code", monospace',
          fontSize: 12,
          lineHeight: 1.65,
        }}
      >
        {lines.length === 0 && !loading && (
          <span style={{ color: 'var(--muted)' }}>
            {data?.path === null ? 'No log file found for this worker.' : 'Log file is empty.'}
          </span>
        )}
        {lines.map((line, i) => (
          <div key={i} style={{ color: colorLine(line), whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
            {line}
          </div>
        ))}
      </div>
    </div>
  )
}

export default function LogsPage() {
  const [activeLog, setActiveLog] = useState('pipeline')
  const [refreshKey, setRefreshKey] = useState(0)

  const workersLoader = useCallback(() => api.workerStatus(), [refreshKey])
  const runsLoader = useCallback(() => api.liveRuns('?limit=20'), [])

  const { data: workersRaw, refresh: refreshWorkers } = usePollingQuery(workersLoader, 6000)
  const { data: runs = [] } = usePollingQuery(runsLoader, 10000)

  const workers = workersRaw || {}

  function handleWorkerAction() {
    setRefreshKey((k) => k + 1)
    setTimeout(refreshWorkers, 1000)
  }

  const runningCount = WORKERS.filter((w) => workers[w]?.running).length

  const tabStyle = (active) => ({
    padding: '8px 18px',
    borderRadius: '10px 10px 0 0',
    border: '1px solid var(--line)',
    borderBottom: active ? '1px solid var(--panel)' : '1px solid var(--line)',
    background: active ? 'var(--panel)' : 'transparent',
    color: active ? 'var(--text)' : 'var(--muted)',
    fontWeight: active ? 700 : 400,
    cursor: 'pointer',
    fontSize: 13,
  })

  return (
    <div className="page-stack">
      <PageHeader
        title="Logs"
        subtitle="Worker status, pipeline run history and live log tails."
      />

      <div className="stats-grid" style={{ gridTemplateColumns: 'repeat(3, minmax(0,1fr))' }}>
        {WORKERS.map((name) => (
          <WorkerCard key={name} name={name} info={workers[name]} onAction={handleWorkerAction} />
        ))}
      </div>

      <section className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
          <h2 style={{ margin: 0 }}>Pipeline runs</h2>
          <span style={{ fontSize: 13, color: 'var(--muted)' }}>{runningCount} / {WORKERS.length} workers running</span>
        </div>
        <RunsTable runs={runs} />
      </section>

      <section className="panel">
        <h2 style={{ marginBottom: 14 }}>Log viewer</h2>
        <div style={{ display: 'flex', gap: 4, marginBottom: -1 }}>
          {WORKERS.map((w) => (
            <button key={w} style={tabStyle(activeLog === w)} onClick={() => setActiveLog(w)}>
              {dot(workers[w]?.running)}{w}
            </button>
          ))}
        </div>
        <div style={{ border: '1px solid var(--line)', borderRadius: '0 12px 12px 12px', padding: 16, background: 'var(--panel)' }}>
          <LogViewer key={activeLog} worker={activeLog} />
        </div>
      </section>
    </div>
  )
}
