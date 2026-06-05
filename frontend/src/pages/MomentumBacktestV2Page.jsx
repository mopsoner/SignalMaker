import { useCallback, useMemo, useState } from 'react'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import FoldableTable from '../components/FoldableTable'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'

const API_BASE = import.meta.env.VITE_API_BASE || ''
const req = async (path, options = {}) => {
  const res = await fetch(`${API_BASE}${path}`, { headers: { 'Content-Type': 'application/json' }, ...options })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}
const bt = {
  runs: () => req('/api/v1/momentum-backtest/runs?limit=20'),
  create: (settings) => req('/api/v1/momentum-backtest/runs', { method: 'POST', body: JSON.stringify({ settings }) }),
  sweep: () => req('/api/v1/momentum-backtest/runs/rsi-sweep', { method: 'POST', body: JSON.stringify({ ranges: [{ min: 45, max: 55 }, { min: 48, max: 60 }, { min: 50, max: 62 }, { min: 55, max: 65 }], base_settings: liveEngineSettings() }) }),
  compare: (ids) => req('/api/v1/momentum-backtest/compare?limit=800&run_ids=' + ids.join(',')),
}
function liveEngineSettings() {
  return { name: 'Live momentum engine rules', initial_capital: 1000, cadence_hours: 4, min_momentum_score: 0, fee_pct: 0.001, slippage_pct: 0.0005, max_symbols: 300, warmup_candles: 96 }
}
function n(v, d = 2) { return v == null || Number.isNaN(Number(v)) ? '—' : Number(v).toFixed(d) }
function Chart({ data }) {
  const lines = useMemo(() => {
    const all = (data || []).flatMap((s) => s.equity || [])
    if (!all.length) return []
    const vals = all.map((r) => Number(r.equity || 0)); const min = Math.min(...vals); const max = Math.max(...vals); const spread = max - min || 1
    return data.map((s, si) => ({ label: s.run?.settings?.name || s.run?.run_id, points: (s.equity || []).map((r, i, arr) => `${arr.length < 2 ? 0 : i / (arr.length - 1) * 100},${100 - ((Number(r.equity || 0) - min) / spread) * 100}`).join(' '), opacity: si === 0 ? 1 : 0.55 }))
  }, [data])
  if (!lines.length) return <p style={{ color: 'var(--muted)' }}>No curve yet.</p>
  return <><svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: '100%', height: 280, background: 'rgba(255,255,255,.03)', border: '1px solid var(--line)', borderRadius: 12 }}>{lines.map((l, i) => <polyline key={i} points={l.points} fill="none" stroke="currentColor" opacity={l.opacity} strokeWidth={i === 0 ? 1.8 : 1.1} vectorEffect="non-scaling-stroke" />)}</svg><div style={{ color: 'var(--muted)', fontSize: 12, marginTop: 8 }}>{lines.map((l, i) => <span key={i} style={{ marginRight: 12 }}>{i + 1}. {l.label}</span>)}</div></>
}
export default function MomentumBacktestV2Page() {
  const [busy, setBusy] = useState(false); const [msg, setMsg] = useState(''); const [settings, setSettings] = useState(JSON.stringify(liveEngineSettings(), null, 2))
  const { data: runs = [], refresh } = usePollingQuery(useCallback(() => bt.runs(), []), 6000)
  const completed = runs.filter((r) => r.status === 'completed')
  const ids = completed.slice(0, 5).map((r) => r.run_id)
  const { data: curves = [], refresh: refreshCurves } = usePollingQuery(useCallback(() => bt.compare(ids), [ids.join(',')]), 10000)
  const { data: workers = {}, refresh: refreshWorkers } = usePollingQuery(useCallback(() => api.workerStatus(), []), 6000)
  const latest = runs[0]
  async function action(fn, ok) { setBusy(true); setMsg(''); try { await fn(); await refresh(); await refreshCurves(); setMsg(ok) } catch (e) { setMsg(e.message || String(e)) } finally { setBusy(false) } }
  const cols = [{ key: 'name', title: 'Name', render: (r) => r.settings?.name || r.run_id }, { key: 'status', title: 'Status' }, { key: 'total_pnl_pct', title: 'PnL %', render: (r) => r.total_pnl_pct == null ? '—' : n(r.total_pnl_pct) + '%' }, { key: 'max_drawdown_pct', title: 'DD %', render: (r) => r.max_drawdown_pct == null ? '—' : n(r.max_drawdown_pct) + '%' }, { key: 'trade_count', title: 'Trades' }, { key: 'winrate', title: 'Winrate', render: (r) => r.winrate == null ? '—' : n(r.winrate, 1) + '%' }, { key: 'profit_factor', title: 'PF', render: (r) => n(r.profit_factor) }]
  return <div className="page-stack"><PageHeader title="Momentum Backtesting V2" subtitle="Backtest the exact same momentum engine and rules used by live execution." />
    <div className="stats-grid"><StatCard label="Latest" value={latest?.status || '—'} hint={latest?.settings?.name || latest?.run_id || ''} /><StatCard label="Worker" value={workers?.momentum_backtest?.running ? 'Running' : 'Stopped'} /><StatCard label="Latest PnL" value={latest?.total_pnl_pct == null ? '—' : n(latest.total_pnl_pct) + '%'} hint={`Equity ${n(latest?.final_equity)}`} /><StatCard label="Completed runs" value={completed.length} /></div>
    <section className="panel"><h2>Controls</h2><div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}><button className="button" disabled={busy || workers?.momentum_backtest?.running} onClick={() => action(() => api.startWorker('momentum_backtest').then(refreshWorkers), 'Worker started')}>Start worker</button><button className="button" disabled={busy} onClick={() => action(() => bt.create(liveEngineSettings()), 'Live-engine backtest queued')}>Run live-engine backtest</button><button className="button" disabled={busy} onClick={() => action(() => bt.sweep(), 'Compatibility runs queued')}>Run compatibility set</button><button className="button" disabled={busy} onClick={() => { refresh(); refreshCurves(); refreshWorkers() }}>Refresh</button>{msg ? <span style={{ color: 'var(--muted)' }}>{msg}</span> : null}</div><p style={{ color: 'var(--muted)', fontSize: 13 }}>Le backtest appelle le même MomentumEngineService que le live: mêmes règles d’entrée trade-ready, même structure 15m pour la sortie, mêmes constantes de pool et RSI. Les réglages custom ne changent que la simulation (capital, frais, slippage, cadence, taille dataset).</p></section>
    <section className="panel two-col"><div><h2>Custom config</h2><textarea value={settings} onChange={(e) => setSettings(e.target.value)} style={{ width: '100%', minHeight: 200, background: 'rgba(255,255,255,.03)', color: 'var(--text)', border: '1px solid var(--line)', borderRadius: 12, padding: 12 }} /><button className="button" style={{ marginTop: 10 }} disabled={busy} onClick={() => action(() => bt.create(JSON.parse(settings)), 'Custom backtest queued')}>Run custom</button></div><div><h2>What changed</h2><p>Les règles stratégie ne sont plus dupliquées ici: toute modification du moteur live est automatiquement utilisée par ce backtest. Les champs custom servent uniquement aux paramètres de simulation.</p></div></section>
    <section className="panel"><h2>Compare equity curves</h2><Chart data={curves} /></section><FoldableTable title="Backtest runs" columns={cols} rows={runs} empty="No runs yet" /></div>
}
