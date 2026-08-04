import { useCallback, useMemo, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate } from '../lib/format'

const REQUIRED_TIMEFRAMES = ['15m', '1h', '4h']
const TERMINAL_PHASES = new Set(['completed', 'insufficient_data', 'skipped', 'failed'])

const list = (value) => Array.isArray(value) ? value : []
const count = (value) => Array.isArray(value) ? value.length : Number(value || 0)
const errorText = (value) => value?.message || String(value || '')

export function availableTimeframes(data = {}) {
  const direct = data.available_timeframes || data.timeframes
  if (Array.isArray(direct)) return direct
  if (direct && typeof direct === 'object') return Object.keys(direct).filter((key) => Number(direct[key]) > 0)
  return []
}

export function missingTimeframes(data = {}) {
  const available = new Set(availableTimeframes(data))
  return REQUIRED_TIMEFRAMES.filter((timeframe) => !available.has(timeframe))
}

function duration(run) {
  if (run?.duration_seconds != null) return `${Number(run.duration_seconds).toFixed(1)}s`
  if (!run?.started_at) return '—'
  const end = run.finished_at || run.completed_at || (String(run.status).toLowerCase() === 'running' ? new Date().toISOString() : null)
  return end ? `${Math.max(0, (new Date(end) - new Date(run.started_at)) / 1000).toFixed(1)}s` : '—'
}

function progress(run = {}) {
  const counters = run.counters || run.summary || run.progress || {}
  const total = Number(counters.total ?? run.total ?? 0)
  const processed = Number(counters.processed ?? run.processed ?? 0)
  const percent = Number(run.percentage ?? run.percent ?? (total ? processed / total * 100 : TERMINAL_PHASES.has(String(run.status).toLowerCase()) ? 100 : 0))
  return { counters, total, processed, percent: Math.max(0, Math.min(100, percent)) }
}

export function PreviewSummary({ preview }) {
  if (!preview) return null
  const eligible = preview.eligible ?? preview.eligible_count ?? preview.asset_count ?? 0
  const ignored = preview.ignored ?? preview.ignored_count ?? 0
  const current = preview.already_up_to_date ?? preview.up_to_date ?? preview.skipped ?? 0
  const incomplete = preview.incomplete ?? preview.incomplete_count ?? preview.insufficient_data ?? 0
  return <div className="panel" data-testid="analysis-preview" style={{ marginTop: 12 }}>
    <h3>Prévisualisation avant confirmation</h3>
    <div className="stats-grid">
      <StatCard label="Actifs éligibles" value={eligible} />
      <StatCard label="Ignorés" value={ignored} />
      <StatCard label="Déjà à jour" value={current} />
      <StatCard label="Incomplets" value={incomplete} />
    </div>
    <p className="stat-hint">Vérifiez ce périmètre avant de mettre le job en file ou de l’exécuter immédiatement.</p>
  </div>
}

export function AnalysisSection({ title, description, defaultEngine, data, universes, onMessage, refresh }) {
  const [engine, setEngine] = useState(defaultEngine)
  const [universe, setUniverse] = useState('')
  const [assetType, setAssetType] = useState('')
  const [preview, setPreview] = useState(null)
  const [busy, setBusy] = useState(false)
  const missing = missingTimeframes(data)
  const blocked = missing.length > 0
  const payload = { action: 'analysis', job_type: 'analysis', market_scope: 'stock_etf', engine, universe: universe || undefined, asset_type: assetType || undefined, timeframes: REQUIRED_TIMEFRAMES, limit: 500 }

  async function perform(label, request, confirm = false) {
    if (blocked) return
    if (confirm && !preview) { onMessage('Prévisualisez le périmètre avant confirmation.'); return }
    if (confirm && !window.confirm(`${label} pour ${preview?.eligible ?? preview?.eligible_count ?? preview?.asset_count ?? 0} actif(s) éligible(s) ?`)) return
    setBusy(true)
    onMessage(`${label}…`)
    try {
      const result = await request(payload)
      if (label === 'Prévisualiser') setPreview(result)
      onMessage(`${label} : ${result?.message || 'terminé'}`)
      refresh?.()
    } catch (error) { onMessage(`${label} impossible : ${errorText(error)}`) } finally { setBusy(false) }
  }

  return <section className="panel" data-testid={`analysis-${defaultEngine}`}>
    <h2>{title}</h2><p className="stat-hint">{description}</p>
    <div className="page-actions" style={{ flexWrap: 'wrap', marginTop: 12 }}>
      <label>Moteur <select aria-label={`${title} moteur`} value={engine} onChange={(event) => { setEngine(event.target.value); setPreview(null) }}><option value="wyckoff_smc">wyckoff_smc</option><option value="momentum">momentum</option><option value="both">Les deux</option></select></label>
      <label>Univers <select aria-label={`${title} univers`} value={universe} onChange={(event) => { setUniverse(event.target.value); setPreview(null) }}><option value="">Tous</option>{universes.map((item) => <option key={item.id || item.name} value={item.name}>{item.name}</option>)}</select></label>
      <label>Type d’actif <select aria-label={`${title} type d'actif`} value={assetType} onChange={(event) => { setAssetType(event.target.value); setPreview(null) }}><option value="">Tous</option><option value="STOCK">STOCK</option><option value="ETF">ETF</option></select></label>
    </div>
    <p className="stat-hint"><strong>Timeframes requis :</strong> {REQUIRED_TIMEFRAMES.join(' · ')}. Le daily ne remplace jamais une donnée intraday.</p>
    {blocked ? <div className="panel error" role="alert" data-testid="insufficient-data"><strong>Exécution bloquée — données indispensables absentes :</strong> {missing.join(', ')}.</div> : null}
    <div className="page-actions" style={{ flexWrap: 'wrap', marginTop: 12 }}>
      <button className="button" disabled={busy || blocked} onClick={() => perform('Prévisualiser', api.previewMarketAction)}>Prévisualiser</button>
      <button className="button" disabled={busy || blocked || !preview} onClick={() => perform('Mettre en file', api.queueMarketJob, true)}>Mettre en file</button>
      <button className="button" disabled={busy || blocked || !preview} onClick={() => perform('Exécuter maintenant', api.runMarketAnalysis, true)}>Exécuter maintenant</button>
    </div>
    <PreviewSummary preview={preview} />
  </section>
}

export function JobRow({ job, onRetry }) {
  const { counters, total, processed, percent } = progress(job)
  const status = String(job.status || 'queued').toLowerCase()
  const retryable = status === 'failed' && (job.retryable ?? Number(job.attempts || 0) < Number(job.max_attempts || 3))
  return <tr data-testid={`job-${job.id}`}><td>#{job.id}</td><td>{status}</td><td>{job.phase || counters.phase || 'queued'}</td><td style={{ minWidth: 170 }}><progress value={percent} max="100" /> <strong>{percent.toFixed(0)}%</strong><div className="stat-hint">{processed} / {total || '—'}</div></td><td>{fmtDate(job.heartbeat_at)}</td><td>{duration(job)}</td><td>{job.last_error || '—'}</td><td><button className="button" disabled={!retryable} onClick={() => onRetry(job)}>Relancer les échecs</button></td></tr>
}

export function WorkerSection({ workers = {}, controlSupported, onAction }) {
  const rows = Object.entries(workers)
  return <section className="panel" data-testid="workers"><h2>Workers</h2>
    {!rows.length ? <p className="stat-hint">Aucun état de worker disponible.</p> : <div className="table-wrap"><table className="data-table"><thead><tr><th>Worker</th><th>Process</th><th>Heartbeat</th><th>Profondeur de file</th><th>Dernier job</th>{controlSupported ? <th>Actions</th> : null}</tr></thead><tbody>{rows.map(([name, worker]) => {
      const running = worker.process_state ? worker.process_state === 'running' : Boolean(worker.running)
      const queue = worker.queue || {}
      const depth = worker.queue_depth ?? Object.entries(queue.counts || {}).filter(([key]) => ['queued', 'running'].includes(key)).reduce((sum, [, value]) => sum + Number(value), 0)
      return <tr key={name}><td><strong>{name}</strong></td><td>{running ? 'running' : 'stopped'}</td><td>{fmtDate(worker.heartbeat_at)}</td><td>{depth}</td><td>{worker.last_job?.id || worker.last_job_id || queue.last_job_id || '—'}</td>{controlSupported ? <td><button className="button" disabled={running} onClick={() => onAction('start', name)}>Start</button> <button className="button" disabled={!running} onClick={() => onAction('stop', name)}>Stop</button></td> : null}</tr>
    })}</tbody></table></div>}
    {!controlSupported ? <p className="stat-hint">Le contrôle start/stop est masqué pour ce mode de déploiement.</p> : null}
  </section>
}

export default function MarketDataAdminPage() {
  const [message, setMessage] = useState('')
  const settingsQuery = usePollingQuery(useCallback(() => api.marketDataSettings(), []), 10000)
  const envQuery = usePollingQuery(useCallback(() => api.envSettings(), []), 30000)
  const assetsQuery = usePollingQuery(useCallback(() => api.stockEtfAssets('?limit=500'), []), 30000)
  const workersQuery = usePollingQuery(useCallback(() => api.workerStatus(), []), 6000)
  const data = settingsQuery.data || {}
  const assets = Array.isArray(assetsQuery.data) ? assetsQuery.data : list(assetsQuery.data?.assets)
  const universes = list(data.universes)
  const jobs = list(data.job_requests)
  const runs = [...list(data.analysis_runs), ...list(data.import_runs)].sort((a, b) => String(b.started_at || '').localeCompare(String(a.started_at || '')))
  const freshness = data.freshness_by_universe || data.universe_freshness || {}
  const lastImport = data.last_import_run || {}
  const columns = useMemo(() => [
    { key: 'symbol', title: 'Symbole', render: (row) => <strong>{row.provider_symbol}</strong>, sortValue: (row) => row.provider_symbol },
    { key: 'name', title: 'Nom', render: (row) => row.name || '—' }, { key: 'universe', title: 'Univers', render: (row) => row.universe_name || '—' },
    { key: 'type', title: 'Type', render: (row) => row.asset_type }, { key: 'status', title: 'Données', render: (row) => row.data_status || '—' },
  ], [])

  async function feederAction(label, payload) {
    setMessage(`${label}…`)
    try { const result = payload ? await api.queueMarketJob(payload) : await api.syncMarketAssets(); setMessage(`${label} : ${result?.message || 'terminé'}`); settingsQuery.refresh?.(); assetsQuery.refresh?.() } catch (error) { setMessage(`${label} impossible : ${errorText(error)}`) }
  }
  async function retry(job) {
    if (!window.confirm(`Relancer uniquement les échecs admissibles du job #${job.id} ?`)) return
    let payload = job.payload || {}
    if (typeof payload === 'string') {
      try { payload = JSON.parse(payload || '{}') } catch { payload = {} }
    }
    await feederAction('Retry partiel', { ...payload, job_type: job.job_type, retry_of: job.id, retry_failed_only: true })
  }
  async function workerAction(action, name) {
    if (action === 'stop' && !window.confirm(`Arrêter le worker ${name} ?`)) return
    try { await (action === 'start' ? api.startWorker(name) : api.stopWorker(name)); setMessage(`Worker ${name} ${action === 'start' ? 'démarré' : 'arrêté'}.`); workersQuery.refresh?.() } catch (error) { setMessage(`Action worker impossible : ${errorText(error)}`) }
  }

  const controlSupported = Boolean(envQuery.data?.worker_control_supported || envQuery.data?.deployment?.worker_control_supported)
  return <div className="page-stack">
    <PageHeader title="Admin · Données ETF & actions" subtitle="Pilotage du feeder IBKR et des analyses Stock/ETF, sans export ni intégration Confluence." />
    {settingsQuery.loading ? <div className="panel" data-testid="loading">Chargement des données de marché…</div> : null}
    {[settingsQuery.error, envQuery.error, assetsQuery.error, workersQuery.error].filter(Boolean).map((error, index) => <div className="panel error" role="alert" key={index}><strong>Chargement impossible.</strong> {errorText(error)}</div>)}
    {message ? <div className="panel" role="status">{message}</div> : null}

    <section className="panel"><h2>État du feeder</h2><div className="stats-grid">
      <StatCard label="Dernière synchro actifs" value={fmtDate(data.last_asset_sync_at || data.last_sync_at)} />
      <StatCard label="Dernier import" value={fmtDate(lastImport.finished_at || lastImport.started_at)} />
      <StatCard label="Actifs mis à jour" value={lastImport.assets_updated ?? lastImport.updated_assets ?? 0} />
      <StatCard label="Chandelles insérées" value={lastImport.candles_inserted ?? lastImport.inserted_candles ?? 0} />
      <StatCard label="Erreurs" value={count(lastImport.errors ?? lastImport.error_count)} />
      <StatCard label="Timeframes disponibles" value={availableTimeframes(data).join(' · ') || 'Aucun'} />
    </div><div className="page-actions" style={{ marginTop: 12 }}><button className="button" onClick={() => feederAction('Synchronisation des actifs')}>Synchroniser les actifs</button><button className="button" onClick={() => feederAction('Import mis en file', { job_type: 'backfill', action: 'backfill', market_scope: 'stock_etf' })}>Mettre un import en file</button></div>
    <h3>Fraîcheur par univers</h3>{Object.keys(freshness).length ? <ul>{Object.entries(freshness).map(([name, value]) => <li key={name}><strong>{name}</strong> : {typeof value === 'object' ? value.status || fmtDate(value.last_candle_at) : String(value)}</li>)}</ul> : <p className="stat-hint">Fraîcheur indisponible.</p>}</section>

    <section className="panel"><h2>Couverture des données</h2><div className="stats-grid"><StatCard label="Actifs" value={data.total_assets || assets.length} /><StatCard label="Chandelles" value={data.total_candles || 0} /><StatCard label="Complets" value={data.complete_assets ?? 0} /><StatCard label="Incomplets" value={data.incomplete_assets ?? 0} /></div><FoldableTable rows={assets} columns={columns} defaultSortKey="symbol" empty="Aucun actif Stock/ETF." /></section>
    <AnalysisSection title="Configuration Wyckoff / SMC" description="Contexte 4H, setup 1H et alignement 15m, avec les phases des dashboards : validating, analyzing et persisting." defaultEngine="wyckoff_smc" data={data} universes={universes} onMessage={setMessage} refresh={settingsQuery.refresh} />
    <AnalysisSection title="Configuration Momentum" description="Momentum partagé sur 15m, 1h et 4h, sans repli silencieux sur le daily." defaultEngine="momentum" data={data} universes={universes} onMessage={setMessage} refresh={settingsQuery.refresh} />

    <section className="panel"><h2>Files d’attente</h2>{jobs.length ? <div className="table-wrap"><table className="data-table"><thead><tr><th>Job</th><th>État</th><th>Phase courante</th><th>Progression</th><th>Heartbeat</th><th>Durée</th><th>Dernière erreur</th><th>Retry</th></tr></thead><tbody>{jobs.map((job) => <JobRow key={job.id} job={job} onRetry={retry} />)}</tbody></table></div> : <p className="stat-hint">Aucun job en file.</p>}</section>
    <WorkerSection workers={workersQuery.data || {}} controlSupported={controlSupported} onAction={workerAction} />
    <section className="panel"><h2>Historique des runs</h2>{runs.length ? <div className="table-wrap"><table className="data-table"><thead><tr><th>Run</th><th>Type / moteur</th><th>État</th><th>Phase</th><th>Début</th><th>Durée</th><th>Erreur</th></tr></thead><tbody>{runs.map((run, index) => <tr key={run.id || `${run.started_at}-${index}`}><td>#{run.id || '—'}</td><td>{run.engine || run.run_type || 'import'}</td><td>{run.status || '—'}</td><td>{run.phase || '—'}</td><td>{fmtDate(run.started_at)}</td><td>{duration(run)}</td><td>{run.last_error || run.error || '—'}</td></tr>)}</tbody></table></div> : <p className="stat-hint">Aucun run enregistré.</p>}</section>
  </div>
}
