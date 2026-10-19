import { useState } from 'react'
import { api } from '../lib/api'
import { fmtDate } from '../lib/format'

const modes = {
  global: 'Diagnostic global', losses: 'Audit des pertes',
  execution: 'Entrées et sorties', improvements: 'Plan d’amélioration',
}

export default function MomentumAnalysis() {
  const [busy, setBusy] = useState(false)
  const [previewBusy, setPreviewBusy] = useState(false)
  const [preview, setPreview] = useState(null)
  const [reports, setReports] = useState([])
  const [selected, setSelected] = useState(0)
  const [mode, setMode] = useState('global')
  const [days, setDays] = useState(7)
  const [question, setQuestion] = useState('')
  const [error, setError] = useState('')
  const result = reports[selected]

  async function inspect() {
    setPreviewBusy(true)
    setError('')
    try { setPreview(await api.momentumAnalysisPreview(days)) }
    catch (err) { setError(err.message || String(err)) }
    finally { setPreviewBusy(false) }
  }
  async function analyze() {
    setBusy(true)
    setError('')
    try {
      const report = await api.momentumAnalysis({ mode, days, question })
      setReports(previous => [report, ...previous].slice(0, 10))
      setSelected(0)
    } catch (err) { setError(err.message || String(err)) }
    finally { setBusy(false) }
  }
  function download() {
    if (!result) return
    const content = `# ${modes[result.mode]}\n\nInstantané : ${result.as_of}\nHistorique : ${result.days} jour(s)\nModèle : ${result.model}\nQuestion : ${result.question || '—'}\n\n${result.report}\n\nSources consultées : ${JSON.stringify(result.tool_calls || [])}\n\nLimites : ${JSON.stringify(result.limits)}\nTroncature : ${JSON.stringify(result.truncated)}\n`
    const url = URL.createObjectURL(new Blob([content], { type: 'text/markdown;charset=utf-8' }))
    const link = document.createElement('a')
    link.href = url
    link.download = `momentum-ia-${result.mode}-${result.as_of.replace(/[:.]/g, '-')}.md`
    link.click()
    setTimeout(() => URL.revokeObjectURL(url), 1000)
  }
  return <>
    <section className="panel">
      <h2>Centre d’analyse IA</h2>
      <p>Analyse globale du portefeuille et du marché pour identifier les risques et les règles à améliorer.</p>
      <div className="stats-grid">
        <label>Objectif<br /><select value={mode} onChange={e => setMode(e.target.value)} disabled={busy}>
          {Object.entries(modes).map(([key, title]) => <option value={key} key={key}>{title}</option>)}
        </select></label>
        <label>Historique des transactions et décisions<br /><select value={days} disabled={busy || previewBusy} onChange={e => { setDays(Number(e.target.value)); setPreview(null) }}>
          <option value={1}>24 heures</option><option value={7}>7 jours</option><option value={30}>30 jours</option>
        </select></label>
      </div>
      <p>Les positions et indicateurs reflètent l’état actuel. Les bougies couvrent les 32 dernières périodes disponibles par unité de temps ; elles ne couvrent pas nécessairement toute la durée sélectionnée.</p>
      <label style={{ display: 'block' }}>Question complémentaire (facultative)
        <textarea value={question} maxLength={1000} disabled={busy} onChange={e => setQuestion(e.target.value)} rows={3} style={{ display: 'block', width: '100%', boxSizing: 'border-box' }} placeholder="Ex. Les entrées tardives ou les rotations fréquentes expliquent-elles les pertes observées ?" />
      </label>
      <p>Chaque analyse transmet un instantané à OpenAI et utilise l’API. Les propositions doivent être testées avant application.</p>
      <button onClick={inspect} disabled={previewBusy || busy}>{previewBusy ? 'Chargement…' : 'Vérifier les données disponibles'}</button>{' '}
      <button onClick={analyze} disabled={busy || previewBusy}>{busy ? 'Analyse en cours…' : 'Lancer l’analyse IA'}</button>
      {error && <p className="error" role="alert">{error}</p>}
    </section>
    {preview && <section className="panel">
      <h2>Données disponibles</h2>
      <p>Instantané : {fmtDate(preview.as_of)} · depuis le {fmtDate(preview.history_since)} · {preview.configured ? `IA configurée (${preview.model})` : 'Clé OpenAI à configurer sur le serveur'}</p>
      <div className="stats-grid">
        <div>Positions ouvertes : <strong>{preview.metrics.open_positions}</strong></div>
        <div>Positions en perte latente : <strong>{preview.metrics.losing_open_positions}</strong></div>
        <div>Ventes en perte : <strong>{preview.metrics.losing_sell_trades} / {preview.metrics.sell_trades}</strong></div>
        <div>Décisions : <strong>{preview.counts.decisions}</strong> · Ordres liés : <strong>{preview.counts.orders}</strong></div>
      </div>
      <p>Comptages calculés sur l’échantillon chargé. PnL indisponible pour {preview.metrics.missing_pnl_positions} position(s) ouverte(s). Les ordres sans lien avec les positions momentum ne sont pas inclus.</p>
      {Object.values(preview.truncated).some(Boolean) && <p>Échantillon tronqué : {Object.entries(preview.truncated).filter(([, value]) => value).map(([key]) => key).join(', ')}.</p>}
      <details><summary>Couverture des historiques de prix</summary>
        {preview.coverage.length ? <ul>{preview.coverage.map(row => <li key={`${row.symbol}-${row.interval}`}>
          {row.symbol} · {row.interval} : {row.count} bougies · dernière clôture : {row.last_close ? fmtDate(new Date(row.last_close).toISOString()) : 'aucune'}
        </li>)}</ul> : <p>Aucun historique de prix disponible.</p>}
      </details>
    </section>}
    {reports.length > 0 && <section className="panel" aria-live="polite">
      <h2>Rapports de cette session</h2>
      <p>Les 10 derniers rapports restent disponibles jusqu’à la fermeture ou au rechargement de cette page.</p>
      <label>Rapport <select value={selected} onChange={e => setSelected(Number(e.target.value))}>
        {reports.map((report, index) => <option key={`${report.as_of}-${index}`} value={index}>{modes[report.mode]} · {fmtDate(report.as_of)} · {report.days} j</option>)}
      </select></label>{' '}<button onClick={download}>Exporter le rapport (.md)</button>
      {result && <>
        <p>Instantané du {fmtDate(result.as_of)} · {result.model}</p>
        {result.tool_calls?.length > 0 && <details><summary>Sources consultées par l’IA ({result.tool_calls.length} appels)</summary>
          <ul>{result.tool_calls.map((call, index) => <li key={index}>{call.arguments?.dataset || call.name} · {call.arguments?.symbol || 'ensemble du marché'} · {call.status === 'ok' ? 'lecture réussie' : 'lecture non aboutie'}<pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(call.arguments, null, 2)}</pre></li>)}</ul>
        </details>}
        {result.question && <p>Question : {result.question}</p>}
        {Object.values(result.truncated || {}).some(Boolean) && <p>Analyse sur un échantillon limité.</p>}
        <div style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{result.report}</div>
      </>}
    </section>}
  </>
}
