import { useCallback, useEffect, useRef, useState } from 'react'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtNumber } from '../lib/format'
const seriesColor = (symbol) => {
  const base = symbol.replace(/USDC$|USD$/, '')
  const known = { BTC: '#ffa62b', XBT: '#ffa62b', ETH: '#b58aff', SOL: '#22d3b6' }
  let hash = 0
  for (const char of base) hash = (hash * 31 + char.charCodeAt(0)) >>> 0
  return known[base] || `hsl(${hash % 360} 75% 68%)`
}
const dateLabel = (d) => new Intl.DateTimeFormat('fr-FR', { day: '2-digit', month: 'short', timeZone: 'UTC' }).format(new Date(`${d}T12:00:00Z`))
export default function KrakenProfitChart() {
  const [days, setDays] = useState(30), [currency, setCurrency] = useState('USD')
  const [hidden, setHidden] = useState([]), [selected, setSelected] = useState(null), [width, setWidth] = useState(700)
  const holder = useRef(null)
  const { data, error } = usePollingQuery(useCallback(() => api.positionsProfitHistory(days), [days]), 30000, { queryKey: `kraken-profit-${days}` })
  useEffect(() => {
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(240, entry.contentRect.width)))
    observer.observe(holder.current)
    return () => observer.disconnect()
  }, [])
  const ready = data?.days === days
  const currencies = [...new Set((data?.series || []).map((s) => s.currency))]
  const unit = currencies.includes(currency) ? currency : currencies[0] || currency
  const series = (data?.series || []).filter((s) => s.currency === unit)
  const visible = series.filter((s) => !hidden.includes(s.symbol)), dates = data?.dates || []
  const index = dates.includes(selected) ? dates.indexOf(selected) : dates.length - 1
  const values = visible.flatMap((s) => s.values), lo = Math.min(0, ...values), hi = Math.max(0, ...values)
  const span = hi - lo || 1, min = lo - span * .1, max = hi + span * .1
  const x = (i) => 78 + i * (width - 96) / Math.max(1, dates.length - 1)
  const y = (v) => 16 + (max - v) / (max - min) * 239
  const color = (s) => seriesColor(s.symbol)
  const signed = (v) => `${v > 0 ? '+' : ''}${fmtNumber(v, 2)}`
  const tickEvery = Math.max(1, Math.ceil((dates.length - 1) / (width < 450 ? 3 : 6)))
  return <section className="panel kraken-profit" aria-label="Historique des profits Kraken">
    <div className="kraken-profit-heading"><div><h2>Profit réalisé par jour</h2><p className="stat-hint">Kraken · {unit} · hors frais · journées Europe/Paris</p></div>
      <div className="kraken-profit-controls" aria-label="Période">
        {[7, 30, 90].map((n) => <button type="button" className={`filter-chip ${days === n ? 'active' : ''}`} aria-pressed={days === n} onClick={() => { setDays(n); setSelected(null) }} key={n}>{n} j</button>)}
        {currencies.length > 1 && <label>Devise <select value={unit} onChange={(e) => setCurrency(e.target.value)}>{currencies.map((c) => <option key={c}>{c}</option>)}</select></label>}
      </div>
    </div>
    {error && <div className="panel error" role="alert">Historique indisponible : {error}</div>}
    <div ref={holder}>
      {!ready ? <p role="status">Chargement de l’historique…</p> : !series.length ? <p>Aucun profit réalisé vérifié sur cette période.</p> : <>
        <svg width="100%" viewBox={`0 0 ${width} 300`} role="img" aria-label={`Profits quotidiens en ${unit} par crypto. Détails sous le graphique.`}>
          {Array.from({ length: 5 }, (_, i) => min + i * (max - min) / 4).map((v, i) => <g key={i}><line x1="78" x2={width - 18} y1={y(v)} y2={y(v)} stroke="var(--line)" /><text x="70" y={y(v) + 4} textAnchor="end" fill="var(--muted)" fontSize="11">{fmtNumber(v, 2)}</text></g>)}
          <line x1="78" x2={width - 18} y1={y(0)} y2={y(0)} stroke="var(--text)" opacity=".5" />
          {dates.map((d, i) => i % tickEvery === 0 || i === dates.length - 1 ? <text key={d} x={x(i)} y="285" textAnchor={i === dates.length - 1 ? 'end' : i === 0 ? 'start' : 'middle'} fill="var(--muted)" fontSize="11">{dateLabel(d)}</text> : null)}
          {visible.map((s) => <g key={s.symbol}><polyline fill="none" stroke={color(s)} strokeWidth="2" points={s.values.map((v, i) => `${x(i)},${y(v)}`).join(' ')} />{s.values.map((v, i) => <circle key={i} cx={x(i)} cy={y(v)} r={index === i ? 4 : 2.5} fill={color(s)} onMouseEnter={() => setSelected(dates[i])}><title>{s.symbol} · {dateLabel(dates[i])} : {signed(v)} {unit}</title></circle>)}</g>)}
        </svg>
        <div className="kraken-profit-legend" aria-label="Afficher ou masquer les cryptos">{series.map((s) => <button type="button" key={s.symbol} className="filter-chip" aria-pressed={!hidden.includes(s.symbol)} onClick={() => setHidden((old) => old.includes(s.symbol) ? old.filter((v) => v !== s.symbol) : [...old, s.symbol])}><span style={{ background: color(s) }} />{s.symbol}</button>)}</div>
        <div className="kraken-profit-detail"><label>Détail du <select value={dates[index] || ''} onChange={(e) => setSelected(e.target.value)}>{dates.map((d) => <option value={d} key={d}>{dateLabel(d)}</option>)}</select></label><div aria-live="polite">{visible.map((s) => <span key={s.symbol}>{s.symbol} : <strong>{signed(s.values[index])} {unit}</strong></span>)}{!visible.length && 'Sélectionnez une crypto dans la légende.'}</div></div>
      </>}
    </div>
    <p className="stat-hint">Totaux journaliers enregistrés à chaque clôture vérifiée · hors frais, sans cumul. Anciennes clôtures non vérifiées non incluses.</p>
  </section>
}
