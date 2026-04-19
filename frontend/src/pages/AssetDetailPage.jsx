import { useCallback } from 'react'
import { Link, useParams } from 'react-router-dom'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtNumber, fmtDate } from '../lib/format'

function summarizeContext(value) {
  if (!value || typeof value !== 'object') return '—'
  const type = value.type || '—'
  const level = value.level !== null && value.level !== undefined ? fmtNumber(value.level, 4) : '—'
  return `${type} @ ${level}`
}

function DebugRow({ label, value }) {
  return (
    <div className="debug-row">
      <div className="debug-label">{label}</div>
      <div className="debug-value">{typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value ?? '—')}</div>
    </div>
  )
}

export default function AssetDetailPage() {
  const { symbol = '' } = useParams()
  const loadAsset = useCallback(() => api.asset(symbol), [symbol])
  const { data: asset, loading, error } = usePollingQuery(loadAsset, 15000)
  const payload = asset?.state_payload || {}
  const breakdown = payload?.score_breakdown || {}

  return (
    <div className="page-stack">
      <PageHeader title={`${symbol} debug view`} subtitle="Detailed signal view with score, liquidity, target and session diagnostics." />
      <div className="page-actions">
        <Link className="button" to="/">Back to dashboard</Link>
      </div>
      {loading ? <div className="panel">Loading asset…</div> : null}
      {error ? <div className="panel error">{error}</div> : null}
      {asset ? (
        <>
          <div className="stats-grid">
            <StatCard label="Stage" value={asset.stage} />
            <StatCard label="Bias" value={asset.bias || '—'} />
            <StatCard label="Score" value={fmtNumber(asset.score, 2)} hint={`Updated ${fmtDate(asset.updated_at)}`} />
            <StatCard label="Session" value={payload.session_phase || asset.session || '—'} hint={`Filter ${payload.session_confirm_filter_enabled ? 'ON' : 'OFF'}`} />
          </div>
          <section className="panel two-col">
            <div>
              <h2>Signal summary</h2>
              <DebugRow label="Price" value={fmtNumber(asset.price, 6)} />
              <DebugRow label="RSI 5M" value={fmtNumber(asset.rsi_5m, 2)} />
              <DebugRow label="RSI 1H" value={fmtNumber(asset.rsi_1h, 2)} />
              <DebugRow label="RSI 4H" value={fmtNumber(payload.rsi_macro, 2)} />
              <DebugRow label="Zone quality" value={payload.zone_quality} />
              <DebugRow label="Confirm blocked by session" value={payload.confirm_blocked_by_session} />
              <DebugRow label="Confirm source" value={payload.confirm_source} />
            </div>
            <div>
              <h2>Contexts and target</h2>
              <DebugRow label="Macro liquidity" value={summarizeContext(payload.macro_liquidity_context || asset.liquidity_context)} />
              <DebugRow label="Entry liquidity" value={summarizeContext(payload.entry_liquidity_context)} />
              <DebugRow label="Execution target" value={summarizeContext(asset.execution_target)} />
              <DebugRow label="Projected target" value={summarizeContext(payload.projected_target)} />
              <DebugRow label="Trade" value={payload.trade} />
            </div>
          </section>
          <section className="panel two-col">
            <div>
              <h2>Score breakdown</h2>
              <DebugRow label="Liquidity" value={breakdown.liquidity} />
              <DebugRow label="Structure" value={breakdown.structure} />
              <DebugRow label="Confirmation" value={breakdown.confirmation} />
              <DebugRow label="Session" value={breakdown.session} />
              <DebugRow label="Quality" value={breakdown.quality} />
              <DebugRow label="Volume" value={breakdown.volume} />
              <DebugRow label="HTF alignment" value={breakdown.htf_alignment} />
              <DebugRow label="Market quality" value={breakdown.market_quality} />
              <DebugRow label="Target quality" value={breakdown.target_quality} />
            </div>
            <div>
              <h2>MSS / BOS structure</h2>
              <DebugRow label="MSS bull / bear" value={`${payload.mss_bull} / ${payload.mss_bear}`} />
              <DebugRow label="BOS bull / bear" value={`${payload.bos_bull} / ${payload.bos_bear}`} />
              <DebugRow label="Internal bear pivot high" value={fmtNumber(payload.internal_bear_pivot_high, 4)} />
              <DebugRow label="Internal bull pivot low" value={fmtNumber(payload.internal_bull_pivot_low, 4)} />
              <DebugRow label="External swing high" value={fmtNumber(payload.external_swing_high, 4)} />
              <DebugRow label="External swing low" value={fmtNumber(payload.external_swing_low, 4)} />
            </div>
          </section>
          <section className="panel two-col">
            <div>
              <h2>HTF debug</h2>
              <DebugRow label="Previous day high / low" value={`${fmtNumber(payload.previous_day_high, 4)} / ${fmtNumber(payload.previous_day_low, 4)}`} />
              <DebugRow label="Previous week high / low" value={`${fmtNumber(payload.previous_week_high, 4)} / ${fmtNumber(payload.previous_week_low, 4)}`} />
              <DebugRow label="EQH / EQL 1H" value={`${payload.equal_highs_1h} / ${payload.equal_lows_1h}`} />
              <DebugRow label="EQH / EQL 4H" value={`${payload.equal_highs_4h} / ${payload.equal_lows_4h}`} />
              <DebugRow label="Volume debug" value={payload.volume_debug} />
              <DebugRow label="Market quality debug" value={payload.market_quality_debug} />
            </div>
            <div>
              <h2>Raw state payload</h2>
              <DebugRow label="Payload" value={payload} />
            </div>
          </section>
        </>
      ) : null}
    </div>
  )
}
