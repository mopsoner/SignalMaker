import { useEffect, useMemo, useState } from 'react'
import PageHeader from '../components/PageHeader'
import { api } from '../lib/api'

const EMPTY_SETTINGS = {
  general: {
    app_name: '',
    app_env: '',
    cors_origins: '',
    create_tables_on_boot: true,
  },
  binance: {
    binance_rest_base: '',
    binance_quote_assets: '',
    binance_symbol_status: '',
    binance_max_symbols: 25,
    binance_lookback_1m: 180,
    binance_lookback_5m: 180,
    binance_lookback_1h: 180,
    binance_lookback_4h: 120,
  },
  strategy: {
    session_timezone_offset_hours: -4,
    signal_rsi_period: 14,
    signal_swing_window: 8,
    signal_equal_level_tolerance_pct: 0.0015,
    signal_overbought: 70,
    signal_oversold: 30,
    signal_price_near_extreme_pct: 0.0025,
    signal_session_confirm_filter_enabled: true,
    planner_min_score: 4,
    planner_min_rr: 0.8,
  },
}

const gridStyle = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
  gap: '16px',
}

const fieldStyle = { display: 'grid', gap: '8px' }
const inputStyle = {
  width: '100%',
  background: 'rgba(9, 14, 24, 0.7)',
  color: 'var(--text)',
  border: '1px solid var(--line)',
  borderRadius: '12px',
  padding: '12px 14px',
}
const toolbarStyle = { display: 'flex', gap: '12px', flexWrap: 'wrap' }

function Field({ label, hint, children }) {
  return (
    <label style={fieldStyle}>
      <span style={{ fontSize: 14, fontWeight: 600 }}>{label}</span>
      {children}
      {hint ? <span className="stat-hint">{hint}</span> : null}
    </label>
  )
}

function Section({ title, description, children }) {
  return (
    <section className="panel">
      <div style={{ marginBottom: 16 }}>
        <h2>{title}</h2>
        <p className="stat-hint" style={{ marginTop: 6 }}>{description}</p>
      </div>
      <div style={gridStyle}>{children}</div>
    </section>
  )
}

export default function AdminSettingsPage() {
  const [settings, setSettings] = useState(EMPTY_SETTINGS)
  const [initialSettings, setInitialSettings] = useState(EMPTY_SETTINGS)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState('')

  async function loadSettings() {
    setLoading(true)
    try {
      const data = await api.adminSettings()
      setSettings(data)
      setInitialSettings(data)
      setMessage('')
    } catch (error) {
      setMessage(error.message || 'Failed to load settings')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadSettings()
  }, [])

  const dirty = useMemo(
    () => JSON.stringify(settings) !== JSON.stringify(initialSettings),
    [settings, initialSettings],
  )

  function updateField(section, key, rawValue, type = 'text') {
    const value =
      type === 'number'
        ? Number(rawValue)
        : type === 'checkbox'
          ? Boolean(rawValue)
          : rawValue

    setSettings((current) => ({
      ...current,
      [section]: {
        ...current[section],
        [key]: value,
      },
    }))
  }

  async function saveSettings() {
    setSaving(true)
    setMessage('')
    try {
      const saved = await api.updateAdminSettings(settings)
      setSettings(saved)
      setInitialSettings(saved)
      setMessage('Settings saved')
    } catch (error) {
      setMessage(error.message || 'Failed to save settings')
    } finally {
      setSaving(false)
    }
  }

  function resetChanges() {
    setSettings(initialSettings)
    setMessage('Unsaved changes discarded')
  }

  return (
    <div className="page-stack">
      <PageHeader
        title="Admin Settings"
        subtitle="Configure runtime, Binance collection defaults and signal engine thresholds."
      />

      <section className="panel">
        <div style={toolbarStyle}>
          <button className="button" onClick={saveSettings} disabled={loading || saving || !dirty}>
            {saving ? 'Saving…' : 'Save settings'}
          </button>
          <button className="button" onClick={resetChanges} disabled={loading || !dirty}>
            Reset changes
          </button>
          <button className="button" onClick={() => api.runPipeline()} disabled={loading}>
            Run pipeline once
          </button>
          <button className="button" onClick={() => api.runExecutor()} disabled={loading}>
            Run executor once
          </button>
        </div>
        {message ? <p className="stat-hint" style={{ marginTop: 14 }}>{message}</p> : null}
      </section>

      <Section title="General" description="Application identity and startup behavior.">
        <Field label="App name">
          <input style={inputStyle} value={settings.general.app_name} onChange={(event) => updateField('general', 'app_name', event.target.value)} disabled={loading} />
        </Field>
        <Field label="Environment">
          <input style={inputStyle} value={settings.general.app_env} onChange={(event) => updateField('general', 'app_env', event.target.value)} disabled={loading} />
        </Field>
        <Field label="CORS origins" hint="Comma-separated origins.">
          <input style={inputStyle} value={settings.general.cors_origins} onChange={(event) => updateField('general', 'cors_origins', event.target.value)} disabled={loading} />
        </Field>
        <Field label="Create tables on boot">
          <label style={{ display: 'inline-flex', gap: 10, minHeight: 46, alignItems: 'center' }}>
            <input type="checkbox" checked={Boolean(settings.general.create_tables_on_boot)} onChange={(event) => updateField('general', 'create_tables_on_boot', event.target.checked, 'checkbox')} disabled={loading} />
            <span>Enabled</span>
          </label>
        </Field>
      </Section>

      <Section title="Binance collector" description="Market data collection and filtering parameters.">
        <Field label="REST base URL">
          <input style={inputStyle} value={settings.binance.binance_rest_base} onChange={(event) => updateField('binance', 'binance_rest_base', event.target.value)} disabled={loading} />
        </Field>
        <Field label="Quote assets">
          <input style={inputStyle} value={settings.binance.binance_quote_assets} onChange={(event) => updateField('binance', 'binance_quote_assets', event.target.value)} disabled={loading} />
        </Field>
        <Field label="Symbol status">
          <input style={inputStyle} value={settings.binance.binance_symbol_status} onChange={(event) => updateField('binance', 'binance_symbol_status', event.target.value)} disabled={loading} />
        </Field>
        <Field label="Max symbols">
          <input style={inputStyle} type="number" value={settings.binance.binance_max_symbols} onChange={(event) => updateField('binance', 'binance_max_symbols', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Lookback 1m">
          <input style={inputStyle} type="number" value={settings.binance.binance_lookback_1m} onChange={(event) => updateField('binance', 'binance_lookback_1m', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Lookback 5m">
          <input style={inputStyle} type="number" value={settings.binance.binance_lookback_5m} onChange={(event) => updateField('binance', 'binance_lookback_5m', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Lookback 1h">
          <input style={inputStyle} type="number" value={settings.binance.binance_lookback_1h} onChange={(event) => updateField('binance', 'binance_lookback_1h', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Lookback 4h">
          <input style={inputStyle} type="number" value={settings.binance.binance_lookback_4h} onChange={(event) => updateField('binance', 'binance_lookback_4h', event.target.value, 'number')} disabled={loading} />
        </Field>
      </Section>

      <Section title="Signal engine" description="Thresholds for the current strategy and planning layer.">
        <Field label="Session timezone offset hours">
          <input style={inputStyle} type="number" value={settings.strategy.session_timezone_offset_hours} onChange={(event) => updateField('strategy', 'session_timezone_offset_hours', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="RSI period">
          <input style={inputStyle} type="number" value={settings.strategy.signal_rsi_period} onChange={(event) => updateField('strategy', 'signal_rsi_period', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Swing window">
          <input style={inputStyle} type="number" value={settings.strategy.signal_swing_window} onChange={(event) => updateField('strategy', 'signal_swing_window', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Equal level tolerance pct">
          <input style={inputStyle} type="number" step="0.0001" value={settings.strategy.signal_equal_level_tolerance_pct} onChange={(event) => updateField('strategy', 'signal_equal_level_tolerance_pct', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Overbought">
          <input style={inputStyle} type="number" value={settings.strategy.signal_overbought} onChange={(event) => updateField('strategy', 'signal_overbought', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Oversold">
          <input style={inputStyle} type="number" value={settings.strategy.signal_oversold} onChange={(event) => updateField('strategy', 'signal_oversold', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Price near extreme pct">
          <input style={inputStyle} type="number" step="0.0001" value={settings.strategy.signal_price_near_extreme_pct} onChange={(event) => updateField('strategy', 'signal_price_near_extreme_pct', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Session confirm filter">
          <label style={{ display: 'inline-flex', gap: 10, minHeight: 46, alignItems: 'center' }}>
            <input type="checkbox" checked={Boolean(settings.strategy.signal_session_confirm_filter_enabled)} onChange={(event) => updateField('strategy', 'signal_session_confirm_filter_enabled', event.target.checked, 'checkbox')} disabled={loading} />
            <span>Enabled</span>
          </label>
        </Field>
        <Field label="Planner min score">
          <input style={inputStyle} type="number" step="0.1" value={settings.strategy.planner_min_score} onChange={(event) => updateField('strategy', 'planner_min_score', event.target.value, 'number')} disabled={loading} />
        </Field>
        <Field label="Planner min risk/reward">
          <input style={inputStyle} type="number" step="0.1" value={settings.strategy.planner_min_rr} onChange={(event) => updateField('strategy', 'planner_min_rr', event.target.value, 'number')} disabled={loading} />
        </Field>
      </Section>
    </div>
  )
}
