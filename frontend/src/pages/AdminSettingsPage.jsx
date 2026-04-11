import { useEffect, useMemo, useState } from 'react'
import PageHeader from '../components/PageHeader'
import { api } from '../lib/api'

const EMPTY_SETTINGS = {
  general: { app_name: '', app_env: '', cors_origins: '', create_tables_on_boot: true },
  binance: { binance_rest_base: '', binance_quote_assets: '', binance_symbol_status: '', binance_max_symbols: 25, binance_lookback_1m: 180, binance_lookback_5m: 180, binance_lookback_1h: 180, binance_lookback_4h: 120 },
  strategy: { session_timezone_offset_hours: -4, signal_rsi_period: 14, signal_swing_window: 8, signal_equal_level_tolerance_pct: 0.0015, signal_overbought: 70, signal_oversold: 30, signal_price_near_extreme_pct: 0.0025, signal_session_confirm_filter_enabled: false, planner_min_score: 4, planner_min_rr: 0.8 },
  notifications: { telegram_chat_id: '', telegram_secret: '', discord_url: '' },
  bot: { bot_pipeline_enabled: true, bot_executor_enabled: true, bot_scheduler_enabled: true, bot_pipeline_interval_sec: 60, bot_executor_interval_sec: 30, bot_scheduler_interval_sec: 30, bot_executor_limit: 10, bot_executor_quantity: 1.0 },
}

const gridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '16px' }
const inputStyle = { width: '100%', background: 'rgba(9, 14, 24, 0.7)', color: 'var(--text)', border: '1px solid var(--line)', borderRadius: '12px', padding: '12px 14px' }
const fieldStyle = { display: 'grid', gap: '8px' }

function Field({ label, children }) {
  return <label style={fieldStyle}><span style={{ fontSize: 14, fontWeight: 600 }}>{label}</span>{children}</label>
}

function Section({ title, description, children }) {
  return <section className="panel"><div style={{ marginBottom: 16 }}><h2>{title}</h2><p className="stat-hint" style={{ marginTop: 6 }}>{description}</p></div><div style={gridStyle}>{children}</div></section>
}

export default function AdminSettingsPage() {
  const [settings, setSettings] = useState(EMPTY_SETTINGS)
  const [initialSettings, setInitialSettings] = useState(EMPTY_SETTINGS)
  const [workers, setWorkers] = useState({})
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState('')
  const [operatorKey, setOperatorKey] = useState(api.getOperatorKey())

  async function loadSettings() {
    setLoading(true)
    try {
      const [data, workerData] = await Promise.all([api.adminSettings(), api.workerStatus()])
      setSettings({ ...EMPTY_SETTINGS, ...data, strategy: { ...EMPTY_SETTINGS.strategy, ...(data.strategy || {}) } })
      setInitialSettings({ ...EMPTY_SETTINGS, ...data, strategy: { ...EMPTY_SETTINGS.strategy, ...(data.strategy || {}) } })
      setWorkers(workerData)
      setMessage('')
    } catch (error) {
      setMessage(error.message || 'Failed to load settings')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadSettings() }, [])

  const dirty = useMemo(() => JSON.stringify(settings) !== JSON.stringify(initialSettings), [settings, initialSettings])

  function updateField(section, key, rawValue, type = 'text') {
    const value = type === 'number' ? Number(rawValue) : type === 'checkbox' ? Boolean(rawValue) : rawValue
    setSettings((current) => ({ ...current, [section]: { ...current[section], [key]: value } }))
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

  async function doAction(action) {
    try {
      const result = await action()
      setMessage(JSON.stringify(result))
      const workerData = await api.workerStatus()
      setWorkers(workerData)
    } catch (error) {
      setMessage(error.message || 'Action failed')
    }
  }

  return (
    <div className="page-stack">
      <PageHeader title="Admin Settings" subtitle="Runtime config, worker control and service tests." />

      <section className="panel">
        <div style={gridStyle}>
          <Field label="Operator key">
            <input style={inputStyle} type="password" value={operatorKey} onChange={(e) => setOperatorKey(e.target.value)} placeholder="Optional backend operator key" />
          </Field>
        </div>
        <div className="page-actions" style={{ marginTop: 16 }}>
          <button className="button" onClick={() => { api.setOperatorKey(operatorKey); loadSettings() }}>Apply key</button>
          <button className="button" onClick={saveSettings} disabled={loading || saving || !dirty}>{saving ? 'Saving…' : 'Save settings'}</button>
          <button className="button" onClick={() => doAction(api.testBinance)} disabled={loading}>Test Binance</button>
          <button className="button" onClick={() => doAction(api.testNotifications)} disabled={loading}>Test notifications</button>
        </div>
        {message ? <p className="stat-hint" style={{ marginTop: 14 }}>{message}</p> : null}
      </section>

      <Section title="General" description="Application identity and startup behavior.">
        <Field label="App name"><input style={inputStyle} value={settings.general.app_name} onChange={(e) => updateField('general', 'app_name', e.target.value)} disabled={loading} /></Field>
        <Field label="Environment"><input style={inputStyle} value={settings.general.app_env} onChange={(e) => updateField('general', 'app_env', e.target.value)} disabled={loading} /></Field>
        <Field label="CORS origins"><input style={inputStyle} value={settings.general.cors_origins} onChange={(e) => updateField('general', 'cors_origins', e.target.value)} disabled={loading} /></Field>
        <Field label="Create tables on boot"><input type="checkbox" checked={Boolean(settings.general.create_tables_on_boot)} onChange={(e) => updateField('general', 'create_tables_on_boot', e.target.checked, 'checkbox')} disabled={loading} /></Field>
      </Section>

      <Section title="Binance" description="Collector limits and exchange connectivity.">
        <Field label="REST base URL"><input style={inputStyle} value={settings.binance.binance_rest_base} onChange={(e) => updateField('binance', 'binance_rest_base', e.target.value)} disabled={loading} /></Field>
        <Field label="Quote assets"><input style={inputStyle} value={settings.binance.binance_quote_assets} onChange={(e) => updateField('binance', 'binance_quote_assets', e.target.value)} disabled={loading} /></Field>
        <Field label="Symbol status"><input style={inputStyle} value={settings.binance.binance_symbol_status} onChange={(e) => updateField('binance', 'binance_symbol_status', e.target.value)} disabled={loading} /></Field>
        <Field label="Max symbols"><input style={inputStyle} type="number" value={settings.binance.binance_max_symbols} onChange={(e) => updateField('binance', 'binance_max_symbols', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Lookback 1m"><input style={inputStyle} type="number" value={settings.binance.binance_lookback_1m} onChange={(e) => updateField('binance', 'binance_lookback_1m', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Lookback 5m"><input style={inputStyle} type="number" value={settings.binance.binance_lookback_5m} onChange={(e) => updateField('binance', 'binance_lookback_5m', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Lookback 1h"><input style={inputStyle} type="number" value={settings.binance.binance_lookback_1h} onChange={(e) => updateField('binance', 'binance_lookback_1h', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Lookback 4h"><input style={inputStyle} type="number" value={settings.binance.binance_lookback_4h} onChange={(e) => updateField('binance', 'binance_lookback_4h', e.target.value, 'number')} disabled={loading} /></Field>
      </Section>

      <Section title="Strategy" description="Signal engine and planner thresholds.">
        <Field label="Session timezone offset"><input style={inputStyle} type="number" value={settings.strategy.session_timezone_offset_hours} onChange={(e) => updateField('strategy', 'session_timezone_offset_hours', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Session confirm filter enabled"><input type="checkbox" checked={Boolean(settings.strategy.signal_session_confirm_filter_enabled)} onChange={(e) => updateField('strategy', 'signal_session_confirm_filter_enabled', e.target.checked, 'checkbox')} disabled={loading} /></Field>
        <Field label="RSI period"><input style={inputStyle} type="number" value={settings.strategy.signal_rsi_period} onChange={(e) => updateField('strategy', 'signal_rsi_period', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Swing window"><input style={inputStyle} type="number" value={settings.strategy.signal_swing_window} onChange={(e) => updateField('strategy', 'signal_swing_window', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Overbought"><input style={inputStyle} type="number" value={settings.strategy.signal_overbought} onChange={(e) => updateField('strategy', 'signal_overbought', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Oversold"><input style={inputStyle} type="number" value={settings.strategy.signal_oversold} onChange={(e) => updateField('strategy', 'signal_oversold', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Planner min score"><input style={inputStyle} type="number" step="0.1" value={settings.strategy.planner_min_score} onChange={(e) => updateField('strategy', 'planner_min_score', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Planner min RR"><input style={inputStyle} type="number" step="0.1" value={settings.strategy.planner_min_rr} onChange={(e) => updateField('strategy', 'planner_min_rr', e.target.value, 'number')} disabled={loading} /></Field>
      </Section>

      <Section title="Notifications" description="Quick validation for outbound channels.">
        <Field label="Telegram chat id"><input style={inputStyle} value={settings.notifications.telegram_chat_id || ''} onChange={(e) => updateField('notifications', 'telegram_chat_id', e.target.value)} disabled={loading} /></Field>
        <Field label="Telegram secret"><input style={inputStyle} type="password" value={settings.notifications.telegram_secret || ''} onChange={(e) => updateField('notifications', 'telegram_secret', e.target.value)} disabled={loading} /></Field>
        <Field label="Discord URL"><input style={inputStyle} type="password" value={settings.notifications.discord_url || ''} onChange={(e) => updateField('notifications', 'discord_url', e.target.value)} disabled={loading} /></Field>
      </Section>

      <Section title="Bot runtime" description="Enable workers and control cycle timings.">
        <Field label="Pipeline enabled"><input type="checkbox" checked={Boolean(settings.bot.bot_pipeline_enabled)} onChange={(e) => updateField('bot', 'bot_pipeline_enabled', e.target.checked, 'checkbox')} disabled={loading} /></Field>
        <Field label="Executor enabled"><input type="checkbox" checked={Boolean(settings.bot.bot_executor_enabled)} onChange={(e) => updateField('bot', 'bot_executor_enabled', e.target.checked, 'checkbox')} disabled={loading} /></Field>
        <Field label="Scheduler enabled"><input type="checkbox" checked={Boolean(settings.bot.bot_scheduler_enabled)} onChange={(e) => updateField('bot', 'bot_scheduler_enabled', e.target.checked, 'checkbox')} disabled={loading} /></Field>
        <Field label="Pipeline interval sec"><input style={inputStyle} type="number" value={settings.bot.bot_pipeline_interval_sec} onChange={(e) => updateField('bot', 'bot_pipeline_interval_sec', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Executor interval sec"><input style={inputStyle} type="number" value={settings.bot.bot_executor_interval_sec} onChange={(e) => updateField('bot', 'bot_executor_interval_sec', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Scheduler interval sec"><input style={inputStyle} type="number" value={settings.bot.bot_scheduler_interval_sec} onChange={(e) => updateField('bot', 'bot_scheduler_interval_sec', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Executor limit"><input style={inputStyle} type="number" value={settings.bot.bot_executor_limit} onChange={(e) => updateField('bot', 'bot_executor_limit', e.target.value, 'number')} disabled={loading} /></Field>
        <Field label="Executor quantity"><input style={inputStyle} type="number" step="0.1" value={settings.bot.bot_executor_quantity} onChange={(e) => updateField('bot', 'bot_executor_quantity', e.target.value, 'number')} disabled={loading} /></Field>
      </Section>

      <section className="panel">
        <h2>Workers</h2>
        <div style={gridStyle}>
          {['pipeline', 'executor', 'scheduler'].map((name) => (
            <div key={name} className="stat-card">
              <div className="stat-label">{name}</div>
              <div className="stat-value">{workers[name]?.running ? 'Running' : 'Stopped'}</div>
              <div className="page-actions">
                <button className="button" onClick={() => doAction(() => api.startWorker(name))}>Start</button>
                <button className="button" onClick={() => doAction(() => api.stopWorker(name))}>Stop</button>
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
