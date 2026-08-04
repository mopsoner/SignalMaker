import assert from 'node:assert/strict'
import test, { after } from 'node:test'
import React from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { createServer } from 'vite'

const vite = await createServer({ server: { middlewareMode: true }, appType: 'custom' })
after(() => vite.close())
const { AnalysisSection, JobRow, PreviewSummary, WorkerSection, availableTimeframes, missingTimeframes } = await vite.ssrLoadModule('/src/pages/MarketDataAdminPage.jsx')
const render = (element) => renderToStaticMarkup(element)

test('loading and API error states use explicit, accessible status panels', async () => {
  const source = await (await import('node:fs/promises')).readFile(new URL('./MarketDataAdminPage.jsx', import.meta.url), 'utf8')
  assert.match(source, /data-testid="loading"/)
  assert.match(source, /role="alert"/)
  assert.match(source, /Chargement impossible/)
})

test('insufficient intraday data blocks every analysis action without daily substitution', () => {
  assert.deepEqual(availableTimeframes({ available_timeframes: ['1d', '1h'] }), ['1d', '1h'])
  assert.deepEqual(missingTimeframes({ available_timeframes: ['1d', '1h'] }), ['15m', '4h'])
  const html = render(React.createElement(AnalysisSection, { title: 'Configuration Momentum', description: 'test', defaultEngine: 'momentum', data: { available_timeframes: ['1d', '1h'] }, universes: [], onMessage() {} }))
  assert.match(html, /Exécution bloquée/)
  assert.match(html, /15m, 4h/)
  assert.equal((html.match(/disabled=""/g) || []).length, 3)
  assert.match(html, /daily ne remplace jamais/)
})

test('preview reports eligible, ignored, current and incomplete assets before confirmation', () => {
  const html = render(React.createElement(PreviewSummary, { preview: { eligible: 12, ignored: 2, already_up_to_date: 5, incomplete: 3 } }))
  for (const text of ['Actifs éligibles', 'Ignorés', 'Déjà à jour', 'Incomplets', '12', '2', '5', '3', 'avant confirmation']) assert.match(html, new RegExp(text))
})

test('analysis actions are distinct and queue/run remain unavailable until preview confirmation', () => {
  const html = render(React.createElement(AnalysisSection, { title: 'Configuration Wyckoff / SMC', description: 'test', defaultEngine: 'wyckoff_smc', data: { available_timeframes: ['15m', '1h', '4h'] }, universes: [], onMessage() {} }))
  for (const label of ['Prévisualiser', 'Mettre en file', 'Exécuter maintenant']) assert.match(html, new RegExp(label))
  assert.equal((html.match(/disabled=""/g) || []).length, 2)
  assert.match(html, /wyckoff_smc/)
  assert.match(html, /momentum/)
  assert.match(html, /Les deux/)
})

test('running job displays backend counters, phase, heartbeat, duration and last error', () => {
  const html = render(React.createElement('table', null, React.createElement('tbody', null, React.createElement(JobRow, { job: { id: 8, status: 'running', phase: 'analyzing', counters: { total: 20, processed: 5 }, heartbeat_at: '2026-08-04T10:00:00Z', started_at: '2026-08-04T09:59:00Z', last_error: 'provider timeout' }, onRetry() {} }))))
  for (const text of ['analyzing', '25%', '5 / 20', 'provider timeout']) assert.match(html, new RegExp(text.replace('%', '%')))
  assert.match(html, /Relancer les échecs/)
  assert.match(html, /disabled=""/)
})

test('stopped worker is visible while process controls depend on deployment mode', () => {
  const workers = { stock_etf_analysis: { process_state: 'stopped', heartbeat_at: '2026-08-04T10:00:00Z', queue: { counts: { queued: 3, running: 0 } }, last_job_id: 41 } }
  const managed = render(React.createElement(WorkerSection, { workers, controlSupported: false, onAction() {} }))
  assert.match(managed, /stopped/); assert.match(managed, />3</); assert.match(managed, />41</); assert.doesNotMatch(managed, />Start</)
  const controllable = render(React.createElement(WorkerSection, { workers, controlSupported: true, onAction() {} }))
  assert.match(controllable, />Start</); assert.match(controllable, />Stop</)
})

test('partial retry is enabled only for admissible failed jobs', () => {
  const eligible = render(React.createElement('table', null, React.createElement('tbody', null, React.createElement(JobRow, { job: { id: 9, status: 'failed', attempts: 1, max_attempts: 3 }, onRetry() {} }))))
  const exhausted = render(React.createElement('table', null, React.createElement('tbody', null, React.createElement(JobRow, { job: { id: 10, status: 'failed', attempts: 3, max_attempts: 3 }, onRetry() {} }))))
  assert.doesNotMatch(eligible, /disabled=""/)
  assert.match(exhausted, /disabled=""/)
})
