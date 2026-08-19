import assert from 'node:assert/strict'
import test from 'node:test'

import { canonicalWorkerId, getWorkerMetadata, isWorkerRunning, MANAGED_WORKERS, normalizeWorkerStatuses, WORKERS_BY_CATEGORY } from '../lib/workerStatus.js'

test('logs page displays every managed worker', () => {
  assert.deepEqual(MANAGED_WORKERS, [
    'pipeline',
    'wyckoff_paper',
    'kraken_candle_feed',
    'momentum_paper',
    'momentum_live',
    'wyckoff_live',
    'ibkr_ingestion',
    'stock_etf_analysis',
    'scheduler',
  ])
})

test('worker metadata provides unambiguous labels and categories', () => {
  assert.deepEqual(WORKERS_BY_CATEGORY.map(({ type, label }) => ({ type, label })), [
    { type: 'live', label: 'LIVE — appels réels' },
    { type: 'paper', label: 'PAPER — simulation' },
    { type: 'infrastructure', label: 'Infrastructure' },
  ])
  assert.deepEqual(getWorkerMetadata('momentum_live'), { id: 'momentum_live', label: 'Momentum — LIVE (Kraken)', type: 'live', logFile: 'momentum_live.log' })
  assert.deepEqual(getWorkerMetadata('wyckoff_live'), { id: 'wyckoff_live', label: 'Wyckoff / SMC — LIVE (Kraken)', type: 'live', logFile: 'wyckoff_live.log' })
  assert.equal(getWorkerMetadata('momentum_paper').label, 'Momentum — Paper')
  assert.equal(getWorkerMetadata('wyckoff_paper').label, 'Wyckoff / SMC — Paper')
  assert.equal(getWorkerMetadata('momentum_live').label.includes('momentum_live'), false)
})

test('live log entries retain exact backend identifiers', () => {
  const liveWorkers = WORKERS_BY_CATEGORY.find(({ type }) => type === 'live').workers
  assert.deepEqual(liveWorkers.map(({ id }) => id), ['momentum_live', 'wyckoff_live'])
  assert.deepEqual(liveWorkers.map(({ logFile }) => logFile), ['momentum_live.log', 'wyckoff_live.log'])
  assert.deepEqual(liveWorkers.map(({ id }) => `/api/v1/admin/logs/${id}?lines=300`), [
    '/api/v1/admin/logs/momentum_live?lines=300',
    '/api/v1/admin/logs/wyckoff_live?lines=300',
  ])
})

test('worker state follows the canonical process state', () => {
  assert.equal(isWorkerRunning({ process_state: 'running', running: false, pid: 42 }), true)
  assert.equal(isWorkerRunning({ process_state: 'stopped', running: true, pid: null }), false)
  assert.equal(isWorkerRunning({ running: true }), true)
})

test('legacy worker names are normalized to the paper worker names', () => {
  assert.equal(canonicalWorkerId('executor'), 'wyckoff_paper')
  assert.equal(canonicalWorkerId('momentum_engine'), 'momentum_paper')
  assert.equal(getWorkerMetadata('executor').label, 'Wyckoff / SMC — Paper')

  const statuses = normalizeWorkerStatuses({
    executor: { running: true, pid: 12 },
    momentum_engine: { running: true, pid: 13 },
  })
  assert.deepEqual(Object.keys(statuses), ['wyckoff_paper', 'momentum_paper'])
  assert.equal(statuses.wyckoff_paper.pid, 12)
  assert.equal(statuses.momentum_paper.pid, 13)
})

test('canonical worker status takes precedence over a legacy alias', () => {
  const statuses = normalizeWorkerStatuses({
    executor: { process_state: 'running', pid: 12 },
    wyckoff_paper: { process_state: 'stopped', pid: null },
  })
  assert.equal(isWorkerRunning(statuses.wyckoff_paper), false)
})
