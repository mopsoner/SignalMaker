import assert from 'node:assert/strict'
import test from 'node:test'

import { isWorkerRunning, MANAGED_WORKERS } from '../lib/workerStatus.js'

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

test('worker state follows the canonical process state', () => {
  assert.equal(isWorkerRunning({ process_state: 'running', running: false, pid: 42 }), true)
  assert.equal(isWorkerRunning({ process_state: 'stopped', running: true, pid: null }), false)
  assert.equal(isWorkerRunning({ running: true }), true)
})
