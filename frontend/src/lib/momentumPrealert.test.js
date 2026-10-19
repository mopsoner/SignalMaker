import assert from 'node:assert/strict'
import test from 'node:test'

import { isMomentumPrealert } from './momentumPrealert.js'

const candidate = {
  momentum_delta_15m: 0.2,
  momentum_delta_1h: 0.3,
  momentum_acceleration_1h: 0.1,
}

test('prealert can identify an early recovery before the asset becomes Bull', () => {
  assert.equal(isMomentumPrealert({ ...candidate, classification: 'bear', momentum_score: -8 }), true)
})

test('a flat or falling input prevents prealert even if the other inputs are positive', () => {
  for (const field of Object.keys(candidate)) {
    for (const value of [0, -0.01]) {
      assert.equal(isMomentumPrealert({ ...candidate, [field]: value }), false, `${field}=${value}`)
    }
  }
})

test('missing and invalid indicators cannot create false prealerts', () => {
  assert.equal(isMomentumPrealert(null), false)
  assert.equal(isMomentumPrealert({}), false)
  for (const field of Object.keys(candidate)) {
    for (const value of [undefined, null, NaN, Infinity, -Infinity, true, '', ' ', 'invalid', []]) {
      assert.equal(isMomentumPrealert({ ...candidate, [field]: value }), false, field)
    }
  }
})

test('strictly positive small changes qualify without an additional threshold', () => {
  assert.equal(isMomentumPrealert({
    momentum_delta_15m: 0.00001,
    momentum_delta_1h: 0.00001,
    momentum_acceleration_1h: 0.00001,
  }), true)
})

test('finite numeric strings follow the same boundary as numbers', () => {
  assert.equal(isMomentumPrealert({
    momentum_delta_15m: '0.2', momentum_delta_1h: '0.3', momentum_acceleration_1h: '0.1',
  }), true)
  assert.equal(isMomentumPrealert({ ...candidate, momentum_delta_1h: '0' }), false)
  assert.equal(isMomentumPrealert({ ...candidate, momentum_acceleration_1h: 'Infinity' }), false)
})

test('scanner selection preserves ranking order and updates when an input stops rising', () => {
  const rows = [
    { ...candidate, symbol: 'FIRST' },
    { ...candidate, symbol: 'FLAT', momentum_delta_1h: 0 },
    { ...candidate, symbol: 'SECOND' },
  ]
  assert.deepEqual(rows.filter(isMomentumPrealert).map((row) => row.symbol), ['FIRST', 'SECOND'])
  rows[0].momentum_acceleration_1h = -0.1
  assert.deepEqual(rows.filter(isMomentumPrealert).map((row) => row.symbol), ['SECOND'])
})
