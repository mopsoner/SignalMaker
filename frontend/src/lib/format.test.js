import assert from 'node:assert/strict'
import test from 'node:test'

import { fmtPrice } from './format.js'

test('fmtPrice keeps enough precision for low-priced trade candidates', () => {
  assert.equal(fmtPrice(0.00001234), '0.00001234')
  assert.equal(fmtPrice(0.123456), '0.123456')
  assert.equal(fmtPrice(123.45678), '123.4568')
})

test('fmtPrice distinguishes missing and zero prices', () => {
  assert.equal(fmtPrice(null), '—')
  assert.equal(fmtPrice(0), '0.0000')
})
