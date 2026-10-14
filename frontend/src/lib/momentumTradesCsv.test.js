import assert from 'node:assert/strict'
import test from 'node:test'
import { loadAllMomentumTrades, momentumTradesCsv } from './momentumTradesCsv.js'

test('export loads all decisions beyond the first API page', async () => {
  const first = Array.from({ length: 1000 }, (_, trade_id) => ({ trade_id }))
  const requests = []
  const rows = await loadAllMomentumTrades(async (params) => {
    requests.push(params)
    return { total: 1001, items: requests.length === 1 ? first : [{ trade_id: 1000 }] }
  })
  assert.equal(rows.length, 1001)
  assert.deepEqual(requests, ['?limit=1000&offset=0', '?limit=1000&offset=1000'])
})

test('CSV preserves precision, quotes, multiline text, nulls and spreadsheet safety', () => {
  const csv = momentumTradesCsv([{ action: 'SELL', symbol: 'BTC/USD', price: 1.23456789, pnl: -2.5, reason: 'rank, "first"\nexit', price_source: '=formula', created_at: '2026-10-14T12:00:00Z' }])
  assert.ok(csv.startsWith('\uFEFFtrade_id,action,symbol,'))
  assert.ok(csv.includes('"1.23456789"'))
  assert.ok(csv.includes('"-2.5"'))
  assert.ok(csv.includes('"rank, ""first""\nexit"'))
  assert.ok(csv.includes('"\'=formula"'))
  assert.ok(csv.includes('"2026-10-14T12:00:00Z"'))
  assert.equal(momentumTradesCsv([]).split('\r\n').length, 2)
})

test('failed or incomplete retrieval never returns a partial export', async () => {
  await assert.rejects(loadAllMomentumTrades(async () => ({ total: 1, items: [] })), /Incomplete/)
  await assert.rejects(loadAllMomentumTrades(async () => { throw new Error('network failure') }), /network failure/)
})
