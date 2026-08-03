import assert from 'node:assert/strict'
import test from 'node:test'

import { CLEAR_ALL_STOCK_ETF_CONFIRMATION, confirmAndClearAllStockEtfData } from './clearAllStockEtfData.js'


test('cancellation sends no request', async () => {
  let requests = 0
  const result = await confirmAndClearAllStockEtfData({
    confirm: (message) => {
      assert.match(message, /assets, universes, imported candles, analysis results, and run\/job history/)
      assert.equal(message, CLEAR_ALL_STOCK_ETF_CONFIRMATION)
      return false
    },
    request: async () => { requests += 1 },
    refreshSettings: async () => {},
    refreshAssets: async () => {},
  })
  assert.equal(result, null)
  assert.equal(requests, 0)
})


test('confirmation invokes deletion once and refreshes counts and assets', async () => {
  let requests = 0
  let settingsRefreshes = 0
  let assetRefreshes = 0
  const payload = { deleted: 7 }
  const result = await confirmAndClearAllStockEtfData({
    confirm: () => true,
    request: async () => { requests += 1; return payload },
    refreshSettings: async () => { settingsRefreshes += 1 },
    refreshAssets: async () => { assetRefreshes += 1 },
  })
  assert.equal(result, payload)
  assert.equal(requests, 1)
  assert.equal(settingsRefreshes, 1)
  assert.equal(assetRefreshes, 1)
})
