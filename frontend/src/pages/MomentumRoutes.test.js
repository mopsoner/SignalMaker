import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const shared = readFileSync(new URL('./MomentumPage.jsx', import.meta.url), 'utf8')
const stock = readFileSync(new URL('./StockEtfMomentumPage.jsx', import.meta.url), 'utf8')
const app = readFileSync(new URL('../App.jsx', import.meta.url), 'utf8')

test('both momentum routes render the shared view with familiar actions', () => {
  assert.match(app, /path="\/momentum" element={<MomentumPage/)
  assert.match(app, /path="\/stocks-etfs\/momentum" element={<StockEtfMomentumPage/)
  assert.match(shared, /Run engine now/)
  assert.match(shared, /Run only if due/)
  assert.match(shared, /Cadence/)
  assert.match(stock, /<MomentumView/)
})

test('market adapters call their scoped endpoints and stock filters are exposed', () => {
  assert.match(shared, /\/api\/v1\/momentum-engine\/run-once/)
  assert.match(stock, /\/api\/v1\/stocks-etfs\/momentum\/ranking/)
  assert.match(stock, /\/api\/v1\/stocks-etfs\/momentum\/status/)
  assert.match(stock, /\/api\/v1\/stocks-etfs\/momentum\/run-once/)
  assert.match(shared, /aria-label="Universe"/)
  assert.match(shared, /aria-label="Asset type"/)
})
