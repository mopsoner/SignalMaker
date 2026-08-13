import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const page = readFileSync(new URL('./MomentumPositionsPage.jsx', import.meta.url), 'utf8')
const app = readFileSync(new URL('../App.jsx', import.meta.url), 'utf8')
const api = readFileSync(new URL('../lib/api.js', import.meta.url), 'utf8')

test('momentum positions route and navigation link are registered', () => {
  assert.match(app, /import MomentumPositionsPage/)
  assert.match(app, /to="\/momentum-positions"[^>]*>Positions</)
  assert.match(app, /path="\/momentum-positions" element={<MomentumPositionsPage/)
})

test('page loads only dedicated momentum data clients', () => {
  assert.match(api, /momentumPositions:.*momentum-engine\/positions/)
  assert.match(api, /momentumTrades:.*momentum-engine\/trades/)
  assert.match(page, /api\.momentumPositions\('\?limit=1000'\)/)
  assert.match(page, /api\.momentumTrades\('\?limit=1000'\)/)
  assert.doesNotMatch(page, /api\.(positions|orders)\(/)
  assert.match(page, /Loading Momentum positions and trades/)
})

test('page separates positions and highlights BUY and SELL operations', () => {
  assert.match(page, /row\.status === 'open'/)
  assert.match(page, /row\.status === 'closed'/)
  assert.match(page, /Open Momentum positions/)
  assert.match(page, /Closed Momentum positions/)
  assert.match(page, /kind === 'BUY'/)
  assert.match(page, /kind === 'SELL'/)
  assert.match(page, /key: 'reason'/)
})
