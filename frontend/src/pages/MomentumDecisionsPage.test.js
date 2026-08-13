import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const page = readFileSync(new URL('./MomentumDecisionsPage.jsx', import.meta.url), 'utf8')
const api = readFileSync(new URL('../lib/api.js', import.meta.url), 'utf8')

test('momentum decisions page requests, filters, and paginates executed actions', () => {
  assert.match(api, /momentumDecisions:.*momentum-engine\/decisions/)
  assert.match(page, /api\.momentumDecisions\(\)/)
  assert.match(page, /decision\?\.should_trade === true/)
  assert.match(page, /Executed momentum actions/)
  assert.match(page, /paginated initialPageSize=\{50\}/)
})
