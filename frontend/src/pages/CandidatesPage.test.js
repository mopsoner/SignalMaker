import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const page = readFileSync(new URL('./CandidatesPage.jsx', import.meta.url), 'utf8')
const api = readFileSync(new URL('../lib/api.js', import.meta.url), 'utf8')

test('each candidate row exposes an explicitly confirmed live execution action', () => {
  assert.match(page, /title: 'Live'/)
  assert.match(page, /'RUN LIVE'/)
  assert.match(page, /window\.confirm/)
  assert.match(page, /api\.runLiveCandidate\(row\.candidate_id, 1\)/)
  assert.match(api, /executor\/live\/candidates/)
  assert.match(api, /X-Confirm-Live-Execution.*EXECUTE-WYCKOFF-LIVE/)
})
