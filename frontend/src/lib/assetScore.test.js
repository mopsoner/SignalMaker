import assert from 'node:assert/strict'
import test from 'node:test'

import { assetScore } from './assetScore.js'

test('persisted asset score takes precedence over stale diagnostic scores', () => {
  assert.equal(assetScore({
    score: 18,
    state_payload: { score: 33.25, final_score: 33.25, gated_score: 12 },
  }), 18)
})

test('legacy asset responses use the same diagnostic fallback order in every view', () => {
  assert.equal(assetScore({ state_payload: { score: 20, final_score: 19, gated_score: 18 } }), 20)
  assert.equal(assetScore({ state_payload: { final_score: 19, gated_score: 18 } }), 19)
  assert.equal(assetScore({ state_payload: { gated_score: 18 } }), 18)
  assert.equal(assetScore(null), 0)
})
