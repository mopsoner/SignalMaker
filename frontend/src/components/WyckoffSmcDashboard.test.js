import assert from 'node:assert/strict'
import test, { after } from 'node:test'
import React from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import { createServer } from 'vite'

const vite = await createServer({ server: { middlewareMode: true }, appType: 'custom' })
after(() => vite.close())
const { assetScore, DecisionPath, MarketFilterChips, MobileAssetCards } = await vite.ssrLoadModule('/src/components/WyckoffSmcDashboard.jsx')
const row = { id: 1, symbol: 'BTCUSD', provider_symbol: 'IWDA.AS', stage: 'trade_candidate', bias: 'bull', score: 8.5, rsi_1h: 57, state_payload: { state: 'markup', pipeline: { liquidity: true }, projected_target: { type: 'buy_side', level: 101 }, confirmation_model: { confirmed_by_1h: true, fifteen_min_alignment: 'aligned' }, one_hour_decision: { valid: true, side: 'bull', source: 'spring_mss' }, zone_validity: { valid: true }, planner_candidate_status: 'candidate_watch' } }
const render = (element) => renderToStaticMarkup(React.createElement(MemoryRouter, null, element))

test('crypto and ETF/Stock payloads render the exact same five decision steps', () => {
  const cryptoPath = render(React.createElement(DecisionPath, { row }))
  const stockPath = render(React.createElement(DecisionPath, { row: { ...row, symbol: row.provider_symbol } }))
  assert.equal(stockPath, cryptoPath)
  for (const label of ['Context', 'Target', '1H setup', '15m align', 'Trade']) assert.match(cryptoPath, new RegExp(`✓ ${label}`))
  assert.match(cryptoPath, /spring_mss/)
})

test('shared filters keep counts, labels and active badges identical', () => {
  const filters = [['actionable', 'Actionable (1)'], ['trade_candidate', 'Trade candidate (1)'], ['all', 'All (1)']]
  const crypto = render(React.createElement(MarketFilterChips, { filters, marketFilter: 'actionable', setMarketFilter() {} }))
  const stocks = render(React.createElement(MarketFilterChips, { filters, marketFilter: 'actionable', setMarketFilter() {} }))
  assert.equal(stocks, crypto)
  assert.match(crypto, /filter-chip active/)
  assert.match(crypto, /Trade candidate \(1\)/)
})

test('mobile cards share stage badge, statistics and path while market actions stay configurable', () => {
  const crypto = render(React.createElement(MobileAssetCards, { rows: [row], detailUrl: () => '/assets/BTCUSD', tradingViewLink: () => 'https://tv/crypto' }))
  const stocks = render(React.createElement(MobileAssetCards, { rows: [{ ...row, id: undefined }], symbolFor: (item) => item.provider_symbol, assetMeta: () => 'ETF · Europe ETF', tradingViewLink: () => 'https://tv/stock' }))
  for (const markup of [crypto, stocks]) {
    assert.match(markup, /mobile-asset-card/)
    assert.match(markup, /badge green/)
    assert.match(markup, /Score/)
    assert.match(markup, /RSI 1H/)
    assert.match(markup, /15m align/)
    assert.match(markup, /Trade/)
  }
  assert.match(crypto, /Debug view/)
  assert.doesNotMatch(stocks, /Debug view|\/assets\//)
  assert.match(stocks, /IWDA\.AS/)
  assert.match(stocks, /ETF · Europe ETF/)
})

test('dashboard score matches the persisted score shown by the debug view', () => {
  const staleDiagnostics = {
    score: 19.82,
    state_payload: { score: 12.5, final_score: 12.5, gated_score: 27.25 },
  }

  assert.equal(assetScore(staleDiagnostics), 19.82)
  const markup = render(React.createElement(MobileAssetCards, {
    rows: [{ ...row, ...staleDiagnostics }],
    detailUrl: () => '/assets/BTCUSD',
    tradingViewLink: () => 'https://tv/crypto',
  }))
  assert.match(markup, /19\.82/)
  assert.doesNotMatch(markup, /27\.25/)
})
