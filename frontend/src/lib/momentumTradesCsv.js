const fields = ['trade_id', 'action', 'symbol', 'price', 'quantity', 'value', 'pnl', 'pnl_pct', 'price_source', 'reason', 'created_at']

function csvCell(value) {
  let text = String(value ?? '')
  // Keep external text from being interpreted as a spreadsheet formula.
  if (typeof value === 'string' && /^[\s]*[=+@-]/.test(text)) text = `'${text}`
  return `"${text.replaceAll('"', '""')}"`
}

export function momentumTradesCsv(trades) {
  return '\uFEFF' + [fields.join(','), ...trades.map((trade) => fields.map((field) => csvCell(trade[field])).join(','))].join('\r\n') + '\r\n'
}

export async function loadAllMomentumTrades(fetchPage) {
  const trades = []
  let total
  do {
    const page = await fetchPage(`?limit=1000&offset=${trades.length}`)
    total = page.total
    if (!page.items.length && trades.length < total) throw new Error('Incomplete Momentum decisions export. Please try again.')
    trades.push(...page.items)
  } while (trades.length < total)
  return trades
}
