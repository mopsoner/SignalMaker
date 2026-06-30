export function tradingViewCryptoSymbol(symbol) {
  const normalized = String(symbol || '').trim().toUpperCase().replace(/[-_/\s:]/g, '')
  if (!normalized) return ''
  return `KRAKEN:${normalized}`
}

export function tradingViewStockEtfSymbol(symbol) {
  const normalized = String(symbol || '').trim().toUpperCase()
  if (!normalized) return ''
  if (normalized.endsWith('.PA')) return `EURONEXT:${normalized.replace('.PA', '')}`
  if (normalized.endsWith('.US')) return `AMEX:${normalized.replace('.US', '')}`
  return normalized
}

export function tradingViewUrl(symbol, { market = 'crypto' } = {}) {
  const tvSymbol = market === 'stock-etf' ? tradingViewStockEtfSymbol(symbol) : tradingViewCryptoSymbol(symbol)
  return `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(tvSymbol)}`
}
