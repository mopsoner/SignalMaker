export function fmtNumber(value, digits = 4) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  return Number(value).toFixed(digits)
}

export function fmtPrice(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  const price = Number(value)
  const absolutePrice = Math.abs(price)
  if (absolutePrice > 0 && absolutePrice < 0.01) return price.toFixed(8)
  if (absolutePrice > 0 && absolutePrice < 1) return price.toFixed(6)
  return price.toFixed(4)
}

export function fmtDate(value) {
  if (!value) return '—'
  try {
    return new Date(value).toLocaleString()
  } catch {
    return String(value)
  }
}

export function stageBadgeClass(stage) {
  const normalized = String(stage || '').toLowerCase()
  if (['trade', 'trade_ready', 'trade_candidate', 'momentum_trade'].includes(normalized)) return 'badge green'
  if (['confirm', 'confirm_watch'].includes(normalized)) return 'badge blue'
  if (['waiting_1h_event', 'wyckoff_watch'].includes(normalized)) return 'badge orange'
  if (['zone', 'zone_watch'].includes(normalized)) return 'badge orange'
  if (['macro_watch', 'context_invalid', 'context_target_overlap', 'target_watch'].includes(normalized)) return 'badge gray'
  return 'badge gray'
}
