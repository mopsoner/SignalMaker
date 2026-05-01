export function fmtNumber(value, digits = 4) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  return Number(value).toFixed(digits)
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
  if (['trade', 'trade_ready', 'trade_candidate'].includes(normalized)) return 'badge green'
  if (['confirm', 'confirm_watch'].includes(normalized)) return 'badge blue'
  if (['waiting_1h_event', 'wyckoff_watch'].includes(normalized)) return 'badge orange'
  if (['zone', 'zone_watch'].includes(normalized)) return 'badge orange'
  if (['macro_watch', 'context_invalid', 'context_target_overlap', 'target_watch'].includes(normalized)) return 'badge gray'
  return 'badge gray'
}
