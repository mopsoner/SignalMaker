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
  if (normalized === 'trade') return 'badge green'
  if (normalized === 'confirm') return 'badge blue'
  if (normalized === 'zone') return 'badge orange'
  return 'badge gray'
}
