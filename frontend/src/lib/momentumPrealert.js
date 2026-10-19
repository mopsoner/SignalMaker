const PREALERT_FIELDS = ['momentum_delta_15m', 'momentum_delta_1h', 'momentum_acceleration_1h']

export function isMomentumPrealert(row) {
  return PREALERT_FIELDS.every((field) => {
    const value = row?.[field]
    if (typeof value !== 'number' && typeof value !== 'string') return false
    if (typeof value === 'string' && value.trim() === '') return false
    const number = Number(value)
    return Number.isFinite(number) && number > 0
  })
}
