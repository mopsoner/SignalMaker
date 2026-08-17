// Asset rows expose the persisted score at the top level. Payload scores are
// retained for diagnostics and are only fallbacks for legacy API responses.
export function assetScore(asset) {
  const payload = asset?.state_payload || {}
  return Number(asset?.score ?? payload.score ?? payload.final_score ?? payload.gated_score ?? 0)
}
