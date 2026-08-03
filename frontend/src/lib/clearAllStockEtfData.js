export const CLEAR_ALL_STOCK_ETF_CONFIRMATION =
  'Permanently remove all ETF/stock assets, universes, imported candles, analysis results, and run/job history? This cannot be undone.'

export async function confirmAndClearAllStockEtfData({ confirm, request, refreshSettings, refreshAssets, onConfirmed = () => {} }) {
  if (!confirm(CLEAR_ALL_STOCK_ETF_CONFIRMATION)) return null
  onConfirmed()
  const result = await request()
  await Promise.all([refreshSettings(), refreshAssets()])
  return result
}
