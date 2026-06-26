import { useCallback, useMemo, useState } from 'react'
import FoldableTable from '../components/FoldableTable'
import PageHeader from '../components/PageHeader'
import StatCard from '../components/StatCard'
import { usePollingQuery } from '../hooks/usePollingQuery'
import { api } from '../lib/api'
import { fmtDate, fmtNumber } from '../lib/format'

function safe(value) {
  if (value === null || value === undefined || value === '') return '—'
  return String(value)
}

function boolBadge(value, labelTrue = 'yes', labelFalse = 'no') {
  const label = value ? labelTrue : labelFalse
  const color = value ? '#166534' : '#374151'
  return <span className="badge" style={{ background: color, color: 'white' }}>{label}</span>
}

function tradingViewSymbol(row) {
  const symbol = encodeURIComponent(row.symbol || '')
  if (row.currency === 'EUR') return `EURONEXT%3A${symbol}`
  return `AMEX%3A${symbol}`
}

export default function ETFStocksPage() {
  const [filter, setFilter] = useState('resolved')
  const [symbol, setSymbol] = useState('')
  const querySymbol = symbol.trim().toUpperCase()
  const contractParams = new URLSearchParams({ limit: '1000' })
  if (querySymbol) contractParams.set('symbol', querySymbol)

  const { data: contracts = [], loading: contractsLoading, error: contractsError } = usePollingQuery(
    useCallback(() => api.ibkrContracts(`?${contractParams.toString()}`), [contractParams.toString()]),
    30000,
  )
  const { data: summary = [], loading: summaryLoading, error: summaryError } = usePollingQuery(
    useCallback(() => api.ibkrCandleSummary(querySymbol ? `?symbol=${encodeURIComponent(querySymbol)}` : ''), [querySymbol]),
    30000,
  )
  const { data: candles = [] } = usePollingQuery(
    useCallback(() => api.ibkrCandles(`${querySymbol ? `?symbol=${encodeURIComponent(querySymbol)}&` : '?'}limit=200`), [querySymbol]),
    30000,
  )
  const { data: runs = [] } = usePollingQuery(useCallback(() => api.ibkrImportRuns('?limit=20'), []), 30000)

  const summaryByKey = useMemo(() => {
    const map = new Map()
    for (const row of summary) map.set(`${row.symbol}-${row.conid}-${row.timeframe}`, row)
    return map
  }, [summary])

  const latestCandleByKey = useMemo(() => {
    const map = new Map()
    for (const row of candles) {
      const key = `${row.symbol}-${row.conid}-${row.timeframe}`
      if (!map.has(key)) map.set(key, row)
    }
    return map
  }, [candles])

  const rows = useMemo(() => contracts.map((contract) => {
    const candleSummary = summaryByKey.get(`${contract.symbol}-${contract.conid}-1d`) || {}
    const latestCandle = latestCandleByKey.get(`${contract.symbol}-${contract.conid}-1d`) || {}
    return { ...contract, candle_summary: candleSummary, latest_candle: latestCandle }
  }), [contracts, summaryByKey, latestCandleByKey])

  const filteredRows = useMemo(() => {
    if (filter === 'resolved') return rows.filter((row) => row.resolved)
    if (filter === 'ambiguous') return rows.filter((row) => row.ambiguous)
    if (filter === 'unresolved') return rows.filter((row) => !row.resolved)
    if (filter === 'with_candles') return rows.filter((row) => Number(row.candle_summary?.candle_count || 0) > 0)
    if (filter === 'errors') return rows.filter((row) => row.last_error)
    return rows
  }, [rows, filter])

  const counts = useMemo(() => ({
    total: rows.length,
    resolved: rows.filter((row) => row.resolved).length,
    ambiguous: rows.filter((row) => row.ambiguous).length,
    withCandles: rows.filter((row) => Number(row.candle_summary?.candle_count || 0) > 0).length,
    candleRows: summary.reduce((sum, row) => sum + Number(row.candle_count || 0), 0),
  }), [rows, summary])

  const filters = [
    ['resolved', `Resolved (${counts.resolved})`],
    ['with_candles', `With daily candles (${counts.withCandles})`],
    ['ambiguous', `Manual validation (${counts.ambiguous})`],
    ['unresolved', `Unresolved (${counts.total - counts.resolved})`],
    ['errors', 'Errors'],
    ['all', `All (${counts.total})`],
  ]

  const contractColumns = [
    { key: 'symbol', title: 'Ticker', render: (row) => <div style={{ display: 'grid', gap: 6 }}><strong>{row.symbol}</strong><a href={`https://www.tradingview.com/chart/?symbol=${tradingViewSymbol(row)}`} target="_blank" rel="noreferrer">TradingView</a></div>, sortValue: (row) => row.symbol },
    { key: 'asset', title: 'Asset type', render: (row) => safe(row.sec_type), sortValue: (row) => row.sec_type || '' },
    { key: 'currency', title: 'Currency' },
    { key: 'exchange', title: 'Route', render: (row) => `${safe(row.exchange)}${row.primary_exchange ? ` / ${row.primary_exchange}` : ''}`, sortValue: (row) => `${row.exchange || ''}-${row.primary_exchange || ''}` },
    { key: 'conid', title: 'IBKR conId', render: (row) => safe(row.conid), sortValue: (row) => Number(row.conid || 0) },
    { key: 'resolved', title: 'Resolved', render: (row) => boolBadge(row.resolved, 'resolved', 'pending'), sortValue: (row) => row.resolved ? 1 : 0 },
    { key: 'ambiguous', title: 'Manual validation', render: (row) => boolBadge(row.ambiguous, 'ambiguous', 'ok'), sortValue: (row) => row.ambiguous ? 1 : 0 },
    { key: 'candles', title: 'Daily candles', render: (row) => safe(row.candle_summary?.candle_count), sortValue: (row) => Number(row.candle_summary?.candle_count || 0) },
    { key: 'last', title: 'Last daily bar', render: (row) => fmtDate(row.candle_summary?.last_timestamp), sortValue: (row) => row.candle_summary?.last_timestamp || '' },
    { key: 'close', title: 'Last close', render: (row) => fmtNumber(row.latest_candle?.close, 4), sortValue: (row) => Number(row.latest_candle?.close || 0) },
    { key: 'last_error', title: 'Last error', render: (row) => safe(row.last_error), sortValue: (row) => row.last_error || '' },
  ]

  const summaryColumns = [
    { key: 'symbol', title: 'Ticker' },
    { key: 'conid', title: 'conId' },
    { key: 'timeframe', title: 'Timeframe' },
    { key: 'candle_count', title: 'Candles', render: (row) => safe(row.candle_count), sortValue: (row) => Number(row.candle_count || 0) },
    { key: 'first_timestamp', title: 'First daily bar', render: (row) => fmtDate(row.first_timestamp) },
    { key: 'last_timestamp', title: 'Last daily bar', render: (row) => fmtDate(row.last_timestamp) },
    { key: 'last_imported_at', title: 'Imported', render: (row) => fmtDate(row.last_imported_at) },
  ]

  const candleColumns = [
    { key: 'symbol', title: 'Ticker' },
    { key: 'timeframe', title: 'TF' },
    { key: 'timestamp', title: 'Daily bar date', render: (row) => fmtDate(row.timestamp) },
    { key: 'open', title: 'Open', render: (row) => fmtNumber(row.open, 4) },
    { key: 'high', title: 'High', render: (row) => fmtNumber(row.high, 4) },
    { key: 'low', title: 'Low', render: (row) => fmtNumber(row.low, 4) },
    { key: 'close', title: 'Close', render: (row) => fmtNumber(row.close, 4) },
    { key: 'volume', title: 'Volume', render: (row) => fmtNumber(row.volume, 0) },
    { key: 'source', title: 'Source' },
  ]

  const runColumns = [
    { key: 'run_type', title: 'Run' },
    { key: 'status', title: 'Status' },
    { key: 'started_at', title: 'Started', render: (row) => fmtDate(row.started_at) },
    { key: 'finished_at', title: 'Finished', render: (row) => fmtDate(row.finished_at) },
    { key: 'total_assets', title: 'Total' },
    { key: 'success_count', title: 'OK' },
    { key: 'failed_count', title: 'Failed' },
    { key: 'error_message', title: 'Error', render: (row) => safe(row.error_message) },
  ]

  return <div className="page-stack">
    <PageHeader title="ETF & Stocks" subtitle="IBKR Paper Gateway daily market-data view. Uses only isolated IBKR contracts and candles, not the Binance/Wyckoff candle pipeline." />
    <div className="stats-grid"><StatCard label="IBKR contracts" value={counts.total} hint="ibkr_contracts" /><StatCard label="Resolved conIds" value={counts.resolved} /><StatCard label="Need manual validation" value={counts.ambiguous} /><StatCard label="Daily candle rows" value={counts.candleRows.toLocaleString()} hint="ibkr_candles only" /></div>
    <div className="panel"><div className="market-toolbar"><input value={symbol} onChange={(event) => setSymbol(event.target.value)} placeholder="Filter ticker: AAPL, SPY, AIR…" style={{ minWidth: 260 }} /><div className="filter-chips" role="tablist" aria-label="IBKR filters">{filters.map(([key, label]) => <button key={key} type="button" className={`filter-chip ${filter === key ? 'active' : ''}`} onClick={() => setFilter(key)}>{label}</button>)}</div><div className="market-toolbar-hint">Showing {filteredRows.length} / {rows.length}</div></div></div>
    {contractsLoading || summaryLoading ? <div className="panel">Loading IBKR data…</div> : null}
    {contractsError ? <div className="panel error">Contracts: {contractsError}</div> : null}
    {summaryError ? <div className="panel error">Candles: {summaryError}</div> : null}
    <FoldableTable title="IBKR ETF & stock universe" columns={contractColumns} rows={filteredRows} empty="No IBKR contracts found. Run the IBKR migration/seed and resolve contracts first." defaultSortKey="symbol" paginated initialPageSize={25} pageSizeOptions={[25, 50, 100, 250]} />
    <FoldableTable title="Daily candle coverage" columns={summaryColumns} rows={summary} empty="No IBKR daily candles found yet." defaultSortKey="symbol" />
    <FoldableTable title="Recent IBKR daily candles" columns={candleColumns} rows={candles} empty="No IBKR candle rows found yet." defaultSortKey="timestamp" defaultSortDir="desc" />
    <FoldableTable title="IBKR import runs" columns={runColumns} rows={runs} empty="No IBKR import runs recorded yet." defaultSortKey="started_at" defaultSortDir="desc" />
  </div>
}
