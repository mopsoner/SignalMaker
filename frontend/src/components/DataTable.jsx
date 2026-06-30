import { useEffect, useMemo, useState } from 'react'

function defaultCompare(a, b) {
  if (a === b) return 0
  if (a === null || a === undefined) return 1
  if (b === null || b === undefined) return -1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
}

export default function DataTable({
  columns,
  rows,
  empty = 'No data',
  defaultSortKey = null,
  defaultSortDir = 'desc',
  paginated = false,
  initialPageSize = 25,
  pageSizeOptions = [25, 50, 100],
}) {
  const firstSortable = columns.find((col) => col.sortValue || col.sortable !== false)
  const [sortKey, setSortKey] = useState(defaultSortKey || firstSortable?.key || null)
  const [sortDir, setSortDir] = useState(defaultSortDir)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(initialPageSize)

  const sortedRows = useMemo(() => {
    if (!sortKey) return rows
    const column = columns.find((col) => col.key === sortKey)
    if (!column) return rows
    const getValue = column.sortValue || ((row) => row[sortKey])
    const compare = column.compare || defaultCompare
    return [...rows].sort((left, right) => {
      const result = compare(getValue(left), getValue(right), left, right)
      return sortDir === 'asc' ? result : -result
    })
  }, [columns, rows, sortDir, sortKey])

  const totalRows = sortedRows.length
  const totalPages = paginated ? Math.max(1, Math.ceil(totalRows / pageSize)) : 1
  const visibleRows = useMemo(() => {
    if (!paginated) return sortedRows
    const start = (page - 1) * pageSize
    return sortedRows.slice(start, start + pageSize)
  }, [page, pageSize, paginated, sortedRows])

  useEffect(() => {
    setPage(1)
  }, [rows, sortDir, sortKey, pageSize])

  useEffect(() => {
    if (page > totalPages) setPage(totalPages)
  }, [page, totalPages])

  function onSort(column) {
    if (column.sortable === false) return
    if (sortKey === column.key) {
      setSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'))
      return
    }
    setSortKey(column.key)
    setSortDir('desc')
  }

  function sortIndicator(column) {
    if (column.sortable === false) return ''
    if (sortKey !== column.key) return ' ↕'
    return sortDir === 'asc' ? ' ↑' : ' ↓'
  }

  return (
    <>
      {paginated && totalRows > 0 ? (
        <div className="table-pager">
          <div className="market-toolbar-hint">
            Page {page} / {totalPages} · rows {(page - 1) * pageSize + 1}-{Math.min(page * pageSize, totalRows)} of {totalRows}
          </div>
          <div className="table-pager-actions">
            <label className="market-toolbar-hint">
              Rows/page{' '}
              <select value={pageSize} onChange={(event) => setPageSize(Number(event.target.value))}>
                {pageSizeOptions.map((option) => <option key={option} value={option}>{option}</option>)}
              </select>
            </label>
            <button className="filter-chip" type="button" onClick={() => setPage(1)} disabled={page <= 1}>First</button>
            <button className="filter-chip" type="button" onClick={() => setPage((value) => Math.max(1, value - 1))} disabled={page <= 1}>Prev</button>
            <button className="filter-chip" type="button" onClick={() => setPage((value) => Math.min(totalPages, value + 1))} disabled={page >= totalPages}>Next</button>
            <button className="filter-chip" type="button" onClick={() => setPage(totalPages)} disabled={page >= totalPages}>Last</button>
          </div>
        </div>
      ) : null}
      <div className="table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col.key}>
                  <button className="sort-button" type="button" onClick={() => onSort(col)} disabled={col.sortable === false}>
                    {col.title}{sortIndicator(col)}
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visibleRows.length ? visibleRows.map((row, idx) => (
              <tr key={row.id || row.key || row.position_id || row.candidate_id || idx}>
                {columns.map((col) => (
                  <td key={col.key}>{col.render ? col.render(row) : row[col.key]}</td>
                ))}
              </tr>
            )) : (
              <tr>
                <td colSpan={columns.length} className="empty-cell">{empty}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  )
}
