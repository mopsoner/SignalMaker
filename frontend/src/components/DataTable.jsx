import { useMemo, useState } from 'react'

function defaultCompare(a, b) {
  if (a === b) return 0
  if (a === null || a === undefined) return 1
  if (b === null || b === undefined) return -1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
}

export default function DataTable({ columns, rows, empty = 'No data', defaultSortKey = null, defaultSortDir = 'desc' }) {
  const firstSortable = columns.find((col) => col.sortValue || col.sortable !== false)
  const [sortKey, setSortKey] = useState(defaultSortKey || firstSortable?.key || null)
  const [sortDir, setSortDir] = useState(defaultSortDir)

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
          {sortedRows.length ? sortedRows.map((row, idx) => (
            <tr key={row.id || row.key || idx}>
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
  )
}
