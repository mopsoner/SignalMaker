export default function DataTable({ columns, rows, empty = 'No data' }) {
  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.key}>{col.title}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length ? rows.map((row, idx) => (
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
