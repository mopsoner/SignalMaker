import DataTable from './DataTable'

export default function FoldableTable({
  title,
  columns,
  rows,
  empty,
  defaultOpen = true,
  defaultSortKey,
  defaultSortDir,
  hint,
  actions,
  paginated = false,
  initialPageSize = 25,
  pageSizeOptions,
}) {
  const count = Array.isArray(rows) ? rows.length : 0
  return (
    <details className="panel collapsible-panel" open={defaultOpen}>
      <summary>
        <div>
          <h2>{title}</h2>
          {hint ? <p className="stat-hint" style={{ marginTop: 4 }}>{hint}</p> : null}
        </div>
        <span className="collapse-indicator">⌄</span>
      </summary>
      {actions ? <div className="page-actions" style={{ marginBottom: 10 }}>{actions}</div> : null}
      <div className="market-toolbar-hint" style={{ marginBottom: 10 }}>Rows: {count}</div>
      <DataTable
        columns={columns}
        rows={rows}
        empty={empty}
        defaultSortKey={defaultSortKey}
        defaultSortDir={defaultSortDir}
        paginated={paginated}
        initialPageSize={initialPageSize}
        pageSizeOptions={pageSizeOptions}
      />
    </details>
  )
}
