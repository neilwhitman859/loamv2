import DataTable, { Column, confidenceRenderer } from '../components/DataTable'
import { useInsightsTable } from '../hooks/useInsightsTable'

const columns: Column<Record<string, unknown>>[] = [
  {
    key: 'name',
    label: 'Grape',
    render: (row) => {
      const grape = row.grape as Record<string, unknown> | null
      return <span className="font-medium text-earth-900">{grape?.name as string ?? '—'}</span>
    },
  },
  {
    key: 'color',
    label: 'Color',
    render: (row) => {
      const grape = row.grape as Record<string, unknown> | null
      const color = grape?.color as string
      return (
        <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
          color === 'red' ? 'bg-red-100 text-red-800' : 'bg-yellow-100 text-yellow-800'
        }`}>
          {color || '—'}
        </span>
      )
    },
    className: 'w-20',
  },
  { key: 'confidence', label: 'Confidence', render: confidenceRenderer, className: 'w-24' },
  {
    key: 'ai_overview',
    label: 'Overview',
    render: (row) => (
      <span className="text-earth-600 line-clamp-2 text-xs">{row.ai_overview as string}</span>
    ),
  },
]

export default function GrapeInsights() {
  const table = useInsightsTable({
    table: 'grape_insights',
    nameColumn: 'confidence',
    joinSelect: '*, grape:grapes(name, color)',
  })

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Grape Insights</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount} grapes enriched with AI-generated tasting notes, food pairings, and regional profiles</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search grapes..."
        detailPath={(row) => `/insights/grapes/${row.grape_id}`}
        page={table.page}
        pageSize={table.pageSize}
        totalCount={table.totalCount}
        onPageChange={table.setPage}
        sortColumn={table.sortColumn}
        sortDirection={table.sortDirection}
        onSort={table.handleSort}
      />
    </div>
  )
}
