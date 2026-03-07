import DataTable, { type Column, confidenceRenderer } from '../components/DataTable'
import { useInsightsTable } from '../hooks/useInsightsTable'

const columns: Column<Record<string, unknown>>[] = [
  {
    key: 'name',
    label: 'Appellation',
    render: (row) => {
      const app = row.appellation as Record<string, unknown> | null
      return <span className="font-medium text-earth-900">{app?.name as string ?? '—'}</span>
    },
  },
  {
    key: 'country',
    label: 'Country',
    render: (row) => {
      const app = row.appellation as Record<string, unknown> | null
      const country = app?.country as Record<string, unknown> | null
      return <span className="text-earth-600 text-xs">{country?.name as string ?? '—'}</span>
    },
    className: 'w-32',
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

export default function AppellationInsights() {
  const table = useInsightsTable({
    table: 'appellation_insights',
    nameColumn: 'confidence',
    joinSelect: '*, appellation:appellations(name, country:countries(name))',
  })

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Appellation Insights</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount} appellations enriched with climate, soil, and style profiles</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search appellations..."
        detailPath={(row) => `/insights/appellations/${row.appellation_id}`}
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
