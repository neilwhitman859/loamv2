import DataTable, { Column, confidenceRenderer } from '../components/DataTable'
import { useInsightsTable } from '../hooks/useInsightsTable'

const columns: Column<Record<string, unknown>>[] = [
  {
    key: 'name',
    label: 'Region',
    render: (row) => {
      const region = row.region as Record<string, unknown> | null
      return <span className="font-medium text-earth-900">{region?.name as string ?? '—'}</span>
    },
  },
  {
    key: 'country',
    label: 'Country',
    render: (row) => {
      const region = row.region as Record<string, unknown> | null
      const country = region?.country as Record<string, unknown> | null
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

export default function RegionInsights() {
  const table = useInsightsTable({
    table: 'region_insights',
    nameColumn: 'confidence',
    joinSelect: '*, region:regions(name, country:countries(name))',
  })

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Region Insights</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount} regions enriched with climate, sub-region, and style profiles</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search regions..."
        detailPath={(row) => `/insights/regions/${row.region_id}`}
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
