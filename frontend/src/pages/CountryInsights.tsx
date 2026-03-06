import DataTable, { Column, confidenceRenderer } from '../components/DataTable'
import { useInsightsTable } from '../hooks/useInsightsTable'

const columns: Column<Record<string, unknown>>[] = [
  {
    key: 'name',
    label: 'Country',
    render: (row) => {
      const country = row.country as Record<string, unknown> | null
      return <span className="font-medium text-earth-900">{country?.name as string ?? '—'}</span>
    },
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

export default function CountryInsights() {
  const table = useInsightsTable({
    table: 'country_insights',
    nameColumn: 'confidence',
    joinSelect: '*, country:countries(name)',
  })

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Country Insights</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount} countries enriched with wine history, key regions, and regulatory overview</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search countries..."
        detailPath={(row) => `/insights/countries/${row.country_id}`}
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
