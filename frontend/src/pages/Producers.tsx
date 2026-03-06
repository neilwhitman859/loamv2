import DataTable, { Column } from '../components/DataTable'
import { useInsightsTable } from '../hooks/useInsightsTable'

const columns: Column<Record<string, unknown>>[] = [
  {
    key: 'name',
    label: 'Producer',
    render: (row) => <span className="font-medium text-earth-900">{row.name as string}</span>,
  },
  {
    key: 'country',
    label: 'Country',
    render: (row) => {
      const c = row.country as Record<string, unknown> | null
      return <span className="text-earth-600 text-xs">{c?.name as string ?? '—'}</span>
    },
    className: 'w-32',
  },
  {
    key: 'region',
    label: 'Region',
    render: (row) => {
      const r = row.region as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{r?.name as string ?? '—'}</span>
    },
    className: 'w-40',
  },
]

export default function Producers() {
  const table = useInsightsTable({
    table: 'producers',
    nameColumn: 'name',
    joinSelect: 'id, name, country:countries(name), region:regions(name)',
    pageSize: 50,
  })

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Producers</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount.toLocaleString()} producers in the database</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search producers..."
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
