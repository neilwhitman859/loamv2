import DataTable, { type Column } from '../components/DataTable'
import { useInsightsTable } from '../hooks/useInsightsTable'

const columns: Column<Record<string, unknown>>[] = [
  {
    key: 'name',
    label: 'Wine',
    render: (row) => <span className="font-medium text-earth-900">{row.name as string}</span>,
  },
  {
    key: 'producer',
    label: 'Producer',
    render: (row) => {
      const p = row.producer as Record<string, unknown> | null
      return <span className="text-earth-600 text-xs">{p?.name as string ?? '—'}</span>
    },
    className: 'w-48',
  },
  {
    key: 'appellation',
    label: 'Appellation',
    render: (row) => {
      const a = row.appellation as Record<string, unknown> | null
      return <span className="text-earth-600 text-xs">{a?.name as string ?? '—'}</span>
    },
    className: 'w-40',
  },
  {
    key: 'country',
    label: 'Country',
    render: (row) => {
      const c = row.country as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{c?.name as string ?? '—'}</span>
    },
    className: 'w-28',
  },
]

export default function Wines() {
  const table = useInsightsTable({
    table: 'wines',
    nameColumn: 'name',
    joinSelect: 'id, name, producer:producers(name), appellation:appellations(name), country:countries(name)',
    pageSize: 50,
  })

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Wines</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount.toLocaleString()} wines in the database</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search wines..."
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
