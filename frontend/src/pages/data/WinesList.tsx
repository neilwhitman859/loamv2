import DataTable, { type Column } from '../../components/DataTable'
import { useInsightsTable } from '../../hooks/useInsightsTable'

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
      return <span className="text-earth-600 text-xs">{(p?.name as string) ?? '—'}</span>
    },
    className: 'w-48',
  },
  {
    key: 'varietal_category',
    label: 'Varietal',
    render: (row) => {
      const v = row.varietal_category as Record<string, unknown> | null
      if (!v) return <span className="text-earth-300 text-xs">—</span>
      const color = v.color as string
      return (
        <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
          color === 'red' ? 'bg-red-100 text-red-800' : color === 'white' ? 'bg-yellow-100 text-yellow-800' : 'bg-pink-100 text-pink-800'
        }`}>
          {v.name as string}
        </span>
      )
    },
    className: 'w-40',
  },
  {
    key: 'appellation',
    label: 'Appellation',
    render: (row) => {
      const a = row.appellation as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{(a?.name as string) ?? '—'}</span>
    },
    className: 'w-40',
  },
  {
    key: 'country',
    label: 'Country',
    render: (row) => {
      const c = row.country as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{(c?.name as string) ?? '—'}</span>
    },
    className: 'w-28',
  },
]

export default function WinesList() {
  const table = useInsightsTable({
    table: 'wines',
    nameColumn: 'name',
    joinSelect: 'id, name, producer:producers!producer_id(id, name), appellation:appellations!appellation_id(id, name), country:countries!country_id(id, name), region:regions!region_id(id, name), varietal_category:varietal_categories!varietal_category_id(name, color)',
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
        detailPath={(row) => `/data/wines/${row.id}`}
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
