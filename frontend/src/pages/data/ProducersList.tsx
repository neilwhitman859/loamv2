import DataTable, { type Column } from '../../components/DataTable'
import { useInsightsTable } from '../../hooks/useInsightsTable'

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
      return <span className="text-earth-600 text-xs">{(c?.name as string) ?? '—'}</span>
    },
    className: 'w-32',
  },
  {
    key: 'region',
    label: 'Region',
    render: (row) => {
      const r = row.region as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{(r?.name as string) ?? '—'}</span>
    },
    className: 'w-40',
  },
  {
    key: 'website',
    label: 'Website',
    render: (row) => {
      const url = row.website_url as string | null
      if (!url) return <span className="text-earth-300 text-xs">—</span>
      return <span className="text-wine-600 text-xs truncate max-w-[120px] inline-block">{url.replace(/^https?:\/\//, '')}</span>
    },
    className: 'w-36',
    sortable: false,
  },
]

export default function ProducersList() {
  const table = useInsightsTable({
    table: 'producers',
    nameColumn: 'name',
    joinSelect: 'id, name, website_url, country:countries!country_id(id, name), region:regions!region_id(id, name)',
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
        detailPath={(row) => `/data/producers/${row.id}`}
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
