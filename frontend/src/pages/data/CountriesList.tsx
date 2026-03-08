import { useEffect, useState } from 'react'
import DataTable, { type Column } from '../../components/DataTable'
import { useInsightsTable } from '../../hooks/useInsightsTable'
import { supabase } from '../../lib/supabase'

export default function CountriesList() {
  const [wineCounts, setWineCounts] = useState<Map<string, number>>(new Map())

  useEffect(() => {
    supabase.rpc('country_wine_counts').then(({ data }) => {
      if (data) {
        const m = new Map<string, number>()
        for (const r of data as { country_id: string; wine_count: number }[]) {
          m.set(r.country_id, r.wine_count)
        }
        setWineCounts(m)
      }
    })
  }, [])

  const table = useInsightsTable({
    table: 'countries',
    nameColumn: 'name',
    joinSelect: 'id, name, slug, iso_code',
    pageSize: 50,
  })

  const columns: Column<Record<string, unknown>>[] = [
    {
      key: 'name',
      label: 'Country',
      render: (row) => <span className="font-medium text-earth-900">{row.name as string}</span>,
    },
    {
      key: 'iso_code',
      label: 'ISO',
      render: (row) => {
        const iso = row.iso_code as string | null
        return iso ? <span className="text-xs font-mono text-earth-500">{iso}</span> : <span className="text-earth-300 text-xs">—</span>
      },
      className: 'w-16',
    },
    {
      key: 'wine_count',
      label: 'Wines',
      render: (row) => {
        const count = wineCounts.get(row.id as string)
        if (count == null) return <span className="text-earth-300 text-xs">—</span>
        return <span className="text-earth-700 text-xs font-medium">{count.toLocaleString()}</span>
      },
      sortable: false,
      className: 'w-20',
    },
  ]

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Countries</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount.toLocaleString()} countries in the database</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search countries..."
        detailPath={(row) => `/data/countries/${row.id}`}
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
