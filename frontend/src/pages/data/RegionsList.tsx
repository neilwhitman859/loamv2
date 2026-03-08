import { useEffect, useState } from 'react'
import DataTable, { type Column } from '../../components/DataTable'
import { useInsightsTable } from '../../hooks/useInsightsTable'
import { supabase } from '../../lib/supabase'

export default function RegionsList() {
  const [wineCounts, setWineCounts] = useState<Map<string, number>>(new Map())

  useEffect(() => {
    supabase.rpc('region_wine_counts').then(({ data }) => {
      if (data) {
        const m = new Map<string, number>()
        for (const r of data as { region_id: string; wine_count: number }[]) {
          m.set(r.region_id, r.wine_count)
        }
        setWineCounts(m)
      }
    })
  }, [])

  const table = useInsightsTable({
    table: 'regions',
    nameColumn: 'name',
    joinSelect: 'id, name, slug, country:countries!country_id(id, name), parent:regions!parent_id(id, name)',
    pageSize: 50,
  })

  const columns: Column<Record<string, unknown>>[] = [
    {
      key: 'name',
      label: 'Region',
      render: (row) => <span className="font-medium text-earth-900">{row.name as string}</span>,
    },
    {
      key: 'country',
      label: 'Country',
      render: (row) => {
        const c = row.country as Record<string, unknown> | null
        return <span className="text-earth-600 text-xs">{(c?.name as string) ?? '—'}</span>
      },
      className: 'w-28',
    },
    {
      key: 'parent',
      label: 'Parent Region',
      render: (row) => {
        const p = row.parent as Record<string, unknown> | null
        return <span className="text-earth-500 text-xs">{(p?.name as string) ?? '—'}</span>
      },
      className: 'w-40',
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
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Regions</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount.toLocaleString()} regions in the database</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search regions..."
        detailPath={(row) => `/data/regions/${row.id}`}
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
