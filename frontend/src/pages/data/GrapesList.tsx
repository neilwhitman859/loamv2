import { useEffect, useState } from 'react'
import DataTable, { type Column } from '../../components/DataTable'
import { useInsightsTable } from '../../hooks/useInsightsTable'
import { supabase } from '../../lib/supabase'

export default function GrapesList() {
  const [wineCounts, setWineCounts] = useState<Map<string, number>>(new Map())

  useEffect(() => {
    supabase.rpc('grape_wine_counts').then(({ data }) => {
      if (data) {
        const m = new Map<string, number>()
        for (const r of data as { grape_id: string; wine_count: number }[]) {
          m.set(r.grape_id, r.wine_count)
        }
        setWineCounts(m)
      }
    })
  }, [])

  const table = useInsightsTable({
    table: 'grapes',
    nameColumn: 'name',
    joinSelect: 'id, name, slug, color, origin_country:countries!origin_country_id(id, name)',
    pageSize: 50,
  })

  const columns: Column<Record<string, unknown>>[] = [
    {
      key: 'name',
      label: 'Grape',
      render: (row) => <span className="font-medium text-earth-900">{row.name as string}</span>,
    },
    {
      key: 'color',
      label: 'Color',
      render: (row) => {
        const color = row.color as string
        return (
          <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
            color === 'red' ? 'bg-red-100 text-red-800' : color === 'white' ? 'bg-yellow-100 text-yellow-800' : 'bg-pink-100 text-pink-800'
          }`}>
            {color || '—'}
          </span>
        )
      },
      className: 'w-20',
    },
    {
      key: 'origin_country',
      label: 'Origin',
      render: (row) => {
        const c = row.origin_country as Record<string, unknown> | null
        return <span className="text-earth-500 text-xs">{(c?.name as string) ?? '—'}</span>
      },
      className: 'w-32',
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
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Grapes</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount.toLocaleString()} grape varieties in the database</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search grapes..."
        detailPath={(row) => `/data/grapes/${row.id}`}
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
