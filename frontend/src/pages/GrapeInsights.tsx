import { useEffect, useState } from 'react'
import DataTable, { type Column } from '../components/DataTable'
import { useInsightsTable } from '../hooks/useInsightsTable'
import { supabase } from '../lib/supabase'

function confidenceBar(row: Record<string, unknown>) {
  const conf = row.confidence as number
  if (conf == null) return <span className="text-earth-300">—</span>
  const pct = Math.round(conf * 100)
  const color = conf >= 0.85 ? 'bg-emerald-500' : conf >= 0.7 ? 'bg-amber-500' : conf >= 0.5 ? 'bg-orange-500' : 'bg-red-400'
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-2 bg-earth-100 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-earth-600 w-8 text-right">{pct}%</span>
    </div>
  )
}

export default function GrapeInsights() {
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
    table: 'grape_insights',
    nameColumn: 'confidence',
    searchColumn: 'ai_overview',
    joinSelect: '*, grape:grapes(name, color)',
    defaultSortColumn: 'confidence',
    defaultSortDirection: 'desc',
  })

  const columns: Column<Record<string, unknown>>[] = [
    {
      key: 'name',
      label: 'Grape',
      render: (row) => {
        const grape = row.grape as Record<string, unknown> | null
        return <span className="font-medium text-earth-900">{grape?.name as string ?? '—'}</span>
      },
    },
    {
      key: 'color',
      label: 'Color',
      render: (row) => {
        const grape = row.grape as Record<string, unknown> | null
        const color = grape?.color as string
        return (
          <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
            color === 'red' ? 'bg-red-100 text-red-800' : 'bg-yellow-100 text-yellow-800'
          }`}>
            {color || '—'}
          </span>
        )
      },
      className: 'w-20',
    },
    {
      key: 'wine_count',
      label: 'Wines',
      render: (row) => {
        const id = row.grape_id as string
        const count = wineCounts.get(id)
        if (count == null) return <span className="text-earth-300 text-xs">—</span>
        return <span className="text-earth-700 text-xs font-medium">{count.toLocaleString()}</span>
      },
      sortable: false,
      className: 'w-20',
    },
    { key: 'confidence', label: 'Confidence', render: confidenceBar, className: 'w-32' },
    {
      key: 'ai_overview',
      label: 'Overview',
      render: (row) => (
        <span className="text-earth-600 line-clamp-2 text-xs">{row.ai_overview as string}</span>
      ),
    },
  ]

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Grape Insights</h1>
      <p className="text-sm text-earth-500 mb-6">{table.totalCount} grapes enriched with AI-generated tasting notes, food pairings, and regional profiles</p>
      <DataTable
        data={table.data}
        columns={columns}
        loading={table.loading}
        search={table.search}
        onSearchChange={table.setSearch}
        searchPlaceholder="Search grapes..."
        detailPath={(row) => `/insights/grapes/${row.grape_id}`}
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
