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

export default function CountryInsights() {
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
    table: 'country_insights',
    nameColumn: 'confidence',
    searchColumn: 'ai_overview',
    joinSelect: '*, country:countries(name)',
    defaultSortColumn: 'confidence',
    defaultSortDirection: 'desc',
  })

  const columns: Column<Record<string, unknown>>[] = [
    {
      key: 'name',
      label: 'Country',
      render: (row) => {
        const country = row.country as Record<string, unknown> | null
        return <span className="font-medium text-earth-900">{country?.name as string ?? '—'}</span>
      },
    },
    {
      key: 'wine_count',
      label: 'Wines',
      render: (row) => {
        const id = row.country_id as string
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
