import { useEffect, useState } from 'react'
import { supabase } from '../lib/supabase'

interface TableStat {
  label: string
  table: string
  count: number | null
  insightsTable?: string
  insightsCount?: number | null
}

interface ConfDist {
  label: string
  high: number
  moderate: number
  low: number
  veryLow: number
  total: number
}

export default function Dashboard() {
  const [stats, setStats] = useState<TableStat[]>([])
  const [confDists, setConfDists] = useState<ConfDist[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      const tables = [
        { label: 'Wines', table: 'wines' },
        { label: 'Producers', table: 'producers' },
        { label: 'Grapes', table: 'grapes', insightsTable: 'grape_insights' },
        { label: 'Appellations', table: 'appellations', insightsTable: 'appellation_insights' },
        { label: 'Regions', table: 'regions', insightsTable: 'region_insights' },
        { label: 'Countries', table: 'countries', insightsTable: 'country_insights' },
      ]

      const results: TableStat[] = []
      const dists: ConfDist[] = []

      for (const t of tables) {
        const { count } = await supabase.from(t.table).select('*', { count: 'exact', head: true })
        let insightsCount = null
        if (t.insightsTable) {
          const { count: ic } = await supabase.from(t.insightsTable).select('*', { count: 'exact', head: true })
          insightsCount = ic

          // Confidence distribution
          const { data: rows } = await supabase.from(t.insightsTable).select('confidence')
          if (rows) {
            const high = rows.filter(r => (r.confidence as number) >= 0.85).length
            const moderate = rows.filter(r => (r.confidence as number) >= 0.7 && (r.confidence as number) < 0.85).length
            const low = rows.filter(r => (r.confidence as number) >= 0.5 && (r.confidence as number) < 0.7).length
            const veryLow = rows.filter(r => (r.confidence as number) < 0.5).length
            dists.push({ label: t.label, high, moderate, low, veryLow, total: rows.length })
          }
        }
        results.push({ ...t, count, insightsCount })
      }

      setStats(results)
      setConfDists(dists)
      setLoading(false)
    }
    load()
  }, [])

  if (loading) {
    return <div className="text-earth-400 py-12 text-center">Loading dashboard...</div>
  }

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Dashboard</h1>
      <p className="text-sm text-earth-500 mb-6">Loam v2 wine intelligence database overview</p>

      {/* Core counts */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-8">
        {stats.map((s) => (
          <div key={s.table} className="bg-white rounded-lg border border-earth-200 p-4">
            <div className="text-2xl font-bold text-earth-900">{s.count?.toLocaleString() ?? '—'}</div>
            <div className="text-xs text-earth-500 mt-1">{s.label}</div>
            {s.insightsTable && (
              <div className="text-xs text-wine-600 mt-2">
                {s.insightsCount ?? 0} insights
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Confidence distributions */}
      <h2 className="text-lg font-semibold text-earth-900 mb-3">Confidence Distribution</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8">
        {confDists.map((d) => (
          <div key={d.label} className="bg-white rounded-lg border border-earth-200 p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="font-semibold text-earth-800">{d.label}</h3>
              <span className="text-xs text-earth-400">{d.total} total</span>
            </div>
            <div className="space-y-2">
              <ConfBar label="High (≥85%)" count={d.high} total={d.total} color="bg-emerald-500" />
              <ConfBar label="Moderate (70-84%)" count={d.moderate} total={d.total} color="bg-amber-500" />
              <ConfBar label="Low (50-69%)" count={d.low} total={d.total} color="bg-orange-500" />
              <ConfBar label="Very low (<50%)" count={d.veryLow} total={d.total} color="bg-red-400" />
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function ConfBar({ label, count, total, color }: { label: string; count: number; total: number; color: string }) {
  const pct = total > 0 ? (count / total) * 100 : 0
  return (
    <div className="flex items-center gap-3 text-xs">
      <span className="w-28 text-earth-600 shrink-0">{label}</span>
      <div className="flex-1 h-4 bg-earth-100 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full transition-all`} style={{ width: `${pct}%` }} />
      </div>
      <span className="w-10 text-right text-earth-500">{count}</span>
    </div>
  )
}
