import { useEffect, useState } from 'react'
import { supabase } from '../lib/supabase'
import { Link } from 'react-router-dom'

interface TableStat {
  label: string
  table: string
  count: number | null
  insightsTable?: string
  insightsCount?: number | null
  link?: string
}

interface ConfDist {
  label: string
  high: number
  moderate: number
  low: number
  veryLow: number
  total: number
}

interface DataHealth {
  label: string
  count: number
  total: number
  description: string
  severity: 'good' | 'warn' | 'bad'
}

interface DashboardStats {
  wines_with_grapes: number
  wines_with_scores: number
  wines_with_prices: number
  total_scores: number
  total_prices: number
  total_vintages: number
  total_wine_grapes: number
  wines_without_grapes: number
  avg_grapes_per_wine: number
}

export default function Dashboard() {
  const [stats, setStats] = useState<TableStat[]>([])
  const [confDists, setConfDists] = useState<ConfDist[]>([])
  const [dbStats, setDbStats] = useState<DashboardStats | null>(null)
  const [healthChecks, setHealthChecks] = useState<DataHealth[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      const tables = [
        { label: 'Wines', table: 'wines', link: '/data/wines' },
        { label: 'Producers', table: 'producers', link: '/data/producers' },
        { label: 'Grapes', table: 'grapes', insightsTable: 'grape_insights' },
        { label: 'Appellations', table: 'appellations', insightsTable: 'appellation_insights' },
        { label: 'Regions', table: 'regions', insightsTable: 'region_insights' },
        { label: 'Countries', table: 'countries', insightsTable: 'country_insights' },
      ]

      // Fetch core + dashboard stats in parallel
      const [coreResults, dashStats] = await Promise.all([
        Promise.all(tables.map(async (t) => {
          const { count } = await supabase.from(t.table).select('*', { count: 'exact', head: true })
          let insightsCount = null
          let confDist: ConfDist | null = null

          if (t.insightsTable) {
            const { count: ic } = await supabase.from(t.insightsTable).select('*', { count: 'exact', head: true })
            insightsCount = ic

            const { data: rows } = await supabase.from(t.insightsTable).select('confidence')
            if (rows) {
              const high = rows.filter(r => (r.confidence as number) >= 0.85).length
              const moderate = rows.filter(r => (r.confidence as number) >= 0.7 && (r.confidence as number) < 0.85).length
              const low = rows.filter(r => (r.confidence as number) >= 0.5 && (r.confidence as number) < 0.7).length
              const veryLow = rows.filter(r => (r.confidence as number) < 0.5).length
              confDist = { label: t.label, high, moderate, low, veryLow, total: rows.length }
            }
          }
          return { stat: { ...t, count, insightsCount }, confDist }
        })),
        // Dashboard aggregate stats via RPC
        supabase.rpc('get_dashboard_stats'),
      ])

      // Set core stats
      const results: TableStat[] = coreResults.map(r => r.stat)
      const dists: ConfDist[] = coreResults.map(r => r.confDist).filter((d): d is ConfDist => d !== null)
      setStats(results)
      setConfDists(dists)

      // Set aggregate stats from RPC
      const ds = dashStats.data as DashboardStats | null
      if (ds) {
        setDbStats(ds)

        // Build health checks from RPC data
        const wineCount = results.find(r => r.table === 'wines')?.count ?? 0
        const health: DataHealth[] = []

        health.push({
          label: 'Wines with grape data',
          count: ds.wines_with_grapes,
          total: wineCount,
          description: `${ds.wines_without_grapes.toLocaleString()} wines have no grape data`,
          severity: ds.wines_with_grapes / wineCount > 0.8 ? 'good' : ds.wines_with_grapes / wineCount > 0.4 ? 'warn' : 'bad',
        })

        health.push({
          label: 'Wines with scores',
          count: ds.wines_with_scores,
          total: wineCount,
          description: `${(wineCount - ds.wines_with_scores).toLocaleString()} wines have no scores`,
          severity: ds.wines_with_scores / wineCount > 0.5 ? 'good' : ds.wines_with_scores / wineCount > 0.2 ? 'warn' : 'bad',
        })

        health.push({
          label: 'Wines with prices',
          count: ds.wines_with_prices,
          total: wineCount,
          description: `${(wineCount - ds.wines_with_prices).toLocaleString()} wines have no price data`,
          severity: ds.wines_with_prices / wineCount > 0.5 ? 'good' : ds.wines_with_prices / wineCount > 0.2 ? 'warn' : 'bad',
        })

        // Insights coverage
        const totalInsights = dists.reduce((s, d) => s + d.total, 0)
        const totalEntities = results.filter(r => r.insightsTable).reduce((s, r) => s + (r.count ?? 0), 0)
        health.push({
          label: 'Entities with insights',
          count: totalInsights,
          total: totalEntities,
          description: `${(totalEntities - totalInsights).toLocaleString()} entities need enrichment`,
          severity: totalInsights / totalEntities > 0.8 ? 'good' : totalInsights / totalEntities > 0.5 ? 'warn' : 'bad',
        })

        setHealthChecks(health)
      }

      setLoading(false)
    }
    load()
  }, [])

  if (loading) {
    return <div className="text-earth-400 py-12 text-center">Loading dashboard...</div>
  }

  const wineCount = stats.find(s => s.table === 'wines')?.count ?? 1

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Dashboard</h1>
      <p className="text-sm text-earth-500 mb-6">Loam v2 wine intelligence database overview</p>

      {/* Core counts */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-8">
        {stats.map((s) => (
          <div key={s.table} className="bg-white rounded-lg border border-earth-200 p-4">
            <div className="text-2xl font-bold text-earth-900">{s.count?.toLocaleString() ?? '—'}</div>
            <div className="text-xs text-earth-500 mt-1">
              {s.link ? (
                <Link to={s.link} className="hover:text-wine-600 hover:underline">{s.label}</Link>
              ) : s.label}
            </div>
            {s.insightsTable && (
              <div className="text-xs text-wine-600 mt-2">
                {s.insightsCount ?? 0} insights
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Scores & Prices */}
      {dbStats && (
        <>
          <h2 className="text-lg font-semibold text-earth-900 mb-3">Scores & Prices</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-8">
            <StatCard value={dbStats.total_scores.toLocaleString()} label="Critic Scores" sublabel="From Vivino, Wine-Searcher, etc." color="text-purple-700" />
            <StatCard value={dbStats.total_prices.toLocaleString()} label="Price Points" sublabel="USD prices with merchants" color="text-emerald-700" />
            <StatCard value={dbStats.total_vintages.toLocaleString()} label="Vintages Tracked" sublabel="Individual vintage records" color="text-blue-700" />
            <StatCard
              value={`${Math.round((dbStats.wines_with_scores / wineCount) * 100)}%`}
              label="Score Coverage"
              sublabel={`${dbStats.wines_with_scores.toLocaleString()} of ${wineCount.toLocaleString()} wines`}
              color="text-amber-700"
            />
          </div>
        </>
      )}

      {/* Data health */}
      {healthChecks.length > 0 && (
        <>
          <h2 className="text-lg font-semibold text-earth-900 mb-3">Data Health</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-8">
            {healthChecks.map((h) => {
              const pct = h.total > 0 ? (h.count / h.total) * 100 : 0
              return (
                <div key={h.label} className="bg-white rounded-lg border border-earth-200 p-4">
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-semibold text-earth-800">{h.label}</h3>
                    <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
                      h.severity === 'good' ? 'bg-emerald-100 text-emerald-700' :
                      h.severity === 'warn' ? 'bg-amber-100 text-amber-700' :
                      'bg-red-100 text-red-700'
                    }`}>
                      {Math.round(pct)}%
                    </span>
                  </div>
                  <div className="h-2 bg-earth-100 rounded-full overflow-hidden mb-2">
                    <div
                      className={`h-full rounded-full ${
                        h.severity === 'good' ? 'bg-emerald-500' :
                        h.severity === 'warn' ? 'bg-amber-500' :
                        'bg-red-400'
                      }`}
                      style={{ width: `${Math.min(pct, 100)}%` }}
                    />
                  </div>
                  <div className="flex justify-between text-xs text-earth-500">
                    <span>{h.count.toLocaleString()} / {h.total.toLocaleString()}</span>
                    <span>{h.description}</span>
                  </div>
                </div>
              )
            })}
          </div>
        </>
      )}

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
              <ConfBar label="High (&ge;85%)" count={d.high} total={d.total} color="bg-emerald-500" />
              <ConfBar label="Moderate (70-84%)" count={d.moderate} total={d.total} color="bg-amber-500" />
              <ConfBar label="Low (50-69%)" count={d.low} total={d.total} color="bg-orange-500" />
              <ConfBar label="Very low (<50%)" count={d.veryLow} total={d.total} color="bg-red-400" />
            </div>
          </div>
        ))}
      </div>

      {/* Quick links */}
      <h2 className="text-lg font-semibold text-earth-900 mb-3">Developer Tools</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-8">
        <Link
          to="/dev/schema"
          className="bg-white rounded-lg border border-earth-200 p-4 hover:border-wine-300 hover:shadow-sm transition-all group"
        >
          <h3 className="font-semibold text-earth-800 group-hover:text-wine-700">Schema Explorer</h3>
          <p className="text-xs text-earth-500 mt-1">Interactive diagram of all tables, columns, and foreign key relationships</p>
        </Link>
        <Link
          to="/dev/tables"
          className="bg-white rounded-lg border border-earth-200 p-4 hover:border-wine-300 hover:shadow-sm transition-all group"
        >
          <h3 className="font-semibold text-earth-800 group-hover:text-wine-700">Table Browser</h3>
          <p className="text-xs text-earth-500 mt-1">Browse, search, and inspect data in any table with FK navigation</p>
        </Link>
      </div>
    </div>
  )
}

function StatCard({ value, label, sublabel, color }: { value: string; label: string; sublabel: string; color: string }) {
  return (
    <div className="bg-white rounded-lg border border-earth-200 p-4">
      <div className={`text-2xl font-bold ${color}`}>{value}</div>
      <div className="text-xs text-earth-700 font-medium mt-1">{label}</div>
      <div className="text-[10px] text-earth-400 mt-0.5">{sublabel}</div>
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
