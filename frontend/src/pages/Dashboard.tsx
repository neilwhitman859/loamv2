import { useEffect, useState } from 'react'
import { supabase } from '../lib/supabase'
import { Link } from 'react-router-dom'

interface TableStat {
  label: string
  table: string
  count: number | null
  insightsTable?: string
  insightsCount?: number | null
  link: string
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
  const [dbStats, setDbStats] = useState<DashboardStats | null>(null)
  const [healthChecks, setHealthChecks] = useState<DataHealth[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      const tables = [
        { label: 'Wines', table: 'wines', link: '/data/wines', insightsTable: 'wine_insights' },
        { label: 'Producers', table: 'producers', link: '/data/producers', insightsTable: 'producer_insights' },
        { label: 'Grapes', table: 'grapes', link: '/data/grapes', insightsTable: 'grape_insights' },
        { label: 'Appellations', table: 'appellations', link: '/data/appellations', insightsTable: 'appellation_insights' },
        { label: 'Regions', table: 'regions', link: '/data/regions', insightsTable: 'region_insights' },
        { label: 'Countries', table: 'countries', link: '/data/countries', insightsTable: 'country_insights' },
      ]

      const [coreResults, dashStats] = await Promise.all([
        Promise.all(tables.map(async (t) => {
          const { count } = await supabase.from(t.table).select('*', { count: 'exact', head: true })
          let insightsCount = null
          if (t.insightsTable) {
            const { count: ic } = await supabase.from(t.insightsTable).select('*', { count: 'exact', head: true })
            insightsCount = ic
          }
          return { ...t, count, insightsCount } as TableStat
        })),
        supabase.rpc('get_dashboard_stats'),
      ])

      setStats(coreResults)

      const ds = dashStats.data as DashboardStats | null
      if (ds) {
        setDbStats(ds)

        const wineCount = coreResults.find(r => r.table === 'wines')?.count ?? 0
        const health: DataHealth[] = []

        health.push({
          label: 'Wines with grape data',
          count: ds.wines_with_grapes,
          total: wineCount,
          description: `${ds.wines_without_grapes.toLocaleString()} missing`,
          severity: ds.wines_with_grapes / wineCount > 0.8 ? 'good' : ds.wines_with_grapes / wineCount > 0.4 ? 'warn' : 'bad',
        })

        health.push({
          label: 'Wines with scores',
          count: ds.wines_with_scores,
          total: wineCount,
          description: `${(wineCount - ds.wines_with_scores).toLocaleString()} missing`,
          severity: ds.wines_with_scores / wineCount > 0.5 ? 'good' : ds.wines_with_scores / wineCount > 0.2 ? 'warn' : 'bad',
        })

        health.push({
          label: 'Wines with prices',
          count: ds.wines_with_prices,
          total: wineCount,
          description: `${(wineCount - ds.wines_with_prices).toLocaleString()} missing`,
          severity: ds.wines_with_prices / wineCount > 0.5 ? 'good' : ds.wines_with_prices / wineCount > 0.2 ? 'warn' : 'bad',
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
      <p className="text-sm text-earth-500 mb-6">Loam wine intelligence database overview</p>

      {/* Core counts — fully clickable */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-8">
        {stats.map((s) => (
          <Link
            key={s.table}
            to={s.link}
            className="bg-white rounded-lg border border-earth-200 p-4 hover:border-wine-300 hover:shadow-sm transition-all group"
          >
            <div className="text-2xl font-bold text-earth-900 group-hover:text-wine-700">{s.count?.toLocaleString() ?? '—'}</div>
            <div className="text-xs text-earth-500 mt-1 group-hover:text-wine-600">{s.label}</div>
          </Link>
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
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-8">
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

      {/* Enrichment coverage */}
      {stats.some(s => s.insightsTable) && (
        <>
          <h2 className="text-lg font-semibold text-earth-900 mb-3">Enrichment Coverage</h2>
          <div className="bg-white rounded-lg border border-earth-200 p-4 mb-8">
            <div className="space-y-3">
              {stats.filter(s => s.insightsTable).map((s) => {
                const pct = s.count && s.insightsCount != null ? (s.insightsCount / s.count) * 100 : 0
                const full = pct >= 99
                return (
                  <div key={s.table} className="flex items-center gap-3 text-sm">
                    <span className="w-24 text-earth-700 font-medium shrink-0">{s.label}</span>
                    <div className="flex-1 h-3 bg-earth-100 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${full ? 'bg-emerald-500' : pct > 50 ? 'bg-amber-500' : 'bg-red-400'}`}
                        style={{ width: `${Math.min(pct, 100)}%` }}
                      />
                    </div>
                    <span className={`w-14 text-right text-xs font-medium ${full ? 'text-emerald-600' : pct > 50 ? 'text-amber-600' : 'text-red-500'}`}>
                      {Math.round(pct)}%
                    </span>
                    <span className="w-20 text-right text-xs text-earth-400">
                      {(s.insightsCount ?? 0).toLocaleString()} / {(s.count ?? 0).toLocaleString()}
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        </>
      )}

      {/* Quick links */}
      <h2 className="text-lg font-semibold text-earth-900 mb-3">Developer Tools</h2>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-8">
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
        <Link
          to="/dev/strategy"
          className="bg-white rounded-lg border border-earth-200 p-4 hover:border-wine-300 hover:shadow-sm transition-all group"
        >
          <h3 className="font-semibold text-earth-800 group-hover:text-wine-700">Strategy</h3>
          <p className="text-xs text-earth-500 mt-1">Strategic roadmap, priorities, and next moves</p>
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
