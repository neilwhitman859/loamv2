import { useEffect, useState } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'

interface SearchResult {
  id: string
  name: string
  snippet?: string
}

interface ResultGroup {
  type: 'wines' | 'producers' | 'grapes' | 'appellations' | 'regions' | 'countries'
  label: string
  results: SearchResult[]
  total: number
}

const ENTITY_TYPES = [
  { type: 'wines' as const, label: 'Wines', table: 'wines', insightTable: 'wine_insights', insightFK: 'wine_id' },
  { type: 'producers' as const, label: 'Producers', table: 'producers', insightTable: 'producer_insights', insightFK: 'producer_id' },
  { type: 'grapes' as const, label: 'Grapes', table: 'grapes', insightTable: 'grape_insights', insightFK: 'grape_id' },
  { type: 'appellations' as const, label: 'Appellations', table: 'appellations', insightTable: 'appellation_insights', insightFK: 'appellation_id' },
  { type: 'regions' as const, label: 'Regions', table: 'regions', insightTable: 'region_insights', insightFK: 'region_id' },
  { type: 'countries' as const, label: 'Countries', table: 'countries', insightTable: 'country_insights', insightFK: 'country_id' },
]

export default function SearchResults() {
  const [params] = useSearchParams()
  const query = params.get('q') || ''
  const [groups, setGroups] = useState<ResultGroup[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!query.trim()) { setGroups([]); return }
    setLoading(true)

    const q = `%${query.trim()}%`

    // Search entity names + insight ai_overview in parallel
    const searches = ENTITY_TYPES.map(async (entity) => {
      // Search by name
      const { data: nameResults, count } = await supabase
        .from(entity.table)
        .select('id, name', { count: 'exact' })
        .ilike('name', q)
        .limit(10)

      // Search by AI overview text
      const { data: insightResults } = await supabase
        .from(entity.insightTable)
        .select('*')
        .ilike('ai_overview', q)
        .limit(5) as { data: Record<string, unknown>[] | null }

      // Merge: name results first, then insight-only results
      const nameIds = new Set((nameResults ?? []).map((r: Record<string, unknown>) => r.id as string))
      const results: SearchResult[] = (nameResults ?? []).map((r: Record<string, unknown>) => ({
        id: r.id as string,
        name: r.name as string,
      }))

      // For insight matches not already in name results, fetch the entity name
      if (insightResults) {
        const insightOnly = (insightResults as Record<string, unknown>[]).filter(r => !nameIds.has(r[entity.insightFK] as string))
        if (insightOnly.length > 0) {
          const ids = insightOnly.map(r => r[entity.insightFK] as string)
          const { data: entities } = await supabase.from(entity.table).select('id, name').in('id', ids)
          if (entities) {
            for (const e of entities as Record<string, unknown>[]) {
              const insightRow = insightOnly.find(r => r[entity.insightFK] === e.id)
              results.push({
                id: e.id as string,
                name: e.name as string,
                snippet: (insightRow?.ai_overview as string)?.slice(0, 200),
              })
            }
          }
        }
      }

      // Add snippets from insights to name-matched results
      if (insightResults) {
        for (const r of results) {
          if (!r.snippet) {
            const insightRow = (insightResults as Record<string, unknown>[]).find(i => i[entity.insightFK] === r.id)
            if (insightRow?.ai_overview) {
              const overview = insightRow.ai_overview as string
              const idx = overview.toLowerCase().indexOf(query.toLowerCase())
              if (idx >= 0) {
                const start = Math.max(0, idx - 40)
                const end = Math.min(overview.length, idx + query.length + 80)
                r.snippet = (start > 0 ? '...' : '') + overview.slice(start, end) + (end < overview.length ? '...' : '')
              }
            }
          }
        }
      }

      return {
        type: entity.type,
        label: entity.label,
        results,
        total: count ?? results.length,
      }
    })

    Promise.all(searches).then((results) => {
      setGroups(results.filter(g => g.results.length > 0))
      setLoading(false)
    })
  }, [query])

  const totalResults = groups.reduce((s, g) => s + g.results.length, 0)

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Search Results</h1>
      {query && (
        <p className="text-sm text-earth-500 mb-6">
          {loading ? 'Searching...' : `${totalResults} results for "${query}"`}
        </p>
      )}

      {!query && <p className="text-earth-400 py-12 text-center">Enter a search term to find wines, producers, grapes, regions, and more.</p>}

      {!loading && query && groups.length === 0 && (
        <p className="text-earth-400 py-12 text-center">No results found for "{query}"</p>
      )}

      <div className="space-y-8">
        {groups.map((group) => (
          <div key={group.type}>
            <h2 className="text-sm font-semibold uppercase tracking-wider text-wine-600 mb-3">
              {group.label} <span className="text-earth-400 font-normal">({group.total})</span>
            </h2>
            <div className="space-y-2">
              {group.results.map((r) => (
                <Link
                  key={r.id}
                  to={`/data/${group.type}/${r.id}`}
                  className="block bg-white rounded-lg border border-earth-200 px-4 py-3 hover:border-wine-300 hover:shadow-sm transition-all"
                >
                  <div className="font-medium text-earth-900">{r.name}</div>
                  {r.snippet && (
                    <div className="text-xs text-earth-500 mt-1 line-clamp-2">{r.snippet}</div>
                  )}
                </Link>
              ))}
              {group.total > group.results.length && (
                <p className="text-xs text-earth-400 pl-1">...and {group.total - group.results.length} more</p>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
