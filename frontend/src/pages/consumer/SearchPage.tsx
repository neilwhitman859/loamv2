import { useState, useEffect } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'

interface SearchResult {
  entity_type: string
  id: string
  name: string
  slug: string
  subtitle: string
  score: number
}

const TYPE_LABELS: Record<string, string> = {
  wine: 'Wine',
  producer: 'Producer',
  region: 'Region',
  appellation: 'Appellation',
  grape: 'Grape',
}

const TYPE_PATHS: Record<string, string> = {
  wine: '/wine',
  producer: '/producer',
  region: '/region',
  appellation: '/appellation',
  grape: '/grape',
}

const TYPE_COLORS: Record<string, string> = {
  wine: 'bg-wine-50 text-wine-600',
  producer: 'bg-amber-50 text-amber-700',
  grape: 'bg-green-50 text-green-700',
  region: 'bg-blue-50 text-blue-700',
  appellation: 'bg-earth-100 text-earth-600',
}

export default function SearchPage() {
  const [searchParams] = useSearchParams()
  const query = searchParams.get('q') || ''
  const [results, setResults] = useState<SearchResult[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!query) {
      setResults([])
      setLoading(false)
      return
    }

    setLoading(true)
    supabase.rpc('search_catalog', {
      query,
      result_limit: 50,
      entity_types: ['wine', 'producer', 'region', 'appellation', 'grape'],
    }).then(({ data, error }) => {
      if (!error && data) setResults(data)
      setLoading(false)
    })
  }, [query])

  // Group results by type
  const grouped = results.reduce<Record<string, SearchResult[]>>((acc, r) => {
    if (!acc[r.entity_type]) acc[r.entity_type] = []
    acc[r.entity_type].push(r)
    return acc
  }, {})

  const typeOrder = ['wine', 'producer', 'region', 'appellation', 'grape']
  const sortedTypes = typeOrder.filter(t => grouped[t]?.length)

  return (
    <div className="max-w-3xl mx-auto px-4 py-6">
      <h1 className="font-display text-2xl font-semibold text-earth-800 mb-1">
        {query ? `Results for "${query}"` : 'Search'}
      </h1>
      <p className="text-sm text-earth-400 mb-6">
        {loading ? 'Searching...' : `${results.length} result${results.length !== 1 ? 's' : ''}`}
      </p>

      {!loading && results.length === 0 && query && (
        <div className="text-center py-16">
          <p className="text-earth-500 mb-2">No results found</p>
          <p className="text-sm text-earth-400">
            Try a different spelling, or search by region or grape variety
          </p>
        </div>
      )}

      {sortedTypes.map(type => (
        <section key={type} className="mb-8">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-earth-400 mb-3">
            {TYPE_LABELS[type]}s ({grouped[type].length})
          </h2>
          <div className="space-y-1">
            {grouped[type].map(r => (
              <Link
                key={r.id}
                to={`${TYPE_PATHS[r.entity_type]}/${r.id}`}
                className="block px-4 py-3 bg-white rounded-lg hover:bg-earth-50 transition-colors border border-earth-100"
              >
                <div className="flex items-start gap-3">
                  <span className={`shrink-0 mt-0.5 text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded ${TYPE_COLORS[r.entity_type]}`}>
                    {TYPE_LABELS[r.entity_type]}
                  </span>
                  <div className="min-w-0">
                    <div className="text-sm font-medium text-earth-800">{r.name}</div>
                    {r.subtitle && (
                      <div className="text-xs text-earth-400 mt-0.5">{r.subtitle}</div>
                    )}
                  </div>
                </div>
              </Link>
            ))}
          </div>
        </section>
      ))}
    </div>
  )
}
