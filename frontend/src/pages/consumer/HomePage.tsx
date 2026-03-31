import { useState, useEffect, useRef, useCallback } from 'react'
import { useNavigate, Link } from 'react-router-dom'
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

export default function HomePage() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchResult[]>([])
  const [loading, setLoading] = useState(false)
  const [showResults, setShowResults] = useState(false)
  const navigate = useNavigate()
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined)
  const resultsRef = useRef<HTMLDivElement>(null)

  const search = useCallback(async (q: string) => {
    if (q.length < 2) {
      setResults([])
      setShowResults(false)
      return
    }
    setLoading(true)
    const { data, error } = await supabase.rpc('search_catalog', {
      query: q,
      result_limit: 8,
      entity_types: ['wine', 'producer', 'region', 'appellation', 'grape'],
    })
    if (!error && data) {
      setResults(data)
      setShowResults(true)
    }
    setLoading(false)
  }, [])

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => search(query), 250)
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [query, search])

  // Close results on click outside
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (resultsRef.current && !resultsRef.current.contains(e.target as Node)) {
        setShowResults(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (query.trim()) {
      setShowResults(false)
      navigate(`/search?q=${encodeURIComponent(query.trim())}`)
    }
  }

  function navigateToResult(result: SearchResult) {
    setShowResults(false)
    setQuery('')
    navigate(`${TYPE_PATHS[result.entity_type]}/${result.id}`)
  }

  return (
    <div className="min-h-screen flex flex-col">
      {/* Hero section */}
      <div className="flex-1 flex flex-col items-center justify-center px-6 pb-24 pt-16">
        {/* Brand */}
        <h1 className="font-display text-6xl md:text-8xl font-bold text-wine-800 tracking-wide mb-3">
          Loam
        </h1>
        <p className="text-lg md:text-xl text-earth-500 font-light mb-12">
          Every wine, connected
        </p>

        {/* Search */}
        <div className="w-full max-w-lg relative" ref={resultsRef}>
          <form onSubmit={handleSubmit}>
            <div className="relative">
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onFocus={() => results.length > 0 && setShowResults(true)}
                placeholder="Search a wine, producer, or region..."
                autoFocus
                className="w-full h-14 pl-12 pr-4 bg-white rounded-2xl text-base text-earth-800 placeholder:text-earth-400 border border-earth-200 shadow-sm focus:border-wine-400 focus:outline-none focus:ring-2 focus:ring-wine-400/20 transition-all"
              />
              <svg className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-earth-400" fill="none" stroke="currentColor" strokeWidth={2} viewBox="0 0 24 24">
                <circle cx="11" cy="11" r="8" />
                <path d="m21 21-4.35-4.35" />
              </svg>
              {loading && (
                <div className="absolute right-4 top-1/2 -translate-y-1/2">
                  <div className="w-4 h-4 border-2 border-wine-300 border-t-transparent rounded-full animate-spin" />
                </div>
              )}
            </div>
          </form>

          {/* Instant results dropdown */}
          {showResults && results.length > 0 && (
            <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl border border-earth-200 shadow-lg overflow-hidden z-50">
              {results.map((r) => (
                <button
                  key={`${r.entity_type}-${r.id}`}
                  onClick={() => navigateToResult(r)}
                  className="w-full text-left px-4 py-3 hover:bg-earth-50 transition-colors border-b border-earth-100 last:border-b-0 flex items-start gap-3"
                >
                  <span className={`shrink-0 mt-0.5 text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded ${
                    r.entity_type === 'wine' ? 'bg-wine-50 text-wine-600' :
                    r.entity_type === 'producer' ? 'bg-amber-50 text-amber-700' :
                    r.entity_type === 'grape' ? 'bg-green-50 text-green-700' :
                    'bg-earth-100 text-earth-600'
                  }`}>
                    {TYPE_LABELS[r.entity_type]}
                  </span>
                  <div className="min-w-0">
                    <div className="text-sm font-medium text-earth-800 truncate">{r.name}</div>
                    {r.subtitle && (
                      <div className="text-xs text-earth-400 truncate">{r.subtitle}</div>
                    )}
                  </div>
                </button>
              ))}
              <Link
                to={`/search?q=${encodeURIComponent(query)}`}
                onClick={() => setShowResults(false)}
                className="block text-center px-4 py-2.5 text-xs font-medium text-wine-600 hover:bg-wine-50 transition-colors"
              >
                View all results
              </Link>
            </div>
          )}

          {showResults && results.length === 0 && query.length >= 2 && !loading && (
            <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl border border-earth-200 shadow-lg p-6 text-center z-50">
              <p className="text-sm text-earth-500">No results for "{query}"</p>
              <p className="text-xs text-earth-400 mt-1">Try a different spelling or search by region</p>
            </div>
          )}
        </div>

        {/* Quick links */}
        <div className="mt-10 flex flex-wrap justify-center gap-2">
          {[
            { label: 'Bordeaux', path: '/search?q=bordeaux' },
            { label: 'Burgundy', path: '/search?q=burgundy' },
            { label: 'Napa Valley', path: '/search?q=napa valley' },
            { label: 'Barolo', path: '/search?q=barolo' },
            { label: 'Pinot Noir', path: '/search?q=pinot noir' },
            { label: 'Riesling', path: '/search?q=riesling' },
          ].map(({ label, path }) => (
            <Link
              key={label}
              to={path}
              className="px-3 py-1.5 text-xs font-medium text-earth-500 bg-earth-100 hover:bg-earth-200 rounded-full transition-colors"
            >
              {label}
            </Link>
          ))}
        </div>
      </div>

      {/* Bottom tagline */}
      <div className="text-center pb-8 px-6">
        <p className="text-xs text-earth-400">
          189,000+ wines. Place, grapes, people.
        </p>
      </div>
    </div>
  )
}
