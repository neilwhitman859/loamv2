import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'

interface Producer {
  id: string
  name: string
  producer_type: string | null
  year_established: number | null
  website_url: string | null
  philosophy: string | null
  description: string | null
  hectares_under_vine: number | null
  total_production_cases: number | null
  address: string | null
  country: { id: string; name: string } | null
  region: { id: string; name: string } | null
  appellation: { id: string; name: string } | null
}

interface WineSummary {
  id: string
  name: string
  color: string | null
  wine_type: string | null
}

const COLOR_DOTS: Record<string, string> = {
  red: 'bg-red-700',
  white: 'bg-amber-100 border border-amber-300',
  rose: 'bg-pink-300',
  orange: 'bg-orange-300',
}

export default function ProducerPage() {
  const { id } = useParams<{ id: string }>()
  const [producer, setProducer] = useState<Producer | null>(null)
  const [wines, setWines] = useState<WineSummary[]>([])
  const [wineCount, setWineCount] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    Promise.all([
      supabase
        .from('producers')
        .select(`
          id, name, producer_type, year_established, website_url, philosophy, description,
          hectares_under_vine, total_production_cases, address,
          country:countries!producers_country_id_fkey(id, name),
          region:regions!producers_region_id_fkey(id, name),
          appellation:appellations!producers_appellation_id_fkey(id, name)
        `)
        .eq('id', id)
        .single(),

      supabase
        .from('wines')
        .select('id, name, color, wine_type')
        .eq('producer_id', id)
        .is('deleted_at', null)
        .order('name')
        .limit(50),

      supabase
        .from('wines')
        .select('id', { count: 'exact', head: true })
        .eq('producer_id', id)
        .is('deleted_at', null),
    ]).then(([prodRes, wineRes, countRes]) => {
      if (prodRes.data) setProducer(prodRes.data as unknown as Producer)
      if (wineRes.data) setWines(wineRes.data)
      if (countRes.count != null) setWineCount(countRes.count)
      setLoading(false)
    })
  }, [id])

  if (loading) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-12">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-earth-200 rounded w-1/2" />
          <div className="h-5 bg-earth-100 rounded w-1/4" />
        </div>
      </div>
    )
  }

  if (!producer) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-16 text-center">
        <p className="text-earth-500">Producer not found</p>
        <Link to="/" className="text-wine-600 text-sm mt-2 inline-block hover:underline">Back to search</Link>
      </div>
    )
  }

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">

      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-4 flex items-center gap-1.5 flex-wrap">
        {producer.country && (
          <>
            <Link to={`/country/${producer.country.id}`} className="hover:text-earth-600 transition-colors">{producer.country.name}</Link>
            <span>/</span>
          </>
        )}
        {producer.region && (
          <>
            <Link to={`/region/${producer.region.id}`} className="hover:text-earth-600 transition-colors">{producer.region.name}</Link>
            <span>/</span>
          </>
        )}
        <span className="text-earth-500">{producer.name}</span>
      </nav>

      {/* Header */}
      <header className="mb-8">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900 leading-tight">
          {producer.name}
        </h1>

        <div className="flex flex-wrap gap-2 mt-3">
          {producer.producer_type && (
            <span className="text-xs font-medium px-2 py-0.5 rounded-full bg-earth-100 text-earth-600 capitalize">
              {producer.producer_type}
            </span>
          )}
          {producer.year_established && (
            <span className="text-xs font-medium px-2 py-0.5 rounded-full bg-earth-100 text-earth-600">
              Est. {producer.year_established}
            </span>
          )}
        </div>

        {producer.website_url && (
          <a
            href={producer.website_url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block mt-3 text-xs text-wine-500 hover:text-wine-600 transition-colors"
          >
            {producer.website_url.replace(/^https?:\/\/(www\.)?/, '').replace(/\/$/, '')}
            <span className="ml-1">&#8599;</span>
          </a>
        )}
      </header>

      {/* Description / Philosophy */}
      {(producer.description || producer.philosophy) && (
        <section className="mb-8">
          {producer.description && (
            <p className="text-sm text-earth-700 leading-relaxed mb-3">{producer.description}</p>
          )}
          {producer.philosophy && (
            <p className="text-sm text-earth-600 leading-relaxed italic border-l-2 border-earth-300 pl-4">{producer.philosophy}</p>
          )}
        </section>
      )}

      {/* Key facts */}
      {(producer.hectares_under_vine || producer.total_production_cases || producer.address) && (
        <section className="mb-8">
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            {producer.hectares_under_vine && (
              <Stat label="Vineyard area" value={`${producer.hectares_under_vine} ha`} />
            )}
            {producer.total_production_cases && (
              <Stat label="Production" value={`${producer.total_production_cases.toLocaleString()} cases`} />
            )}
            {producer.address && (
              <Stat label="Location" value={producer.address} />
            )}
          </div>
        </section>
      )}

      {/* Wines */}
      <section>
        <h3 className="font-display text-lg font-semibold text-earth-800 mb-3">
          Wines {wineCount > 0 && <span className="text-earth-400 font-normal text-sm">({wineCount})</span>}
        </h3>

        {wines.length === 0 ? (
          <p className="text-sm text-earth-400">No wines in catalog yet.</p>
        ) : (
          <div className="space-y-1">
            {wines.map(w => (
              <Link
                key={w.id}
                to={`/wine/${w.id}`}
                className="flex items-center gap-3 px-3 py-2.5 rounded-lg hover:bg-earth-100 transition-colors"
              >
                {w.color && (
                  <div className={`w-3 h-3 rounded-full shrink-0 ${COLOR_DOTS[w.color] || 'bg-earth-300'}`} />
                )}
                <span className="text-sm text-earth-700 font-medium truncate">{w.name}</span>
                {w.wine_type && w.wine_type !== 'table' && (
                  <span className="text-[10px] text-earth-400 uppercase tracking-wider shrink-0">{w.wine_type}</span>
                )}
              </Link>
            ))}
            {wineCount > 50 && (
              <p className="text-xs text-earth-400 px-3 pt-2">Showing 50 of {wineCount} wines</p>
            )}
          </div>
        )}
      </section>
    </div>
  )
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs text-earth-400 mb-0.5">{label}</div>
      <div className="text-sm text-earth-700 font-medium">{value}</div>
    </div>
  )
}
