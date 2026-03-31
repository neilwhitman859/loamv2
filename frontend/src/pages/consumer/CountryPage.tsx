import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'

interface Country {
  id: string
  name: string
  iso_code: string | null
}

interface Region {
  id: string
  name: string
  is_catch_all: boolean | null
}

export default function CountryPage() {
  const { id } = useParams<{ id: string }>()
  const [country, setCountry] = useState<Country | null>(null)
  const [regions, setRegions] = useState<Region[]>([])
  const [wineCount, setWineCount] = useState(0)
  const [producerCount, setProducerCount] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    Promise.all([
      supabase.from('countries').select('id, name, iso_code').eq('id', id).single(),
      supabase.from('regions').select('id, name')
        .eq('country_id', id).is('deleted_at', null)
        .is('parent_id', null)
        .order('name'),
      supabase.from('wines').select('id', { count: 'exact', head: true })
        .eq('country_id', id).is('deleted_at', null),
      supabase.from('producers').select('id', { count: 'exact', head: true })
        .eq('country_id', id).is('deleted_at', null),
    ]).then(([cRes, rRes, wRes, pRes]) => {
      if (cRes.data) setCountry(cRes.data)
      if (rRes.data) setRegions(rRes.data)
      if (wRes.count != null) setWineCount(wRes.count)
      if (pRes.count != null) setProducerCount(pRes.count)
      setLoading(false)
    })
  }, [id])

  if (loading) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-12">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-earth-200 rounded w-1/3" />
          <div className="h-4 bg-earth-100 rounded w-1/2" />
        </div>
      </div>
    )
  }

  if (!country) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-16 text-center">
        <p className="text-earth-500">Country not found</p>
        <Link to="/" className="text-wine-600 text-sm mt-2 inline-block hover:underline">Back to search</Link>
      </div>
    )
  }

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">
      <header className="mb-8">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{country.name}</h1>
        <p className="text-sm text-earth-400 mt-1">
          {[
            wineCount > 0 && `${wineCount.toLocaleString()} wines`,
            producerCount > 0 && `${producerCount.toLocaleString()} producers`,
          ].filter(Boolean).join(' \u00b7 ')}
        </p>
      </header>

      {regions.length > 0 && (
        <section>
          <h3 className="font-display text-lg font-semibold text-earth-800 mb-3">
            Regions ({regions.length})
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {regions.map(r => (
              <Link
                key={r.id}
                to={`/region/${r.id}`}
                className="px-4 py-3 bg-white rounded-lg border border-earth-100 hover:border-wine-200 hover:bg-earth-50 transition-colors"
              >
                <span className="text-sm font-medium text-earth-700">{r.name}</span>
              </Link>
            ))}
          </div>
        </section>
      )}
    </div>
  )
}
