import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import EntityMap from '../../components/EntityMap'

interface Country {
  id: string
  name: string
  iso_code: string | null
}

interface CountryInsight {
  ai_overview: string | null
  ai_wine_history: string | null
  ai_key_regions: string | null
  ai_signature_grapes: string | null
}

interface Region { id: string; name: string }
interface CountryGrape { display_name: string; grape_id: string }

export default function CountryPage() {
  const { id } = useParams<{ id: string }>()
  const [country, setCountry] = useState<Country | null>(null)
  const [insight, setInsight] = useState<CountryInsight | null>(null)
  const [regions, setRegions] = useState<Region[]>([])
  const [grapes, setGrapes] = useState<CountryGrape[]>([])
  const [wineCount, setWineCount] = useState(0)
  const [producerCount, setProducerCount] = useState(0)
  const [appellationCount, setAppellationCount] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    Promise.all([
      supabase.from('countries').select('id, name, iso_code').eq('id', id).single(),
      supabase.from('country_insights')
        .select('ai_overview, ai_wine_history, ai_key_regions, ai_signature_grapes')
        .eq('country_id', id).maybeSingle(),
      supabase.from('regions').select('id, name')
        .eq('country_id', id).is('deleted_at', null).is('parent_id', null).order('name'),
      supabase.from('country_grapes')
        .select('grape_id, grape:grapes!country_grapes_grape_id_fkey(display_name)')
        .eq('country_id', id).limit(30),
      supabase.from('wines').select('id', { count: 'exact', head: true })
        .eq('country_id', id).is('deleted_at', null),
      supabase.from('producers').select('id', { count: 'exact', head: true })
        .eq('country_id', id).is('deleted_at', null),
      supabase.from('appellations').select('id', { count: 'exact', head: true })
        .eq('country_id', id).is('deleted_at', null),
    ]).then(([cRes, iRes, rRes, gRes, wRes, pRes, aRes]) => {
      if (cRes.data) setCountry(cRes.data)
      if (iRes.data) setInsight(iRes.data)
      if (rRes.data) setRegions(rRes.data)
      if (gRes.data) setGrapes(gRes.data.map((r: any) => ({
        display_name: r.grape?.display_name || '', grape_id: r.grape_id,
      })))
      if (wRes.count != null) setWineCount(wRes.count)
      if (pRes.count != null) setProducerCount(pRes.count)
      if (aRes.count != null) setAppellationCount(aRes.count)
      setLoading(false)
    })
  }, [id])

  if (loading) return <Loading />
  if (!country) return <NotFound label="Country" />

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">
      {/* Header */}
      <header className="mb-4">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{country.name}</h1>
        <div className="flex flex-wrap gap-1.5 mt-2">
          {country.iso_code && <Tag>{country.iso_code}</Tag>}
          {wineCount > 0 && <Tag>{wineCount.toLocaleString()} wines</Tag>}
          {producerCount > 0 && <Tag>{producerCount.toLocaleString()} producers</Tag>}
          {appellationCount > 0 && <Tag>{appellationCount.toLocaleString()} appellations</Tag>}
          {regions.length > 0 && <Tag>{regions.length} regions</Tag>}
        </div>
      </header>

      {/* Map */}
      <Section title="Map">
        <EntityMap entityType="country" entityId={country.id} label={country.name} />
      </Section>

      {/* Key grapes */}
      {grapes.length > 0 && (
        <Section title="Key grape varieties">
          <div className="flex flex-wrap gap-1.5">
            {grapes.map((g, i) => (
              <Link key={i} to={`/grape/${g.grape_id}`}
                className="text-xs px-2.5 py-1 bg-earth-100 text-earth-600 rounded-full hover:bg-earth-200 transition-colors">
                {g.display_name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Regions */}
      {regions.length > 0 && (
        <Section title={`Regions (${regions.length})`}>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-1.5">
            {regions.map(r => (
              <Link key={r.id} to={`/region/${r.id}`}
                className="px-3 py-2 bg-earth-50 border border-earth-100 rounded-lg text-sm text-earth-700 hover:bg-earth-100 transition-colors font-medium">
                {r.name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* AI overview */}
      {insight?.ai_overview && (
        <Section title="Overview">
          <p className="text-sm text-earth-600">{insight.ai_overview}</p>
        </Section>
      )}
    </div>
  )
}

/* ── Shared ───────────────────────────────────────────────── */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mb-5">
      <h3 className="font-display text-base font-semibold text-earth-800 mb-2 pb-1 border-b border-earth-100">{title}</h3>
      {children}
    </section>
  )
}

function Tag({ children }: { children: React.ReactNode }) {
  return <span className="inline-block text-xs font-medium px-2 py-0.5 rounded-full bg-earth-100 text-earth-600">{children}</span>
}

function Loading() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-12">
      <div className="animate-pulse space-y-4">
        <div className="h-8 bg-earth-200 rounded w-1/3" />
        <div className="h-4 bg-earth-100 rounded w-1/2" />
      </div>
    </div>
  )
}

function NotFound({ label }: { label: string }) {
  return (
    <div className="max-w-3xl mx-auto px-4 py-16 text-center">
      <p className="text-earth-500">{label} not found</p>
      <Link to="/" className="text-wine-600 text-sm mt-2 inline-block hover:underline">Back to search</Link>
    </div>
  )
}
