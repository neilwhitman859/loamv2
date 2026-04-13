import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'

interface Grape {
  id: string
  display_name: string
  name: string
  color: string | null
  primary_name: string | null
  country_of_origin: string | null
  parent_grape_id: string | null
}

interface GrapeInsight {
  ai_overview: string | null
  ai_flavor_profile: string | null
  ai_growing_conditions: string | null
  ai_food_pairing: string | null
  ai_regions_of_note: string | null
  ai_aging_characteristics: string | null
}

interface Synonym { synonym: string; language: string | null; country: { name: string } | null }
interface AppellationLink { appellation: { id: string; name: string }; association_type: string }
interface RegionLink { region: { id: string; name: string }; association_type: string }
interface CountryLink { country: { id: string; name: string }; association_type: string }
interface ChildGrape { id: string; display_name: string }

export default function GrapePage() {
  const { id } = useParams<{ id: string }>()
  const [grape, setGrape] = useState<Grape | null>(null)
  const [insight, setInsight] = useState<GrapeInsight | null>(null)
  const [synonyms, setSynonyms] = useState<Synonym[]>([])
  const [appellations, setAppellations] = useState<AppellationLink[]>([])
  const [regions, setRegions] = useState<RegionLink[]>([])
  const [countries, setCountries] = useState<CountryLink[]>([])
  const [children, setChildren] = useState<ChildGrape[]>([])
  const [parentGrape, setParentGrape] = useState<{ id: string; display_name: string } | null>(null)
  const [wineCount, setWineCount] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    supabase.from('grapes')
      .select('id, display_name, name, color, primary_name, country_of_origin, parent_grape_id')
      .eq('id', id).single()
      .then(({ data }) => {
        if (data) {
          const g = data as Grape
          setGrape(g)
          const p: PromiseLike<void>[] = []

          p.push(supabase.from('grape_insights')
            .select('ai_overview, ai_flavor_profile, ai_growing_conditions, ai_food_pairing, ai_regions_of_note, ai_aging_characteristics')
            .eq('grape_id', id).maybeSingle()
            .then(({ data: d }) => { if (d) setInsight(d) }))

          p.push(supabase.from('grape_synonyms')
            .select('synonym, language, country:countries!grape_synonyms_country_id_fkey(name)')
            .eq('grape_id', id).limit(30)
            .then(({ data: d }) => { if (d) setSynonyms(d as unknown as Synonym[]) }))

          p.push(supabase.from('appellation_grapes')
            .select('association_type, appellation:appellations!appellation_grapes_appellation_id_fkey(id, name)')
            .eq('grape_id', id).limit(30)
            .then(({ data: d }) => { if (d) setAppellations(d as unknown as AppellationLink[]) }))

          p.push(supabase.from('region_grapes')
            .select('association_type, region:regions!region_grapes_region_id_fkey(id, name)')
            .eq('grape_id', id).limit(20)
            .then(({ data: d }) => { if (d) setRegions(d as unknown as RegionLink[]) }))

          p.push(supabase.from('country_grapes')
            .select('association_type, country:countries!country_grapes_country_id_fkey(id, name)')
            .eq('grape_id', id).limit(20)
            .then(({ data: d }) => { if (d) setCountries(d as unknown as CountryLink[]) }))

          p.push(supabase.from('grapes').select('id, display_name')
            .eq('parent_grape_id', id).is('deleted_at', null).order('display_name').limit(20)
            .then(({ data: d }) => { if (d) setChildren(d) }))

          if (g.parent_grape_id) {
            p.push(supabase.from('grapes').select('id, display_name')
              .eq('id', g.parent_grape_id).single()
              .then(({ data: d }) => { if (d) setParentGrape(d) }))
          }

          p.push(supabase.from('wine_grapes')
            .select('wine_id', { count: 'exact', head: true })
            .eq('grape_id', id)
            .then(({ count }) => { if (count != null) setWineCount(count) }))

          Promise.all(p).then(() => setLoading(false))
        } else {
          setLoading(false)
        }
      })
  }, [id])

  if (loading) return <Loading />
  if (!grape) return <NotFound label="Grape" />

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">
      {/* Header */}
      <header className="mb-4">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{grape.display_name}</h1>
        <div className="flex flex-wrap gap-1.5 mt-2">
          {grape.color && <Tag>{grape.color} grape</Tag>}
          {wineCount > 0 && <Tag>{wineCount.toLocaleString()} wines</Tag>}
          {parentGrape && (
            <Link to={`/grape/${parentGrape.id}`} className="inline-block text-xs font-medium px-2 py-0.5 rounded-full bg-earth-100 text-wine-600 hover:bg-earth-200 transition-colors">
              Parent: {parentGrape.display_name}
            </Link>
          )}
        </div>
      </header>

      {/* AI overview */}
      {insight?.ai_overview && (
        <Section title="Overview">
          <p className="text-sm text-earth-600">{insight.ai_overview} <AiLabel /></p>
        </Section>
      )}

      {/* Identity */}
      <Section title="Identity">
        <FactGrid>
          {grape.name !== grape.display_name && <Fact label="VIVC name" value={grape.name} />}
          {grape.primary_name && grape.primary_name !== grape.display_name && <Fact label="Primary name" value={grape.primary_name} />}
          {grape.color && <Fact label="Berry color" value={grape.color} />}
          {grape.country_of_origin && <Fact label="Origin" value={grape.country_of_origin} />}
        </FactGrid>
      </Section>

      {/* Synonyms */}
      {synonyms.length > 0 && (
        <Section title={`Synonyms (${synonyms.length})`}>
          <div className="flex flex-wrap gap-1.5">
            {synonyms.map((s, i) => (
              <span key={i} className="text-xs px-2 py-0.5 bg-earth-50 border border-earth-150 rounded text-earth-600">
                {s.synonym}
                {s.country && <span className="text-earth-400 ml-1">({s.country.name})</span>}
              </span>
            ))}
          </div>
        </Section>
      )}

      {/* AI flavor & growing */}
      {(insight?.ai_flavor_profile || insight?.ai_growing_conditions) && (
        <Section title="Profile">
          {insight?.ai_flavor_profile && (
            <div className="mb-2"><MiniLabel>Flavor</MiniLabel><p className="text-sm text-earth-700">{insight.ai_flavor_profile} <AiLabel /></p></div>
          )}
          {insight?.ai_growing_conditions && (
            <div className="mb-2"><MiniLabel>Growing conditions</MiniLabel><p className="text-sm text-earth-700">{insight.ai_growing_conditions} <AiLabel /></p></div>
          )}
        </Section>
      )}

      {/* Where it grows: countries */}
      {countries.length > 0 && (
        <Section title="Countries">
          <div className="flex flex-wrap gap-2">
            {countries.map((c, i) => (
              <Link key={i} to={`/country/${c.country.id}`}
                className="text-sm px-3 py-1.5 bg-earth-100 rounded-lg text-earth-700 hover:bg-earth-200 transition-colors">
                {c.country.name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Regions */}
      {regions.length > 0 && (
        <Section title="Regions">
          <div className="flex flex-wrap gap-2">
            {regions.map((r, i) => (
              <Link key={i} to={`/region/${r.region.id}`}
                className="text-sm px-3 py-1.5 bg-earth-100 rounded-lg text-earth-700 hover:bg-earth-200 transition-colors">
                {r.region.name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Appellations */}
      {appellations.length > 0 && (
        <Section title={`Appellations (${appellations.length})`}>
          <div className="flex flex-wrap gap-1.5">
            {appellations.map((a, i) => (
              <Link key={i} to={`/appellation/${a.appellation.id}`}
                className="text-xs px-2.5 py-1 bg-earth-100 text-earth-600 rounded-full hover:bg-earth-200 transition-colors">
                {a.appellation.name}
                {a.association_type === 'required' && <span className="text-wine-500 ml-1">req</span>}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Child grapes (crosses, mutations) */}
      {children.length > 0 && (
        <Section title="Related varieties">
          <div className="flex flex-wrap gap-2">
            {children.map(c => (
              <Link key={c.id} to={`/grape/${c.id}`}
                className="text-sm px-3 py-1.5 bg-earth-100 rounded-lg text-earth-700 hover:bg-earth-200 transition-colors">
                {c.display_name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Regions of note */}
      {insight?.ai_regions_of_note && (
        <Section title="Regions of note">
          <p className="text-sm text-earth-600">{insight.ai_regions_of_note} <AiLabel /></p>
        </Section>
      )}

      {/* Food & Aging */}
      {insight?.ai_food_pairing && (
        <Section title="Food pairing">
          <p className="text-sm text-earth-600">{insight.ai_food_pairing} <AiLabel /></p>
        </Section>
      )}
      {insight?.ai_aging_characteristics && (
        <Section title="Aging">
          <p className="text-sm text-earth-600">{insight.ai_aging_characteristics} <AiLabel /></p>
        </Section>
      )}

      {!insight && synonyms.length === 0 && appellations.length === 0 && (
        <div className="text-center py-12">
          <p className="text-sm text-earth-400">Grape data is being built out. Check back soon.</p>
        </div>
      )}
    </div>
  )
}

/* ── Shared ───────────────────────────────────────────────── */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mb-5">
      <h2 className="font-display text-base font-semibold text-earth-800 mb-2 pb-1 border-b border-earth-100">{title}</h2>
      {children}
    </section>
  )
}

function Tag({ children }: { children: React.ReactNode }) {
  return <span className="inline-block text-xs font-medium px-2 py-0.5 rounded-full bg-earth-100 text-earth-600 capitalize">{children}</span>
}

function FactGrid({ children }: { children: React.ReactNode }) {
  const filtered = Array.isArray(children) ? children.filter(Boolean) : children ? [children] : []
  if (filtered.length === 0) return null
  return <div className="grid grid-cols-2 sm:grid-cols-3 gap-x-4 gap-y-2 mb-2">{filtered}</div>
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] uppercase tracking-wider text-earth-400 leading-tight">{label}</div>
      <div className="text-sm font-medium text-earth-800">{value}</div>
    </div>
  )
}

function MiniLabel({ children }: { children: React.ReactNode }) {
  return <div className="text-[10px] uppercase tracking-wider text-earth-400 mb-1">{children}</div>
}

function AiLabel() {
  return <span className="inline-block text-[9px] uppercase tracking-wider text-earth-400 bg-earth-100 px-1.5 py-0.5 rounded ml-1 align-middle" title="Generated by AI and may contain errors">AI</span>
}

function Loading() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-12">
      <div className="animate-pulse space-y-4">
        <div className="h-8 bg-earth-200 rounded w-1/3" />
        <div className="h-4 bg-earth-100 rounded w-full" />
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
