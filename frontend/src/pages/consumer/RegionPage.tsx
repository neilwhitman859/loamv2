import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import EntityMap from '../../components/EntityMap'

interface Region {
  id: string
  name: string
  is_catch_all: boolean | null
  parent_id: string | null
  country: { id: string; name: string } | null
}

interface RegionInsight {
  ai_overview: string | null
  ai_climate_profile: string | null
  ai_sub_region_comparison: string | null
  ai_signature_style: string | null
  ai_history: string | null
  ai_insider_take: string | null
}

interface SubRegion { id: string; name: string }
interface Appellation { id: string; name: string; designation_type: string | null }
interface RegionGrape { display_name: string; grape_id: string; association_type: string }
interface ProducerSummary { id: string; name: string }

export default function RegionPage() {
  const { id } = useParams<{ id: string }>()
  const [region, setRegion] = useState<Region | null>(null)
  const [parentRegion, setParentRegion] = useState<{ id: string; name: string } | null>(null)
  const [insight, setInsight] = useState<RegionInsight | null>(null)
  const [subRegions, setSubRegions] = useState<SubRegion[]>([])
  const [appellations, setAppellations] = useState<Appellation[]>([])
  const [grapes, setGrapes] = useState<RegionGrape[]>([])
  const [wineCount, setWineCount] = useState(0)
  const [producerCount, setProducerCount] = useState(0)
  const [producers, setProducers] = useState<ProducerSummary[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    supabase.from('regions')
      .select('id, name, is_catch_all, parent_id, country:countries!regions_country_id_fkey(id, name)')
      .eq('id', id).single()
      .then(({ data }) => {
        if (data) {
          const reg = data as unknown as Region
          setRegion(reg)
          const p: PromiseLike<void>[] = []

          if (reg.parent_id) {
            p.push(supabase.from('regions').select('id, name').eq('id', reg.parent_id).single()
              .then(({ data: d }) => { if (d) setParentRegion(d) }))
          }

          p.push(supabase.from('region_insights')
            .select('ai_overview, ai_climate_profile, ai_sub_region_comparison, ai_signature_style, ai_history, ai_insider_take')
            .eq('region_id', id).maybeSingle()
            .then(({ data: d }) => { if (d) setInsight(d) }))

          p.push(supabase.from('regions').select('id, name')
            .eq('parent_id', id).is('deleted_at', null).order('name')
            .then(({ data: d }) => { if (d) setSubRegions(d) }))

          p.push(supabase.from('appellations').select('id, name, designation_type')
            .eq('region_id', id).is('deleted_at', null).order('name').limit(200)
            .then(({ data: d }) => { if (d) setAppellations(d) }))

          p.push(supabase.from('region_grapes')
            .select('association_type, grape_id, grape:grapes!region_grapes_grape_id_fkey(display_name)')
            .eq('region_id', id).limit(30)
            .then(({ data: d }) => {
              if (d) setGrapes(d.map((r: any) => ({
                display_name: r.grape?.display_name || '', grape_id: r.grape_id,
                association_type: r.association_type,
              })))
            }))

          p.push(supabase.from('wines').select('id', { count: 'exact', head: true })
            .eq('region_id', id).is('deleted_at', null)
            .then(({ count }) => { if (count != null) setWineCount(count) }))

          p.push(supabase.from('producers').select('id', { count: 'exact', head: true })
            .eq('region_id', id).is('deleted_at', null)
            .then(({ count }) => { if (count != null) setProducerCount(count) }))

          p.push(supabase.from('producers').select('id, name')
            .eq('region_id', id).is('deleted_at', null).order('name').limit(30)
            .then(({ data: d }) => { if (d) setProducers(d) }))

          Promise.all(p).then(() => setLoading(false))
        } else {
          setLoading(false)
        }
      })
  }, [id])

  if (loading) return <Loading />
  if (!region) return <NotFound label="Region" />

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">
      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-3 flex items-center gap-1.5 flex-wrap">
        {region.country && <><Link to={`/country/${region.country.id}`} className="hover:text-earth-600">{region.country.name}</Link><span>/</span></>}
        {parentRegion && <><Link to={`/region/${parentRegion.id}`} className="hover:text-earth-600">{parentRegion.name}</Link><span>/</span></>}
        <span className="text-earth-500">{region.name}</span>
      </nav>

      {/* Header */}
      <header className="mb-4">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{region.name}</h1>
        <div className="flex flex-wrap gap-1.5 mt-2">
          {region.country && <Tag>{region.country.name}</Tag>}
          {wineCount > 0 && <Tag>{wineCount.toLocaleString()} wines</Tag>}
          {producerCount > 0 && <Tag>{producerCount.toLocaleString()} producers</Tag>}
          {appellations.length > 0 && <Tag>{appellations.length} appellations</Tag>}
          {subRegions.length > 0 && <Tag>{subRegions.length} sub-regions</Tag>}
        </div>
      </header>

      {/* AI overview */}
      {insight?.ai_overview && (
        <Section title="Overview">
          <p className="text-sm text-earth-600">{insight.ai_overview} <AiLabel /></p>
        </Section>
      )}

      {/* Map */}
      <Section title="Map">
        <EntityMap entityType="region" entityId={region.id} label={region.name} />
      </Section>

      {/* AI terroir summary */}
      {(insight?.ai_climate_profile || insight?.ai_signature_style) && (
        <Section title="Terroir">
          {insight?.ai_climate_profile && (
            <div className="mb-2"><MiniLabel>Climate</MiniLabel><p className="text-sm text-earth-700">{insight.ai_climate_profile} <AiLabel /></p></div>
          )}
          {insight?.ai_signature_style && (
            <div className="mb-2"><MiniLabel>Signature style</MiniLabel><p className="text-sm text-earth-600">{insight.ai_signature_style} <AiLabel /></p></div>
          )}
        </Section>
      )}

      {/* Grapes */}
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

      {/* Sub-regions */}
      {subRegions.length > 0 && (
        <Section title="Sub-regions">
          {insight?.ai_sub_region_comparison && (
            <p className="text-sm text-earth-600 mb-3">{insight.ai_sub_region_comparison} <AiLabel /></p>
          )}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-1.5">
            {subRegions.map(sr => (
              <Link key={sr.id} to={`/region/${sr.id}`}
                className="px-3 py-2 bg-earth-50 border border-earth-100 rounded-lg text-sm text-earth-700 hover:bg-earth-100 transition-colors">
                {sr.name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Appellations */}
      {appellations.length > 0 && (
        <Section title={`Appellations (${appellations.length})`}>
          <div className="space-y-0.5">
            {appellations.map(a => (
              <Link key={a.id} to={`/appellation/${a.id}`}
                className="flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-earth-100 transition-colors">
                <span className="text-sm text-earth-700 font-medium">{a.name}</span>
                {a.designation_type && <span className="text-[10px] text-earth-400 uppercase tracking-wider">{a.designation_type}</span>}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Producers */}
      {producers.length > 0 && (
        <Section title={`Producers${producerCount > 0 ? ` (${producerCount})` : ''}`}>
          <div className="flex flex-wrap gap-2">
            {producers.map(p => (
              <Link key={p.id} to={`/producer/${p.id}`}
                className="text-sm px-3 py-1.5 bg-earth-100 rounded-lg text-earth-700 hover:bg-earth-200 transition-colors">
                {p.name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* History */}
      {insight?.ai_history && (
        <Section title="History">
          <p className="text-sm text-earth-600">{insight.ai_history} <AiLabel /></p>
        </Section>
      )}

      {/* Insider take */}
      {insight?.ai_insider_take && (
        <Section title="Insider take">
          <p className="text-sm text-earth-600">{insight.ai_insider_take} <AiLabel /></p>
        </Section>
      )}

      {!insight && subRegions.length === 0 && appellations.length === 0 && (
        <div className="text-center py-12">
          <p className="text-sm text-earth-400">Region data is being built out. Check back soon.</p>
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
  return <span className="inline-block text-xs font-medium px-2 py-0.5 rounded-full bg-earth-100 text-earth-600">{children}</span>
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
        <div className="h-8 bg-earth-200 rounded w-1/2" />
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
