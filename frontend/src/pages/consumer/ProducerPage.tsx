import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import EntityMap from '../../components/EntityMap'

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
  latitude: number | null
  longitude: number | null
  parent_producer_id: string | null
  parent_company: string | null
  country: { id: string; name: string } | null
  region: { id: string; name: string } | null
  appellation: { id: string; name: string } | null
}

interface WineSummary {
  id: string
  name: string
  color: string | null
  wine_type: string | null
  appellation: { name: string } | null
}

interface FarmingCert { name: string; certification_status: string | null; certified_since: number | null }
interface BioCert { name: string }
interface ProducerAlias { alias: string; alias_type: string | null }
interface ChildProducer { id: string; name: string }
interface ProducerInsight {
  ai_overview: string | null
  ai_winemaking_style: string | null
  ai_reputation: string | null
  ai_value_assessment: string | null
  ai_portfolio_summary: string | null
  ai_insider_take: string | null
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
  const [farmingCerts, setFarmingCerts] = useState<FarmingCert[]>([])
  const [bioCerts, setBioCerts] = useState<BioCert[]>([])
  const [aliases, setAliases] = useState<ProducerAlias[]>([])
  const [children, setChildren] = useState<ChildProducer[]>([])
  const [parentProducer, setParentProducer] = useState<{ id: string; name: string } | null>(null)
  const [insight, setInsight] = useState<ProducerInsight | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    supabase
      .from('producers')
      .select(`
        id, name, producer_type, year_established, website_url, philosophy, description,
        hectares_under_vine, total_production_cases, address, latitude, longitude,
        parent_producer_id, parent_company,
        country:countries!producers_country_id_fkey(id, name),
        region:regions!producers_region_id_fkey(id, name),
        appellation:appellations!producers_appellation_id_fkey(id, name)
      `)
      .eq('id', id)
      .single()
      .then(({ data }) => {
        if (data) {
          const prod = data as unknown as Producer
          setProducer(prod)
          const p: PromiseLike<void>[] = []

          p.push(supabase.from('wines')
            .select('id, name, color, wine_type, appellation:appellations!wines_appellation_id_fkey(name)')
            .eq('producer_id', id).is('deleted_at', null).order('name').limit(100)
            .then(({ data: d }) => { if (d) setWines(d as unknown as WineSummary[]) }))

          p.push(supabase.from('wines')
            .select('id', { count: 'exact', head: true })
            .eq('producer_id', id).is('deleted_at', null)
            .then(({ count }) => { if (count != null) setWineCount(count) }))

          p.push(supabase.from('producer_farming_certifications')
            .select('certification_status, certified_since, farming_certification:farming_certifications!producer_farming_certifications_farming_certification_id_fkey(name)')
            .eq('producer_id', id)
            .then(({ data: d }) => {
              if (d) setFarmingCerts(d.map((r: any) => ({
                name: r.farming_certification?.name || '',
                certification_status: r.certification_status,
                certified_since: r.certified_since,
              })))
            }))

          p.push(supabase.from('producer_biodiversity_certifications')
            .select('biodiversity_certification:biodiversity_certifications!producer_biodiversity_certifications_biodiversity_certification_id_fkey(name)')
            .eq('producer_id', id)
            .then(({ data: d }) => {
              if (d) setBioCerts(d.map((r: any) => ({ name: r.biodiversity_certification?.name || '' })))
            }))

          p.push(supabase.from('producer_aliases')
            .select('alias, alias_type')
            .eq('producer_id', id)
            .then(({ data: d }) => { if (d) setAliases(d) }))

          p.push(supabase.from('producer_insights')
            .select('ai_overview, ai_winemaking_style, ai_reputation, ai_value_assessment, ai_portfolio_summary, ai_insider_take')
            .eq('producer_id', id).maybeSingle()
            .then(({ data: d }) => { if (d) setInsight(d) }))

          // Child producers
          p.push(supabase.from('producers')
            .select('id, name')
            .eq('parent_producer_id', id).is('deleted_at', null).order('name')
            .then(({ data: d }) => { if (d) setChildren(d) }))

          // Parent producer
          if (prod.parent_producer_id) {
            p.push(supabase.from('producers')
              .select('id, name')
              .eq('id', prod.parent_producer_id).single()
              .then(({ data: d }) => { if (d) setParentProducer(d) }))
          }

          Promise.all(p).then(() => setLoading(false))
        } else {
          setLoading(false)
        }
      })
  }, [id])

  if (loading) return <Loading />
  if (!producer) return <NotFound label="Producer" />

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">
      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-3 flex items-center gap-1.5 flex-wrap">
        {producer.country && <><Link to={`/country/${producer.country.id}`} className="hover:text-earth-600">{producer.country.name}</Link><span>/</span></>}
        {producer.region && <><Link to={`/region/${producer.region.id}`} className="hover:text-earth-600">{producer.region.name}</Link><span>/</span></>}
        {parentProducer && <><Link to={`/producer/${parentProducer.id}`} className="hover:text-earth-600">{parentProducer.name}</Link><span>/</span></>}
        <span className="text-earth-500">{producer.name}</span>
      </nav>

      {/* Header */}
      <header className="mb-4">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{producer.name}</h1>
        <div className="flex flex-wrap gap-1.5 mt-2">
          {producer.producer_type && <Tag>{producer.producer_type}</Tag>}
          {producer.year_established && <Tag>Est. {producer.year_established}</Tag>}
          {producer.parent_company && <Tag variant="muted">{producer.parent_company}</Tag>}
        </div>
        {aliases.length > 0 && (
          <p className="text-xs text-earth-400 mt-1">Also known as: {aliases.map(a => a.alias).join(', ')}</p>
        )}
      </header>

      {/* AI Overview */}
      {insight?.ai_overview && (
        <Section title="Overview">
          <p className="text-sm text-earth-600 whitespace-pre-line">{insight.ai_overview} <AiLabel /></p>
        </Section>
      )}

      {/* Facts */}
      <Section title="Details">
        <FactGrid>
          {producer.country && <Fact label="Country" value={producer.country.name} link={`/country/${producer.country.id}`} />}
          {producer.region && <Fact label="Region" value={producer.region.name} link={`/region/${producer.region.id}`} />}
          {producer.appellation && <Fact label="Appellation" value={producer.appellation.name} link={`/appellation/${producer.appellation.id}`} />}
          {producer.hectares_under_vine && <Fact label="Hectares" value={`${producer.hectares_under_vine}`} />}
          {producer.total_production_cases && <Fact label="Production" value={`${producer.total_production_cases.toLocaleString()} cases`} />}
          {producer.address && <Fact label="Address" value={producer.address} />}
          {producer.latitude && producer.longitude && (
            <Fact label="Coordinates" value={`${producer.latitude.toFixed(4)}, ${producer.longitude.toFixed(4)}`} />
          )}
          {producer.website_url && (
            <div className="min-w-0">
              <div className="text-[10px] uppercase tracking-wider text-earth-400 leading-tight">Website</div>
              <a href={producer.website_url.startsWith('http') ? producer.website_url : `https://${producer.website_url}`}
                target="_blank" rel="noopener noreferrer"
                className="text-sm font-medium text-wine-600 hover:text-wine-700 transition-colors truncate block">
                {producer.website_url.replace(/^https?:\/\/(www\.)?/, '').replace(/\/$/, '')}
              </a>
            </div>
          )}
        </FactGrid>
      </Section>

      {/* Certifications */}
      {(farmingCerts.length > 0 || bioCerts.length > 0) && (
        <Section title="Certifications">
          <div className="flex flex-wrap gap-1.5">
            {farmingCerts.map((c, i) => (
              <span key={i} className="text-xs px-2 py-0.5 bg-green-50 border border-green-200 rounded text-green-700">
                {c.name}
                {c.certification_status && c.certification_status !== 'certified' && ` (${c.certification_status})`}
                {c.certified_since && ` since ${c.certified_since}`}
              </span>
            ))}
            {bioCerts.map((c, i) => (
              <span key={`bio-${i}`} className="text-xs px-2 py-0.5 bg-emerald-50 border border-emerald-200 rounded text-emerald-700">
                {c.name}
              </span>
            ))}
          </div>
        </Section>
      )}

      {/* Philosophy */}
      {producer.philosophy && (
        <Section title="Philosophy">
          <p className="text-sm text-earth-600">{producer.philosophy}</p>
        </Section>
      )}

      {/* Winemaking */}
      {insight?.ai_winemaking_style && (
        <Section title="Winemaking">
          <p className="text-sm text-earth-600 whitespace-pre-line">{insight.ai_winemaking_style} <AiLabel /></p>
        </Section>
      )}

      {/* Reputation */}
      {insight?.ai_reputation && (
        <Section title="Reputation">
          <p className="text-sm text-earth-600 whitespace-pre-line">{insight.ai_reputation} <AiLabel /></p>
        </Section>
      )}

      {/* Portfolio */}
      {insight?.ai_portfolio_summary && (
        <Section title="Portfolio">
          <p className="text-sm text-earth-600 whitespace-pre-line">{insight.ai_portfolio_summary} <AiLabel /></p>
        </Section>
      )}

      {/* Value */}
      {insight?.ai_value_assessment && (
        <Section title="Value">
          <p className="text-sm text-earth-600 whitespace-pre-line">{insight.ai_value_assessment} <AiLabel /></p>
        </Section>
      )}

      {/* Insider Take */}
      {insight?.ai_insider_take && (
        <Section title="Insider take">
          <p className="text-sm text-earth-600 whitespace-pre-line">{insight.ai_insider_take} <AiLabel /></p>
        </Section>
      )}

      {/* Map */}
      {producer.region && (
        <Section title="Location">
          <EntityMap entityType="region" entityId={producer.region.id} label={producer.region.name} />
        </Section>
      )}

      {/* Child producers / estates */}
      {children.length > 0 && (
        <Section title="Estates & Labels">
          <div className="flex flex-wrap gap-2">
            {children.map(c => (
              <Link key={c.id} to={`/producer/${c.id}`}
                className="text-sm px-3 py-1.5 bg-earth-100 rounded-lg text-earth-700 hover:bg-earth-200 transition-colors">
                {c.name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Wines */}
      <Section title={`Wines${wineCount > 0 ? ` (${wineCount})` : ''}`}>
        {wines.length === 0 ? (
          <p className="text-sm text-earth-400">No wines in catalog yet.</p>
        ) : (
          <div className="space-y-0.5">
            {wines.map(w => (
              <Link key={w.id} to={`/wine/${w.id}`}
                className="flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-earth-100 transition-colors">
                {w.color && <div className={`w-3 h-3 rounded-full shrink-0 ${COLOR_DOTS[w.color] || 'bg-earth-300'}`} aria-label={`${w.color} wine`} />}
                <span className="text-sm text-earth-700 font-medium truncate">{w.name}</span>
                {w.appellation && <span className="text-[10px] text-earth-400 truncate shrink-0 ml-auto">{w.appellation.name}</span>}
              </Link>
            ))}
            {wineCount > 100 && <p className="text-xs text-earth-400 px-3 pt-2">Showing 100 of {wineCount}</p>}
          </div>
        )}
      </Section>
    </div>
  )
}

/* ── Shared components ────────────────────────────────────── */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mb-5">
      <h2 className="font-display text-base font-semibold text-earth-800 mb-2 pb-1 border-b border-earth-100">{title}</h2>
      {children}
    </section>
  )
}

function Tag({ children, variant = 'default' }: { children: React.ReactNode; variant?: 'default' | 'accent' | 'muted' }) {
  const s = { default: 'bg-earth-100 text-earth-600', accent: 'bg-wine-50 text-wine-700', muted: 'bg-stone-100 text-stone-500' }
  return <span className={`inline-block text-xs font-medium px-2 py-0.5 rounded-full capitalize ${s[variant]}`}>{children}</span>
}

function FactGrid({ children }: { children: React.ReactNode }) {
  const filtered = Array.isArray(children) ? children.filter(Boolean) : children ? [children] : []
  if (filtered.length === 0) return null
  return <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-x-4 gap-y-2 mb-2">{filtered}</div>
}

function Fact({ label, value, link }: { label: string; value: string; link?: string }) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] uppercase tracking-wider text-earth-400 leading-tight">{label}</div>
      {link ? (
        <Link to={link} className="text-sm font-medium text-earth-800 hover:text-wine-600 transition-colors truncate block">{value}</Link>
      ) : (
        <div className="text-sm font-medium text-earth-800 truncate" title={value}>{value}</div>
      )}
    </div>
  )
}

function AiLabel() {
  return <span className="inline-block text-[9px] font-semibold uppercase tracking-widest text-earth-300 ml-1 align-middle" aria-label="AI-generated content">AI</span>
}

function Loading() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-12">
      <div className="animate-pulse space-y-4">
        <div className="h-8 bg-earth-200 rounded w-1/2" />
        <div className="h-5 bg-earth-100 rounded w-1/4" />
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
