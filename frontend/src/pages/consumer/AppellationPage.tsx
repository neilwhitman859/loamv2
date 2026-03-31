import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import EntityMap from '../../components/EntityMap'

interface Appellation {
  id: string
  name: string
  designation_type: string | null
  established_year: number | null
  area_ha: number | null
  max_yield_hl_ha: number | null
  min_alcohol_pct: number | null
  min_aging_months: number | null
  elevation_min_m: number | null
  elevation_max_m: number | null
  regulatory_body: string | null
  regulatory_url: string | null
  allowed_grapes_description: string | null
  classification_level: string | null
  baseline_gdd: number | null
  baseline_rainfall_mm: number | null
  baseline_harvest_temp_c: number | null
  hemisphere: string | null
  growing_season_start_month: number | null
  growing_season_end_month: number | null
  country: { id: string; name: string } | null
  region: { id: string; name: string } | null
}

interface AppellationInsight {
  ai_overview: string | null
  ai_climate_profile: string | null
  ai_soil_profile: string | null
  ai_signature_style: string | null
  ai_key_grapes: string | null
  ai_aging_generalization: string | null
  ai_notable_producers_summary: string | null
}

interface GrapeAssoc {
  association_type: string | null
  min_percentage: number | null
  max_percentage: number | null
  grape: { id: string; display_name: string; color: string | null }
}

interface ProducerSummary { id: string; name: string }

const MONTHS = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

export default function AppellationPage() {
  const { id } = useParams<{ id: string }>()
  const [app, setApp] = useState<Appellation | null>(null)
  const [insight, setInsight] = useState<AppellationInsight | null>(null)
  const [grapes, setGrapes] = useState<GrapeAssoc[]>([])
  const [wineCount, setWineCount] = useState(0)
  const [producerCount, setProducerCount] = useState(0)
  const [producers, setProducers] = useState<ProducerSummary[]>([])
  const [childApps, setChildApps] = useState<{ id: string; name: string; designation_type: string | null }[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    Promise.all([
      supabase.from('appellations')
        .select(`id, name, designation_type, established_year, area_ha, max_yield_hl_ha, min_alcohol_pct, min_aging_months,
          elevation_min_m, elevation_max_m, regulatory_body, regulatory_url, allowed_grapes_description,
          classification_level, baseline_gdd, baseline_rainfall_mm, baseline_harvest_temp_c,
          hemisphere, growing_season_start_month, growing_season_end_month,
          country:countries!appellations_country_id_fkey(id, name),
          region:regions!appellations_region_id_fkey(id, name)`)
        .eq('id', id).single(),
      supabase.from('appellation_insights')
        .select('ai_overview, ai_climate_profile, ai_soil_profile, ai_signature_style, ai_key_grapes, ai_aging_generalization, ai_notable_producers_summary')
        .eq('appellation_id', id).maybeSingle(),
      supabase.from('appellation_grapes')
        .select('association_type, min_percentage, max_percentage, grape:grapes!appellation_grapes_grape_id_fkey(id, display_name, color)')
        .eq('appellation_id', id).limit(50),
      supabase.from('wines').select('id', { count: 'exact', head: true })
        .eq('appellation_id', id).is('deleted_at', null),
      supabase.from('producers').select('id', { count: 'exact', head: true })
        .eq('appellation_id', id).is('deleted_at', null),
      supabase.from('producers').select('id, name')
        .eq('appellation_id', id).is('deleted_at', null).order('name').limit(30),
    ]).then(([appRes, insRes, grapeRes, wRes, pRes, prodRes]) => {
      if (appRes.data) setApp(appRes.data as unknown as Appellation)
      if (insRes.data) setInsight(insRes.data)
      if (grapeRes.data) setGrapes(grapeRes.data as unknown as GrapeAssoc[])
      if (wRes.count != null) setWineCount(wRes.count)
      if (pRes.count != null) setProducerCount(pRes.count)
      if (prodRes.data) setProducers(prodRes.data)
      setLoading(false)
    })
  }, [id])

  // Fetch child appellations separately (containment hierarchy)
  useEffect(() => {
    if (!id) return
    supabase.from('appellation_containment')
      .select('child:appellations!appellation_containment_child_id_fkey(id, name, designation_type)')
      .eq('parent_id', id)
      .then(({ data }) => {
        if (data) setChildApps(data.map((r: any) => r.child).filter(Boolean))
      })
  }, [id])

  if (loading) return <Loading />
  if (!app) return <NotFound label="Appellation" />

  const requiredGrapes = grapes.filter(g => g.association_type === 'required')
  const typicalGrapes = grapes.filter(g => g.association_type === 'typical')

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">
      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-3 flex items-center gap-1.5 flex-wrap">
        {app.country && <><Link to={`/country/${app.country.id}`} className="hover:text-earth-600">{app.country.name}</Link><span>/</span></>}
        {app.region && <><Link to={`/region/${app.region.id}`} className="hover:text-earth-600">{app.region.name}</Link><span>/</span></>}
        <span className="text-earth-500">{app.name}</span>
      </nav>

      {/* Header */}
      <header className="mb-4">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{app.name}</h1>
        <div className="flex flex-wrap gap-1.5 mt-2">
          {app.designation_type && <Tag variant="accent">{app.designation_type}</Tag>}
          {app.classification_level && <Tag variant="muted">{app.classification_level}</Tag>}
          {wineCount > 0 && <Tag>{wineCount.toLocaleString()} wines</Tag>}
          {producerCount > 0 && <Tag>{producerCount.toLocaleString()} producers</Tag>}
        </div>
      </header>

      {/* Regulatory & identity */}
      <Section title="Appellation details">
        <FactGrid>
          {app.country && <Fact label="Country" value={app.country.name} link={`/country/${app.country.id}`} />}
          {app.region && <Fact label="Region" value={app.region.name} link={`/region/${app.region.id}`} />}
          {app.designation_type && <Fact label="Designation" value={app.designation_type} />}
          {app.established_year && <Fact label="Established" value={app.established_year.toString()} />}
          {app.area_ha && <Fact label="Area" value={`${app.area_ha.toLocaleString()} ha`} />}
          {app.regulatory_body && <Fact label="Regulatory body" value={app.regulatory_body} />}
          {app.classification_level && <Fact label="Classification" value={app.classification_level} />}
        </FactGrid>
      </Section>

      {/* Winemaking rules */}
      {(app.max_yield_hl_ha || app.min_alcohol_pct || app.min_aging_months) && (
        <Section title="Production rules">
          <FactGrid>
            {app.max_yield_hl_ha && <Fact label="Max yield" value={`${app.max_yield_hl_ha} hl/ha`} />}
            {app.min_alcohol_pct && <Fact label="Min ABV" value={`${app.min_alcohol_pct}%`} />}
            {app.min_aging_months && <Fact label="Min aging" value={`${app.min_aging_months} months`} />}
          </FactGrid>
          {app.allowed_grapes_description && (
            <p className="text-xs text-earth-500 mt-1">{app.allowed_grapes_description}</p>
          )}
        </Section>
      )}

      {/* Geography & climate */}
      {(app.elevation_min_m || app.baseline_gdd || app.hemisphere) && (
        <Section title="Geography & climate">
          <FactGrid>
            {app.elevation_min_m != null && app.elevation_max_m != null && (
              <Fact label="Elevation" value={`${app.elevation_min_m}–${app.elevation_max_m}m`} />
            )}
            {app.hemisphere && <Fact label="Hemisphere" value={app.hemisphere} />}
            {app.growing_season_start_month && app.growing_season_end_month && (
              <Fact label="Growing season" value={`${MONTHS[app.growing_season_start_month]}–${MONTHS[app.growing_season_end_month]}`} />
            )}
            {app.baseline_gdd && <Fact label="GDD" value={app.baseline_gdd.toString()} />}
            {app.baseline_rainfall_mm && <Fact label="Rainfall" value={`${app.baseline_rainfall_mm}mm`} />}
            {app.baseline_harvest_temp_c && <Fact label="Harvest temp" value={`${app.baseline_harvest_temp_c}°C`} />}
          </FactGrid>
        </Section>
      )}

      {/* AI climate / soil / style — short text */}
      {(insight?.ai_climate_profile || insight?.ai_soil_profile || insight?.ai_signature_style) && (
        <Section title="Terroir">
          {insight?.ai_soil_profile && (
            <div className="mb-2"><MiniLabel>Soil</MiniLabel><p className="text-sm text-earth-700">{insight.ai_soil_profile}</p></div>
          )}
          {insight?.ai_climate_profile && (
            <div className="mb-2"><MiniLabel>Climate</MiniLabel><p className="text-sm text-earth-700">{insight.ai_climate_profile}</p></div>
          )}
          {insight?.ai_signature_style && (
            <div className="mb-2"><MiniLabel>Signature style</MiniLabel><p className="text-sm text-earth-600">{insight.ai_signature_style}</p></div>
          )}
        </Section>
      )}

      {/* Map */}
      <Section title="Map">
        <EntityMap entityType="appellation" entityId={app.id} label={app.name} />
      </Section>

      {/* Grapes */}
      {grapes.length > 0 && (
        <Section title="Grape varieties">
          {requiredGrapes.length > 0 && (
            <div className="mb-3">
              <MiniLabel>Required</MiniLabel>
              <div className="flex flex-wrap gap-1.5">
                {requiredGrapes.map(g => (
                  <Link key={g.grape.id} to={`/grape/${g.grape.id}`}
                    className="text-xs px-2.5 py-1 bg-wine-50 text-wine-700 rounded-full hover:bg-wine-100 transition-colors font-medium">
                    {g.grape.display_name}
                    {g.min_percentage && g.max_percentage && <span className="text-wine-400 ml-1">{g.min_percentage}–{g.max_percentage}%</span>}
                  </Link>
                ))}
              </div>
            </div>
          )}
          {typicalGrapes.length > 0 && (
            <div>
              <MiniLabel>{requiredGrapes.length > 0 ? 'Also planted' : 'Key varieties'}</MiniLabel>
              <div className="flex flex-wrap gap-1.5">
                {typicalGrapes.map(g => (
                  <Link key={g.grape.id} to={`/grape/${g.grape.id}`}
                    className="text-xs px-2.5 py-1 bg-earth-100 text-earth-600 rounded-full hover:bg-earth-200 transition-colors">
                    {g.grape.display_name}
                  </Link>
                ))}
              </div>
            </div>
          )}
        </Section>
      )}

      {/* Child appellations */}
      {childApps.length > 0 && (
        <Section title="Sub-appellations">
          <div className="space-y-0.5">
            {childApps.map(a => (
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

      {/* Aging generalization */}
      {insight?.ai_aging_generalization && (
        <Section title="Aging">
          <p className="text-sm text-earth-600">{insight.ai_aging_generalization}</p>
        </Section>
      )}

      {!insight && grapes.length === 0 && (
        <div className="text-center py-12">
          <p className="text-sm text-earth-400">Appellation data is being built out. Check back soon.</p>
        </div>
      )}
    </div>
  )
}

/* ── Shared components ────────────────────────────────────── */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mb-5">
      <h3 className="font-display text-base font-semibold text-earth-800 mb-2 pb-1 border-b border-earth-100">{title}</h3>
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

function MiniLabel({ children }: { children: React.ReactNode }) {
  return <div className="text-[10px] uppercase tracking-wider text-earth-400 mb-1">{children}</div>
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
