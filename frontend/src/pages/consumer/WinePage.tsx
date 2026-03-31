import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import EntityMap from '../../components/EntityMap'

/* ── Interfaces ──────────────────────────────────────────── */

interface Wine {
  id: string
  name: string
  color: string | null
  wine_type: string | null
  effervescence: string | null
  vinification_notes: string | null
  first_vintage_year: number | null
  soil_description: string | null
  vine_age_description: string | null
  vineyard_area_ha: number | null
  altitude_m_low: number | null
  altitude_m_high: number | null
  aspect: string | null
  monopole: boolean | null
  producer: { id: string; name: string } | null
  country: { id: string; name: string } | null
  region: { id: string; name: string } | null
  appellation: { id: string; name: string; designation_type: string | null } | null
}

interface WineInsight {
  ai_hook: string | null
  ai_wine_summary: string | null
  ai_terroir_expression: string | null
  ai_vinification_summary: string | null
  ai_food_pairing: string | null
  ai_style_profile: string | null
  ai_cellar_recommendation: string | null
  ai_comparable_wines: string | null
  enrichment_tier: string | null
}

interface RegionInsight {
  ai_overview: string | null
  ai_climate_profile: string | null
  ai_signature_style: string | null
}

interface AppellationInsight {
  ai_overview: string | null
  ai_soil_profile: string | null
  ai_signature_style: string | null
  ai_key_grapes: string | null
}

interface Classification { level_name: string; system_name: string }
interface Score { score: number; publication_name: string; vintage_year: number }
interface GrapeLink { percentage: number | null; grape: { id: string; display_name: string; color: string | null } }
interface AppellationGrape { display_name: string; association_type: string }

interface Vintage {
  vintage_year: number
  abv: number | null
  cases_produced: number | null
  duration_in_oak_months: number | null
  new_oak_pct: number | null
  whole_cluster_pct: number | null
  harvest_start_date: string | null
  harvest_end_date: string | null
  winemaker_notes: string | null
  vintage_notes: string | null
  ph: number | null
  rs_g_l: number | null
  maceration_technique: string | null
}

const COLOR_DOT: Record<string, string> = {
  red: 'bg-red-700',
  white: 'bg-amber-100 border border-amber-300',
  rose: 'bg-pink-300',
  orange: 'bg-orange-300',
}

/* ── Main component ──────────────────────────────────────── */

export default function WinePage() {
  const { id } = useParams<{ id: string }>()
  const [wine, setWine] = useState<Wine | null>(null)
  const [insight, setInsight] = useState<WineInsight | null>(null)
  const [regionInsight, setRegionInsight] = useState<RegionInsight | null>(null)
  const [appInsight, setAppInsight] = useState<AppellationInsight | null>(null)
  const [classifications, setClassifications] = useState<Classification[]>([])
  const [scores, setScores] = useState<Score[]>([])
  const [grapes, setGrapes] = useState<GrapeLink[]>([])
  const [appGrapes, setAppGrapes] = useState<AppellationGrape[]>([])
  const [vintages, setVintages] = useState<Vintage[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    supabase
      .from('wines')
      .select(`
        id, name, color, wine_type, effervescence, vinification_notes, first_vintage_year,
        soil_description, vine_age_description, vineyard_area_ha, altitude_m_low, altitude_m_high, aspect, monopole,
        producer:producers!wines_producer_id_fkey(id, name),
        country:countries!wines_country_id_fkey(id, name),
        region:regions!wines_region_id_fkey(id, name),
        appellation:appellations!wines_appellation_id_fkey(id, name, designation_type)
      `)
      .eq('id', id)
      .single()
      .then(({ data }) => {
        if (data) {
          setWine(data as unknown as Wine)
          const p: PromiseLike<void>[] = []

          p.push(supabase.from('wine_insights')
            .select('ai_hook, ai_wine_summary, ai_terroir_expression, ai_vinification_summary, ai_food_pairing, ai_style_profile, ai_cellar_recommendation, ai_comparable_wines, enrichment_tier')
            .eq('wine_id', id).maybeSingle()
            .then(({ data: d }) => { if (d) setInsight(d) }))

          if ((data.region as any)?.id) {
            p.push(supabase.from('region_insights')
              .select('ai_overview, ai_climate_profile, ai_signature_style')
              .eq('region_id', (data.region as any).id).maybeSingle()
              .then(({ data: d }) => { if (d) setRegionInsight(d) }))
          }

          if ((data.appellation as any)?.id) {
            p.push(supabase.from('appellation_insights')
              .select('ai_overview, ai_soil_profile, ai_signature_style, ai_key_grapes')
              .eq('appellation_id', (data.appellation as any).id).maybeSingle()
              .then(({ data: d }) => { if (d) setAppInsight(d) }))

            // Appellation grapes — what varieties define this place
            p.push(supabase.from('appellation_grapes')
              .select('association_type, grape:grapes!appellation_grapes_grape_id_fkey(display_name)')
              .eq('appellation_id', (data.appellation as any).id)
              .then(({ data: d }) => {
                if (d) setAppGrapes(d.map((r: any) => ({
                  display_name: r.grape?.display_name || '',
                  association_type: r.association_type,
                })))
              }))
          }

          p.push(supabase.from('wine_vintage_scores')
            .select('score, vintage_year, publication:publications!wine_vintage_scores_publication_id_fkey(name)')
            .eq('wine_id', id).order('score', { ascending: false }).limit(10)
            .then(({ data: d }) => {
              if (d) setScores(d.map((r: any) => ({
                score: r.score, publication_name: r.publication?.name || 'Unknown', vintage_year: r.vintage_year,
              })))
            }))

          p.push(supabase.from('wine_grapes')
            .select('percentage, grape:grapes!wine_grapes_grape_id_fkey(id, display_name, color)')
            .eq('wine_id', id).order('percentage', { ascending: false, nullsFirst: false })
            .then(({ data: d }) => { if (d) setGrapes(d as unknown as GrapeLink[]) }))

          p.push(supabase.from('wine_vintages')
            .select('vintage_year, abv, cases_produced, duration_in_oak_months, new_oak_pct, whole_cluster_pct, harvest_start_date, harvest_end_date, winemaker_notes, vintage_notes, ph, rs_g_l, maceration_technique')
            .eq('wine_id', id).order('vintage_year', { ascending: false }).limit(10)
            .then(({ data: d }) => { if (d) setVintages(d) }))

          p.push(supabase.from('entity_classifications')
            .select('classification_level:classification_levels!entity_classifications_classification_level_id_fkey(level_name, classification:classifications!classification_levels_classification_id_fkey(name))')
            .eq('entity_type', 'wine').eq('entity_id', id)
            .then(({ data: d }) => {
              if (d) setClassifications(d.map((r: any) => ({
                level_name: r.classification_level?.level_name || '',
                system_name: r.classification_level?.classification?.name || '',
              })))
            }))

          Promise.all(p).then(() => setLoading(false))
        } else {
          setLoading(false)
        }
      })
  }, [id])

  /* ── Loading / not found ───────────────────────────────── */

  if (loading) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-12">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-earth-200 rounded w-2/3" />
          <div className="h-5 bg-earth-100 rounded w-1/3" />
          <div className="h-4 bg-earth-100 rounded w-full mt-6" />
          <div className="h-4 bg-earth-100 rounded w-5/6" />
        </div>
      </div>
    )
  }

  if (!wine) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-16 text-center">
        <p className="text-earth-500">Wine not found</p>
        <Link to="/" className="text-wine-600 text-sm mt-2 inline-block hover:underline">Back to search</Link>
      </div>
    )
  }

  /* ── Derived data ──────────────────────────────────────── */

  const v = vintages[0] || null // latest vintage
  const hasAnyContent = insight || regionInsight || appInsight || scores.length > 0 || grapes.length > 0 || v

  // Maps
  const maps: { type: 'country' | 'region' | 'appellation'; id: string; label: string }[] = []
  if (wine.region) maps.push({ type: 'region', id: wine.region.id, label: wine.region.name })
  if (wine.appellation) maps.push({ type: 'appellation', id: wine.appellation.id, label: wine.appellation.name })
  if (maps.length === 0 && wine.country) maps.push({ type: 'country', id: wine.country.id, label: wine.country.name })

  // Drink window
  const dw = extractDrinkWindow(insight?.ai_cellar_recommendation || '')

  /* ── Render ────────────────────────────────────────────── */

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">

      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-3 flex items-center gap-1.5 flex-wrap">
        {wine.country && <><Link to={`/country/${wine.country.id}`} className="hover:text-earth-600">{wine.country.name}</Link><span>/</span></>}
        {wine.region && <><Link to={`/region/${wine.region.id}`} className="hover:text-earth-600">{wine.region.name}</Link><span>/</span></>}
        {wine.appellation && <><Link to={`/appellation/${wine.appellation.id}`} className="hover:text-earth-600">{wine.appellation.name}</Link><span>/</span></>}
        <span className="text-earth-500">{wine.name}</span>
      </nav>

      {/* ── Identity ─────────────────────────────────────── */}
      <header className="mb-5">
        <div className="flex items-start gap-3">
          {wine.color && <div className={`w-4 h-4 rounded-full mt-2 shrink-0 ${COLOR_DOT[wine.color] || 'bg-earth-300'}`} />}
          <div>
            <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900 leading-tight">{wine.name}</h1>
            {wine.producer && (
              <Link to={`/producer/${wine.producer.id}`} className="text-base text-wine-600 hover:text-wine-700 font-medium mt-0.5 inline-block">
                {wine.producer.name}
              </Link>
            )}
          </div>
        </div>

        <div className="flex flex-wrap gap-1.5 mt-3">
          {wine.color && <Tag>{wine.color}</Tag>}
          {wine.wine_type && wine.wine_type !== 'table' && <Tag>{wine.wine_type}</Tag>}
          {wine.effervescence && wine.effervescence !== 'still' && <Tag>{wine.effervescence}</Tag>}
          {classifications.map((c, i) => <Tag key={i} variant="accent">{c.level_name}</Tag>)}
          {wine.appellation?.designation_type && <Tag variant="muted">{wine.appellation.designation_type}</Tag>}
        </div>

        {/* Style one-liner */}
        {insight?.ai_style_profile && (
          <p className="text-sm text-earth-500 mt-3">{insight.ai_style_profile}</p>
        )}
      </header>

      {/* ── Scores (compact chips) ───────────────────────── */}
      {scores.length > 0 && (
        <div className="flex flex-wrap gap-3 mb-5">
          {scores.map((s, i) => (
            <div key={i} className="flex items-baseline gap-1.5">
              <span className="text-lg font-display font-bold text-wine-700">{s.score}</span>
              <span className="text-[10px] text-earth-400 uppercase tracking-wider">{abbrevPub(s.publication_name)}</span>
              {s.vintage_year > 0 && <span className="text-[10px] text-earth-300">{s.vintage_year}</span>}
            </div>
          ))}
        </div>
      )}

      {/* ── Hook ─────────────────────────────────────────── */}
      {insight?.ai_hook && (
        <p className="text-sm text-earth-700 leading-relaxed italic border-l-2 border-wine-300 pl-3 mb-5">
          {insight.ai_hook}
        </p>
      )}

      <Divider />

      {/* ── The Facts (structured vintage data) ──────────── */}
      {v && (
        <>
          <section className="mb-6">
            <H3>{v.vintage_year === 0 ? 'NV' : `${v.vintage_year} Vintage`}</H3>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              {v.abv && <Fact label="ABV" value={`${v.abv}%`} />}
              {v.ph && <Fact label="pH" value={v.ph.toString()} />}
              {v.rs_g_l != null && <Fact label="RS" value={`${v.rs_g_l} g/L`} />}
              {v.cases_produced && <Fact label="Production" value={`${v.cases_produced.toLocaleString()} cases`} />}
              {v.duration_in_oak_months && <Fact label="Oak aging" value={`${v.duration_in_oak_months} months`} />}
              {v.new_oak_pct != null && <Fact label="New oak" value={`${v.new_oak_pct}%`} />}
              {v.whole_cluster_pct != null && v.whole_cluster_pct > 0 && <Fact label="Whole cluster" value={`${v.whole_cluster_pct}%`} />}
              {v.maceration_technique && <Fact label="Maceration" value={v.maceration_technique} />}
              {v.harvest_start_date && (
                <Fact label="Harvest" value={
                  v.harvest_end_date
                    ? `${fmtDate(v.harvest_start_date)} – ${fmtDate(v.harvest_end_date)}`
                    : fmtDate(v.harvest_start_date)
                } />
              )}
            </div>

            {/* Vintage notes */}
            {v.vintage_notes && (
              <p className="text-sm text-earth-600 leading-relaxed mt-3 italic">{v.vintage_notes}</p>
            )}
          </section>
          <Divider />
        </>
      )}

      {/* ── Grapes ───────────────────────────────────────── */}
      {grapes.length > 0 && (
        <>
          <section className="mb-6">
            <H3>Grapes</H3>
            <div className="flex flex-wrap gap-2">
              {grapes.map((g, i) => (
                <Link key={i} to={`/grape/${g.grape.id}`}
                  className="text-sm px-3 py-1.5 bg-earth-100 rounded-full text-earth-700 hover:bg-earth-200 transition-colors">
                  {g.grape.display_name}
                  {g.percentage != null && g.percentage < 100 && <span className="text-earth-400 ml-1">{g.percentage}%</span>}
                </Link>
              ))}
            </div>
          </section>
          <Divider />
        </>
      )}

      {/* ── Place ────────────────────────────────────────── */}
      <section className="mb-6">
        <H3>Place</H3>

        {/* Structured geo hierarchy */}
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mb-4">
          {wine.country && <Fact label="Country" value={wine.country.name} link={`/country/${wine.country.id}`} />}
          {wine.region && <Fact label="Region" value={wine.region.name} link={`/region/${wine.region.id}`} />}
          {wine.appellation && <Fact label="Appellation" value={wine.appellation.name} link={`/appellation/${wine.appellation.id}`} />}
          {wine.altitude_m_low && <Fact label="Elevation" value={wine.altitude_m_high ? `${wine.altitude_m_low}–${wine.altitude_m_high}m` : `${wine.altitude_m_low}m`} />}
          {wine.aspect && <Fact label="Aspect" value={wine.aspect} />}
          {wine.vineyard_area_ha && <Fact label="Vineyard" value={`${wine.vineyard_area_ha} ha`} />}
          {wine.vine_age_description && <Fact label="Vine age" value={wine.vine_age_description} />}
        </div>

        {/* Maps */}
        {maps.length > 0 && (
          <div className={maps.length > 1 ? 'grid grid-cols-1 sm:grid-cols-2 gap-3 mb-4' : 'mb-4'}>
            {maps.map((m) => <EntityMap key={m.type} entityType={m.type} entityId={m.id} label={m.label} />)}
          </div>
        )}

        {/* Appellation grapes — what grows here */}
        {appGrapes.length > 0 && (
          <div className="mb-3">
            <div className="text-[10px] uppercase tracking-wider text-earth-400 mb-1">Appellation varieties</div>
            <div className="flex flex-wrap gap-1.5">
              {appGrapes.map((g, i) => (
                <span key={i} className="text-xs px-2 py-0.5 bg-earth-50 border border-earth-150 rounded text-earth-600">
                  {g.display_name}
                  {g.association_type === 'required' && <span className="text-wine-500 ml-1">req</span>}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Soil — from wine or appellation */}
        {(wine.soil_description || appInsight?.ai_soil_profile) && (
          <div className="mb-3">
            <div className="text-[10px] uppercase tracking-wider text-earth-400 mb-1">Soil</div>
            <p className="text-sm text-earth-700 leading-relaxed">{wine.soil_description || appInsight?.ai_soil_profile}</p>
          </div>
        )}

        {/* Climate / signature style from appellation or region */}
        {(appInsight?.ai_signature_style || regionInsight?.ai_signature_style) && (
          <div className="mb-3">
            <div className="text-[10px] uppercase tracking-wider text-earth-400 mb-1">Signature style</div>
            <p className="text-sm text-earth-600 leading-relaxed">{appInsight?.ai_signature_style || regionInsight?.ai_signature_style}</p>
          </div>
        )}
      </section>

      <Divider />

      {/* ── Terroir (wine-specific) ──────────────────────── */}
      {insight?.ai_terroir_expression && (
        <>
          <section className="mb-6">
            <H3>Terroir</H3>
            <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_terroir_expression}</p>
          </section>
          <Divider />
        </>
      )}

      {/* ── Winemaking ───────────────────────────────────── */}
      {(insight?.ai_vinification_summary || wine.vinification_notes) && (
        <>
          <section className="mb-6">
            <H3>Winemaking</H3>
            <p className="text-sm text-earth-700 leading-relaxed">{insight?.ai_vinification_summary || wine.vinification_notes}</p>
          </section>
          <Divider />
        </>
      )}

      {/* ── The Wine (story/context) ─────────────────────── */}
      {insight?.ai_wine_summary && (
        <>
          <section className="mb-6">
            <H3>About this wine</H3>
            <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_wine_summary}</p>
          </section>
          <Divider />
        </>
      )}

      {/* ── Other vintages (compact table) ────────────────── */}
      {vintages.length > 1 && (
        <>
          <section className="mb-6">
            <H3>Other vintages</H3>
            <div className="space-y-1.5">
              {vintages.slice(1).map((ov, i) => (
                <div key={i} className="flex items-baseline gap-4 text-sm py-1 border-b border-earth-100 last:border-0">
                  <span className="font-semibold text-earth-700 w-12 shrink-0">{ov.vintage_year === 0 ? 'NV' : ov.vintage_year}</span>
                  <div className="flex flex-wrap gap-x-4 gap-y-0.5 text-earth-500">
                    {ov.abv && <span>{ov.abv}% ABV</span>}
                    {ov.cases_produced && <span>{ov.cases_produced.toLocaleString()} cases</span>}
                    {ov.duration_in_oak_months && <span>{ov.duration_in_oak_months}mo oak</span>}
                    {ov.new_oak_pct != null && <span>{ov.new_oak_pct}% new</span>}
                  </div>
                </div>
              ))}
            </div>
          </section>
          <Divider />
        </>
      )}

      {/* ── Food pairing ─────────────────────────────────── */}
      {insight?.ai_food_pairing && (
        <>
          <section className="mb-6">
            <H3>Pairs with</H3>
            <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_food_pairing}</p>
          </section>
          <Divider />
        </>
      )}

      {/* ── Drink window ─────────────────────────────────── */}
      {(dw || insight?.ai_cellar_recommendation) && (
        <>
          <section className="mb-6">
            <H3>Cellar</H3>
            {dw && <DrinkWindowBar start={dw.start} end={dw.end} />}
            {insight?.ai_cellar_recommendation && (
              <p className={`text-sm text-earth-700 leading-relaxed ${dw ? 'mt-2' : ''}`}>{insight.ai_cellar_recommendation}</p>
            )}
          </section>
          <Divider />
        </>
      )}

      {/* ── Similar wines ────────────────────────────────── */}
      {insight?.ai_comparable_wines && (
        <section className="mb-6">
          <H3>Similar wines</H3>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_comparable_wines}</p>
        </section>
      )}

      {/* ── Fallback for Grade F wines with no context ───── */}
      {!hasAnyContent && (
        <div className="mt-12 text-center">
          <p className="text-sm text-earth-400">We have this wine in our catalog. More details coming soon.</p>
        </div>
      )}
    </div>
  )
}

/* ── Components ──────────────────────────────────────────── */

function H3({ children }: { children: React.ReactNode }) {
  return <h3 className="font-display text-base font-semibold text-earth-800 mb-2">{children}</h3>
}

function Divider() {
  return <hr className="border-earth-100 mb-6" />
}

function Tag({ children, variant = 'default' }: { children: React.ReactNode; variant?: 'default' | 'accent' | 'muted' }) {
  const s = { default: 'bg-earth-100 text-earth-600', accent: 'bg-wine-50 text-wine-700', muted: 'bg-stone-100 text-stone-500' }
  return <span className={`inline-block text-xs font-medium px-2 py-0.5 rounded-full capitalize ${s[variant]}`}>{children}</span>
}

function Fact({ label, value, link }: { label: string; value: string; link?: string }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-earth-400">{label}</div>
      {link ? (
        <Link to={link} className="text-sm font-medium text-earth-800 hover:text-wine-600 transition-colors">{value}</Link>
      ) : (
        <div className="text-sm font-medium text-earth-800">{value}</div>
      )}
    </div>
  )
}

function DrinkWindowBar({ start, end }: { start: number; end: number }) {
  const now = new Date().getFullYear()
  const span = end - start
  const pct = Math.min(100, Math.max(0, ((Math.max(0, now - start)) / span) * 100))
  const remaining = Math.max(0, end - now)

  return (
    <div>
      <div className="flex justify-between text-xs text-earth-400 mb-1">
        <span>{start}</span>
        <span>{end}</span>
      </div>
      <div className="h-2 bg-earth-100 rounded-full overflow-hidden">
        <div className="h-full bg-gradient-to-r from-wine-300 to-wine-500 rounded-full" style={{ width: `${pct}%` }} />
      </div>
      {remaining > 0 && pct > 0 && (
        <div className="text-[10px] text-earth-400 mt-1">{remaining} years remaining</div>
      )}
    </div>
  )
}

/* ── Helpers ──────────────────────────────────────────────── */

function fmtDate(d: string): string {
  try { return new Date(d + 'T00:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) }
  catch { return d }
}

function abbrevPub(name: string): string {
  const m: Record<string, string> = {
    'Wine Advocate': 'WA', 'Wine Spectator': 'WS', 'Wine Enthusiast': 'WE',
    'James Suckling': 'JS', 'Jeb Dunnuck': 'JD', 'Jancis Robinson': 'JR',
    'Robert Parker': 'RP', 'Vinous': 'Vinous', 'Decanter': 'Decanter',
  }
  return m[name] || name
}

function extractDrinkWindow(text: string): { start: number; end: number } | null {
  const m = text.match(/(\d{4})\s*[-–to]+\s*(\d{4})/)
  if (!m) return null
  const [start, end] = [parseInt(m[1]), parseInt(m[2])]
  const now = new Date().getFullYear()
  return end > start && start >= now - 5 && end <= now + 50 ? { start, end } : null
}
