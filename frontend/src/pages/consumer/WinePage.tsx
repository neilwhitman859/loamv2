import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import EntityMap from '../../components/EntityMap'

interface Wine {
  id: string
  name: string
  color: string | null
  wine_type: string | null
  effervescence: string | null
  data_grade: string | null
  vinification_notes: string | null
  first_vintage_year: number | null
  producer: { id: string; name: string; country_id: string | null; region_id: string | null; website_url: string | null } | null
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

interface Classification {
  level_name: string
  system_name: string
}

interface Score {
  score: number
  publication_name: string
  vintage_year: number
}

interface GrapeLink {
  percentage: number | null
  grape: { id: string; display_name: string; color: string | null }
}

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
}

const COLOR_DOTS: Record<string, string> = {
  red: 'bg-red-700',
  white: 'bg-amber-100 border border-amber-300',
  rose: 'bg-pink-300',
  orange: 'bg-orange-300',
}

export default function WinePage() {
  const { id } = useParams<{ id: string }>()
  const [wine, setWine] = useState<Wine | null>(null)
  const [wineInsight, setWineInsight] = useState<WineInsight | null>(null)
  const [regionInsight, setRegionInsight] = useState<RegionInsight | null>(null)
  const [appellationInsight, setAppellationInsight] = useState<AppellationInsight | null>(null)
  const [classifications, setClassifications] = useState<Classification[]>([])
  const [scores, setScores] = useState<Score[]>([])
  const [grapes, setGrapes] = useState<GrapeLink[]>([])
  const [vintages, setVintages] = useState<Vintage[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    supabase
      .from('wines')
      .select(`
        id, name, color, wine_type, effervescence, data_grade, vinification_notes, first_vintage_year,
        producer:producers!wines_producer_id_fkey(id, name, country_id, region_id, website_url),
        country:countries!wines_country_id_fkey(id, name),
        region:regions!wines_region_id_fkey(id, name),
        appellation:appellations!wines_appellation_id_fkey(id, name, designation_type)
      `)
      .eq('id', id)
      .single()
      .then(({ data }) => {
        if (data) {
          setWine(data as unknown as Wine)
          const promises: PromiseLike<void>[] = []

          promises.push(
            supabase.from('wine_insights').select('ai_hook, ai_wine_summary, ai_terroir_expression, ai_vinification_summary, ai_food_pairing, ai_style_profile, ai_cellar_recommendation, ai_comparable_wines, enrichment_tier')
              .eq('wine_id', id).maybeSingle()
              .then(({ data: d }) => { if (d) setWineInsight(d) })
          )

          if (data.region && (data.region as any).id) {
            promises.push(
              supabase.from('region_insights').select('ai_overview, ai_climate_profile, ai_signature_style')
                .eq('region_id', (data.region as any).id).maybeSingle()
                .then(({ data: d }) => { if (d) setRegionInsight(d) })
            )
          }

          if (data.appellation && (data.appellation as any).id) {
            promises.push(
              supabase.from('appellation_insights').select('ai_overview, ai_soil_profile, ai_signature_style, ai_key_grapes')
                .eq('appellation_id', (data.appellation as any).id).maybeSingle()
                .then(({ data: d }) => { if (d) setAppellationInsight(d) })
            )
          }

          promises.push(
            supabase.from('wine_vintage_scores')
              .select('score, vintage_year, publication:publications!wine_vintage_scores_publication_id_fkey(name)')
              .eq('wine_id', id)
              .order('score', { ascending: false })
              .limit(10)
              .then(({ data: d }) => {
                if (d) setScores(d.map((row: any) => ({
                  score: row.score,
                  publication_name: row.publication?.name || 'Unknown',
                  vintage_year: row.vintage_year,
                })))
              })
          )

          promises.push(
            supabase.from('wine_grapes')
              .select('percentage, grape:grapes!wine_grapes_grape_id_fkey(id, display_name, color)')
              .eq('wine_id', id)
              .order('percentage', { ascending: false, nullsFirst: false })
              .then(({ data: d }) => { if (d) setGrapes(d as unknown as GrapeLink[]) })
          )

          promises.push(
            supabase.from('wine_vintages')
              .select('vintage_year, abv, cases_produced, duration_in_oak_months, new_oak_pct, whole_cluster_pct, harvest_start_date, harvest_end_date, winemaker_notes, vintage_notes, ph')
              .eq('wine_id', id)
              .order('vintage_year', { ascending: false })
              .limit(10)
              .then(({ data: d }) => { if (d) setVintages(d) })
          )

          promises.push(
            supabase.from('entity_classifications')
              .select('classification_level:classification_levels!entity_classifications_classification_level_id_fkey(level_name, classification:classifications!classification_levels_classification_id_fkey(name))')
              .eq('entity_type', 'wine')
              .eq('entity_id', id)
              .then(({ data: d }) => {
                if (d) {
                  setClassifications(d.map((row: any) => ({
                    level_name: row.classification_level?.level_name || '',
                    system_name: row.classification_level?.classification?.name || '',
                  })))
                }
              })
          )

          Promise.all(promises).then(() => setLoading(false))
        } else {
          setLoading(false)
        }
      })
  }, [id])

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

  const hasWineInsight = wineInsight && (wineInsight.ai_hook || wineInsight.ai_wine_summary)
  const hasGeoContext = regionInsight || appellationInsight

  // Maps
  const mapLayers: { type: 'country' | 'region' | 'appellation'; id: string; label: string }[] = []
  if (wine.region) mapLayers.push({ type: 'region', id: wine.region.id, label: wine.region.name })
  if (wine.appellation) mapLayers.push({ type: 'appellation', id: wine.appellation.id, label: wine.appellation.name })
  if (mapLayers.length === 0 && wine.country) mapLayers.push({ type: 'country', id: wine.country.id, label: wine.country.name })

  // Latest vintage
  const v = vintages.length > 0 ? vintages[0] : null

  // Build fact rows
  const facts: { label: string; value: string }[] = []
  if (v?.abv) facts.push({ label: 'ABV', value: `${v.abv}%` })
  if (v?.ph) facts.push({ label: 'pH', value: v.ph.toString() })
  if (v?.duration_in_oak_months) facts.push({ label: 'Oak', value: `${v.duration_in_oak_months} mo` })
  if (v?.new_oak_pct != null) facts.push({ label: 'New oak', value: `${v.new_oak_pct}%` })
  if (v?.whole_cluster_pct != null && v.whole_cluster_pct > 0) facts.push({ label: 'Whole cluster', value: `${v.whole_cluster_pct}%` })
  if (v?.cases_produced) facts.push({ label: 'Production', value: `${v.cases_produced.toLocaleString()} cases` })
  if (v?.harvest_start_date) {
    facts.push({
      label: 'Harvest',
      value: v.harvest_end_date
        ? `${fmtDate(v.harvest_start_date)} – ${fmtDate(v.harvest_end_date)}`
        : fmtDate(v.harvest_start_date)
    })
  }

  // Cellar — extract drink window years
  const cellarYears = extractDrinkWindow(wineInsight?.ai_cellar_recommendation || '')

  // Food — parse into list
  const foodList = parseFoodList(wineInsight?.ai_food_pairing || '')

  // Similar — parse into names
  const similarNames = parseWineNames(wineInsight?.ai_comparable_wines || '')

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">

      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-3 flex items-center gap-1.5 flex-wrap">
        {wine.country && (
          <><Link to={`/country/${wine.country.id}`} className="hover:text-earth-600 transition-colors">{wine.country.name}</Link><span>/</span></>
        )}
        {wine.region && (
          <><Link to={`/region/${wine.region.id}`} className="hover:text-earth-600 transition-colors">{wine.region.name}</Link><span>/</span></>
        )}
        {wine.appellation && (
          <><Link to={`/appellation/${wine.appellation.id}`} className="hover:text-earth-600 transition-colors">{wine.appellation.name}</Link><span>/</span></>
        )}
        <span className="text-earth-500">{wine.name}</span>
      </nav>

      {/* ── Identity ─────────────────────────────────────── */}
      <header className="mb-5">
        <div className="flex items-start gap-3">
          {wine.color && (
            <div className={`w-4 h-4 rounded-full mt-2 shrink-0 ${COLOR_DOTS[wine.color] || 'bg-earth-300'}`} />
          )}
          <div>
            <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900 leading-tight">
              {wine.name}
            </h1>
            {wine.producer && (
              <Link to={`/producer/${wine.producer.id}`} className="text-base text-wine-600 hover:text-wine-700 transition-colors font-medium mt-0.5 inline-block">
                {wine.producer.name}
              </Link>
            )}
          </div>
        </div>

        {/* Tags */}
        <div className="flex flex-wrap gap-1.5 mt-3">
          {wine.color && <Tag>{wine.color}</Tag>}
          {wine.wine_type && wine.wine_type !== 'table' && <Tag>{wine.wine_type}</Tag>}
          {wine.effervescence && wine.effervescence !== 'still' && <Tag>{wine.effervescence}</Tag>}
          {classifications.map((c, i) => <Tag key={i} variant="accent">{c.level_name}</Tag>)}
          {wine.appellation?.designation_type && <Tag variant="muted">{wine.appellation.designation_type}</Tag>}
        </div>
      </header>

      {/* ── Score chips (CellarTracker-style inline) ────── */}
      {scores.length > 0 && (
        <div className="flex flex-wrap gap-x-4 gap-y-1 mb-4">
          {scores.map((s, i) => (
            <span key={i} className="text-sm">
              <span className="font-bold text-wine-700">{s.score}</span>
              <span className="text-earth-400 ml-1 text-xs uppercase tracking-wider">{abbreviatePublication(s.publication_name)}</span>
            </span>
          ))}
        </div>
      )}

      {/* ── Facts card ───────────────────────────────────── */}
      {(facts.length > 0 || grapes.length > 0) && (
        <section className="mb-5 bg-earth-50 rounded-lg border border-earth-150 p-3">
          {v && (
            <div className="text-[10px] font-semibold uppercase tracking-wider text-earth-400 mb-2">
              {v.vintage_year === 0 ? 'NV' : v.vintage_year}
            </div>
          )}

          {facts.length > 0 && (
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-1.5 mb-2">
              {facts.map((f, i) => (
                <div key={i}>
                  <span className="text-[10px] uppercase tracking-wider text-earth-400">{f.label}</span>
                  <span className="text-sm font-medium text-earth-800 ml-1.5">{f.value}</span>
                </div>
              ))}
            </div>
          )}

          {grapes.length > 0 && (
            <div className="flex flex-wrap gap-1.5 pt-2 border-t border-earth-200/50">
              {grapes.map((g, i) => (
                <Link key={i} to={`/grape/${g.grape.id}`}
                  className="text-xs px-2 py-0.5 bg-white rounded-full text-earth-700 hover:bg-earth-100 transition-colors border border-earth-200/50">
                  {g.grape.display_name}{g.percentage != null && g.percentage < 100 && <span className="text-earth-400 ml-1">{g.percentage}%</span>}
                </Link>
              ))}
            </div>
          )}
        </section>
      )}

      {/* ── Hook — the one personality sentence ──────────── */}
      {wineInsight?.ai_hook && (
        <p className="text-sm text-earth-600 leading-relaxed italic border-l-2 border-wine-300 pl-3 mb-5">
          {wineInsight.ai_hook}
        </p>
      )}

      <Divider />

      {/* ── Place ────────────────────────────────────────── */}
      <section className="mb-6">
        <SectionTitle>Place</SectionTitle>
        <div className="flex flex-wrap gap-x-5 gap-y-1 text-sm mb-3">
          {wine.country && (
            <span><span className="text-earth-400">Country</span> <Link to={`/country/${wine.country.id}`} className="text-earth-700 hover:text-wine-600 font-medium">{wine.country.name}</Link></span>
          )}
          {wine.region && (
            <span><span className="text-earth-400">Region</span> <Link to={`/region/${wine.region.id}`} className="text-earth-700 hover:text-wine-600 font-medium">{wine.region.name}</Link></span>
          )}
          {wine.appellation && (
            <span><span className="text-earth-400">Appellation</span> <Link to={`/appellation/${wine.appellation.id}`} className="text-earth-700 hover:text-wine-600 font-medium">{wine.appellation.name}</Link></span>
          )}
        </div>

        {mapLayers.length > 0 && (
          <div className={mapLayers.length > 1 ? 'grid grid-cols-1 sm:grid-cols-2 gap-3' : ''}>
            {mapLayers.map((m) => (
              <EntityMap key={m.type} entityType={m.type} entityId={m.id} label={m.label} />
            ))}
          </div>
        )}

        {/* Geo context — only for Grade F wines with no wine-specific content */}
        {!wineInsight?.ai_terroir_expression && !wineInsight?.ai_wine_summary && (
          <>
            {appellationInsight?.ai_overview && (
              <div className="mt-3 bg-earth-100/60 rounded-lg p-3 border border-earth-200/50">
                <p className="text-xs font-semibold uppercase tracking-wider text-earth-400 mb-1">
                  About <Link to={`/appellation/${wine.appellation?.id}`} className="text-wine-500 hover:text-wine-600 normal-case tracking-normal font-medium">{wine.appellation?.name}</Link>
                </p>
                <p className="text-sm text-earth-600 leading-relaxed">{truncate(appellationInsight.ai_overview, 200)}</p>
              </div>
            )}
            {regionInsight?.ai_overview && !appellationInsight?.ai_overview && (
              <div className="mt-3 bg-earth-100/60 rounded-lg p-3 border border-earth-200/50">
                <p className="text-xs font-semibold uppercase tracking-wider text-earth-400 mb-1">
                  About <Link to={`/region/${wine.region?.id}`} className="text-wine-500 hover:text-wine-600 normal-case tracking-normal font-medium">{wine.region?.name}</Link>
                </p>
                <p className="text-sm text-earth-600 leading-relaxed">{truncate(regionInsight.ai_overview, 200)}</p>
              </div>
            )}
          </>
        )}
      </section>

      <Divider />

      {/* ── Terroir — 1-2 sentences max ──────────────────── */}
      {wineInsight?.ai_terroir_expression && (
        <>
          <section className="mb-6">
            <SectionTitle>Terroir</SectionTitle>
            <p className="text-sm text-earth-700 leading-relaxed">{truncate(wineInsight.ai_terroir_expression, 300)}</p>
          </section>
          <Divider />
        </>
      )}

      {/* ── Winemaking — facts grid + 1-2 sentence summary ─ */}
      {(wineInsight?.ai_vinification_summary || wine.vinification_notes) && (
        <>
          <section className="mb-6">
            <SectionTitle>Winemaking</SectionTitle>
            <p className="text-sm text-earth-700 leading-relaxed">{truncate(wineInsight?.ai_vinification_summary || wine.vinification_notes || '', 250)}</p>
          </section>
          <Divider />
        </>
      )}

      {/* ── Vintage notes ────────────────────────────────── */}
      {v?.vintage_notes && (
        <>
          <section className="mb-6">
            <SectionTitle>{v.vintage_year === 0 ? 'NV' : v.vintage_year} Vintage</SectionTitle>
            <p className="text-sm text-earth-600 leading-relaxed italic">{v.vintage_notes}</p>
          </section>
          <Divider />
        </>
      )}

      {/* ── Other vintages (compact) ─────────────────────── */}
      {vintages.length > 1 && (
        <>
          <section className="mb-6">
            <SectionTitle>Other Vintages</SectionTitle>
            <div className="space-y-2">
              {vintages.slice(1).map((ov, i) => (
                <div key={i} className="flex items-baseline gap-3 text-sm">
                  <span className="font-semibold text-earth-700 w-10">{ov.vintage_year === 0 ? 'NV' : ov.vintage_year}</span>
                  {ov.abv && <span className="text-earth-500">{ov.abv}%</span>}
                  {ov.cases_produced && <span className="text-earth-500">{ov.cases_produced.toLocaleString()} cases</span>}
                  {ov.duration_in_oak_months && <span className="text-earth-500">{ov.duration_in_oak_months}mo oak</span>}
                </div>
              ))}
            </div>
          </section>
          <Divider />
        </>
      )}

      {/* ── Food — short list ────────────────────────────── */}
      {foodList.length > 0 && (
        <>
          <section className="mb-6">
            <SectionTitle>Pairs with</SectionTitle>
            <div className="flex flex-wrap gap-2">
              {foodList.map((item, i) => (
                <span key={i} className="text-sm px-2.5 py-1 bg-earth-50 border border-earth-150 rounded-lg text-earth-700">
                  {item}
                </span>
              ))}
            </div>
          </section>
          <Divider />
        </>
      )}

      {/* ── Cellar — drink window bar only ────────────────── */}
      {cellarYears && (
        <>
          <section className="mb-6">
            <SectionTitle>Drink window</SectionTitle>
            <DrinkWindowBar start={cellarYears.start} end={cellarYears.end} />
          </section>
          <Divider />
        </>
      )}

      {/* ── Similar wines — names only ────────────────────── */}
      {similarNames.length > 0 && (
        <section className="mb-6">
          <SectionTitle>Similar wines</SectionTitle>
          <div className="flex flex-wrap gap-2">
            {similarNames.map((name, i) => (
              <span key={i} className="text-sm px-2.5 py-1 bg-earth-50 border border-earth-150 rounded-lg text-earth-600">
                {name}
              </span>
            ))}
          </div>
        </section>
      )}

      {/* Empty state */}
      {!hasWineInsight && !hasGeoContext && facts.length === 0 && scores.length === 0 && grapes.length === 0 && (
        <div className="mt-12 text-center">
          <p className="text-sm text-earth-400">We have this wine in our catalog. More details coming soon.</p>
        </div>
      )}
    </div>
  )
}

/* ── Components ──────────────────────────────────────────── */

function SectionTitle({ children }: { children: React.ReactNode }) {
  return <h3 className="font-display text-base font-semibold text-earth-800 mb-2">{children}</h3>
}

function Divider() {
  return <hr className="border-earth-100 mb-6" />
}

function Tag({ children, variant = 'default' }: { children: React.ReactNode; variant?: 'default' | 'accent' | 'muted' }) {
  const styles = {
    default: 'bg-earth-100 text-earth-600',
    accent: 'bg-wine-50 text-wine-700',
    muted: 'bg-stone-100 text-stone-500',
  }
  return (
    <span className={`inline-block text-xs font-medium px-2 py-0.5 rounded-full capitalize ${styles[variant]}`}>
      {children}
    </span>
  )
}

function DrinkWindowBar({ start, end }: { start: number; end: number }) {
  const now = new Date().getFullYear()
  const span = end - start
  const elapsed = Math.max(0, now - start)
  const pct = Math.min(100, Math.max(0, (elapsed / span) * 100))
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
      {remaining > 0 && elapsed > 0 && (
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

function truncate(text: string, maxLen: number): string {
  if (text.length <= maxLen) return text
  // Cut at last sentence boundary before maxLen
  const cut = text.substring(0, maxLen)
  const lastPeriod = cut.lastIndexOf('. ')
  if (lastPeriod > maxLen * 0.5) return cut.substring(0, lastPeriod + 1)
  return cut.trimEnd() + '…'
}

/** Shorten publication names for chip display */
function abbreviatePublication(name: string): string {
  const abbrevs: Record<string, string> = {
    'Wine Advocate': 'WA',
    'Wine Spectator': 'WS',
    'Wine Enthusiast': 'WE',
    'James Suckling': 'JS',
    'Jeb Dunnuck': 'JD',
    'Jancis Robinson': 'JR',
    'Vinous': 'Vinous',
    'Decanter': 'Decanter',
    'Robert Parker': 'RP',
  }
  return abbrevs[name] || name
}

/** Extract drink window year range from cellar text */
function extractDrinkWindow(text: string): { start: number; end: number } | null {
  const m = text.match(/(\d{4})\s*[-–to]+\s*(\d{4})/)
  if (!m) return null
  const start = parseInt(m[1])
  const end = parseInt(m[2])
  const now = new Date().getFullYear()
  if (end > start && start >= now - 5 && end <= now + 50) return { start, end }
  return null
}

/** Parse food text into short dish names */
function parseFoodList(text: string): string[] {
  if (!text) return []
  // Extract dish-like phrases: look for nouns after "with", commas, semicolons
  // Strategy: split on commas/semicolons/periods and take short phrases
  const candidates = text
    .split(/[;.]/)
    .flatMap(s => s.split(','))
    .map(s => s.replace(/^(and|or|also|try|pairs? (well )?(with)?|great with|good with|match(es)? (well )?(with)?)\s+/i, '').trim())
    .filter(s => s.length > 3 && s.length < 50 && s.split(' ').length <= 6)
    .map(s => s.replace(/^\w/, c => c.toUpperCase()))

  // Deduplicate and take top items
  const unique = [...new Set(candidates)]
  return unique.length >= 3 ? unique.slice(0, 8) : []
}

/** Parse similar wine names from prose */
function parseWineNames(text: string): string[] {
  if (!text) return []
  // Look for "Producer WineName" patterns, or text between commas after "look for"
  const names: string[] = []
  // Try to extract names between known patterns
  const afterLookFor = text.match(/look for\s+(.+)/i)?.[1] || text
  const parts = afterLookFor.split(/,\s*(?:or\s+)?|(?:\.\s*)|(?:\s+and\s+)/)
  for (const part of parts) {
    const clean = part
      .replace(/\(.*?\)/g, '') // remove parentheticals
      .replace(/if you like.*$/i, '')
      .replace(/for a .*? parallel.*$/i, '')
      .replace(/which shares.*$/i, '')
      .replace(/similar .*$/i, '')
      .trim()
    if (clean.length > 5 && clean.length < 60 && clean.split(' ').length >= 2 && clean.split(' ').length <= 8) {
      names.push(clean)
    }
  }
  return names.slice(0, 5)
}
