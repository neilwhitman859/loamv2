import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import EntityMap from '../../components/EntityMap'

/* ── Interfaces ──────────────────────────────────────────── */

interface Wine {
  id: string
  name: string
  display_name: string | null
  color: string | null
  wine_type: string | null
  effervescence: string | null
  sweetness_level: string | null
  sparkling_method: string | null
  vinification_notes: string | null
  first_vintage_year: number | null
  soil_description: string | null
  vine_age_description: string | null
  vineyard_area_ha: number | null
  altitude_m_low: number | null
  altitude_m_high: number | null
  aspect: string | null
  slope_pct: number | null
  monopole: boolean | null
  commune: string | null
  data_grade: string | null
  barcode: string | null
  lwin: string | null
  producer: { id: string; name: string; producer_type: string | null; year_established: number | null; hectares_under_vine: number | null; total_production_cases: number | null; website_url: string | null; philosophy: string | null } | null
  country: { id: string; name: string } | null
  region: { id: string; name: string } | null
  appellation: {
    id: string; name: string; designation_type: string | null
    established_year: number | null; area_ha: number | null
    max_yield_hl_ha: number | null; min_alcohol_pct: number | null
    elevation_min_m: number | null; elevation_max_m: number | null
    regulatory_body: string | null; allowed_grapes_description: string | null
  } | null
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
  typical_drinking_window_min_years: number | null
  typical_drinking_window_max_years: number | null
  typical_aging_potential_years: number | null
}


interface AppellationInsight {
  ai_overview: string | null
  ai_soil_profile: string | null
  ai_signature_style: string | null
  ai_key_grapes: string | null
}

interface Price {
  price_usd: number | null
  currency: string | null
  merchant_name: string | null
  vintage_year: number | null
  price_date: string | null
}
interface ExternalId {
  system: string
  external_id: string
}
interface Classification { level_name: string; system_name: string }
interface Score {
  score: number; score_low: number | null; score_high: number | null
  publication_name: string; vintage_year: number; critic: string | null
  tasting_note: string | null; review_date: string | null
  critic_drinking_window_start: number | null; critic_drinking_window_end: number | null
  medal: string | null
}
interface GrapeLink { percentage: number | null; grape: { id: string; display_name: string; color: string | null } }
interface AppellationGrape { display_name: string; grape_id: string; association_type: string }
interface LabelDesignation { canonical_name: string; category: string }
interface FarmingCert { name: string; certification_status: string | null }

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
  ta_g_l: number | null
  rs_g_l: number | null
  va_g_l: number | null
  so2_free_mg_l: number | null
  so2_total_mg_l: number | null
  brix_at_harvest: number | null
  maceration_technique: string | null
  maceration_days: number | null
  fermentation_vessel: string | null
  oak_origin: string | null
  yeast_type: string | null
  fining: string | null
  filtration: string | null
  closure: string | null
  lees_aging_months: number | null
  batonnage: boolean | null
  skin_contact_days: number | null
  aging_vessel: string | null
  aging_vessel_size_l: number | null
  yield_hl_ha: number | null
  bottling_date: string | null
  release_date: string | null
  disgorgement_date: string | null
  age_statement_years: number | null
  bottle_format_ml: number | null
  release_price_usd: number | null
  neutral_oak_pct: number | null
  carbonic_maceration: boolean | null
  mlf: boolean | null
  pradikat: string | null
  ingredients: string | null
  allergens: string[] | null
  energy_kcal_per_100ml: number | null
  maturity_status: string | null
  bottle_aging_months: number | null
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
  const [appInsight, setAppInsight] = useState<AppellationInsight | null>(null)
  const [classifications, setClassifications] = useState<Classification[]>([])
  const [scores, setScores] = useState<Score[]>([])
  const [grapes, setGrapes] = useState<GrapeLink[]>([])
  const [appGrapes, setAppGrapes] = useState<AppellationGrape[]>([])
  const [labelDesignations, setLabelDesignations] = useState<LabelDesignation[]>([])
  const [farmingCerts, setFarmingCerts] = useState<FarmingCert[]>([])
  const [prices, setPrices] = useState<Price[]>([])
  const [externalIds, setExternalIds] = useState<ExternalId[]>([])
  const [vintages, setVintages] = useState<Vintage[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    supabase
      .from('wines')
      .select(`
        id, name, display_name, color, wine_type, effervescence, sweetness_level, sparkling_method,
        vinification_notes, first_vintage_year,
        soil_description, vine_age_description, vineyard_area_ha,
        altitude_m_low, altitude_m_high, aspect, slope_pct, monopole, commune,
        data_grade, barcode, lwin,
        producer:producers!wines_producer_id_fkey(id, name, producer_type, year_established, hectares_under_vine, total_production_cases, website_url, philosophy),
        country:countries!wines_country_id_fkey(id, name),
        region:regions!wines_region_id_fkey(id, name),
        appellation:appellations!wines_appellation_id_fkey(id, name, designation_type, established_year, area_ha, max_yield_hl_ha, min_alcohol_pct, elevation_min_m, elevation_max_m, regulatory_body, allowed_grapes_description)
      `)
      .eq('id', id)
      .single()
      .then(({ data }) => {
        if (data) {
          setWine(data as unknown as Wine)
          const p: PromiseLike<void>[] = []

          p.push(supabase.from('wine_insights')
            .select('ai_hook, ai_wine_summary, ai_terroir_expression, ai_vinification_summary, ai_food_pairing, ai_style_profile, ai_cellar_recommendation, ai_comparable_wines, enrichment_tier, typical_drinking_window_min_years, typical_drinking_window_max_years, typical_aging_potential_years')
            .eq('wine_id', id).maybeSingle()
            .then(({ data: d }) => { if (d) setInsight(d) }))

          if ((data.appellation as any)?.id) {
            p.push(supabase.from('appellation_insights')
              .select('ai_overview, ai_soil_profile, ai_signature_style, ai_key_grapes')
              .eq('appellation_id', (data.appellation as any).id).maybeSingle()
              .then(({ data: d }) => { if (d) setAppInsight(d) }))

            p.push(supabase.from('appellation_grapes')
              .select('association_type, grape_id, grape:grapes!appellation_grapes_grape_id_fkey(display_name)')
              .eq('appellation_id', (data.appellation as any).id)
              .then(({ data: d }) => {
                if (d) setAppGrapes(d.map((r: any) => ({
                  display_name: r.grape?.display_name || '',
                  grape_id: r.grape_id,
                  association_type: r.association_type,
                })))
              }))
          }

          p.push(supabase.from('wine_vintage_scores')
            .select('score, score_low, score_high, vintage_year, critic, tasting_note, review_date, critic_drinking_window_start, critic_drinking_window_end, medal, publication:publications!wine_vintage_scores_publication_id_fkey(name)')
            .eq('wine_id', id).order('score', { ascending: false }).limit(20)
            .then(({ data: d }) => {
              if (d) setScores(d.map((r: any) => ({
                score: r.score, score_low: r.score_low, score_high: r.score_high,
                publication_name: r.publication?.name || 'Unknown',
                vintage_year: r.vintage_year, critic: r.critic,
                tasting_note: r.tasting_note, review_date: r.review_date,
                critic_drinking_window_start: r.critic_drinking_window_start,
                critic_drinking_window_end: r.critic_drinking_window_end,
                medal: r.medal,
              })))
            }))

          p.push(supabase.from('wine_grapes')
            .select('percentage, grape:grapes!wine_grapes_grape_id_fkey(id, display_name, color)')
            .eq('wine_id', id).order('percentage', { ascending: false, nullsFirst: false })
            .then(({ data: d }) => { if (d) setGrapes(d as unknown as GrapeLink[]) }))

          p.push(supabase.from('wine_vintage_prices')
            .select('price_usd, currency, merchant_name, vintage_year, price_date')
            .eq('wine_id', id).order('price_usd', { ascending: true }).limit(10)
            .then(({ data: d }) => { if (d) setPrices(d as Price[]) }))

          p.push(supabase.from('external_ids')
            .select('system, external_id')
            .eq('entity_type', 'wine').eq('entity_id', id)
            .then(({ data: d }) => { if (d) setExternalIds(d as ExternalId[]) }))

          p.push(supabase.from('wine_vintages')
            .select('vintage_year, abv, cases_produced, duration_in_oak_months, new_oak_pct, neutral_oak_pct, whole_cluster_pct, harvest_start_date, harvest_end_date, winemaker_notes, vintage_notes, ph, ta_g_l, rs_g_l, va_g_l, so2_free_mg_l, so2_total_mg_l, brix_at_harvest, maceration_technique, maceration_days, fermentation_vessel, oak_origin, yeast_type, fining, filtration, closure, lees_aging_months, batonnage, skin_contact_days, aging_vessel, aging_vessel_size_l, yield_hl_ha, bottling_date, release_date, disgorgement_date, age_statement_years, bottle_format_ml, bottle_aging_months, release_price_usd, carbonic_maceration, mlf, pradikat, ingredients, allergens, energy_kcal_per_100ml, maturity_status')
            .eq('wine_id', id).order('vintage_year', { ascending: false }).limit(20)
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

          // Label designations
          p.push(supabase.from('wine_label_designations')
            .select('label_designation:label_designations!wine_label_designations_label_designation_id_fkey(canonical_name, category)')
            .eq('wine_id', id)
            .then(({ data: d }) => {
              if (d) setLabelDesignations(d.map((r: any) => ({
                canonical_name: r.label_designation?.canonical_name || '',
                category: r.label_designation?.category || '',
              })))
            }))

          // Farming certifications (from producer)
          if ((data.producer as any)?.id) {
            p.push(supabase.from('producer_farming_certifications')
              .select('certification_status, farming_certification:farming_certifications!producer_farming_certifications_farming_certification_id_fkey(name)')
              .eq('producer_id', (data.producer as any).id)
              .then(({ data: d }) => {
                if (d) setFarmingCerts(d.map((r: any) => ({
                  name: r.farming_certification?.name || '',
                  certification_status: r.certification_status,
                })))
              }))
          }

          Promise.all(p).then(() => setLoading(false))
        } else {
          setLoading(false)
        }
      })
  }, [id])

  /* ── Track page view ─────────────────────────────────────── */
  useEffect(() => {
    if (!wine?.id) return
    supabase.from('wine_lookups')
      .insert({ wine_id: wine.id, source: 'web' })
      .then(() => {}) // fire-and-forget
  }, [wine?.id])

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

  const v = vintages[0] || null
  const hasAnyContent = insight || appInsight || scores.length > 0 || grapes.length > 0 || v

  // Maps
  const maps: { type: 'country' | 'region' | 'appellation'; id: string; label: string }[] = []
  if (wine.region) maps.push({ type: 'region', id: wine.region.id, label: wine.region.name })
  if (wine.appellation) maps.push({ type: 'appellation', id: wine.appellation.id, label: wine.appellation.name })
  if (maps.length === 0 && wine.country) maps.push({ type: 'country', id: wine.country.id, label: wine.country.name })

  /* ── Render ────────────────────────────────────────────── */

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">

      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-3 flex items-center gap-1.5 flex-wrap">
        {wine.country && <><Link to={`/country/${wine.country.id}`} className="hover:text-earth-600">{wine.country.name}</Link><span>/</span></>}
        {wine.region && <><Link to={`/region/${wine.region.id}`} className="hover:text-earth-600">{wine.region.name}</Link><span>/</span></>}
        {wine.appellation && <><Link to={`/appellation/${wine.appellation.id}`} className="hover:text-earth-600">{wine.appellation.name}</Link><span>/</span></>}
        <span className="text-earth-500">{wine.display_name || wine.name}</span>
      </nav>

      {/* ── Header ─────────────────────────────────────── */}
      <header className="mb-4">
        <div className="flex items-start gap-3">
          {wine.color && <div className={`w-4 h-4 rounded-full mt-2 shrink-0 ${COLOR_DOT[wine.color] || 'bg-earth-300'}`} aria-label={`${wine.color} wine`} />}
          <div>
            <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900 leading-tight">{wine.display_name || wine.name}</h1>
            {wine.producer && (
              <Link to={`/producer/${wine.producer.id}`} className="text-base text-wine-600 hover:text-wine-700 font-medium mt-0.5 inline-block">
                {wine.producer.name}
              </Link>
            )}
          </div>
        </div>

        {/* Tags */}
        <div className="flex flex-wrap gap-1.5 mt-2">
          {wine.color && <Tag>{wine.color}</Tag>}
          {wine.wine_type && wine.wine_type !== 'table' && <Tag>{wine.wine_type}</Tag>}
          {wine.effervescence && wine.effervescence !== 'still' && <Tag>{wine.effervescence}</Tag>}
          {wine.sweetness_level && <Tag>{wine.sweetness_level}</Tag>}
          {wine.sparkling_method && <Tag>{wine.sparkling_method}</Tag>}
          {classifications.map((c, i) => <Tag key={i} variant="accent">{c.system_name ? `${c.system_name}: ${c.level_name}` : c.level_name}</Tag>)}
          {labelDesignations.map((ld, i) => <Tag key={`ld-${i}`} variant="muted">{ld.canonical_name}</Tag>)}
          {wine.monopole && <Tag variant="accent">Monopole</Tag>}
          {wine.data_grade && <Tag variant="muted">Grade {wine.data_grade.toUpperCase()}</Tag>}
        </div>

        {/* One-liner hook */}
        {insight?.ai_hook && (
          <p className="text-sm text-earth-500 mt-2 italic">{insight.ai_hook} <AiLabel /></p>
        )}
      </header>

      {/* ── Scores ─────────────────────────────────────── */}
      {scores.length > 0 && (
        <Section title="Scores">
          <div className="space-y-1">
            {scores.map((s, i) => (
              <div key={i} className="flex items-baseline gap-3 py-1 border-b border-earth-50 last:border-0">
                <span className="text-lg font-display font-bold text-wine-700 w-10 shrink-0">{s.score}</span>
                {s.score_low && s.score_high && s.score_low !== s.score_high && (
                  <span className="text-xs text-earth-400">({s.score_low}–{s.score_high})</span>
                )}
                <span className="text-xs font-medium text-earth-600 uppercase tracking-wider">{abbrevPub(s.publication_name)}</span>
                {s.critic && <span className="text-xs text-earth-400">{s.critic}</span>}
                {s.vintage_year > 0 && <span className="text-xs text-earth-400">{s.vintage_year}</span>}
                {s.medal && <span className="text-xs text-amber-600 font-medium">{s.medal}</span>}
                {s.critic_drinking_window_start && s.critic_drinking_window_end && (
                  <span className="text-xs text-earth-400 ml-auto">{s.critic_drinking_window_start}–{s.critic_drinking_window_end}</span>
                )}
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* ── Prices ─────────────────────────────────────── */}
      {prices.length > 0 && (
        <Section title="Prices">
          <div className="space-y-1">
            {prices.map((p, i) => (
              <div key={i} className="flex items-baseline gap-3 py-1 border-b border-earth-50 last:border-0">
                <span className="text-lg font-display font-bold text-emerald-700 w-20 shrink-0">
                  {p.price_usd ? `$${Number(p.price_usd).toFixed(0)}` : '–'}
                </span>
                {p.currency && p.currency !== 'USD' && (
                  <span className="text-xs text-earth-400">{p.currency}</span>
                )}
                <span className="text-xs text-earth-600 truncate">{p.merchant_name || 'Retailer'}</span>
                {p.vintage_year && p.vintage_year > 0 && (
                  <span className="text-xs text-earth-400 ml-auto">{p.vintage_year}</span>
                )}
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* ── Grapes ─────────────────────────────────────── */}
      {grapes.length > 0 && (
        <Section title="Grapes">
          <div className="flex flex-wrap gap-2">
            {grapes.map((g, i) => (
              <Link key={i} to={`/grape/${g.grape.id}`}
                className="text-sm px-3 py-1.5 bg-earth-100 rounded-full text-earth-700 hover:bg-earth-200 transition-colors">
                {g.grape.display_name}
                {g.percentage != null && g.percentage < 100 && <span className="text-earth-400 ml-1">{g.percentage}%</span>}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* ── Vintage Data ───────────────────────────────── */}
      {v && (
        <Section title={v.vintage_year === 0 ? 'NV' : `${v.vintage_year} Vintage`}>
          {/* Chemistry */}
          <FactGrid>
            {v.abv && <Fact label="ABV" value={`${v.abv}%`} />}
            {v.ph && <Fact label="pH" value={v.ph.toString()} />}
            {v.ta_g_l && <Fact label="Total acidity" value={`${v.ta_g_l} g/L`} />}
            {v.rs_g_l != null && <Fact label="Residual sugar" value={`${v.rs_g_l} g/L`} />}
            {v.va_g_l && <Fact label="Volatile acidity" value={`${v.va_g_l} g/L`} />}
            {v.so2_free_mg_l && <Fact label="Free SO₂" value={`${v.so2_free_mg_l} mg/L`} />}
            {v.so2_total_mg_l && <Fact label="Total SO₂" value={`${v.so2_total_mg_l} mg/L`} />}
            {v.brix_at_harvest && <Fact label="Brix at harvest" value={`${v.brix_at_harvest}°`} />}
            {v.energy_kcal_per_100ml && <Fact label="Energy" value={`${v.energy_kcal_per_100ml} kcal/100ml`} />}
          </FactGrid>

          {/* Production */}
          <FactGrid>
            {v.cases_produced && <Fact label="Production" value={`${v.cases_produced.toLocaleString()} cases`} />}
            {v.yield_hl_ha && <Fact label="Yield" value={`${v.yield_hl_ha} hl/ha`} />}
            {v.bottle_format_ml && v.bottle_format_ml !== 750 && <Fact label="Format" value={`${v.bottle_format_ml}ml`} />}
            {v.release_price_usd && <Fact label="Release price" value={`$${v.release_price_usd}`} />}
            {v.age_statement_years && <Fact label="Age statement" value={`${v.age_statement_years} years`} />}
            {v.pradikat && <Fact label="Prädikat" value={v.pradikat} />}
            {v.maturity_status && <Fact label="Maturity" value={v.maturity_status} />}
          </FactGrid>

          {/* Winemaking */}
          <FactGrid>
            {v.harvest_start_date && (
              <Fact label="Harvest" value={
                v.harvest_end_date
                  ? `${fmtDate(v.harvest_start_date)} – ${fmtDate(v.harvest_end_date)}`
                  : fmtDate(v.harvest_start_date)
              } />
            )}
            {v.fermentation_vessel && <Fact label="Fermentation" value={v.fermentation_vessel} />}
            {v.yeast_type && <Fact label="Yeast" value={v.yeast_type} />}
            {v.maceration_technique && <Fact label="Maceration" value={v.maceration_technique} />}
            {v.maceration_days && <Fact label="Maceration" value={`${v.maceration_days} days`} />}
            {v.skin_contact_days && <Fact label="Skin contact" value={`${v.skin_contact_days} days`} />}
            {v.whole_cluster_pct != null && v.whole_cluster_pct > 0 && <Fact label="Whole cluster" value={`${v.whole_cluster_pct}%`} />}
            {v.carbonic_maceration && <Fact label="Carbonic" value="Yes" />}
            {v.mlf != null && <Fact label="MLF" value={v.mlf ? 'Yes' : 'No'} />}
          </FactGrid>

          {/* Aging */}
          <FactGrid>
            {v.duration_in_oak_months && <Fact label="Oak aging" value={`${v.duration_in_oak_months} months`} />}
            {v.new_oak_pct != null && <Fact label="New oak" value={`${v.new_oak_pct}%`} />}
            {v.neutral_oak_pct != null && <Fact label="Neutral oak" value={`${v.neutral_oak_pct}%`} />}
            {v.oak_origin && <Fact label="Oak origin" value={v.oak_origin} />}
            {v.aging_vessel && <Fact label="Aging vessel" value={v.aging_vessel} />}
            {v.aging_vessel_size_l && <Fact label="Vessel size" value={`${v.aging_vessel_size_l}L`} />}
            {v.lees_aging_months && <Fact label="Lees aging" value={`${v.lees_aging_months} months`} />}
            {v.batonnage && <Fact label="Bâtonnage" value="Yes" />}
            {v.bottle_aging_months && <Fact label="Bottle aging" value={`${v.bottle_aging_months} months`} />}
          </FactGrid>

          {/* Finishing */}
          <FactGrid>
            {v.fining && <Fact label="Fining" value={v.fining} />}
            {v.filtration && <Fact label="Filtration" value={v.filtration} />}
            {v.closure && <Fact label="Closure" value={v.closure} />}
            {v.bottling_date && <Fact label="Bottled" value={fmtDate(v.bottling_date)} />}
            {v.disgorgement_date && <Fact label="Disgorged" value={fmtDate(v.disgorgement_date)} />}
            {v.release_date && <Fact label="Released" value={fmtDate(v.release_date)} />}
          </FactGrid>

          {/* EU e-label */}
          {(v.ingredients || (v.allergens && v.allergens.length > 0)) && (
            <FactGrid>
              {v.ingredients && <Fact label="Ingredients" value={v.ingredients} />}
              {v.allergens && v.allergens.length > 0 && <Fact label="Allergens" value={v.allergens.join(', ')} />}
            </FactGrid>
          )}

          {/* Notes (the small amount of prose) */}
          {v.vintage_notes && (
            <p className="text-sm text-earth-600 leading-relaxed mt-2 italic">{v.vintage_notes}</p>
          )}
          {v.winemaker_notes && (
            <p className="text-sm text-earth-500 leading-relaxed mt-1">{v.winemaker_notes}</p>
          )}
        </Section>
      )}

      {/* ── Vinification (wine-level default) ──────────── */}
      {wine.vinification_notes && !v && (
        <Section title="Vinification">
          <p className="text-sm text-earth-600 leading-relaxed">{wine.vinification_notes}</p>
        </Section>
      )}

      {/* ── Place ──────────────────────────────────────── */}
      <Section title="Place">
        <FactGrid>
          {wine.country && <Fact label="Country" value={wine.country.name} link={`/country/${wine.country.id}`} />}
          {wine.region && <Fact label="Region" value={wine.region.name} link={`/region/${wine.region.id}`} />}
          {wine.appellation && <Fact label="Appellation" value={wine.appellation.name} link={`/appellation/${wine.appellation.id}`} />}
          {wine.appellation?.designation_type && <Fact label="Designation" value={wine.appellation.designation_type} />}
          {wine.commune && <Fact label="Commune" value={wine.commune} />}
          {wine.altitude_m_low && <Fact label="Elevation" value={wine.altitude_m_high ? `${wine.altitude_m_low}–${wine.altitude_m_high}m` : `${wine.altitude_m_low}m`} />}
          {wine.aspect && <Fact label="Aspect" value={wine.aspect} />}
          {wine.slope_pct && <Fact label="Slope" value={`${wine.slope_pct}%`} />}
          {wine.vineyard_area_ha && <Fact label="Vineyard area" value={`${wine.vineyard_area_ha} ha`} />}
          {wine.vine_age_description && <Fact label="Vine age" value={wine.vine_age_description} />}
          {wine.first_vintage_year && <Fact label="First vintage" value={wine.first_vintage_year.toString()} />}
        </FactGrid>

        {/* Soil */}
        {(wine.soil_description || appInsight?.ai_soil_profile) && (
          <div className="mt-3">
            <MiniLabel>Soil</MiniLabel>
            <p className="text-sm text-earth-700">{wine.soil_description || appInsight?.ai_soil_profile}</p>
          </div>
        )}

        {/* Maps */}
        {maps.length > 0 && (
          <div className={`mt-3 ${maps.length > 1 ? 'grid grid-cols-1 sm:grid-cols-2 gap-3' : ''}`}>
            {maps.map((m) => <EntityMap key={m.type} entityType={m.type} entityId={m.id} label={m.label} />)}
          </div>
        )}
      </Section>

      {/* ── Appellation Details ─────────────────────────── */}
      {wine.appellation && (wine.appellation.established_year || wine.appellation.area_ha || wine.appellation.max_yield_hl_ha || wine.appellation.min_alcohol_pct || wine.appellation.regulatory_body || appGrapes.length > 0) && (
        <Section title={`${wine.appellation.name} (Appellation)`}>
          <FactGrid>
            {wine.appellation.designation_type && <Fact label="Type" value={wine.appellation.designation_type} />}
            {wine.appellation.established_year && <Fact label="Established" value={wine.appellation.established_year.toString()} />}
            {wine.appellation.area_ha && <Fact label="Area" value={`${wine.appellation.area_ha.toLocaleString()} ha`} />}
            {wine.appellation.max_yield_hl_ha && <Fact label="Max yield" value={`${wine.appellation.max_yield_hl_ha} hl/ha`} />}
            {wine.appellation.min_alcohol_pct && <Fact label="Min ABV" value={`${wine.appellation.min_alcohol_pct}%`} />}
            {wine.appellation.elevation_min_m != null && wine.appellation.elevation_max_m != null && (
              <Fact label="Elevation range" value={`${wine.appellation.elevation_min_m}–${wine.appellation.elevation_max_m}m`} />
            )}
            {wine.appellation.regulatory_body && <Fact label="Regulatory body" value={wine.appellation.regulatory_body} />}
          </FactGrid>

          {/* Appellation grapes */}
          {appGrapes.length > 0 && (
            <div className="mt-3">
              <MiniLabel>Permitted / typical varieties</MiniLabel>
              <div className="flex flex-wrap gap-1.5">
                {appGrapes.map((g, i) => (
                  <Link key={i} to={`/grape/${g.grape_id}`}
                    className="text-xs px-2 py-0.5 bg-earth-50 border border-earth-150 rounded text-earth-600 hover:bg-earth-100 transition-colors">
                    {g.display_name}
                    {g.association_type === 'required' && <span className="text-wine-500 ml-1">req</span>}
                  </Link>
                ))}
              </div>
            </div>
          )}

          {wine.appellation.allowed_grapes_description && (
            <div className="mt-2">
              <MiniLabel>Grape regulations</MiniLabel>
              <p className="text-xs text-earth-500">{wine.appellation.allowed_grapes_description}</p>
            </div>
          )}

          {/* Appellation signature style — one short line from AI */}
          {appInsight?.ai_signature_style && (
            <div className="mt-2">
              <MiniLabel>Signature style</MiniLabel>
              <p className="text-sm text-earth-600">{appInsight.ai_signature_style}</p>
            </div>
          )}
        </Section>
      )}

      {/* ── Producer ───────────────────────────────────── */}
      {wine.producer && (wine.producer.producer_type || wine.producer.year_established || wine.producer.hectares_under_vine || wine.producer.total_production_cases || farmingCerts.length > 0) && (
        <Section title={wine.producer.name}>
          <FactGrid>
            {wine.producer.producer_type && <Fact label="Type" value={wine.producer.producer_type} />}
            {wine.producer.year_established && <Fact label="Established" value={wine.producer.year_established.toString()} />}
            {wine.producer.hectares_under_vine && <Fact label="Hectares" value={wine.producer.hectares_under_vine.toString()} />}
            {wine.producer.total_production_cases && <Fact label="Production" value={`${wine.producer.total_production_cases.toLocaleString()} cases`} />}
            {wine.producer.website_url && (
              <div className="min-w-0">
                <div className="text-[10px] uppercase tracking-wider text-earth-400 leading-tight">Website</div>
                <a href={wine.producer.website_url.startsWith('http') ? wine.producer.website_url : `https://${wine.producer.website_url}`}
                  target="_blank" rel="noopener noreferrer"
                  className="text-sm font-medium text-wine-600 hover:text-wine-700 transition-colors truncate block">
                  {wine.producer.website_url.replace(/^https?:\/\/(www\.)?/, '').replace(/\/$/, '')}
                </a>
              </div>
            )}
          </FactGrid>

          {/* Farming certifications */}
          {farmingCerts.length > 0 && (
            <div className="mt-2">
              <MiniLabel>Certifications</MiniLabel>
              <div className="flex flex-wrap gap-1.5">
                {farmingCerts.map((c, i) => (
                  <span key={i} className="text-xs px-2 py-0.5 bg-green-50 border border-green-200 rounded text-green-700">
                    {c.name}
                    {c.certification_status && c.certification_status !== 'certified' && (
                      <span className="text-green-500 ml-1">({c.certification_status})</span>
                    )}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Philosophy — short text ok here */}
          {wine.producer.philosophy && (
            <div className="mt-2">
              <MiniLabel>Philosophy</MiniLabel>
              <p className="text-sm text-earth-600">{wine.producer.philosophy}</p>
            </div>
          )}
        </Section>
      )}

      {/* ── Cellar ─────────────────────────────────────── */}
      {(insight?.typical_drinking_window_min_years || insight?.typical_aging_potential_years || insight?.ai_cellar_recommendation) && (
        <Section title="Cellar">
          <FactGrid>
            {insight?.typical_drinking_window_min_years && insight?.typical_drinking_window_max_years && (
              <Fact label="Drinking window" value={`${insight.typical_drinking_window_min_years}–${insight.typical_drinking_window_max_years} years from vintage`} />
            )}
            {insight?.typical_aging_potential_years && (
              <Fact label="Aging potential" value={`${insight.typical_aging_potential_years} years`} />
            )}
          </FactGrid>
          {insight?.ai_cellar_recommendation && (
            <p className="text-sm text-earth-600 mt-2">{insight.ai_cellar_recommendation} <AiLabel /></p>
          )}
        </Section>
      )}

      {/* ── Food ───────────────────────────────────────── */}
      {insight?.ai_food_pairing && (
        <Section title="Food pairing">
          <p className="text-sm text-earth-600">{insight.ai_food_pairing} <AiLabel /></p>
        </Section>
      )}

      {/* ── Other Vintages (compact table) ─────────────── */}
      {vintages.length > 1 && (
        <Section title="Other vintages">
          <div className="overflow-x-auto -mx-1">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-[10px] uppercase tracking-wider text-earth-400 border-b border-earth-100">
                  <th className="text-left py-1 px-1 font-medium">Year</th>
                  <th className="text-left py-1 px-1 font-medium">ABV</th>
                  <th className="text-left py-1 px-1 font-medium">Cases</th>
                  <th className="text-left py-1 px-1 font-medium">Oak</th>
                  <th className="text-left py-1 px-1 font-medium">New</th>
                  <th className="text-left py-1 px-1 font-medium">pH</th>
                </tr>
              </thead>
              <tbody>
                {vintages.slice(1).map((ov, i) => (
                  <tr key={i} className="border-b border-earth-50 last:border-0 text-earth-600">
                    <td className="py-1 px-1 font-semibold text-earth-700">{ov.vintage_year === 0 ? 'NV' : ov.vintage_year}</td>
                    <td className="py-1 px-1">{ov.abv ? `${ov.abv}%` : '–'}</td>
                    <td className="py-1 px-1">{ov.cases_produced ? ov.cases_produced.toLocaleString() : '–'}</td>
                    <td className="py-1 px-1">{ov.duration_in_oak_months ? `${ov.duration_in_oak_months}mo` : '–'}</td>
                    <td className="py-1 px-1">{ov.new_oak_pct != null ? `${ov.new_oak_pct}%` : '–'}</td>
                    <td className="py-1 px-1">{ov.ph || '–'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Section>
      )}

      {/* ── Identifiers ────────────────────────────────── */}
      {(wine.lwin || wine.barcode || externalIds.length > 0) && (
        <Section title="Identifiers">
          <FactGrid>
            {wine.lwin && <Fact label="LWIN" value={wine.lwin} />}
            {wine.barcode && <Fact label="Barcode" value={wine.barcode} />}
            {externalIds.filter(e => e.system === 'upc').map((e, i) => (
              <Fact key={`upc-${i}`} label="UPC" value={e.external_id} />
            ))}
            {externalIds.filter(e => e.system === 'cola').length > 0 && (
              <Fact label="COLA IDs" value={`${externalIds.filter(e => e.system === 'cola').length} registered`} />
            )}
          </FactGrid>
        </Section>
      )}

      {/* ── Similar wines (brief) ──────────────────────── */}
      {insight?.ai_comparable_wines && (
        <Section title="Similar wines">
          <p className="text-sm text-earth-600">{insight.ai_comparable_wines} <AiLabel /></p>
        </Section>
      )}

      {/* ── Fallback for Grade F wines ─────────────────── */}
      {!hasAnyContent && (
        <div className="mt-12 text-center">
          <p className="text-sm text-earth-400">We have this wine in our catalog. More details coming soon.</p>
        </div>
      )}
    </div>
  )
}

/* ── Components ──────────────────────────────────────────── */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <>
      <section className="mb-5">
        <h2 className="font-display text-base font-semibold text-earth-800 mb-2 pb-1 border-b border-earth-100">{title}</h2>
        {children}
      </section>
    </>
  )
}

function Tag({ children, variant = 'default' }: { children: React.ReactNode; variant?: 'default' | 'accent' | 'muted' }) {
  const s = { default: 'bg-earth-100 text-earth-600', accent: 'bg-wine-50 text-wine-700', muted: 'bg-stone-100 text-stone-500' }
  return <span className={`inline-block text-xs font-medium px-2 py-0.5 rounded-full capitalize ${s[variant]}`}>{children}</span>
}

function FactGrid({ children }: { children: React.ReactNode }) {
  // Filter out null/false children so we don't render empty grids
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

function AiLabel() {
  return <span className="inline-block text-[9px] uppercase tracking-wider text-earth-400 bg-earth-100 px-1.5 py-0.5 rounded ml-1 align-middle" title="Generated by AI and may contain errors">AI</span>
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
