import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import TabBar from '../../components/TabBar'
import InsightsPanel from '../../components/InsightsPanel'
import EntityLink from '../../components/EntityLink'
import DataTable, { type Column } from '../../components/DataTable'

const insightFields = [
  { key: 'ai_overview', label: 'Overview' },
  { key: 'ai_style_profile', label: 'Style Profile' },
  { key: 'ai_terroir_expression', label: 'Terroir Expression' },
  { key: 'ai_food_pairing', label: 'Food Pairing' },
  { key: 'ai_cellar_recommendation', label: 'Cellar Recommendation' },
  { key: 'ai_comparable_wines', label: 'Comparable Wines' },
]

export default function WineDetail() {
  const { id } = useParams<{ id: string }>()
  const [tab, setTab] = useState('overview')
  const [wine, setWine] = useState<Record<string, unknown> | null>(null)
  const [insights, setInsights] = useState<Record<string, unknown> | null>(null)
  const [grapes, setGrapes] = useState<Record<string, unknown>[]>([])
  const [scores, setScores] = useState<Record<string, unknown>[]>([])
  const [prices, setPrices] = useState<Record<string, unknown>[]>([])
  const [scorePage, setScorePage] = useState(0)
  const [pricePage, setPricePage] = useState(0)
  const [scoreCount, setScoreCount] = useState(0)
  const [priceCount, setPriceCount] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    Promise.all([
      supabase.from('wines').select('*, producer:producers!producer_id(id, name), appellation:appellations!appellation_id(id, name), country:countries!country_id(id, name), region:regions!region_id(id, name), varietal_category:varietal_categories!varietal_category_id(name, color)').eq('id', id).single(),
      supabase.from('wine_insights').select('*').eq('wine_id', id).maybeSingle(),
      supabase.from('wine_grapes').select('grape_id, percentage, grape:grapes!grape_id(id, name, color)').eq('wine_id', id),
    ]).then(([wineRes, insightsRes, grapesRes]) => {
      if (wineRes.data) setWine(wineRes.data as Record<string, unknown>)
      if (insightsRes.data) setInsights(insightsRes.data as Record<string, unknown>)
      if (grapesRes.data) setGrapes(grapesRes.data as Record<string, unknown>[])
      setLoading(false)
    })
  }, [id])

  useEffect(() => {
    if (!id) return
    const from = scorePage * 20
    supabase.from('wine_vintage_scores')
      .select('*, publication:publications!publication_id(name)', { count: 'exact' })
      .eq('wine_id', id)
      .order('vintage_year', { ascending: false })
      .range(from, from + 19)
      .then(({ data, count }) => {
        if (data) setScores(data as Record<string, unknown>[])
        setScoreCount(count ?? 0)
      })
  }, [id, scorePage])

  useEffect(() => {
    if (!id) return
    const from = pricePage * 20
    supabase.from('wine_vintage_prices')
      .select('*', { count: 'exact' })
      .eq('wine_id', id)
      .order('vintage_year', { ascending: false })
      .range(from, from + 19)
      .then(({ data, count }) => {
        if (data) setPrices(data as Record<string, unknown>[])
        setPriceCount(count ?? 0)
      })
  }, [id, pricePage])

  if (loading) return <div className="py-12 text-center text-earth-400">Loading...</div>
  if (!wine) return <div className="py-12 text-center text-earth-400">Wine not found</div>

  const producer = wine.producer as Record<string, unknown> | null
  const appellation = wine.appellation as Record<string, unknown> | null
  const country = wine.country as Record<string, unknown> | null
  const region = wine.region as Record<string, unknown> | null
  const varietal = wine.varietal_category as Record<string, unknown> | null

  const tabs = [
    { key: 'overview', label: 'Overview' },
    { key: 'grapes', label: 'Grapes', count: grapes.length },
    { key: 'scores', label: 'Scores & Prices', count: scoreCount + priceCount },
  ]

  const grapeColumns: Column<Record<string, unknown>>[] = [
    { key: 'name', label: 'Grape', render: (row) => {
      const g = row.grape as Record<string, unknown> | null
      return g ? <EntityLink type="grapes" id={g.id as string} name={g.name as string} /> : <span>—</span>
    }},
    { key: 'color', label: 'Color', render: (row) => {
      const g = row.grape as Record<string, unknown> | null
      const color = g?.color as string
      return color ? <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${color === 'red' ? 'bg-red-100 text-red-800' : 'bg-yellow-100 text-yellow-800'}`}>{color}</span> : <span>—</span>
    }, className: 'w-20' },
    { key: 'percentage', label: '%', render: (row) => {
      const pct = row.percentage as number | null
      return pct != null ? <span className="text-sm">{pct}%</span> : <span className="text-earth-300">—</span>
    }, className: 'w-16' },
  ]

  const scoreColumns: Column<Record<string, unknown>>[] = [
    { key: 'vintage_year', label: 'Vintage', render: (row) => <span className="font-medium">{(row.vintage_year as number) ?? 'NV'}</span>, className: 'w-20' },
    { key: 'publication', label: 'Publication', render: (row) => {
      const p = row.publication as Record<string, unknown> | null
      return <span className="text-sm">{(p?.name as string) ?? '—'}</span>
    }},
    { key: 'score_normalized', label: 'Score', render: (row) => {
      const s = row.score_normalized as number | null
      return s != null ? <span className="font-semibold text-wine-700">{s}</span> : <span className="text-earth-300">—</span>
    }, className: 'w-16' },
    { key: 'tasting_note', label: 'Tasting Note', render: (row) => <span className="text-xs text-earth-600 line-clamp-2">{(row.tasting_note as string) ?? '—'}</span> },
  ]

  const priceColumns: Column<Record<string, unknown>>[] = [
    { key: 'vintage_year', label: 'Vintage', render: (row) => <span className="font-medium">{(row.vintage_year as number) ?? 'NV'}</span>, className: 'w-20' },
    { key: 'price_usd', label: 'Price (USD)', render: (row) => {
      const p = row.price_usd as number | null
      return p != null ? <span className="font-medium">${p.toFixed(2)}</span> : <span className="text-earth-300">—</span>
    }, className: 'w-28' },
    { key: 'merchant_name', label: 'Merchant', render: (row) => <span className="text-sm">{(row.merchant_name as string) ?? '—'}</span> },
    { key: 'bottle_size_ml', label: 'Size', render: (row) => {
      const s = row.bottle_size_ml as number | null
      return s ? <span className="text-xs text-earth-500">{s}ml</span> : <span className="text-earth-300">—</span>
    }, className: 'w-16' },
  ]

  return (
    <div>
      <Link to="/data/wines" className="text-sm text-wine-600 hover:text-wine-800 mb-4 inline-block">← Back to wines</Link>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">{wine.name as string}</h1>
      <div className="flex flex-wrap gap-3 text-sm text-earth-600 mb-6">
        {producer && <EntityLink type="producers" id={producer.id as string} name={producer.name as string} />}
        {varietal && <span className={`px-2 py-0.5 rounded text-xs font-medium ${(varietal.color as string) === 'red' ? 'bg-red-100 text-red-800' : 'bg-yellow-100 text-yellow-800'}`}>{varietal.name as string}</span>}
        {country && <EntityLink type="countries" id={country.id as string} name={country.name as string} className="text-earth-500 hover:text-wine-600 text-xs" />}
      </div>

      <TabBar tabs={tabs} active={tab} onChange={setTab} />

      {tab === 'overview' && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {appellation && <Field label="Appellation" value={<EntityLink type="appellations" id={appellation.id as string} name={appellation.name as string} />} />}
            {region && <Field label="Region" value={<EntityLink type="regions" id={region.id as string} name={region.name as string} />} />}
            {country && <Field label="Country" value={<EntityLink type="countries" id={country.id as string} name={country.name as string} />} />}
            {!!wine.wine_type && <Field label="Type" value={String(wine.wine_type)} />}
            {!!wine.sweetness_level && <Field label="Sweetness" value={String(wine.sweetness_level)} />}
            {!!wine.alcohol_pct && <Field label="ABV" value={`${String(wine.alcohol_pct)}%`} />}
            {!!wine.aging_vessel && <Field label="Aging Vessel" value={String(wine.aging_vessel)} />}
            {!!wine.aging_months && <Field label="Aging" value={`${String(wine.aging_months)} months`} />}
          </div>
          <InsightsPanel fields={insightFields} data={insights} />
        </div>
      )}

      {tab === 'grapes' && (
        <DataTable data={grapes} columns={grapeColumns} />
      )}

      {tab === 'scores' && (
        <div className="space-y-8">
          <div>
            <h3 className="text-lg font-semibold text-earth-900 mb-3">Scores ({scoreCount})</h3>
            <DataTable data={scores} columns={scoreColumns} page={scorePage} pageSize={20} totalCount={scoreCount} onPageChange={setScorePage} />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-earth-900 mb-3">Prices ({priceCount})</h3>
            <DataTable data={prices} columns={priceColumns} page={pricePage} pageSize={20} totalCount={priceCount} onPageChange={setPricePage} />
          </div>
        </div>
      )}
    </div>
  )
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="bg-white rounded border border-earth-200 px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-earth-400 mb-0.5">{label}</div>
      <div className="text-sm text-earth-800">{value}</div>
    </div>
  )
}
