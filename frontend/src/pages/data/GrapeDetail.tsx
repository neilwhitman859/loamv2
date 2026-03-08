import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import TabBar from '../../components/TabBar'
import InsightsPanel from '../../components/InsightsPanel'
import EntityLink from '../../components/EntityLink'
import DataTable, { type Column } from '../../components/DataTable'

const insightFields = [
  { key: 'ai_overview', label: 'Overview' },
  { key: 'ai_flavor_profile', label: 'Flavor Profile' },
  { key: 'ai_growing_conditions', label: 'Growing Conditions' },
  { key: 'ai_food_pairing', label: 'Food Pairing' },
  { key: 'ai_regions_of_note', label: 'Regions of Note' },
  { key: 'ai_aging_characteristics', label: 'Aging Characteristics' },
]

export default function GrapeDetail() {
  const { id } = useParams<{ id: string }>()
  const [tab, setTab] = useState('overview')
  const [grape, setGrape] = useState<Record<string, unknown> | null>(null)
  const [insights, setInsights] = useState<Record<string, unknown> | null>(null)
  const [loading, setLoading] = useState(true)

  // Wines via wine_grapes join - need custom pagination
  const [wines, setWines] = useState<Record<string, unknown>[]>([])
  const [winePage, setWinePage] = useState(0)
  const [wineCount, setWineCount] = useState(0)
  const [winesLoading, setWinesLoading] = useState(false)

  useEffect(() => {
    if (!id) return
    Promise.all([
      supabase.from('grapes').select('*, origin_country:countries!origin_country_id(id, name)').eq('id', id).single(),
      supabase.from('grape_insights').select('*').eq('grape_id', id).maybeSingle(),
    ]).then(([grapeRes, insRes]) => {
      if (grapeRes.data) setGrape(grapeRes.data as Record<string, unknown>)
      if (insRes.data) setInsights(insRes.data as Record<string, unknown>)
      setLoading(false)
    })
  }, [id])

  useEffect(() => {
    if (!id || tab !== 'wines') return
    setWinesLoading(true)
    const from = winePage * 20
    supabase.from('wine_grapes')
      .select('grape_id, percentage, wine:wines!wine_id(id, name, producer:producers!producer_id(name))', { count: 'exact' })
      .eq('grape_id', id)
      .range(from, from + 19)
      .then(({ data, count }) => {
        if (data) setWines(data as Record<string, unknown>[])
        setWineCount(count ?? 0)
        setWinesLoading(false)
      })
  }, [id, tab, winePage])

  if (loading) return <div className="py-12 text-center text-earth-400">Loading...</div>
  if (!grape) return <div className="py-12 text-center text-earth-400">Grape not found</div>

  const origin = grape.origin_country as Record<string, unknown> | null
  const color = grape.color as string

  const tabs = [
    { key: 'overview', label: 'Overview' },
    { key: 'wines', label: 'Wines', count: wineCount },
  ]

  const wineColumns: Column<Record<string, unknown>>[] = [
    { key: 'name', label: 'Wine', render: (row) => {
      const w = row.wine as Record<string, unknown> | null
      return w ? <EntityLink type="wines" id={w.id as string} name={w.name as string} /> : <span>—</span>
    }},
    { key: 'producer', label: 'Producer', render: (row) => {
      const w = row.wine as Record<string, unknown> | null
      const p = w?.producer as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{(p?.name as string) ?? '—'}</span>
    }, className: 'w-40' },
    { key: 'percentage', label: '%', render: (row) => {
      const pct = row.percentage as number | null
      return pct != null ? <span className="text-sm">{pct}%</span> : <span className="text-earth-300">—</span>
    }, className: 'w-16' },
  ]

  return (
    <div>
      <Link to="/data/grapes" className="text-sm text-wine-600 hover:text-wine-800 mb-4 inline-block">← Back to grapes</Link>
      <div className="flex items-center gap-3 mb-1">
        <h1 className="text-2xl font-bold text-earth-900">{grape.name as string}</h1>
        {color && <span className={`px-2 py-0.5 rounded text-xs font-medium ${color === 'red' ? 'bg-red-100 text-red-800' : color === 'white' ? 'bg-yellow-100 text-yellow-800' : 'bg-pink-100 text-pink-800'}`}>{color}</span>}
      </div>
      <div className="text-sm text-earth-500 mb-6">
        {origin && <>Origin: <EntityLink type="countries" id={origin.id as string} name={origin.name as string} className="text-earth-500 hover:text-wine-600" /></>}
      </div>

      <TabBar tabs={tabs} active={tab} onChange={setTab} />

      {tab === 'overview' && (
        <InsightsPanel fields={insightFields} data={insights} />
      )}

      {tab === 'wines' && (
        <DataTable
          data={wines}
          columns={wineColumns}
          loading={winesLoading}
          page={winePage}
          pageSize={20}
          totalCount={wineCount}
          onPageChange={setWinePage}
        />
      )}
    </div>
  )
}
