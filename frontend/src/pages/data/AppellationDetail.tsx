import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import { useRelatedEntities } from '../../hooks/useRelatedEntities'
import TabBar from '../../components/TabBar'
import InsightsPanel from '../../components/InsightsPanel'
import EntityLink from '../../components/EntityLink'
import DataTable, { type Column } from '../../components/DataTable'

const insightFields = [
  { key: 'ai_overview', label: 'Overview' },
  { key: 'ai_climate_profile', label: 'Climate Profile' },
  { key: 'ai_soil_profile', label: 'Soil Profile' },
  { key: 'ai_signature_style', label: 'Signature Style' },
  { key: 'ai_key_grapes', label: 'Key Grapes' },
  { key: 'ai_aging_generalization', label: 'Aging' },
]

export default function AppellationDetail() {
  const { id } = useParams<{ id: string }>()
  const [tab, setTab] = useState('overview')
  const [appellation, setAppellation] = useState<Record<string, unknown> | null>(null)
  const [insights, setInsights] = useState<Record<string, unknown> | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    Promise.all([
      supabase.from('appellations').select('*, country:countries!country_id(id, name), region:regions!region_id(id, name)').eq('id', id).single(),
      supabase.from('appellation_insights').select('*').eq('appellation_id', id).maybeSingle(),
    ]).then(([appRes, insRes]) => {
      if (appRes.data) setAppellation(appRes.data as Record<string, unknown>)
      if (insRes.data) setInsights(insRes.data as Record<string, unknown>)
      setLoading(false)
    })
  }, [id])

  const wines = useRelatedEntities({
    table: 'wines',
    column: 'appellation_id',
    value: id,
    select: 'id, name, producer:producers!producer_id(name), varietal_category:varietal_categories!varietal_category_id(name, color)',
    enabled: tab === 'wines',
  })

  if (loading) return <div className="py-12 text-center text-earth-400">Loading...</div>
  if (!appellation) return <div className="py-12 text-center text-earth-400">Appellation not found</div>

  const country = appellation.country as Record<string, unknown> | null
  const region = appellation.region as Record<string, unknown> | null

  const tabs = [
    { key: 'overview', label: 'Overview' },
    { key: 'wines', label: 'Wines', count: wines.totalCount },
  ]

  const wineColumns: Column<Record<string, unknown>>[] = [
    { key: 'name', label: 'Wine', render: (row) => <EntityLink type="wines" id={row.id as string} name={row.name as string} /> },
    { key: 'producer', label: 'Producer', render: (row) => {
      const p = row.producer as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{(p?.name as string) ?? '—'}</span>
    }, className: 'w-40' },
    { key: 'varietal', label: 'Varietal', render: (row) => {
      const v = row.varietal_category as Record<string, unknown> | null
      if (!v) return <span className="text-earth-300 text-xs">—</span>
      return <span className={`px-2 py-0.5 rounded text-xs font-medium ${(v.color as string) === 'red' ? 'bg-red-100 text-red-800' : 'bg-yellow-100 text-yellow-800'}`}>{v.name as string}</span>
    }, className: 'w-36' },
  ]

  return (
    <div>
      <Link to="/data/appellations" className="text-sm text-wine-600 hover:text-wine-800 mb-4 inline-block">← Back to appellations</Link>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">{appellation.name as string}</h1>
      <div className="flex flex-wrap gap-3 text-sm text-earth-500 mb-6">
        {!!appellation.designation_type && <span className="px-2 py-0.5 rounded bg-earth-100 text-earth-700 text-xs">{String(appellation.designation_type)}</span>}
        {country && <EntityLink type="countries" id={country.id as string} name={country.name as string} className="text-earth-500 hover:text-wine-600" />}
        {region && <EntityLink type="regions" id={region.id as string} name={region.name as string} className="text-earth-500 hover:text-wine-600" />}
      </div>

      <TabBar tabs={tabs} active={tab} onChange={setTab} />

      {tab === 'overview' && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {country && <Field label="Country" value={<EntityLink type="countries" id={country.id as string} name={country.name as string} />} />}
            {region && <Field label="Region" value={<EntityLink type="regions" id={region.id as string} name={region.name as string} />} />}
            {!!appellation.designation_type && <Field label="Designation" value={String(appellation.designation_type)} />}
            {!!appellation.latitude && <Field label="Coordinates" value={`${String(appellation.latitude)}, ${String(appellation.longitude)}`} />}
          </div>
          <InsightsPanel fields={insightFields} data={insights} />
        </div>
      )}

      {tab === 'wines' && (
        <DataTable
          data={wines.data}
          columns={wineColumns}
          loading={wines.loading}
          page={wines.page}
          pageSize={wines.pageSize}
          totalCount={wines.totalCount}
          onPageChange={wines.setPage}
        />
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
