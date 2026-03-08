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
  { key: 'ai_sub_region_comparison', label: 'Sub-Region Comparison' },
  { key: 'ai_signature_style', label: 'Signature Style' },
  { key: 'ai_history', label: 'History' },
]

export default function RegionDetail() {
  const { id } = useParams<{ id: string }>()
  const [tab, setTab] = useState('overview')
  const [region, setRegion] = useState<Record<string, unknown> | null>(null)
  const [insights, setInsights] = useState<Record<string, unknown> | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    Promise.all([
      supabase.from('regions').select('*, country:countries!country_id(id, name), parent:regions!parent_id(id, name)').eq('id', id).single(),
      supabase.from('region_insights').select('*').eq('region_id', id).maybeSingle(),
    ]).then(([regRes, insRes]) => {
      if (regRes.data) setRegion(regRes.data as Record<string, unknown>)
      if (insRes.data) setInsights(insRes.data as Record<string, unknown>)
      setLoading(false)
    })
  }, [id])

  const children = useRelatedEntities({
    table: 'regions',
    column: 'parent_id',
    value: id,
    select: 'id, name',
    pageSize: 100,
    enabled: true,
  })

  const appellations = useRelatedEntities({
    table: 'appellations',
    column: 'region_id',
    value: id,
    select: 'id, name, designation_type',
    pageSize: 100,
    enabled: tab === 'related',
  })

  const wines = useRelatedEntities({
    table: 'wines',
    column: 'region_id',
    value: id,
    select: 'id, name, producer:producers!producer_id(name), varietal_category:varietal_categories!varietal_category_id(name, color)',
    enabled: tab === 'wines',
  })

  if (loading) return <div className="py-12 text-center text-earth-400">Loading...</div>
  if (!region) return <div className="py-12 text-center text-earth-400">Region not found</div>

  const country = region.country as Record<string, unknown> | null
  const parent = region.parent as Record<string, unknown> | null

  const tabs = [
    { key: 'overview', label: 'Overview' },
    { key: 'related', label: 'Sub-regions & Appellations', count: children.totalCount + appellations.totalCount },
    { key: 'wines', label: 'Wines', count: wines.totalCount },
  ]

  const childColumns: Column<Record<string, unknown>>[] = [
    { key: 'name', label: 'Sub-region', render: (row) => <EntityLink type="regions" id={row.id as string} name={row.name as string} /> },
  ]

  const appellationColumns: Column<Record<string, unknown>>[] = [
    { key: 'name', label: 'Appellation', render: (row) => <EntityLink type="appellations" id={row.id as string} name={row.name as string} /> },
    { key: 'designation_type', label: 'Type', render: (row) => {
      const dt = row.designation_type as string | null
      return dt ? <span className="text-xs px-2 py-0.5 rounded bg-earth-100 text-earth-700">{dt}</span> : <span className="text-earth-300">—</span>
    }, className: 'w-24' },
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
      <Link to="/data/regions" className="text-sm text-wine-600 hover:text-wine-800 mb-4 inline-block">← Back to regions</Link>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">{region.name as string}</h1>
      <div className="flex flex-wrap gap-3 text-sm text-earth-500 mb-6">
        {country && <EntityLink type="countries" id={country.id as string} name={country.name as string} className="text-earth-500 hover:text-wine-600" />}
        {parent && <>in <EntityLink type="regions" id={parent.id as string} name={parent.name as string} className="text-earth-500 hover:text-wine-600" /></>}
      </div>

      <TabBar tabs={tabs} active={tab} onChange={setTab} />

      {tab === 'overview' && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {country && <Field label="Country" value={<EntityLink type="countries" id={country.id as string} name={country.name as string} />} />}
            {parent && <Field label="Parent Region" value={<EntityLink type="regions" id={parent.id as string} name={parent.name as string} />} />}
          </div>
          <InsightsPanel fields={insightFields} data={insights} />
        </div>
      )}

      {tab === 'related' && (
        <div className="space-y-8">
          {children.totalCount > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-earth-900 mb-3">Sub-regions ({children.totalCount})</h3>
              <DataTable data={children.data} columns={childColumns} loading={children.loading} />
            </div>
          )}
          {appellations.totalCount > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-earth-900 mb-3">Appellations ({appellations.totalCount})</h3>
              <DataTable data={appellations.data} columns={appellationColumns} loading={appellations.loading} page={appellations.page} pageSize={appellations.pageSize} totalCount={appellations.totalCount} onPageChange={appellations.setPage} />
            </div>
          )}
          {children.totalCount === 0 && appellations.totalCount === 0 && !children.loading && (
            <p className="text-earth-400 text-sm">No sub-regions or appellations found.</p>
          )}
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
