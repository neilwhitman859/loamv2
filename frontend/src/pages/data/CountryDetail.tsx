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
  { key: 'ai_wine_history', label: 'Wine History' },
  { key: 'ai_key_regions', label: 'Key Regions' },
  { key: 'ai_signature_styles', label: 'Signature Styles' },
  { key: 'ai_regulatory_overview', label: 'Regulatory Overview' },
]

export default function CountryDetail() {
  const { id } = useParams<{ id: string }>()
  const [tab, setTab] = useState('overview')
  const [country, setCountry] = useState<Record<string, unknown> | null>(null)
  const [insights, setInsights] = useState<Record<string, unknown> | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    Promise.all([
      supabase.from('countries').select('*').eq('id', id).single(),
      supabase.from('country_insights').select('*').eq('country_id', id).maybeSingle(),
    ]).then(([countryRes, insRes]) => {
      if (countryRes.data) setCountry(countryRes.data as Record<string, unknown>)
      if (insRes.data) setInsights(insRes.data as Record<string, unknown>)
      setLoading(false)
    })
  }, [id])

  const regions = useRelatedEntities({
    table: 'regions',
    column: 'country_id',
    value: id,
    select: 'id, name, parent:regions!parent_id(name)',
    pageSize: 100,
    enabled: tab === 'regions',
  })

  const wines = useRelatedEntities({
    table: 'wines',
    column: 'country_id',
    value: id,
    select: 'id, name, producer:producers!producer_id(name), region:regions!region_id(name)',
    enabled: tab === 'wines',
  })

  if (loading) return <div className="py-12 text-center text-earth-400">Loading...</div>
  if (!country) return <div className="py-12 text-center text-earth-400">Country not found</div>

  const tabs = [
    { key: 'overview', label: 'Overview' },
    { key: 'regions', label: 'Regions', count: regions.totalCount },
    { key: 'wines', label: 'Wines', count: wines.totalCount },
  ]

  const regionColumns: Column<Record<string, unknown>>[] = [
    { key: 'name', label: 'Region', render: (row) => <EntityLink type="regions" id={row.id as string} name={row.name as string} /> },
    { key: 'parent', label: 'Parent', render: (row) => {
      const p = row.parent as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{(p?.name as string) ?? '—'}</span>
    }, className: 'w-40' },
  ]

  const wineColumns: Column<Record<string, unknown>>[] = [
    { key: 'name', label: 'Wine', render: (row) => <EntityLink type="wines" id={row.id as string} name={row.name as string} /> },
    { key: 'producer', label: 'Producer', render: (row) => {
      const p = row.producer as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{(p?.name as string) ?? '—'}</span>
    }, className: 'w-40' },
    { key: 'region', label: 'Region', render: (row) => {
      const r = row.region as Record<string, unknown> | null
      return <span className="text-earth-500 text-xs">{(r?.name as string) ?? '—'}</span>
    }, className: 'w-36' },
  ]

  return (
    <div>
      <Link to="/data/countries" className="text-sm text-wine-600 hover:text-wine-800 mb-4 inline-block">← Back to countries</Link>
      <div className="flex items-center gap-3 mb-1">
        <h1 className="text-2xl font-bold text-earth-900">{country.name as string}</h1>
        {country.iso_code && <span className="text-xs font-mono px-2 py-0.5 rounded bg-earth-100 text-earth-600">{String(country.iso_code)}</span>}
      </div>
      <div className="mb-6" />

      <TabBar tabs={tabs} active={tab} onChange={setTab} />

      {tab === 'overview' && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {country.iso_code && <Field label="ISO Code" value={String(country.iso_code)} />}
          </div>
          <InsightsPanel fields={insightFields} data={insights} />
        </div>
      )}

      {tab === 'regions' && (
        <DataTable
          data={regions.data}
          columns={regionColumns}
          loading={regions.loading}
          page={regions.page}
          pageSize={regions.pageSize}
          totalCount={regions.totalCount}
          onPageChange={regions.setPage}
        />
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
