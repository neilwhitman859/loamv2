import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../lib/supabase'
import ConfidenceBadge from '../components/ConfidenceBadge'

const tableConfig: Record<string, { table: string; idCol: string; nameJoin: string; nameField: string; fields: { key: string; label: string }[] }> = {
  grapes: {
    table: 'grape_insights',
    idCol: 'grape_id',
    nameJoin: 'grape:grapes(name)',
    nameField: 'grape',
    fields: [
      { key: 'ai_overview', label: 'Overview' },
      { key: 'ai_flavor_profile', label: 'Flavor Profile' },
      { key: 'ai_growing_conditions', label: 'Growing Conditions' },
      { key: 'ai_food_pairing', label: 'Food Pairing' },
      { key: 'ai_regions_of_note', label: 'Regions of Note' },
      { key: 'ai_aging_characteristics', label: 'Aging Characteristics' },
    ],
  },
  appellations: {
    table: 'appellation_insights',
    idCol: 'appellation_id',
    nameJoin: 'appellation:appellations(name)',
    nameField: 'appellation',
    fields: [
      { key: 'ai_overview', label: 'Overview' },
      { key: 'ai_climate_profile', label: 'Climate Profile' },
      { key: 'ai_soil_profile', label: 'Soil Profile' },
      { key: 'ai_signature_style', label: 'Signature Style' },
      { key: 'ai_key_grapes', label: 'Key Grapes' },
      { key: 'ai_aging_generalization', label: 'Aging' },
    ],
  },
  regions: {
    table: 'region_insights',
    idCol: 'region_id',
    nameJoin: 'region:regions(name)',
    nameField: 'region',
    fields: [
      { key: 'ai_overview', label: 'Overview' },
      { key: 'ai_climate_profile', label: 'Climate Profile' },
      { key: 'ai_sub_region_comparison', label: 'Sub-Region Comparison' },
      { key: 'ai_signature_style', label: 'Signature Style' },
      { key: 'ai_history', label: 'History' },
    ],
  },
  countries: {
    table: 'country_insights',
    idCol: 'country_id',
    nameJoin: 'country:countries(name)',
    nameField: 'country',
    fields: [
      { key: 'ai_overview', label: 'Overview' },
      { key: 'ai_wine_history', label: 'Wine History' },
      { key: 'ai_key_regions', label: 'Key Regions' },
      { key: 'ai_signature_styles', label: 'Signature Styles' },
      { key: 'ai_regulatory_overview', label: 'Regulatory Overview' },
    ],
  },
}

export default function Detail() {
  const { type, id } = useParams<{ type: string; id: string }>()
  const [data, setData] = useState<Record<string, unknown> | null>(null)
  const [loading, setLoading] = useState(true)

  const config = type ? tableConfig[type] : null

  useEffect(() => {
    if (!config || !id) return
    async function load() {
      const { data: row } = await supabase
        .from(config!.table)
        .select(`*, ${config!.nameJoin}`)
        .eq(config!.idCol, id)
        .single()
      setData(row as Record<string, unknown> | null)
      setLoading(false)
    }
    load()
  }, [config, id])

  if (!config) return <div className="py-12 text-center text-earth-400">Unknown type: {type}</div>
  if (loading) return <div className="py-12 text-center text-earth-400">Loading...</div>
  if (!data) return <div className="py-12 text-center text-earth-400">Not found</div>

  const nameObj = data[config.nameField] as Record<string, unknown> | null
  const name = (nameObj?.name as string) || 'Unknown'
  const confidence = data.confidence as number
  const enrichedAt = data.enriched_at ? new Date(data.enriched_at as string).toLocaleDateString() : null

  return (
    <div>
      <Link to={`/insights/${type}`} className="text-sm text-wine-600 hover:text-wine-800 mb-4 inline-block">
        ← Back to {type}
      </Link>

      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-earth-900">{name}</h1>
          <p className="text-sm text-earth-500 mt-1 capitalize">{type?.slice(0, -1)} insight</p>
        </div>
        <div className="text-right">
          <ConfidenceBadge confidence={confidence} size="md" />
          {enrichedAt && <div className="text-xs text-earth-400 mt-1">Enriched {enrichedAt}</div>}
        </div>
      </div>

      <div className="space-y-6">
        {config.fields.map((field) => {
          const value = data[field.key] as string
          if (!value) return null
          return (
            <div key={field.key} className="bg-white rounded-lg border border-earth-200 p-5">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-wine-600 mb-2">
                {field.label}
              </h3>
              <p className="text-earth-800 leading-relaxed">{value}</p>
            </div>
          )
        })}
      </div>
    </div>
  )
}
