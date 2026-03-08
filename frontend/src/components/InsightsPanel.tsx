import ConfidenceBadge from './ConfidenceBadge'

interface Field {
  key: string
  label: string
}

interface Props {
  fields: Field[]
  data: Record<string, unknown> | null
  loading?: boolean
}

export default function InsightsPanel({ fields, data, loading }: Props) {
  if (loading) return <div className="text-earth-400 text-sm py-4">Loading insights...</div>
  if (!data) return null

  const confidence = data.confidence as number | undefined
  const enrichedAt = data.enriched_at ? new Date(data.enriched_at as string).toLocaleDateString() : null

  const visibleFields = fields.filter((f) => data[f.key])
  if (visibleFields.length === 0 && confidence == null) return null

  return (
    <div className="space-y-4">
      {(confidence != null || enrichedAt) && (
        <div className="flex items-center gap-3">
          {confidence != null && <ConfidenceBadge confidence={confidence} size="sm" />}
          {enrichedAt && <span className="text-xs text-earth-400">Enriched {enrichedAt}</span>}
        </div>
      )}
      {visibleFields.map((field) => (
        <div key={field.key} className="bg-white rounded-lg border border-earth-200 p-4">
          <h4 className="text-xs font-semibold uppercase tracking-wider text-wine-600 mb-1.5">
            {field.label}
          </h4>
          <p className="text-sm text-earth-800 leading-relaxed">{data[field.key] as string}</p>
        </div>
      ))}
    </div>
  )
}
