interface Props {
  confidence: number
  size?: 'sm' | 'md'
}

export default function ConfidenceBadge({ confidence, size = 'sm' }: Props) {
  const pct = Math.round(confidence * 100)
  const color =
    confidence >= 0.85
      ? 'bg-emerald-100 text-emerald-800'
      : confidence >= 0.7
      ? 'bg-amber-100 text-amber-800'
      : confidence >= 0.5
      ? 'bg-orange-100 text-orange-800'
      : 'bg-red-100 text-red-800'

  const sizeClass = size === 'md' ? 'px-3 py-1 text-sm' : 'px-2 py-0.5 text-xs'

  return (
    <span className={`inline-block rounded-full font-medium ${color} ${sizeClass}`}>
      {pct}%
    </span>
  )
}
