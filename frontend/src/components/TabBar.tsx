interface Tab {
  key: string
  label: string
  count?: number
}

interface Props {
  tabs: Tab[]
  active: string
  onChange: (key: string) => void
}

export default function TabBar({ tabs, active, onChange }: Props) {
  return (
    <div className="border-b border-earth-200 mb-6">
      <nav className="flex gap-0 -mb-px">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => onChange(tab.key)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              active === tab.key
                ? 'border-wine-600 text-wine-700'
                : 'border-transparent text-earth-500 hover:text-earth-700 hover:border-earth-300'
            }`}
          >
            {tab.label}
            {tab.count != null && (
              <span className="ml-1.5 text-xs text-earth-400">({tab.count.toLocaleString()})</span>
            )}
          </button>
        ))}
      </nav>
    </div>
  )
}
