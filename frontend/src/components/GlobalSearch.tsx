import { useState } from 'react'
import { useNavigate } from 'react-router-dom'

export default function GlobalSearch() {
  const [query, setQuery] = useState('')
  const navigate = useNavigate()

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const q = query.trim()
    if (q) {
      navigate(`/data/search?q=${encodeURIComponent(q)}`)
    }
  }

  return (
    <form onSubmit={handleSubmit} className="relative">
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Search wines, producers, grapes, regions..."
        className="w-full pl-9 pr-4 py-2 text-sm bg-white border border-earth-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-wine-500/30 focus:border-wine-400 placeholder:text-earth-400"
      />
      <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-earth-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
      </svg>
    </form>
  )
}
