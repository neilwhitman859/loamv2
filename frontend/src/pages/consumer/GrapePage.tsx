import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'

interface Grape {
  id: string
  display_name: string
  name: string
  color: string | null
  primary_name: string | null
}

interface GrapeInsight {
  ai_overview: string | null
  ai_flavor_profile: string | null
  ai_growing_conditions: string | null
  ai_food_pairing: string | null
  ai_regions_of_note: string | null
  ai_aging_characteristics: string | null
}

export default function GrapePage() {
  const { id } = useParams<{ id: string }>()
  const [grape, setGrape] = useState<Grape | null>(null)
  const [insight, setInsight] = useState<GrapeInsight | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    Promise.all([
      supabase.from('grapes')
        .select('id, display_name, name, color, primary_name')
        .eq('id', id).single(),
      supabase.from('grape_insights')
        .select('ai_overview, ai_flavor_profile, ai_growing_conditions, ai_food_pairing, ai_regions_of_note, ai_aging_characteristics')
        .eq('grape_id', id).maybeSingle(),
    ]).then(([gRes, iRes]) => {
      if (gRes.data) setGrape(gRes.data)
      if (iRes.data) setInsight(iRes.data)
      setLoading(false)
    })
  }, [id])

  if (loading) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-12">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-earth-200 rounded w-1/3" />
          <div className="h-4 bg-earth-100 rounded w-full" />
        </div>
      </div>
    )
  }

  if (!grape) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-16 text-center">
        <p className="text-earth-500">Grape not found</p>
        <Link to="/" className="text-wine-600 text-sm mt-2 inline-block hover:underline">Back to search</Link>
      </div>
    )
  }

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">
      <header className="mb-8">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{grape.display_name}</h1>
        <div className="flex items-center gap-2 mt-2">
          {grape.color && (
            <span className="text-xs font-medium px-2 py-0.5 rounded-full bg-earth-100 text-earth-600 capitalize">{grape.color} grape</span>
          )}
          {grape.primary_name && grape.primary_name !== grape.display_name && (
            <span className="text-xs text-earth-400">also known as {grape.primary_name}</span>
          )}
        </div>
      </header>

      {insight?.ai_overview && (
        <section className="mb-8">
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_overview}</p>
        </section>
      )}

      {insight?.ai_flavor_profile && (
        <section className="mb-8">
          <SectionTitle>Flavor Profile</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_flavor_profile}</p>
        </section>
      )}

      {insight?.ai_growing_conditions && (
        <section className="mb-8">
          <SectionTitle>Growing Conditions</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_growing_conditions}</p>
        </section>
      )}

      {insight?.ai_regions_of_note && (
        <section className="mb-8">
          <SectionTitle>Where It Grows</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_regions_of_note}</p>
        </section>
      )}

      {insight?.ai_food_pairing && (
        <section className="mb-8">
          <SectionTitle>Food</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_food_pairing}</p>
        </section>
      )}

      {insight?.ai_aging_characteristics && (
        <section className="mb-8">
          <SectionTitle>Aging</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_aging_characteristics}</p>
        </section>
      )}

      {!insight && (
        <div className="text-center py-12">
          <p className="text-sm text-earth-400">Grape data is being built out. Check back soon.</p>
        </div>
      )}
    </div>
  )
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return <h3 className="font-display text-lg font-semibold text-earth-800 mb-3">{children}</h3>
}
