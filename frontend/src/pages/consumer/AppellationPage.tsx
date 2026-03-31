import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'

interface Appellation {
  id: string
  name: string
  designation_type: string | null
  country: { id: string; name: string } | null
  region: { id: string; name: string } | null
}

interface AppellationInsight {
  ai_overview: string | null
  ai_climate_profile: string | null
  ai_soil_profile: string | null
  ai_signature_style: string | null
  ai_key_grapes: string | null
  ai_aging_generalization: string | null
  ai_notable_producers_summary: string | null
}

interface GrapeAssociation {
  association_type: string | null
  grape: { id: string; display_name: string; color: string | null }
}

export default function AppellationPage() {
  const { id } = useParams<{ id: string }>()
  const [appellation, setAppellation] = useState<Appellation | null>(null)
  const [insight, setInsight] = useState<AppellationInsight | null>(null)
  const [grapes, setGrapes] = useState<GrapeAssociation[]>([])
  const [wineCount, setWineCount] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    Promise.all([
      supabase.from('appellations')
        .select('id, name, designation_type, country:countries!appellations_country_id_fkey(id, name), region:regions!appellations_region_id_fkey(id, name)')
        .eq('id', id).single(),
      supabase.from('appellation_insights')
        .select('ai_overview, ai_climate_profile, ai_soil_profile, ai_signature_style, ai_key_grapes, ai_aging_generalization, ai_notable_producers_summary')
        .eq('appellation_id', id).maybeSingle(),
      supabase.from('appellation_grapes')
        .select('association_type, grape:grapes!appellation_grapes_grape_id_fkey(id, display_name, color)')
        .eq('appellation_id', id)
        .limit(30),
      supabase.from('wines')
        .select('id', { count: 'exact', head: true })
        .eq('appellation_id', id).is('deleted_at', null),
    ]).then(([appRes, insRes, grapeRes, countRes]) => {
      if (appRes.data) setAppellation(appRes.data as unknown as Appellation)
      if (insRes.data) setInsight(insRes.data)
      if (grapeRes.data) setGrapes(grapeRes.data as unknown as GrapeAssociation[])
      if (countRes.count != null) setWineCount(countRes.count)
      setLoading(false)
    })
  }, [id])

  if (loading) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-12">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-earth-200 rounded w-1/2" />
          <div className="h-4 bg-earth-100 rounded w-full" />
          <div className="h-4 bg-earth-100 rounded w-5/6" />
        </div>
      </div>
    )
  }

  if (!appellation) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-16 text-center">
        <p className="text-earth-500">Appellation not found</p>
        <Link to="/" className="text-wine-600 text-sm mt-2 inline-block hover:underline">Back to search</Link>
      </div>
    )
  }

  const requiredGrapes = grapes.filter(g => g.association_type === 'required')
  const typicalGrapes = grapes.filter(g => g.association_type === 'typical')

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">

      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-4 flex items-center gap-1.5 flex-wrap">
        {appellation.country && (
          <>
            <Link to={`/country/${appellation.country.id}`} className="hover:text-earth-600 transition-colors">{appellation.country.name}</Link>
            <span>/</span>
          </>
        )}
        {appellation.region && (
          <>
            <Link to={`/region/${appellation.region.id}`} className="hover:text-earth-600 transition-colors">{appellation.region.name}</Link>
            <span>/</span>
          </>
        )}
        <span className="text-earth-500">{appellation.name}</span>
      </nav>

      {/* Header */}
      <header className="mb-8">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{appellation.name}</h1>
        <div className="flex items-center gap-2 mt-2">
          {appellation.designation_type && (
            <span className="text-xs font-medium px-2 py-0.5 rounded-full bg-wine-50 text-wine-700 uppercase tracking-wider">
              {appellation.designation_type}
            </span>
          )}
          <span className="text-sm text-earth-400">
            {appellation.country?.name}{wineCount > 0 && ` \u00b7 ${wineCount.toLocaleString()} wines`}
          </span>
        </div>
      </header>

      {/* Overview */}
      {insight?.ai_overview && (
        <section className="mb-8">
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_overview}</p>
        </section>
      )}

      {/* Soil */}
      {insight?.ai_soil_profile && (
        <section className="mb-8">
          <SectionTitle>Soil</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_soil_profile}</p>
        </section>
      )}

      {/* Climate */}
      {insight?.ai_climate_profile && (
        <section className="mb-8">
          <SectionTitle>Climate</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_climate_profile}</p>
        </section>
      )}

      {/* Signature style */}
      {insight?.ai_signature_style && (
        <section className="mb-8">
          <SectionTitle>Signature Style</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_signature_style}</p>
        </section>
      )}

      {/* Grapes */}
      {grapes.length > 0 && (
        <section className="mb-8">
          <SectionTitle>Grapes</SectionTitle>
          {/* AI key grapes narrative */}
          {insight?.ai_key_grapes && (
            <p className="text-sm text-earth-600 leading-relaxed mb-3">{insight.ai_key_grapes}</p>
          )}

          {requiredGrapes.length > 0 && (
            <div className="mb-3">
              <h4 className="text-xs font-semibold uppercase tracking-wider text-earth-400 mb-2">Required varieties</h4>
              <div className="flex flex-wrap gap-1.5">
                {requiredGrapes.map(g => (
                  <Link
                    key={g.grape.id}
                    to={`/grape/${g.grape.id}`}
                    className="text-xs px-2.5 py-1 bg-wine-50 text-wine-700 rounded-full hover:bg-wine-100 transition-colors font-medium"
                  >
                    {g.grape.display_name}
                  </Link>
                ))}
              </div>
            </div>
          )}

          {typicalGrapes.length > 0 && (
            <div>
              <h4 className="text-xs font-semibold uppercase tracking-wider text-earth-400 mb-2">
                {requiredGrapes.length > 0 ? 'Also planted' : 'Key varieties'}
              </h4>
              <div className="flex flex-wrap gap-1.5">
                {typicalGrapes.map(g => (
                  <Link
                    key={g.grape.id}
                    to={`/grape/${g.grape.id}`}
                    className="text-xs px-2.5 py-1 bg-earth-100 text-earth-600 rounded-full hover:bg-earth-200 transition-colors"
                  >
                    {g.grape.display_name}
                  </Link>
                ))}
              </div>
            </div>
          )}
        </section>
      )}

      {/* Aging */}
      {insight?.ai_aging_generalization && (
        <section className="mb-8">
          <SectionTitle>Aging</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_aging_generalization}</p>
        </section>
      )}

      {/* Notable producers */}
      {insight?.ai_notable_producers_summary && (
        <section className="mb-8">
          <SectionTitle>Notable Producers</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_notable_producers_summary}</p>
        </section>
      )}

      {/* No content fallback */}
      {!insight && grapes.length === 0 && (
        <div className="text-center py-12">
          <p className="text-sm text-earth-400">Appellation data is being built out. Check back soon.</p>
        </div>
      )}
    </div>
  )
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return <h3 className="font-display text-lg font-semibold text-earth-800 mb-3">{children}</h3>
}
