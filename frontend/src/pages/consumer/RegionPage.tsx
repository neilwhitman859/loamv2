import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'

interface Region {
  id: string
  name: string
  is_catch_all: boolean | null
  parent_id: string | null
  country: { id: string; name: string } | null
}

interface ParentRegion {
  id: string
  name: string
}

interface RegionInsight {
  ai_overview: string | null
  ai_climate_profile: string | null
  ai_sub_region_comparison: string | null
  ai_signature_style: string | null
  ai_history: string | null
}

interface SubRegion {
  id: string
  name: string
}

interface Appellation {
  id: string
  name: string
  designation_type: string | null
}

export default function RegionPage() {
  const { id } = useParams<{ id: string }>()
  const [region, setRegion] = useState<Region | null>(null)
  const [parentRegion, setParentRegion] = useState<ParentRegion | null>(null)
  const [insight, setInsight] = useState<RegionInsight | null>(null)
  const [subRegions, setSubRegions] = useState<SubRegion[]>([])
  const [appellations, setAppellations] = useState<Appellation[]>([])
  const [wineCount, setWineCount] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    Promise.all([
      supabase.from('regions')
        .select('id, name, is_catch_all, parent_id, country:countries!regions_country_id_fkey(id, name)')
        .eq('id', id).single(),
      supabase.from('region_insights')
        .select('ai_overview, ai_climate_profile, ai_sub_region_comparison, ai_signature_style, ai_history')
        .eq('region_id', id).maybeSingle(),
      supabase.from('regions')
        .select('id, name')
        .eq('parent_id', id).is('deleted_at', null).order('name'),
      supabase.from('appellations')
        .select('id, name, designation_type')
        .eq('region_id', id).is('deleted_at', null).order('name').limit(100),
      supabase.from('wines')
        .select('id', { count: 'exact', head: true })
        .eq('region_id', id).is('deleted_at', null),
    ]).then(async ([regRes, insRes, subRes, appRes, countRes]) => {
      if (regRes.data) {
        const reg = regRes.data as unknown as Region
        setRegion(reg)
        // Fetch parent region separately to avoid self-join issues
        if (reg.parent_id) {
          const { data: parentData } = await supabase.from('regions')
            .select('id, name').eq('id', reg.parent_id).single()
          if (parentData) setParentRegion(parentData)
        }
      }
      if (insRes.data) setInsight(insRes.data)
      if (subRes.data) setSubRegions(subRes.data)
      if (appRes.data) setAppellations(appRes.data)
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

  if (!region) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-16 text-center">
        <p className="text-earth-500">Region not found</p>
        <Link to="/" className="text-wine-600 text-sm mt-2 inline-block hover:underline">Back to search</Link>
      </div>
    )
  }

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">

      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-4 flex items-center gap-1.5 flex-wrap">
        {region.country && (
          <>
            <Link to={`/country/${region.country.id}`} className="hover:text-earth-600 transition-colors">{region.country.name}</Link>
            <span>/</span>
          </>
        )}
        {parentRegion && (
          <>
            <Link to={`/region/${parentRegion.id}`} className="hover:text-earth-600 transition-colors">{parentRegion.name}</Link>
            <span>/</span>
          </>
        )}
        <span className="text-earth-500">{region.name}</span>
      </nav>

      {/* Header */}
      <header className="mb-8">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{region.name}</h1>
        <p className="text-sm text-earth-400 mt-1">
          {region.country?.name}{wineCount > 0 && ` \u00b7 ${wineCount.toLocaleString()} wines`}
        </p>
      </header>

      {/* Overview */}
      {insight?.ai_overview && (
        <section className="mb-8">
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_overview}</p>
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

      {/* Sub-regions comparison */}
      {insight?.ai_sub_region_comparison && (
        <section className="mb-8">
          <SectionTitle>Sub-regions</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_sub_region_comparison}</p>
        </section>
      )}

      {/* History */}
      {insight?.ai_history && (
        <section className="mb-8">
          <SectionTitle>History</SectionTitle>
          <p className="text-sm text-earth-700 leading-relaxed">{insight.ai_history}</p>
        </section>
      )}

      {/* Sub-regions list */}
      {subRegions.length > 0 && (
        <section className="mb-8">
          <SectionTitle>Regions within {region.name}</SectionTitle>
          <div className="flex flex-wrap gap-2">
            {subRegions.map(sr => (
              <Link
                key={sr.id}
                to={`/region/${sr.id}`}
                className="text-sm px-3 py-1.5 bg-earth-100 hover:bg-earth-200 rounded-lg text-earth-700 transition-colors"
              >
                {sr.name}
              </Link>
            ))}
          </div>
        </section>
      )}

      {/* Appellations */}
      {appellations.length > 0 && (
        <section className="mb-8">
          <SectionTitle>Appellations ({appellations.length})</SectionTitle>
          <div className="space-y-1">
            {appellations.map(a => (
              <Link
                key={a.id}
                to={`/appellation/${a.id}`}
                className="flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-earth-100 transition-colors"
              >
                <span className="text-sm text-earth-700 font-medium">{a.name}</span>
                {a.designation_type && (
                  <span className="text-[10px] text-earth-400 uppercase tracking-wider">{a.designation_type}</span>
                )}
              </Link>
            ))}
          </div>
        </section>
      )}

      {/* No content fallback */}
      {!insight && subRegions.length === 0 && appellations.length === 0 && (
        <div className="text-center py-12">
          <p className="text-sm text-earth-400">Region data is being built out. Check back soon.</p>
        </div>
      )}
    </div>
  )
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return <h3 className="font-display text-lg font-semibold text-earth-800 mb-3">{children}</h3>
}
