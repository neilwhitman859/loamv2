import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { supabase } from '../../lib/supabase'
import EntityMap from '../../components/EntityMap'

interface Vineyard {
  id: string
  name: string
  elevation_m: number | null
  aspect: string | null
  slope: string | null
  area_ha: number | null
  vine_density_per_ha: number | null
  established_year: number | null
  latitude: number | null
  longitude: number | null
  appellation: { id: string; name: string } | null
  region: { id: string; name: string } | null
  country: { id: string; name: string } | null
}

interface VineyardSoil {
  soil_type: { id: string; name: string; drainage_rate: string | null; heat_retention: string | null; geological_origin: string | null }
}

interface VineyardProducer { producer: { id: string; name: string } }
interface VineyardWine { wine: { id: string; name: string; color: string | null } }

const COLOR_DOTS: Record<string, string> = {
  red: 'bg-red-700',
  white: 'bg-amber-100 border border-amber-300',
  rose: 'bg-pink-300',
  orange: 'bg-orange-300',
}

export default function VineyardPage() {
  const { id } = useParams<{ id: string }>()
  const [vineyard, setVineyard] = useState<Vineyard | null>(null)
  const [soils, setSoils] = useState<VineyardSoil[]>([])
  const [producers, setProducers] = useState<VineyardProducer[]>([])
  const [wines, setWines] = useState<VineyardWine[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)

    supabase.from('vineyards')
      .select(`
        id, name, elevation_m, aspect, slope, area_ha, vine_density_per_ha, established_year,
        latitude, longitude,
        appellation:appellations!vineyards_appellation_id_fkey(id, name),
        region:regions!vineyards_region_id_fkey(id, name),
        country:countries!vineyards_country_id_fkey(id, name)
      `)
      .eq('id', id).single()
      .then(({ data }) => {
        if (data) {
          setVineyard(data as unknown as Vineyard)
          const p: PromiseLike<void>[] = []

          p.push(supabase.from('vineyard_soils')
            .select('soil_type:soil_types!vineyard_soils_soil_type_id_fkey(id, name, drainage_rate, heat_retention, geological_origin)')
            .eq('vineyard_id', id)
            .then(({ data: d }) => { if (d) setSoils(d as unknown as VineyardSoil[]) }))

          p.push(supabase.from('vineyard_producers')
            .select('producer:producers!vineyard_producers_producer_id_fkey(id, name)')
            .eq('vineyard_id', id)
            .then(({ data: d }) => { if (d) setProducers(d as unknown as VineyardProducer[]) }))

          p.push(supabase.from('wine_vineyards')
            .select('wine:wines!wine_vineyards_wine_id_fkey(id, name, color)')
            .eq('vineyard_id', id).limit(50)
            .then(({ data: d }) => { if (d) setWines(d as unknown as VineyardWine[]) }))

          Promise.all(p).then(() => setLoading(false))
        } else {
          setLoading(false)
        }
      })
  }, [id])

  if (loading) return <Loading />
  if (!vineyard) return <NotFound label="Vineyard" />

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 pb-16">
      {/* Breadcrumb */}
      <nav className="text-xs text-earth-400 mb-3 flex items-center gap-1.5 flex-wrap">
        {vineyard.country && <><Link to={`/country/${vineyard.country.id}`} className="hover:text-earth-600">{vineyard.country.name}</Link><span>/</span></>}
        {vineyard.region && <><Link to={`/region/${vineyard.region.id}`} className="hover:text-earth-600">{vineyard.region.name}</Link><span>/</span></>}
        {vineyard.appellation && <><Link to={`/appellation/${vineyard.appellation.id}`} className="hover:text-earth-600">{vineyard.appellation.name}</Link><span>/</span></>}
        <span className="text-earth-500">{vineyard.name}</span>
      </nav>

      {/* Header */}
      <header className="mb-4">
        <h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900">{vineyard.name}</h1>
        <div className="flex flex-wrap gap-1.5 mt-2">
          <Tag>Vineyard</Tag>
          {vineyard.established_year && <Tag>Est. {vineyard.established_year}</Tag>}
        </div>
      </header>

      {/* Site data */}
      <Section title="Site details">
        <FactGrid>
          {vineyard.country && <Fact label="Country" value={vineyard.country.name} link={`/country/${vineyard.country.id}`} />}
          {vineyard.region && <Fact label="Region" value={vineyard.region.name} link={`/region/${vineyard.region.id}`} />}
          {vineyard.appellation && <Fact label="Appellation" value={vineyard.appellation.name} link={`/appellation/${vineyard.appellation.id}`} />}
          {vineyard.elevation_m && <Fact label="Elevation" value={`${vineyard.elevation_m}m`} />}
          {vineyard.aspect && <Fact label="Aspect" value={vineyard.aspect} />}
          {vineyard.slope && <Fact label="Slope" value={vineyard.slope} />}
          {vineyard.area_ha && <Fact label="Area" value={`${vineyard.area_ha} ha`} />}
          {vineyard.vine_density_per_ha && <Fact label="Vine density" value={`${vineyard.vine_density_per_ha.toLocaleString()} /ha`} />}
          {vineyard.established_year && <Fact label="Established" value={vineyard.established_year.toString()} />}
          {vineyard.latitude && vineyard.longitude && (
            <Fact label="Coordinates" value={`${vineyard.latitude.toFixed(4)}, ${vineyard.longitude.toFixed(4)}`} />
          )}
        </FactGrid>
      </Section>

      {/* Soils */}
      {soils.length > 0 && (
        <Section title="Soils">
          {soils.map((s, i) => (
            <div key={i} className="mb-2 p-2 bg-earth-50 rounded-lg">
              <div className="text-sm font-medium text-earth-800">{s.soil_type.name}</div>
              <div className="flex flex-wrap gap-x-4 gap-y-0.5 mt-1">
                {s.soil_type.drainage_rate && <span className="text-xs text-earth-500">Drainage: {s.soil_type.drainage_rate}</span>}
                {s.soil_type.heat_retention && <span className="text-xs text-earth-500">Heat retention: {s.soil_type.heat_retention}</span>}
                {s.soil_type.geological_origin && <span className="text-xs text-earth-500">Origin: {s.soil_type.geological_origin}</span>}
              </div>
            </div>
          ))}
        </Section>
      )}

      {/* Map */}
      {vineyard.appellation && (
        <Section title="Location">
          <EntityMap entityType="appellation" entityId={vineyard.appellation.id} label={vineyard.appellation.name} />
        </Section>
      )}

      {/* Producers */}
      {producers.length > 0 && (
        <Section title="Producers">
          <div className="flex flex-wrap gap-2">
            {producers.map((p, i) => (
              <Link key={i} to={`/producer/${p.producer.id}`}
                className="text-sm px-3 py-1.5 bg-earth-100 rounded-lg text-earth-700 hover:bg-earth-200 transition-colors">
                {p.producer.name}
              </Link>
            ))}
          </div>
        </Section>
      )}

      {/* Wines */}
      {wines.length > 0 && (
        <Section title={`Wines (${wines.length})`}>
          <div className="space-y-0.5">
            {wines.map((w, i) => (
              <Link key={i} to={`/wine/${w.wine.id}`}
                className="flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-earth-100 transition-colors">
                {w.wine.color && <div className={`w-3 h-3 rounded-full shrink-0 ${COLOR_DOTS[w.wine.color] || 'bg-earth-300'}`} aria-label={`${w.wine.color} wine`} />}
                <span className="text-sm text-earth-700 font-medium truncate">{w.wine.name}</span>
              </Link>
            ))}
          </div>
        </Section>
      )}
    </div>
  )
}

/* ── Shared ───────────────────────────────────────────────── */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mb-5">
      <h2 className="font-display text-base font-semibold text-earth-800 mb-2 pb-1 border-b border-earth-100">{title}</h2>
      {children}
    </section>
  )
}

function Tag({ children }: { children: React.ReactNode }) {
  return <span className="inline-block text-xs font-medium px-2 py-0.5 rounded-full bg-earth-100 text-earth-600">{children}</span>
}

function FactGrid({ children, label }: { children: React.ReactNode; label?: string }) {
  const filtered = Array.isArray(children) ? children.filter(Boolean) : children ? [children] : []
  if (filtered.length === 0) {
    if (!label) return null
    return (
      <div className="mb-2">
        <div className="text-[10px] uppercase tracking-wider text-earth-300 mb-0.5">{label}</div>
        <p className="text-xs text-earth-300 italic">Data not available</p>
      </div>
    )
  }
  if (label) {
    return (
      <div className="mb-2">
        <div className="text-[10px] uppercase tracking-wider text-earth-400 mb-1">{label}</div>
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-x-4 gap-y-2">{filtered}</div>
      </div>
    )
  }
  return <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-x-4 gap-y-2 mb-2">{filtered}</div>
}

function Fact({ label, value, link }: { label: string; value: string; link?: string }) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] uppercase tracking-wider text-earth-400 leading-tight">{label}</div>
      {link ? (
        <Link to={link} className="text-sm font-medium text-earth-800 hover:text-wine-600 transition-colors truncate block">{value}</Link>
      ) : (
        <div className="text-sm font-medium text-earth-800 truncate" title={value}>{value}</div>
      )}
    </div>
  )
}

function Loading() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-12">
      <div className="animate-pulse space-y-4">
        <div className="h-8 bg-earth-200 rounded w-1/2" />
        <div className="h-4 bg-earth-100 rounded w-full" />
      </div>
    </div>
  )
}

function NotFound({ label }: { label: string }) {
  return (
    <div className="max-w-3xl mx-auto px-4 py-16 text-center">
      <p className="text-earth-500">{label} not found</p>
      <Link to="/" className="text-wine-600 text-sm mt-2 inline-block hover:underline">Back to search</Link>
    </div>
  )
}
