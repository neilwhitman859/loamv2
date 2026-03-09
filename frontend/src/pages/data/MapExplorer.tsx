import { useState, useEffect, useMemo, useRef } from 'react'
import { MapContainer, TileLayer, Marker, Popup, GeoJSON, useMap } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import { supabase } from '../../lib/supabase'

// ---------------------------------------------------------------------------
// Fix Leaflet default icon paths (broken by bundlers)
// ---------------------------------------------------------------------------
// eslint-disable-next-line @typescript-eslint/no-explicit-any
delete (L.Icon.Default.prototype as any)._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
})

// Smaller dot marker for appellations
const dotIcon = L.divIcon({
  className: '',
  html: '<div style="width:10px;height:10px;border-radius:50%;background:#7c3050;border:2px solid white;box-shadow:0 1px 3px rgba(0,0,0,.4)"></div>',
  iconSize: [10, 10],
  iconAnchor: [5, 5],
})

const activeDotIcon = L.divIcon({
  className: '',
  html: '<div style="width:14px;height:14px;border-radius:50%;background:#c2185b;border:2px solid white;box-shadow:0 2px 6px rgba(0,0,0,.5)"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
})

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface BoundaryPoint {
  id: string
  entity_type: string
  entity_name: string
  country_name: string
  region_name: string | null
  lat: number
  lng: number
  boundary_confidence: string
  boundary_source: string
}

interface BoundaryPolygon {
  id: string
  entity_type: string
  entity_name: string
  country_name: string
  region_name: string | null
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  boundary_geojson: any
}

interface CountryGroup {
  name: string
  regions: RegionGroup[]
  points: BoundaryPoint[]
}

interface RegionGroup {
  name: string
  points: BoundaryPoint[]
}

// ---------------------------------------------------------------------------
// Map movement helper
// ---------------------------------------------------------------------------
function FlyTo({ center, zoom }: { center: [number, number]; zoom: number }) {
  const map = useMap()
  const prevCenter = useRef<string>('')
  useEffect(() => {
    const key = `${center[0]},${center[1]},${zoom}`
    if (key !== prevCenter.current) {
      prevCenter.current = key
      map.flyTo(center, zoom, { duration: 1.2 })
    }
  }, [center, zoom, map])
  return null
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------
export default function MapExplorer() {
  const [points, setPoints] = useState<BoundaryPoint[]>([])
  const [polygons, setPolygons] = useState<BoundaryPolygon[]>([])
  const [loading, setLoading] = useState(true)
  const [sidebarFilter, setSidebarFilter] = useState('')
  const [selectedCountry, setSelectedCountry] = useState<string | null>(null)
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null)
  const [selectedPoint, setSelectedPoint] = useState<string | null>(null)
  const [mapCenter, setMapCenter] = useState<[number, number]>([30, 0])
  const [mapZoom, setMapZoom] = useState(2)

  // Fetch all boundary points and polygons in parallel
  useEffect(() => {
    ;(async () => {
      const [pointsRes, polygonsRes] = await Promise.all([
        supabase.rpc('get_boundary_points'),
        supabase.rpc('get_boundary_polygons'),
      ])
      if (!pointsRes.error && pointsRes.data) {
        setPoints(pointsRes.data as BoundaryPoint[])
      }
      if (!polygonsRes.error && polygonsRes.data) {
        setPolygons(polygonsRes.data as BoundaryPolygon[])
      }
      setLoading(false)
    })()
  }, [])

  // Group by country → region
  const countryGroups = useMemo(() => {
    const map = new Map<string, CountryGroup>()
    for (const p of points) {
      if (!map.has(p.country_name)) {
        map.set(p.country_name, { name: p.country_name, regions: [], points: [] })
      }
      const group = map.get(p.country_name)!
      group.points.push(p)

      if (p.region_name) {
        let region = group.regions.find((r) => r.name === p.region_name)
        if (!region) {
          region = { name: p.region_name, points: [] }
          group.regions.push(region)
        }
        region.points.push(p)
      }
    }
    // Sort
    const result = Array.from(map.values()).sort((a, b) => a.name.localeCompare(b.name))
    for (const g of result) {
      g.regions.sort((a, b) => a.name.localeCompare(b.name))
    }
    return result
  }, [points])

  // Filter sidebar
  const filteredGroups = useMemo(() => {
    if (!sidebarFilter.trim()) return countryGroups
    const q = sidebarFilter.toLowerCase()
    return countryGroups
      .map((cg) => ({
        ...cg,
        regions: cg.regions
          .map((rg) => ({
            ...rg,
            points: rg.points.filter((p) => p.entity_name.toLowerCase().includes(q)),
          }))
          .filter(
            (rg) =>
              rg.name.toLowerCase().includes(q) || rg.points.length > 0
          ),
        points: cg.points.filter(
          (p) =>
            p.entity_name.toLowerCase().includes(q) ||
            (p.region_name && p.region_name.toLowerCase().includes(q))
        ),
      }))
      .filter(
        (cg) =>
          cg.name.toLowerCase().includes(q) ||
          cg.regions.length > 0 ||
          cg.points.length > 0
      )
  }, [countryGroups, sidebarFilter])

  // Visible markers on map
  const visiblePoints = useMemo(() => {
    if (selectedRegion) {
      const country = countryGroups.find((c) => c.name === selectedCountry)
      const region = country?.regions.find((r) => r.name === selectedRegion)
      return region?.points ?? []
    }
    if (selectedCountry) {
      const country = countryGroups.find((c) => c.name === selectedCountry)
      return country?.points ?? []
    }
    return points
  }, [points, countryGroups, selectedCountry, selectedRegion])

  // Visible polygons on map — show polygons matching visible points
  const visiblePolygons = useMemo(() => {
    if (!selectedCountry) return []
    const visibleNames = new Set(visiblePoints.map((p) => p.entity_name))
    // Also include region polygon if a region is selected
    return polygons.filter((poly) => {
      if (selectedRegion && poly.entity_name === selectedRegion && poly.entity_type === 'region') return true
      if (!selectedRegion && selectedCountry && poly.entity_name === selectedCountry && poly.entity_type === 'region') return true
      return visibleNames.has(poly.entity_name)
    })
  }, [polygons, visiblePoints, selectedCountry, selectedRegion])

  // Handlers
  const handleCountryClick = (name: string) => {
    if (selectedCountry === name && !selectedRegion) {
      // Deselect
      setSelectedCountry(null)
      setSelectedRegion(null)
      setSelectedPoint(null)
      setMapCenter([30, 0])
      setMapZoom(2)
      return
    }
    setSelectedCountry(name)
    setSelectedRegion(null)
    setSelectedPoint(null)
    const country = countryGroups.find((c) => c.name === name)
    if (country && country.points.length > 0) {
      const avgLat = country.points.reduce((s, p) => s + p.lat, 0) / country.points.length
      const avgLng = country.points.reduce((s, p) => s + p.lng, 0) / country.points.length
      setMapCenter([avgLat, avgLng])
      setMapZoom(country.points.length === 1 ? 8 : 5)
    }
  }

  const handleRegionClick = (countryName: string, regionName: string) => {
    if (selectedRegion === regionName && selectedCountry === countryName) {
      // Collapse back to country
      setSelectedRegion(null)
      setSelectedPoint(null)
      handleCountryClick(countryName)
      return
    }
    setSelectedCountry(countryName)
    setSelectedRegion(regionName)
    setSelectedPoint(null)
    const country = countryGroups.find((c) => c.name === countryName)
    const region = country?.regions.find((r) => r.name === regionName)
    if (region && region.points.length > 0) {
      const avgLat = region.points.reduce((s, p) => s + p.lat, 0) / region.points.length
      const avgLng = region.points.reduce((s, p) => s + p.lng, 0) / region.points.length
      setMapCenter([avgLat, avgLng])
      setMapZoom(region.points.length === 1 ? 10 : 8)
    }
  }

  const handlePointClick = (p: BoundaryPoint) => {
    setSelectedCountry(p.country_name)
    setSelectedRegion(p.region_name)
    setSelectedPoint(p.id)
    setMapCenter([p.lat, p.lng])
    setMapZoom(11)
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-earth-400">
        Loading geographic data...
      </div>
    )
  }

  return (
    <div className="flex gap-0 -mx-4 md:-mx-6 -mt-12 -mb-6" style={{ height: 'calc(100vh - 3.5rem)' }}>
      {/* Sidebar */}
      <div className="w-72 flex-shrink-0 bg-white border-r border-earth-200 flex flex-col overflow-hidden">
        <div className="p-4 border-b border-earth-200 flex-shrink-0">
          <h2 className="text-sm font-semibold text-earth-800 mb-1">Geographic Boundaries</h2>
          <p className="text-[10px] text-earth-400 mb-2">
            {points.length} centroids mapped
          </p>
          <input
            type="text"
            value={sidebarFilter}
            onChange={(e) => setSidebarFilter(e.target.value)}
            placeholder="Filter..."
            className="w-full px-2 py-1.5 text-xs border border-earth-300 rounded focus:outline-none focus:ring-2 focus:ring-wine-500/30 focus:border-wine-500 bg-white"
          />
        </div>

        <div className="flex-1 overflow-y-auto py-1">
          {filteredGroups.map((country) => (
            <div key={country.name}>
              {/* Country */}
              <button
                onClick={() => handleCountryClick(country.name)}
                className={`flex items-center justify-between w-full text-left px-4 py-1.5 text-xs transition-colors ${
                  selectedCountry === country.name && !selectedRegion
                    ? 'bg-wine-50 text-wine-700 font-semibold'
                    : selectedCountry === country.name
                    ? 'text-wine-600 font-medium'
                    : 'text-earth-700 hover:bg-earth-50'
                }`}
              >
                <span>{country.name}</span>
                <span className="text-[10px] text-earth-400 ml-1">{country.points.length}</span>
              </button>

              {/* Regions (show when country is selected or filter matches) */}
              {(selectedCountry === country.name || sidebarFilter.trim()) &&
                country.regions.map((region) => (
                  <div key={region.name}>
                    <button
                      onClick={() => handleRegionClick(country.name, region.name)}
                      className={`flex items-center justify-between w-full text-left pl-8 pr-4 py-1 text-xs transition-colors ${
                        selectedRegion === region.name && selectedCountry === country.name
                          ? 'bg-wine-50 text-wine-700 font-medium'
                          : 'text-earth-600 hover:bg-earth-50'
                      }`}
                    >
                      <span>{region.name}</span>
                      <span className="text-[10px] text-earth-400 ml-1">{region.points.length}</span>
                    </button>

                    {/* Appellations (show when region is selected) */}
                    {selectedRegion === region.name &&
                      selectedCountry === country.name &&
                      region.points.map((p) => (
                        <button
                          key={p.id}
                          onClick={() => handlePointClick(p)}
                          className={`block w-full text-left pl-12 pr-4 py-1 text-[11px] transition-colors ${
                            selectedPoint === p.id
                              ? 'bg-wine-100 text-wine-800 font-medium'
                              : 'text-earth-500 hover:bg-earth-50 hover:text-earth-700'
                          }`}
                        >
                          {p.entity_name}
                        </button>
                      ))}
                  </div>
                ))}
            </div>
          ))}
        </div>

        {/* Breadcrumb / selection info */}
        {selectedCountry && (
          <div className="p-3 border-t border-earth-200 bg-earth-50 flex-shrink-0">
            <div className="text-[10px] text-earth-400 uppercase tracking-wide mb-1">Viewing</div>
            <div className="text-xs text-earth-700 font-medium">
              {selectedCountry}
              {selectedRegion && <span className="text-earth-400"> › {selectedRegion}</span>}
              {selectedPoint && (
                <span className="text-earth-400">
                  {' › '}
                  {visiblePoints.find((p) => p.id === selectedPoint)?.entity_name}
                </span>
              )}
            </div>
            <div className="text-[10px] text-earth-400 mt-0.5">
              {visiblePoints.length} appellation{visiblePoints.length !== 1 ? 's' : ''} shown
            </div>
          </div>
        )}
      </div>

      {/* Map */}
      <div className="flex-1 relative">
        <MapContainer
          center={mapCenter}
          zoom={mapZoom}
          style={{ height: '100%', width: '100%' }}
          zoomControl={true}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &middot; <a href="https://carto.com">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          />
          <FlyTo center={mapCenter} zoom={mapZoom} />
          {visiblePolygons.map((poly) => (
            <GeoJSON
              key={`poly-${poly.id}-${selectedPoint}`}
              data={poly.boundary_geojson}
              style={{
                color: selectedPoint && visiblePoints.find((p) => p.id === selectedPoint)?.entity_name === poly.entity_name
                  ? '#c2185b'
                  : poly.entity_type === 'region' ? '#4a6741' : '#7c3050',
                weight: poly.entity_type === 'region' ? 2 : 1.5,
                fillColor: poly.entity_type === 'region' ? '#4a6741' : '#7c3050',
                fillOpacity: selectedPoint && visiblePoints.find((p) => p.id === selectedPoint)?.entity_name === poly.entity_name
                  ? 0.25
                  : 0.08,
                dashArray: poly.entity_type === 'region' ? '6 3' : undefined,
              }}
            />
          ))}
          {visiblePoints.map((p) => (
            <Marker
              key={p.id}
              position={[p.lat, p.lng]}
              icon={selectedPoint === p.id ? activeDotIcon : dotIcon}
              eventHandlers={{
                click: () => handlePointClick(p),
              }}
            >
              <Popup>
                <div className="text-xs">
                  <div className="font-semibold text-sm mb-1">{p.entity_name}</div>
                  {p.region_name && (
                    <div className="text-gray-600">{p.region_name}, {p.country_name}</div>
                  )}
                  {!p.region_name && (
                    <div className="text-gray-600">{p.country_name}</div>
                  )}
                  <div className="text-gray-400 mt-1 text-[10px]">
                    {p.lat.toFixed(4)}, {p.lng.toFixed(4)} · {p.boundary_source}
                  </div>
                </div>
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </div>
  )
}
