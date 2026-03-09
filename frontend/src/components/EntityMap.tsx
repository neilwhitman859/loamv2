import { useState, useEffect, useRef } from 'react'
import { MapContainer, TileLayer, Marker, GeoJSON, useMap, AttributionControl } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import { supabase } from '../lib/supabase'

// Dot icon for centroid marker
const centroidIcon = L.divIcon({
  className: '',
  html: '<div style="width:12px;height:12px;border-radius:50%;background:#7c3050;border:2px solid white;box-shadow:0 2px 6px rgba(0,0,0,.4)"></div>',
  iconSize: [12, 12],
  iconAnchor: [6, 6],
})

// Fit map to polygon bounds
function FitBounds({ geojson }: { geojson: GeoJSON.GeoJsonObject }) {
  const map = useMap()
  const fitted = useRef(false)
  useEffect(() => {
    if (fitted.current) return
    try {
      const layer = L.geoJSON(geojson)
      const bounds = layer.getBounds()
      if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [30, 30], maxZoom: 12 })
        fitted.current = true
      }
    } catch {
      // ignore invalid geojson
    }
  }, [geojson, map])
  return null
}

interface BoundaryData {
  lat: number
  lng: number
  boundary_geojson: GeoJSON.GeoJsonObject | null
  boundary_confidence: string
  boundary_source: string
}

interface EntityMapProps {
  entityType: 'country' | 'region' | 'appellation'
  entityId: string
}

export default function EntityMap({ entityType, entityId }: EntityMapProps) {
  const [data, setData] = useState<BoundaryData | null>(null)
  const [loading, setLoading] = useState(true)
  const [notFound, setNotFound] = useState(false)

  useEffect(() => {
    if (!entityId) return
    setLoading(true)
    setNotFound(false)
    supabase
      .rpc('get_entity_boundary', { p_entity_type: entityType, p_entity_id: entityId })
      .then(({ data: rows, error }) => {
        if (error || !rows || rows.length === 0) {
          setNotFound(true)
        } else {
          const row = rows[0]
          setData({
            lat: row.lat,
            lng: row.lng,
            boundary_geojson: row.boundary_geojson
              ? typeof row.boundary_geojson === 'string'
                ? JSON.parse(row.boundary_geojson)
                : row.boundary_geojson
              : null,
            boundary_confidence: row.boundary_confidence,
            boundary_source: row.boundary_source,
          })
        }
        setLoading(false)
      })
  }, [entityType, entityId])

  if (loading) {
    return (
      <div className="bg-white rounded border border-earth-200 overflow-hidden">
        <div className="h-64 flex items-center justify-center text-earth-400 text-sm">
          Loading map...
        </div>
      </div>
    )
  }

  if (notFound || !data) {
    return (
      <div className="bg-white rounded border border-earth-200 overflow-hidden">
        <div className="h-48 flex flex-col items-center justify-center text-earth-300">
          <svg className="w-8 h-8 mb-2" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 6.75V15m6-6v8.25m.503 3.498l4.875-2.437c.381-.19.622-.58.622-1.006V4.82c0-.836-.88-1.38-1.628-1.006l-3.869 1.934c-.317.159-.69.159-1.006 0L9.503 3.252a1.125 1.125 0 00-1.006 0L3.622 5.689C3.24 5.88 3 6.27 3 6.695V19.18c0 .836.88 1.38 1.628 1.006l3.869-1.934c.317-.159.69-.159 1.006 0l4.994 2.497c.317.158.69.158 1.006 0z" />
          </svg>
          <span className="text-sm">No Map Available</span>
        </div>
      </div>
    )
  }

  const hasPolygon = !!data.boundary_geojson
  const defaultZoom = entityType === 'country' ? 4 : entityType === 'region' ? 7 : 10

  return (
    <div className="bg-white rounded border border-earth-200 overflow-hidden">
      <div className="h-64">
        <MapContainer
          center={[data.lat, data.lng]}
          zoom={hasPolygon ? 8 : defaultZoom}
          style={{ height: '100%', width: '100%' }}
          zoomControl={true}
          scrollWheelZoom={false}
          attributionControl={false}
        >
          <AttributionControl position="bottomright" prefix='<a href="https://leafletjs.com" title="A JavaScript library for interactive maps">Leaflet</a>' />
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &middot; <a href="https://carto.com">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          />
          {hasPolygon && (
            <>
              <GeoJSON
                data={data.boundary_geojson!}
                style={{
                  color: '#7c3050',
                  weight: 2,
                  fillColor: '#7c3050',
                  fillOpacity: 0.12,
                }}
              />
              <FitBounds geojson={data.boundary_geojson!} />
            </>
          )}
          {!hasPolygon && <Marker position={[data.lat, data.lng]} icon={centroidIcon} />}
        </MapContainer>
      </div>
      <div className="px-3 py-2 border-t border-earth-100 flex items-center justify-between text-[10px] text-earth-400">
        <span>
          {data.lat.toFixed(4)}, {data.lng.toFixed(4)}
        </span>
        <span className="flex items-center gap-2">
          {hasPolygon && (
            <span className="inline-flex items-center gap-1">
              <span className="w-2 h-2 rounded-sm bg-wine-200 border border-wine-400" />
              Boundary
            </span>
          )}
          <span>{data.boundary_source}</span>
        </span>
      </div>
    </div>
  )
}
