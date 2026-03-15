#!/usr/bin/env node
/**
 * Fetch boundary polygons for ALL wine regions & appellations worldwide
 * from OpenStreetMap Nominatim API, then store in geographic_boundaries.
 *
 * Uses centroid distance validation to avoid false matches.
 * Skips entities that already have a polygon.
 *
 * Usage: node fetch_global_boundaries.mjs [--dry-run] [--limit N] [--country "France"]
 */

import { createClient } from '@supabase/supabase-js'
import { readFileSync } from 'fs'

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const env = Object.fromEntries(
  readFileSync('frontend/.env', 'utf8')
    .split('\n')
    .filter(l => l.includes('='))
    .map(l => { const [k, ...v] = l.split('='); return [k.trim(), v.join('=').trim()] })
)

const SUPABASE_URL = env.VITE_SUPABASE_URL
const SUPABASE_KEY = env.VITE_SUPABASE_ANON_KEY
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const DRY_RUN = process.argv.includes('--dry-run')
const LIMIT = process.argv.includes('--limit')
  ? parseInt(process.argv[process.argv.indexOf('--limit') + 1], 10)
  : Infinity
const COUNTRY_FILTER = process.argv.includes('--country')
  ? process.argv[process.argv.indexOf('--country') + 1]
  : null

const NOMINATIM_BASE = 'https://nominatim.openstreetmap.org/search'
const RATE_LIMIT_MS = 1100
const MAX_DISTANCE_KM = 100 // reject matches further than this from our centroid

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const sleep = ms => new Promise(r => setTimeout(r, ms))

function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLon = (lon2 - lon1) * Math.PI / 180
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

async function nominatimSearch(query) {
  const params = new URLSearchParams({
    q: query,
    format: 'json',
    polygon_geojson: '1',
    limit: '1',
  })
  const url = `${NOMINATIM_BASE}?${params}`
  const res = await fetch(url, {
    headers: { 'User-Agent': 'Loam Wine Platform (neil@loam.wine)' }
  })
  if (!res.ok) throw new Error(`Nominatim ${res.status}: ${res.statusText}`)
  return res.json()
}

function hasPolygon(result) {
  if (!result?.geojson) return false
  const t = result.geojson.type
  return t === 'Polygon' || t === 'MultiPolygon'
}

// Build search queries for an entity — country-aware
function buildQueries(name, regionName, countryName, designationType, isRegion) {
  const queries = []

  if (isRegion) {
    queries.push(`${name}, ${countryName}`)
    queries.push(`${name} wine region, ${countryName}`)
    if (countryName === 'United States') {
      queries.push(`${name} County, ${countryName}`)
    }
  } else {
    // Appellation — try most specific first
    if (regionName) {
      queries.push(`${name}, ${regionName}, ${countryName}`)
    }
    queries.push(`${name}, ${countryName}`)

    // Country-specific designation formats
    if (countryName === 'United States') {
      queries.push(`${name} AVA, ${countryName}`)
    } else if (countryName === 'France') {
      queries.push(`${name} wine region, France`)
    } else if (countryName === 'Italy') {
      queries.push(`${name} wine, Italy`)
    } else if (countryName === 'Spain') {
      queries.push(`${name} denominación, Spain`)
    } else if (countryName === 'Germany') {
      queries.push(`${name} Anbaugebiet, Germany`)
    }

    // Generic fallback
    queries.push(`${name} wine region, ${countryName}`)
  }

  return queries
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== Global Boundary Polygon Fetcher ===`)
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}`)
  if (COUNTRY_FILTER) console.log(`Country filter: ${COUNTRY_FILTER}`)

  // 1. Get all boundary rows that are centroid-only (no polygon)
  //    along with entity names, country, region, and centroid lat/lng
  const { data: rows, error } = await supabase.rpc('get_boundary_points')

  if (error) {
    console.error('Failed to fetch boundary points:', error.message)
    return
  }

  // Also get existing polygon IDs so we skip them
  const { data: polyRows } = await supabase.rpc('get_boundary_polygons')
  const hasPolySet = new Set((polyRows || []).map(p => p.entity_name))

  // Get all regions without polygons
  const { data: allRegions } = await supabase
    .from('regions')
    .select('id, name, countries!inner(name)')

  // Build work list: appellations without polygons
  const work = []

  // Add regions without polygons
  for (const r of (allRegions || [])) {
    const countryName = r.countries?.name
    if (!countryName) continue
    if (COUNTRY_FILTER && countryName !== COUNTRY_FILTER) continue
    if (hasPolySet.has(r.name)) continue

    // Find this region's centroid in the boundary points
    const bp = rows.find(p => p.entity_name === r.name && p.entity_type === 'region')
    if (!bp) continue // no centroid = no boundary row

    work.push({
      type: 'region',
      id: r.id,
      name: r.name,
      regionName: null,
      countryName,
      designationType: null,
      lat: bp.lat,
      lng: bp.lng,
    })
  }

  // Add appellations without polygons
  for (const p of rows) {
    if (p.entity_type !== 'appellation') continue
    if (hasPolySet.has(p.entity_name)) continue
    if (COUNTRY_FILTER && p.country_name !== COUNTRY_FILTER) continue

    work.push({
      type: 'appellation',
      boundaryId: p.id, // geographic_boundaries row ID
      name: p.entity_name,
      regionName: p.region_name,
      countryName: p.country_name,
      designationType: null,
      lat: p.lat,
      lng: p.lng,
    })
  }

  console.log(`Found ${work.length} entities without polygons`)
  if (COUNTRY_FILTER) {
    const byCountry = {}
    for (const w of work) { byCountry[w.countryName] = (byCountry[w.countryName] || 0) + 1 }
    console.log('Breakdown:', JSON.stringify(byCountry))
  }

  const toProcess = work.slice(0, LIMIT)
  let success = 0, failed = 0, tooFar = 0
  const failures = []
  const successes = []
  const tooFarList = []

  for (let i = 0; i < toProcess.length; i++) {
    const item = toProcess[i]
    const isRegion = item.type === 'region'
    const queries = buildQueries(item.name, item.regionName, item.countryName, item.designationType, isRegion)

    process.stdout.write(`[${i + 1}/${toProcess.length}] ${item.countryName}/${item.name}...`)

    let polygon = null
    let source_id = null
    let matchLat = null, matchLng = null

    for (const q of queries) {
      await sleep(RATE_LIMIT_MS)
      try {
        const results = await nominatimSearch(q)
        if (results.length > 0 && hasPolygon(results[0])) {
          matchLat = parseFloat(results[0].lat)
          matchLng = parseFloat(results[0].lon)

          // Distance validation
          const dist = haversineKm(item.lat, item.lng, matchLat, matchLng)
          if (dist > MAX_DISTANCE_KM) {
            console.log(` ✗ polygon too far (${Math.round(dist)}km, query: "${q}")`)
            tooFar++
            tooFarList.push(`${item.name} (${Math.round(dist)}km)`)
            polygon = null
            continue // try next query
          }

          polygon = results[0].geojson
          source_id = `osm/${results[0].osm_type}/${results[0].osm_id}`
          console.log(` ✓ ${polygon.type} (${Math.round(dist)}km, query: "${q}")`)
          break
        }
      } catch (err) {
        console.log(` error: ${err.message}`)
      }
    }

    if (!polygon) {
      if (!tooFarList.includes(item.name)) {
        console.log(' ✗ no polygon found')
      }
      failed++
      failures.push(item.name)
      continue
    }

    if (DRY_RUN) {
      success++
      successes.push({ name: item.name, country: item.countryName, type: polygon.type })
      continue
    }

    // Store in DB
    const geojsonStr = JSON.stringify(polygon)

    if (isRegion) {
      const { error } = await supabase.rpc('insert_boundary_polygon', {
        p_region_id: item.id,
        p_geojson: geojsonStr,
        p_source_id: source_id,
      })
      if (error) {
        console.log(`  DB error: ${error.message}`)
        failed++
        failures.push(`${item.name} (DB: ${error.message})`)
        continue
      }
    } else {
      // Use the boundary ID directly from get_boundary_points()
      const { error } = await supabase.rpc('update_boundary_polygon', {
        p_boundary_id: item.boundaryId,
        p_geojson: geojsonStr,
        p_source_id: source_id,
      })
      if (error) {
        console.log(`  DB update error: ${error.message}`)
        failed++
        failures.push(`${item.name} (DB: ${error.message})`)
        continue
      }
    }

    success++
    successes.push({ name: item.name, country: item.countryName, type: polygon.type })
  }

  console.log(`\n=== Summary ===`)
  console.log(`Success: ${success}  Failed: ${failed}  Too far: ${tooFar}`)
  if (failures.length > 0) {
    console.log(`\nFailed (${failures.length}):`)
    failures.forEach(f => console.log(`  - ${f}`))
  }
  if (tooFarList.length > 0) {
    console.log(`\nRejected (too far from centroid):`)
    tooFarList.forEach(f => console.log(`  - ${f}`))
  }
  if (successes.length > 0) {
    console.log(`\n${DRY_RUN ? 'Would store' : 'Stored'} (${successes.length}):`)
    const byCountry = {}
    for (const s of successes) { byCountry[s.country] = (byCountry[s.country] || 0) + 1 }
    for (const [c, n] of Object.entries(byCountry).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${c}: ${n}`)
    }
  }
}

main().catch(console.error)
