#!/usr/bin/env node
/**
 * Fetch boundary polygons for California wine regions & appellations
 * from OpenStreetMap Nominatim API, then store in geographic_boundaries.
 *
 * Usage: node fetch_california_boundaries.mjs [--dry-run] [--limit N]
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

const NOMINATIM_BASE = 'https://nominatim.openstreetmap.org/search'
const RATE_LIMIT_MS = 1100 // 1 request per second + buffer

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const sleep = ms => new Promise(r => setTimeout(r, ms))

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

// Build search queries for an entity
function buildQueries(name, regionName, isRegion) {
  const queries = []

  if (isRegion) {
    queries.push(`${name} wine region, California, United States`)
    queries.push(`${name}, California, United States`)
    queries.push(`${name} County, California, United States`)
    queries.push(`${name} AVA, California`)
  } else {
    // Appellation
    queries.push(`${name} AVA, California, United States`)
    queries.push(`${name} wine region, ${regionName}, California`)
    queries.push(`${name}, ${regionName}, California, United States`)
    queries.push(`${name}, California, United States`)
    // For county-named appellations
    if (name.includes('County')) {
      queries.push(`${name}, California`)
    }
  }
  return queries
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== California Boundary Polygon Fetcher ===`)
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}`)

  // 1. Get all California appellations with existing boundary rows
  const { data: appellations } = await supabase
    .from('appellations')
    .select(`
      id, name,
      regions!inner(id, name, countries!inner(name))
    `)
    .in('regions.name', [
      'Napa Valley', 'Sonoma County', 'California', 'Central Coast',
      'Paso Robles', 'Monterey', 'Mendocino', 'Santa Barbara County',
      'Sierra Foothills', 'Lodi'
    ])
    .eq('regions.countries.name', 'United States')

  // 2. Get California regions
  const { data: regions } = await supabase
    .from('regions')
    .select('id, name, countries!inner(name)')
    .in('name', [
      'Napa Valley', 'Sonoma County', 'California', 'Central Coast',
      'Paso Robles', 'Monterey', 'Mendocino', 'Santa Barbara County',
      'Sierra Foothills', 'Lodi'
    ])
    .eq('countries.name', 'United States')

  // Build work list
  const work = []

  for (const r of (regions || [])) {
    work.push({
      type: 'region',
      id: r.id,
      name: r.name,
      regionName: null,
    })
  }

  for (const a of (appellations || [])) {
    work.push({
      type: 'appellation',
      id: a.id,
      name: a.name,
      regionName: a.regions?.name,
    })
  }

  console.log(`Found ${work.length} entities (${regions?.length || 0} regions, ${appellations?.length || 0} appellations)`)

  const toProcess = work.slice(0, LIMIT)
  let success = 0, failed = 0, skipped = 0
  const failures = []
  const successes = []

  for (let i = 0; i < toProcess.length; i++) {
    const item = toProcess[i]
    const isRegion = item.type === 'region'
    const queries = buildQueries(item.name, item.regionName, isRegion)

    process.stdout.write(`[${i + 1}/${toProcess.length}] ${item.type}: ${item.name}...`)

    let polygon = null
    let source_id = null

    for (const q of queries) {
      await sleep(RATE_LIMIT_MS)
      try {
        const results = await nominatimSearch(q)
        if (results.length > 0 && hasPolygon(results[0])) {
          polygon = results[0].geojson
          source_id = `osm/${results[0].osm_type}/${results[0].osm_id}`
          console.log(` ✓ polygon (${polygon.type}, query: "${q}")`)
          break
        }
      } catch (err) {
        console.log(` error: ${err.message}`)
      }
    }

    if (!polygon) {
      console.log(' ✗ no polygon found')
      failed++
      failures.push(item.name)
      continue
    }

    if (DRY_RUN) {
      success++
      successes.push({ name: item.name, type: polygon.type })
      continue
    }

    // Store in DB
    const geojsonStr = JSON.stringify(polygon)

    if (isRegion) {
      // Region: INSERT new row (no existing boundary row)
      const { error } = await supabase.rpc('insert_boundary_polygon', {
        p_region_id: item.id,
        p_geojson: geojsonStr,
        p_source_id: source_id,
      })
      if (error) {
        // Try direct SQL via a different approach
        console.log(`  DB insert error: ${error.message}, trying raw...`)
        failed++
        failures.push(`${item.name} (DB: ${error.message})`)
        continue
      }
    } else {
      // Appellation: UPDATE existing row
      // Look up the boundary row
      const { data: gb } = await supabase
        .from('geographic_boundaries')
        .select('id')
        .eq('appellation_id', item.id)
        .single()

      if (!gb) {
        console.log(`  no boundary row found`)
        failed++
        failures.push(`${item.name} (no boundary row)`)
        continue
      }

      const { error } = await supabase.rpc('update_boundary_polygon', {
        p_boundary_id: gb.id,
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
    successes.push({ name: item.name, type: polygon.type })
  }

  console.log(`\n=== Summary ===`)
  console.log(`Success: ${success}  Failed: ${failed}  Skipped: ${skipped}`)
  if (failures.length > 0) {
    console.log(`\nFailed:`)
    failures.forEach(f => console.log(`  - ${f}`))
  }
  if (DRY_RUN && successes.length > 0) {
    console.log(`\nWould store:`)
    successes.forEach(s => console.log(`  - ${s.name} (${s.type})`))
  }
}

main().catch(console.error)
