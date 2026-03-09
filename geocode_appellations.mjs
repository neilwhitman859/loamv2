/**
 * geocode_appellations.mjs
 *
 * Geocodes all appellations that don't yet have a geographic_boundaries row.
 * Uses OpenStreetMap Nominatim (free, 1 req/sec rate limit).
 * Inserts centroid-only rows with confidence='geocoded', source='nominatim'.
 *
 * Usage: node geocode_appellations.mjs [--dry-run] [--limit N]
 */

import { createClient } from '@supabase/supabase-js'

const SUPABASE_URL = 'https://vgbppjhmvbggfjztzobl.supabase.co'
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZnYnBwamhtdmJnZ2ZqenR6b2JsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI1ODU1NDYsImV4cCI6MjA4ODE2MTU0Nn0.KHZiqk6B7XYDnkFcDNJtMIKoT-hf7s8MGkmpOsjgVDk'
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search'
const RATE_LIMIT_MS = 1100 // slightly over 1 second to be safe

const args = process.argv.slice(2)
const DRY_RUN = args.includes('--dry-run')
const limitIdx = args.indexOf('--limit')
const LIMIT = limitIdx >= 0 ? parseInt(args[limitIdx + 1]) : Infinity

const sleep = ms => new Promise(r => setTimeout(r, ms))

// Wine-specific designation prefixes to strip for cleaner geocoding
const DESIGNATION_PREFIXES = [
  'AOC ', 'AOP ', 'DOC ', 'DOCG ', 'DO ', 'AVA ', 'IGP ', 'IGT ',
  'VdP ', 'GI ', 'IG ', 'PDO ', 'PGI ', 'DOP ', 'WO '
]

function cleanAppellationName(name) {
  let cleaned = name
  for (const prefix of DESIGNATION_PREFIXES) {
    if (cleaned.startsWith(prefix)) {
      cleaned = cleaned.slice(prefix.length)
    }
  }
  return cleaned.trim()
}

// Common suffixes in Italian/Spanish DOC names that follow the place name
// e.g., "Aglianico del Vulture" → place is "Vulture"
// e.g., "Brunello di Montalcino" → place is "Montalcino"
// e.g., "Vino Nobile di Montepulciano" → place is "Montepulciano"
const PLACE_EXTRACTORS = [
  /\bdi\s+(.+)$/i,          // "Brunello di Montalcino" → "Montalcino"
  /\bdel\s+(.+)$/i,         // "Aglianico del Vulture" → "Vulture"
  /\bdella\s+(.+)$/i,       // "Vernaccia della Val Tidone" → "Val Tidone"
  /\bdelle\s+(.+)$/i,       // "Colli delle Marche" → "Marche"
  /\bdei\s+(.+)$/i,         // "Colli dei Bolognesi" → "Bolognesi"
  /\bd'\s*(.+)$/i,          // "Montepulciano d'Abruzzo" → "Abruzzo"
  /\bde\s+(.+)$/i,          // "Ribera de Duero" → "Duero" (less useful, but region helps)
]

// Quality tier suffixes commonly appended to Italian DOC/DOCG names
const QUALITY_SUFFIXES = [
  'Superiore', 'Riserva', 'Classico', 'Gran Selezione', 'Passito',
  'Spumante', 'Frizzante', 'Liquoroso', 'Novello'
]

/**
 * Strip quality tier suffixes: "Aglianico del Vulture Superiore" → "Aglianico del Vulture"
 */
function stripQualitySuffix(name) {
  let cleaned = name
  for (const suffix of QUALITY_SUFFIXES) {
    if (cleaned.endsWith(` ${suffix}`)) {
      cleaned = cleaned.slice(0, -(suffix.length + 1))
    }
  }
  return cleaned
}

/**
 * Extract all candidate place names from a wine appellation name.
 * Returns unique place names from both the original and suffix-stripped versions.
 * e.g., "Aglianico del Vulture Superiore" → ["Vulture Superiore", "Vulture"]
 */
function extractPlaceNames(name) {
  const places = []
  const seen = new Set()
  for (const variant of [name, stripQualitySuffix(name)]) {
    for (const regex of PLACE_EXTRACTORS) {
      const match = variant.match(regex)
      if (match) {
        const place = match[1].trim()
        if (!seen.has(place)) { seen.add(place); places.push(place) }
        break // only first regex match per variant
      }
    }
  }
  return places
}

/**
 * Build search queries in priority order — try the most specific first,
 * fall back to broader queries.
 */
function buildSearchQueries(appellation, region, country) {
  const cleanName = cleanAppellationName(appellation)
  const strippedName = stripQualitySuffix(cleanName)
  const placeNames = extractPlaceNames(cleanName)
  const queries = []
  const seen = new Set()

  function add(q) {
    if (!seen.has(q)) { seen.add(q); queries.push(q) }
  }

  // 1. Most specific: "appellation, region, country"
  add(`${cleanName}, ${region}, ${country}`)

  // 2. If quality suffix was stripped, try the base name
  if (strippedName !== cleanName) {
    add(`${strippedName}, ${region}, ${country}`)
  }

  // 3. "appellation wine region, country"
  add(`${cleanName} wine region, ${country}`)

  // 4. Extracted place names (e.g., "Vulture Superiore" then "Vulture")
  for (const place of placeNames) {
    add(`${place}, ${region}, ${country}`)
    add(`${place}, ${country}`)
  }

  // 5. Broader: just "appellation, country"
  if (placeNames.length === 0) {
    add(`${cleanName}, ${country}`)
  }

  // 6. Region fallback — at minimum get region-level accuracy
  if (cleanName.toLowerCase() !== region.toLowerCase()) {
    add(`${region} wine region, ${country}`)
    add(`${region}, ${country}`)
  }

  return queries
}

async function geocodeQuery(query) {
  const url = new URL(NOMINATIM_URL)
  url.searchParams.set('q', query)
  url.searchParams.set('format', 'json')
  url.searchParams.set('limit', '1')
  url.searchParams.set('addressdetails', '0')

  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Loam Wine Intelligence (loam.onrender.com)',
      'Accept-Language': 'en'
    }
  })

  if (!res.ok) {
    throw new Error(`Nominatim ${res.status}: ${res.statusText}`)
  }

  const data = await res.json()
  if (data.length > 0) {
    return {
      lat: parseFloat(data[0].lat),
      lng: parseFloat(data[0].lon),
      display_name: data[0].display_name,
      osm_type: data[0].osm_type,
      osm_id: data[0].osm_id
    }
  }
  return null
}

const VERBOSE = args.includes('--verbose')

async function geocodeAppellation(appellation, region, country) {
  const queries = buildSearchQueries(appellation, region, country)
  const regionFallbackQuery = `${region} wine region, ${country}`

  for (const query of queries) {
    await sleep(RATE_LIMIT_MS)
    if (VERBOSE) process.stdout.write(`     trying: "${query}" ... `)
    const result = await geocodeQuery(query)
    if (result) {
      if (VERBOSE) console.log(`✓`)
      const isRegionFallback = query === regionFallbackQuery
      return { ...result, query, regionFallback: isRegionFallback }
    }
    if (VERBOSE) console.log(`✗`)
  }

  return null
}

async function main() {
  console.log(`🌍 Geocoding appellations${DRY_RUN ? ' (DRY RUN)' : ''}`)
  console.log(`   Rate limit: ${RATE_LIMIT_MS}ms per request\n`)

  // Fetch appellations that don't yet have a boundary row
  const { data: appellations, error: fetchErr } = await supabase
    .from('appellations')
    .select(`
      id,
      name,
      designation_type,
      regions!inner (
        name,
        countries!inner (
          name
        )
      )
    `)
    .order('name')

  if (fetchErr) {
    console.error('Failed to fetch appellations:', fetchErr)
    process.exit(1)
  }

  // Check which already have boundaries
  const { data: existing, error: existErr } = await supabase
    .from('geographic_boundaries')
    .select('appellation_id')
    .not('appellation_id', 'is', null)

  if (existErr) {
    console.error('Failed to fetch existing boundaries:', existErr)
    process.exit(1)
  }

  const existingIds = new Set((existing || []).map(e => e.appellation_id))
  const toGeocode = appellations.filter(a => !existingIds.has(a.id))

  console.log(`   Total appellations: ${appellations.length}`)
  console.log(`   Already geocoded: ${existingIds.size}`)
  console.log(`   To geocode: ${toGeocode.length}`)
  if (LIMIT < Infinity) console.log(`   Limit: ${LIMIT}`)
  console.log()

  const batch = toGeocode.slice(0, LIMIT)
  let success = 0
  let regionFallbacks = 0
  let failed = 0
  const failures = []

  for (let i = 0; i < batch.length; i++) {
    const a = batch[i]
    const regionName = a.regions.name
    const countryName = a.regions.countries.name
    const progress = `[${i + 1}/${batch.length}]`

    try {
      const result = await geocodeAppellation(a.name, regionName, countryName)

      if (result) {
        const fallbackTag = result.regionFallback ? ' [region fallback]' : ''
        if (result.regionFallback) regionFallbacks++
        console.log(`${progress} ✓ ${a.name} (${regionName}, ${countryName}) → ${result.lat.toFixed(4)}, ${result.lng.toFixed(4)}${fallbackTag}`)

        if (!DRY_RUN) {
          // Build the WKT point for PostGIS
          const pointWkt = `SRID=4326;POINT(${result.lng} ${result.lat})`

          const { error: insertErr } = await supabase
            .from('geographic_boundaries')
            .insert({
              appellation_id: a.id,
              centroid: pointWkt,
              boundary_confidence: 'geocoded',
              boundary_source: 'nominatim',
              boundary_source_id: result.osm_id ? `${result.osm_type}/${result.osm_id}` : null,
              boundary_updated_at: new Date().toISOString()
            })

          if (insertErr) {
            console.error(`   ⚠ Insert failed: ${insertErr.message}`)
            failed++
            failures.push({ name: a.name, region: regionName, country: countryName, error: insertErr.message })
            continue
          }
        }
        success++
      } else {
        console.log(`${progress} ✗ ${a.name} (${regionName}, ${countryName}) — no results`)
        failed++
        failures.push({ name: a.name, region: regionName, country: countryName, error: 'no geocode results' })
      }
    } catch (err) {
      console.error(`${progress} ✗ ${a.name} — ${err.message}`)
      failed++
      failures.push({ name: a.name, region: regionName, country: countryName, error: err.message })
    }
  }

  console.log(`\n━━━ Summary ━━━`)
  console.log(`✓ Geocoded:          ${success}`)
  console.log(`  ↳ Region fallback: ${regionFallbacks}`)
  console.log(`✗ Failed:            ${failed}`)
  if (failures.length > 0) {
    console.log(`\nFailed appellations:`)
    for (const f of failures) {
      console.log(`  - ${f.name} (${f.region}, ${f.country}): ${f.error}`)
    }
  }
}

main().catch(console.error)
