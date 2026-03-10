#!/usr/bin/env node
/**
 * Import high-accuracy boundary polygons from authoritative sources:
 * 1. UC Davis AVA Project — official US AVA boundaries (CC0 license)
 * 2. Natural Earth — all country boundaries (public domain)
 *
 * These override any Nominatim-sourced commune polygons with the actual
 * wine appellation or country boundaries.
 *
 * Usage: node import_accurate_boundaries.mjs [--dry-run] [--countries-only] [--avas-only]
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
const COUNTRIES_ONLY = process.argv.includes('--countries-only')
const AVAS_ONLY = process.argv.includes('--avas-only')

// Manual name mapping: Loam name → UC Davis AVA name
const AVA_NAME_MAP = {
  'Mount Veeder': 'Mt. Veeder',
  'Moon Mountain District': 'Moon Mountain District Sonoma County',
  'San Luis Obispo': 'San Luis Obispo Coast',
  'San Benito County': 'San Benito',
  'San Luis Obispo County': 'San Luis Obispo Coast',
  'Contra Costa County': 'Contra Costa',
  'Mendocino County': 'Mendocino',
}

// Country name mapping: Loam name → Natural Earth name
const COUNTRY_NAME_MAP = {
  'United States': 'United States of America',
  'Czech Republic': 'Czechia',
  'South Korea': 'South Korea',
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== Accurate Boundary Importer ===`)
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}`)

  // =========================================================================
  // Part 1: US AVA Boundaries
  // =========================================================================
  if (!COUNTRIES_ONLY) {
    console.log(`\n--- US AVA Boundaries (UC Davis) ---`)

    let avaData
    try {
      avaData = JSON.parse(readFileSync('avas_ucdavis.geojson', 'utf8'))
    } catch {
      console.log('ERROR: avas_ucdavis.geojson not found. Download from https://github.com/UCDavisLibrary/ava')
      if (!AVAS_ONLY) console.log('Skipping AVAs, continuing to countries...')
      else return
    }

    if (avaData) {
      // Build lookup: lowercase name → feature
      const avaLookup = new Map()
      for (const f of avaData.features) {
        avaLookup.set(f.properties.name.toLowerCase(), f)
        // Also index by AKA
        if (f.properties.aka) {
          for (const aka of f.properties.aka.split('|')) {
            avaLookup.set(aka.trim().toLowerCase(), f)
          }
        }
      }
      console.log(`Loaded ${avaData.features.length} AVA boundaries`)

      // Get all US appellations with their boundary rows
      const { data: usApps } = await supabase
        .from('appellations')
        .select('id, name, regions!inner(countries!inner(name))')
        .eq('regions.countries.name', 'United States')

      // Get boundary rows for these appellations
      const appIds = (usApps || []).map(a => a.id)
      const { data: boundaries } = await supabase
        .from('geographic_boundaries')
        .select('id, appellation_id')
        .in('appellation_id', appIds)

      const boundaryByAppId = new Map((boundaries || []).map(b => [b.appellation_id, b.id]))

      let avaSuccess = 0, avaFailed = 0, avaSkipped = 0
      const avaFailures = []

      for (const app of (usApps || [])) {
        const boundaryId = boundaryByAppId.get(app.id)
        if (!boundaryId) {
          avaSkipped++
          continue
        }

        // Try direct match, then mapped name
        let avaFeature = avaLookup.get(app.name.toLowerCase())
        if (!avaFeature && AVA_NAME_MAP[app.name]) {
          avaFeature = avaLookup.get(AVA_NAME_MAP[app.name].toLowerCase())
        }

        if (!avaFeature) {
          avaFailed++
          avaFailures.push(app.name)
          continue
        }

        const geojson = avaFeature.geometry
        if (!geojson || (geojson.type !== 'Polygon' && geojson.type !== 'MultiPolygon')) {
          avaFailed++
          avaFailures.push(`${app.name} (not a polygon: ${geojson?.type})`)
          continue
        }

        process.stdout.write(`  ${app.name} → ${avaFeature.properties.name}...`)

        if (DRY_RUN) {
          console.log(` ✓ (${geojson.type})`)
          avaSuccess++
          continue
        }

        const geojsonStr = JSON.stringify(geojson)
        const { error } = await supabase.rpc('update_boundary_polygon', {
          p_boundary_id: boundaryId,
          p_geojson: geojsonStr,
          p_source_id: `ucdavis-ava/${avaFeature.properties.ava_id}`,
        })

        if (error) {
          console.log(` ✗ DB error: ${error.message}`)
          avaFailed++
          avaFailures.push(`${app.name} (DB: ${error.message})`)
        } else {
          console.log(` ✓`)
          avaSuccess++
        }
      }

      console.log(`\nAVA Results: ${avaSuccess} success, ${avaFailed} failed, ${avaSkipped} skipped (no boundary row)`)
      if (avaFailures.length > 0) {
        console.log('Failed:', avaFailures.join(', '))
      }
    }
  }

  // =========================================================================
  // Part 2: Country Boundaries
  // =========================================================================
  if (!AVAS_ONLY) {
    console.log(`\n--- Country Boundaries (Natural Earth) ---`)

    let countryData
    try {
      countryData = JSON.parse(readFileSync('countries_naturalearth.geojson', 'utf8'))
    } catch {
      console.log('ERROR: countries_naturalearth.geojson not found.')
      return
    }

    // Build lookup: name + ISO codes
    const countryLookup = new Map()
    for (const f of countryData.features) {
      countryLookup.set(f.properties.name.toLowerCase(), f)
      if (f.properties['ISO3166-1-Alpha-2']) {
        countryLookup.set(f.properties['ISO3166-1-Alpha-2'].toLowerCase(), f)
      }
    }
    console.log(`Loaded ${countryData.features.length} country boundaries`)

    // Get all countries in our DB
    const { data: countries } = await supabase
      .from('countries')
      .select('id, name, iso_code')

    let countrySuccess = 0, countryFailed = 0

    for (const c of (countries || [])) {
      // Try ISO code match first, then name, then mapped name
      let feature = null
      if (c.iso_code) feature = countryLookup.get(c.iso_code.toLowerCase())
      if (!feature) feature = countryLookup.get(c.name.toLowerCase())
      if (!feature && COUNTRY_NAME_MAP[c.name]) {
        feature = countryLookup.get(COUNTRY_NAME_MAP[c.name].toLowerCase())
      }

      if (!feature) {
        countryFailed++
        console.log(`  ${c.name} ✗ not found in Natural Earth`)
        continue
      }

      const geojson = feature.geometry
      if (!geojson) {
        countryFailed++
        console.log(`  ${c.name} ✗ no geometry`)
        continue
      }

      process.stdout.write(`  ${c.name} (${c.iso_code || '??'}) → ${feature.properties.name}...`)

      if (DRY_RUN) {
        console.log(` ✓ ${geojson.type}`)
        countrySuccess++
        continue
      }

      const geojsonStr = JSON.stringify(geojson)
      // Use the upsert RPC — handles both insert and update
      const { error } = await supabase.rpc('insert_country_boundary', {
        p_country_id: c.id,
        p_geojson: geojsonStr,
        p_source_id: 'natural-earth',
      })
      if (error) {
        console.log(` ✗ error: ${error.message}`)
        countryFailed++
      } else {
        console.log(` ✓`)
        countrySuccess++
      }
    }

    console.log(`\nCountry Results: ${countrySuccess} success, ${countryFailed} failed`)
  }

  console.log(`\n=== Done ===`)
}

main().catch(console.error)
