#!/usr/bin/env node
/**
 * import_us_avas.mjs
 *
 * Imports all 276 US AVAs from the UC Davis GeoJSON into the Loam database.
 * Three phases:
 *   Phase 0: Create missing state-level regions + fetch Nominatim polygons
 *   Phase 1: Create missing appellation records
 *   Phase 2: Import AVA boundary polygons from UC Davis GeoJSON
 *
 * Usage:
 *   node import_us_avas.mjs              # full run
 *   node import_us_avas.mjs --dry-run    # preview only
 *   node import_us_avas.mjs --skip-regions   # skip Phase 0 (regions already created)
 *   node import_us_avas.mjs --skip-boundaries # skip Phase 2 (just create appellations)
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

// ---------------------------------------------------------------------------
// .env loading (root .env with service role key for write access)
// ---------------------------------------------------------------------------
const envPath = new URL('.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envLines = readFileSync(envPath, 'utf8').split('\n');
const env = {};
for (const l of envLines) {
  const m = l.replace(/\r/g, '').match(/^(\w+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE);

const DRY_RUN = process.argv.includes('--dry-run');
const SKIP_REGIONS = process.argv.includes('--skip-regions');
const SKIP_BOUNDARIES = process.argv.includes('--skip-boundaries');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function slugify(s) {
  return s.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/['']/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Compute centroid from GeoJSON geometry by averaging all coordinates
function computeCentroid(geojson) {
  let totalLat = 0, totalLng = 0, count = 0;
  function extract(coords) {
    if (typeof coords[0] === 'number') {
      totalLng += coords[0];
      totalLat += coords[1];
      count++;
    } else {
      coords.forEach(c => extract(c));
    }
  }
  if (geojson.type === 'GeometryCollection') {
    geojson.geometries.forEach(g => extract(g.coordinates));
  } else {
    extract(geojson.coordinates);
  }
  return count > 0 ? { lat: totalLat / count, lng: totalLng / count } : null;
}

async function nominatimSearch(query) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&polygon_geojson=1&limit=1`;
    const res = await fetch(url, {
      headers: { 'User-Agent': 'LoamWineApp/1.0 (contact@loam.wine)' }
    });
    if (res.status === 429) {
      const wait = 5000 * (attempt + 1);
      process.stdout.write(`[rate-limited ${wait/1000}s] `);
      await sleep(wait);
      continue;
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }
  throw new Error('Rate limited after retries');
}

// ---------------------------------------------------------------------------
// State code → state name mapping
// ---------------------------------------------------------------------------
const STATE_NAMES = {
  'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
  'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
  'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
  'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
  'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
  'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
  'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
  'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
  'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
  'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
  'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
  'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
  'WI': 'Wisconsin', 'WY': 'Wyoming',
};

// Resolve a GeoJSON `state` field (may be "CA", "OR|WA", "Tennessee", etc.) to a primary state code
function primaryStateCode(stateField) {
  if (!stateField) return null;
  // Handle full state names (e.g., "Tennessee", "Washington")
  for (const [code, name] of Object.entries(STATE_NAMES)) {
    if (stateField.toLowerCase() === name.toLowerCase()) return code;
  }
  // Handle pipe-delimited multi-state (e.g., "OR|WA") — use first
  const first = stateField.split(/[|,]/)[0].trim().replace(/\s/g, '');
  if (first.length === 2 && STATE_NAMES[first.toUpperCase()]) return first.toUpperCase();
  return null;
}

// ---------------------------------------------------------------------------
// Name matching: DB appellation name ↔ GeoJSON AVA name
// ---------------------------------------------------------------------------
// DB name → GeoJSON name (for existing DB appellations that have different names in GeoJSON)
const DB_TO_GEOJSON = {
  'Mount Veeder': 'Mt. Veeder',
  'Moon Mountain District': 'Moon Mountain District Sonoma County',
  'San Luis Obispo': 'San Luis Obispo Coast',
  'San Benito County': 'San Benito',
  'Contra Costa County': 'Contra Costa',
  'Mendocino County': 'Mendocino',
  'San Luis Obispo County': 'San Luis Obispo Coast',
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== US AVA Importer ===`);
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}\n`);

  // Load GeoJSON
  const geoData = JSON.parse(readFileSync('avas_ucdavis.geojson', 'utf8'));
  console.log(`Loaded ${geoData.features.length} AVA features from GeoJSON`);

  // Get US country ID
  const { data: usCountry } = await sb.from('countries').select('id').eq('name', 'United States').single();
  if (!usCountry) { console.error('ERROR: United States not found in countries table'); process.exit(1); }
  const usCountryId = usCountry.id;
  console.log(`US country ID: ${usCountryId}`);

  // Determine which states have AVAs
  const statesWithAvas = new Set();
  for (const f of geoData.features) {
    const code = primaryStateCode(f.properties.state);
    if (code) statesWithAvas.add(code);
  }
  console.log(`States with AVAs: ${[...statesWithAvas].sort().join(', ')} (${statesWithAvas.size} states)`);

  // =========================================================================
  // Phase 0: Create missing state-level regions with Nominatim boundaries
  // =========================================================================
  if (!SKIP_REGIONS) {
    console.log(`\n--- Phase 0: State-Level Regions ---`);

    // Get existing US regions
    const { data: existingRegions } = await sb.from('regions')
      .select('id, name, parent_id')
      .eq('country_id', usCountryId);

    const regionByName = new Map(existingRegions.map(r => [r.name.toLowerCase(), r]));

    // Special case: Texas — create parent "Texas" region, re-parent "Texas Hill Country"
    const texasHillCountry = regionByName.get('texas hill country');
    let texasRegion = regionByName.get('texas');

    if (!texasRegion && statesWithAvas.has('TX')) {
      console.log(`  Creating "Texas" parent region...`);
      if (!DRY_RUN) {
        const { data, error } = await sb.from('regions').insert({
          name: 'Texas',
          slug: 'texas',
          country_id: usCountryId,
          is_catch_all: false,
        }).select('id').single();
        if (error) {
          console.error(`    ERROR: ${error.message}`);
        } else {
          texasRegion = { id: data.id, name: 'Texas' };
          regionByName.set('texas', texasRegion);
          console.log(`    Created: ${data.id}`);

          // Re-parent Texas Hill Country under Texas
          if (texasHillCountry && !texasHillCountry.parent_id) {
            const { error: upErr } = await sb.from('regions')
              .update({ parent_id: data.id })
              .eq('id', texasHillCountry.id);
            if (upErr) console.error(`    Re-parent error: ${upErr.message}`);
            else console.log(`    Re-parented "Texas Hill Country" under "Texas"`);
          }
        }
      } else {
        console.log(`    [dry-run] Would create "Texas" and re-parent "Texas Hill Country"`);
      }
    }

    // Create missing state regions
    const missingStates = [];
    for (const code of statesWithAvas) {
      const stateName = STATE_NAMES[code];
      if (!stateName) continue;
      if (regionByName.has(stateName.toLowerCase())) continue;
      missingStates.push({ code, name: stateName });
    }

    console.log(`  Missing state regions: ${missingStates.length}`);
    for (const state of missingStates) {
      process.stdout.write(`  Creating "${state.name}" (${state.code})... `);
      if (!DRY_RUN) {
        const { data, error } = await sb.from('regions').insert({
          name: state.name,
          slug: slugify(state.name),
          country_id: usCountryId,
          is_catch_all: false,
        }).select('id').single();
        if (error) {
          console.log(`ERROR: ${error.message}`);
        } else {
          regionByName.set(state.name.toLowerCase(), { id: data.id, name: state.name });
          console.log(`✓ (${data.id})`);
        }
      } else {
        console.log(`[dry-run]`);
      }
    }

    // Fetch Nominatim boundaries for all newly created + existing-but-no-boundary regions
    console.log(`\n  Fetching Nominatim boundaries for state regions...`);

    // Get existing region boundary IDs
    const allRegionIds = [...regionByName.values()].map(r => r.id);
    const { data: existingBoundaries } = await sb.from('geographic_boundaries')
      .select('id, region_id')
      .in('region_id', allRegionIds);
    const regionsWithBoundary = new Set((existingBoundaries || []).map(b => b.region_id));

    // Only fetch for states that have AVAs AND don't have boundaries yet
    const stateRegionsNeedingBoundary = [];
    for (const code of statesWithAvas) {
      const stateName = STATE_NAMES[code];
      if (!stateName) continue;
      const region = regionByName.get(stateName.toLowerCase());
      if (!region) continue;
      if (regionsWithBoundary.has(region.id)) continue;
      stateRegionsNeedingBoundary.push({ code, name: stateName, id: region.id });
    }

    console.log(`  State regions needing boundaries: ${stateRegionsNeedingBoundary.length}`);

    for (let i = 0; i < stateRegionsNeedingBoundary.length; i++) {
      const state = stateRegionsNeedingBoundary[i];
      process.stdout.write(`  [${i+1}/${stateRegionsNeedingBoundary.length}] ${state.name}... `);

      await sleep(1100); // Nominatim rate limit

      try {
        const results = await nominatimSearch(`${state.name}, United States`);
        if (!results.length) {
          console.log(`✗ no results`);
          continue;
        }
        const result = results[0];
        const geojson = result.geojson;
        const hasPolygon = geojson && ['Polygon', 'MultiPolygon'].includes(geojson.type);

        if (!DRY_RUN) {
          const { error } = await sb.rpc('upsert_region_boundary', {
            p_region_id: state.id,
            p_geojson: hasPolygon ? JSON.stringify(geojson) : null,
            p_source_id: `nominatim/${result.osm_type}/${result.osm_id}`,
            p_confidence: hasPolygon ? 'approximate' : 'geocoded',
            ...(hasPolygon ? {} : { p_lat: parseFloat(result.lat), p_lng: parseFloat(result.lon) }),
          });
          if (error) {
            console.log(`✗ RPC error: ${error.message}`);
          } else {
            console.log(`✓ ${hasPolygon ? geojson.type : 'centroid'}`);
          }
        } else {
          console.log(`[dry-run] ${hasPolygon ? geojson.type : 'centroid'}`);
        }
      } catch (e) {
        console.log(`✗ ${e.message}`);
      }
    }

    // Refresh region map after all creates
    const { data: allRegions } = await sb.from('regions')
      .select('id, name')
      .eq('country_id', usCountryId);
    regionByName.clear();
    for (const r of allRegions) regionByName.set(r.name.toLowerCase(), r);
  }

  // =========================================================================
  // Phase 1: Create missing appellation records
  // =========================================================================
  console.log(`\n--- Phase 1: Appellation Records ---`);

  // Reload current regions
  const { data: usRegions } = await sb.from('regions')
    .select('id, name')
    .eq('country_id', usCountryId);
  const regionByName = new Map(usRegions.map(r => [r.name.toLowerCase(), r]));

  // Build state code → region_id map
  const stateToRegionId = new Map();
  for (const [code, name] of Object.entries(STATE_NAMES)) {
    const region = regionByName.get(name.toLowerCase());
    if (region) stateToRegionId.set(code, region.id);
  }
  console.log(`  State→Region mappings: ${stateToRegionId.size}`);

  // Get existing US appellations
  const { data: existingApps } = await sb.from('appellations')
    .select('id, name')
    .eq('country_id', usCountryId);

  // Build lookup: lowercase name → appellation
  const appByName = new Map();
  for (const a of (existingApps || [])) {
    appByName.set(a.name.toLowerCase(), a);
  }
  console.log(`  Existing US appellations: ${appByName.size}`);

  // Build GeoJSON lookup: lowercase name → feature (including aka)
  const geoLookup = new Map();
  for (const f of geoData.features) {
    geoLookup.set(f.properties.name.toLowerCase(), f);
    if (f.properties.aka) {
      for (const aka of f.properties.aka.split('|')) {
        geoLookup.set(aka.trim().toLowerCase(), f);
      }
    }
  }

  // Match existing DB appellations to GeoJSON features
  const matchedGeoNames = new Set(); // Track which GeoJSON AVAs are already in DB
  let existingMatched = 0;
  for (const [name, app] of appByName) {
    // Direct match
    let geoFeature = geoLookup.get(name);
    // Try mapped name
    if (!geoFeature && DB_TO_GEOJSON[app.name]) {
      geoFeature = geoLookup.get(DB_TO_GEOJSON[app.name].toLowerCase());
    }
    if (geoFeature) {
      matchedGeoNames.add(geoFeature.properties.name.toLowerCase());
      existingMatched++;
    }
  }
  console.log(`  Existing appellations matched to GeoJSON: ${existingMatched}`);

  // Find GeoJSON AVAs not yet in DB
  const toCreate = [];
  for (const f of geoData.features) {
    const geoName = f.properties.name;
    if (matchedGeoNames.has(geoName.toLowerCase())) continue;
    // Also check if the name already exists in DB directly
    if (appByName.has(geoName.toLowerCase())) {
      matchedGeoNames.add(geoName.toLowerCase());
      continue;
    }

    // Resolve region
    const stateCode = primaryStateCode(f.properties.state);
    const regionId = stateCode ? stateToRegionId.get(stateCode) : null;

    if (!regionId) {
      console.log(`  ⚠ No region for "${geoName}" (state: ${f.properties.state})`);
      // Fallback: use the catch-all "United States" region if it exists
      const usRegion = regionByName.get('united states');
      if (usRegion) {
        toCreate.push({ feature: f, regionId: usRegion.id });
      }
      continue;
    }

    toCreate.push({ feature: f, regionId });
  }

  console.log(`  AVAs to create: ${toCreate.length}`);

  let created = 0, createErrors = 0;
  for (const { feature, regionId } of toCreate) {
    const name = feature.properties.name;
    const centroid = computeCentroid(feature.geometry);

    process.stdout.write(`  + ${name}... `);

    if (!DRY_RUN) {
      const { data, error } = await sb.from('appellations').insert({
        name,
        slug: slugify(name),
        designation_type: 'AVA',
        country_id: usCountryId,
        region_id: regionId,
        hemisphere: 'north',
        latitude: centroid ? Math.round(centroid.lat * 1000) / 1000 : null,
        longitude: centroid ? Math.round(centroid.lng * 1000) / 1000 : null,
        growing_season_start_month: 3,
        growing_season_end_month: 10,
      }).select('id').single();

      if (error) {
        console.log(`✗ ${error.message}`);
        createErrors++;
      } else {
        console.log(`✓`);
        appByName.set(name.toLowerCase(), { id: data.id, name });
        created++;
      }
    } else {
      console.log(`[dry-run]`);
      created++;
    }
  }

  console.log(`\n  Phase 1 Results: ${created} created, ${createErrors} errors, ${existingMatched} already existed`);

  // =========================================================================
  // Phase 2: Import boundary polygons
  // =========================================================================
  if (!SKIP_BOUNDARIES) {
    console.log(`\n--- Phase 2: AVA Boundary Polygons ---`);

    // Refresh appellation list
    const { data: allApps } = await sb.from('appellations')
      .select('id, name')
      .eq('country_id', usCountryId);
    const appLookup = new Map(allApps.map(a => [a.name.toLowerCase(), a]));

    // Get existing boundary rows for appellations
    const appIds = allApps.map(a => a.id);
    let existingBounds = [];
    // Batch in groups of 200 to avoid URL length limits
    for (let i = 0; i < appIds.length; i += 200) {
      const batch = appIds.slice(i, i + 200);
      const { data } = await sb.from('geographic_boundaries')
        .select('id, appellation_id')
        .in('appellation_id', batch);
      if (data) existingBounds.push(...data);
    }
    const boundaryByAppId = new Map(existingBounds.map(b => [b.appellation_id, b.id]));
    console.log(`  Existing boundary rows: ${boundaryByAppId.size}`);

    let polySuccess = 0, polyFailed = 0, polySkipped = 0;

    for (const feature of geoData.features) {
      const geoName = feature.properties.name;
      const geojson = feature.geometry;

      if (!geojson || (geojson.type !== 'Polygon' && geojson.type !== 'MultiPolygon')) {
        polySkipped++;
        continue;
      }

      // Find the matching DB appellation
      let app = appLookup.get(geoName.toLowerCase());
      if (!app) {
        // Try aka
        if (feature.properties.aka) {
          for (const aka of feature.properties.aka.split('|')) {
            app = appLookup.get(aka.trim().toLowerCase());
            if (app) break;
          }
        }
      }
      // Try reverse mapping (DB names that map to this GeoJSON name)
      if (!app) {
        for (const [dbName, geoNameMapped] of Object.entries(DB_TO_GEOJSON)) {
          if (geoNameMapped.toLowerCase() === geoName.toLowerCase()) {
            app = appLookup.get(dbName.toLowerCase());
            if (app) break;
          }
        }
      }

      if (!app) {
        console.log(`  ⚠ No DB match for "${geoName}"`);
        polyFailed++;
        continue;
      }

      const boundaryId = boundaryByAppId.get(app.id);

      process.stdout.write(`  ${app.name}... `);

      if (DRY_RUN) {
        console.log(`[dry-run] ${boundaryId ? 'update' : 'create'} ${geojson.type}`);
        polySuccess++;
        continue;
      }

      // Use upsert_appellation_boundary RPC — handles both insert and update
      const { error } = await sb.rpc('upsert_appellation_boundary', {
        p_appellation_id: app.id,
        p_geojson: JSON.stringify(geojson),
        p_source_id: `ucdavis-ava/${feature.properties.ava_id}`,
        p_confidence: 'official',
      });
      if (error) {
        console.log(`✗ ${error.message}`);
        polyFailed++;
      } else {
        console.log(`✓ ${boundaryId ? 'updated' : 'created'}`);
        polySuccess++;
      }
    }

    console.log(`\n  Phase 2 Results: ${polySuccess} polygons set, ${polyFailed} failed, ${polySkipped} skipped (no polygon geometry)`);
  }

  // =========================================================================
  // Summary
  // =========================================================================
  console.log(`\n========================================`);
  console.log(`   US AVA IMPORT COMPLETE`);
  console.log(`========================================`);

  // Final counts
  const { data: finalApps } = await sb.from('appellations')
    .select('id', { count: 'exact' })
    .eq('country_id', usCountryId);
  console.log(`  Total US appellations in DB: ${finalApps?.length || '?'}`);

  const { data: finalBounds } = await sb.from('geographic_boundaries')
    .select('id, appellation_id')
    .not('appellation_id', 'is', null);
  const usAppIds = new Set((await sb.from('appellations').select('id').eq('country_id', usCountryId)).data?.map(a => a.id) || []);
  const usBounds = (finalBounds || []).filter(b => usAppIds.has(b.appellation_id));
  console.log(`  US appellations with boundaries: ${usBounds.length}`);
}

main().catch(console.error);
