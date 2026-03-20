#!/usr/bin/env node
/**
 * import_australia_gi.mjs
 *
 * Imports Australian GI (Geographical Indication) wine regions and subregions
 * from official Wine Australia GeoJSON data into the Loam database.
 *
 * Data source: Wine Australia (https://www.wineaustralia.com)
 * Three GI tiers: Zone (28) → Region (64) → Subregion (14)
 * We import regions + subregions as appellations (78 total).
 *
 * Phases:
 *   Phase 0: Load GeoJSON files
 *   Phase 1: Match/create appellations (regions + subregions)
 *   Phase 2: Import boundary polygons
 *
 * Usage:
 *   node import_australia_gi.mjs              # full run
 *   node import_australia_gi.mjs --dry-run    # preview only
 *   node import_australia_gi.mjs --boundaries-only  # skip Phase 1
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

// ---------------------------------------------------------------------------
// .env loading
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
const BOUNDARIES_ONLY = process.argv.includes('--boundaries-only');

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
  extract(geojson.coordinates);
  return count > 0 ? { lat: totalLat / count, lng: totalLng / count } : null;
}

function simplifyPrecision(geojson) {
  function roundCoords(coords) {
    if (typeof coords[0] === 'number') {
      return [Math.round(coords[0] * 100000) / 100000, Math.round(coords[1] * 100000) / 100000];
    }
    return coords.map(c => roundCoords(c));
  }
  return { type: geojson.type, coordinates: roundCoords(geojson.coordinates) };
}

/**
 * Douglas-Peucker line simplification.
 * Tolerance is in degrees (~0.001 = ~100m at equator).
 */
function simplifyLine(points, tolerance) {
  if (points.length <= 2) return points;
  let maxDist = 0, maxIdx = 0;
  const [ax, ay] = points[0], [bx, by] = points[points.length - 1];
  const dx = bx - ax, dy = by - ay;
  const lenSq = dx * dx + dy * dy;
  for (let i = 1; i < points.length - 1; i++) {
    let dist;
    if (lenSq === 0) {
      dist = Math.sqrt((points[i][0] - ax) ** 2 + (points[i][1] - ay) ** 2);
    } else {
      const t = Math.max(0, Math.min(1, ((points[i][0] - ax) * dx + (points[i][1] - ay) * dy) / lenSq));
      dist = Math.sqrt((points[i][0] - (ax + t * dx)) ** 2 + (points[i][1] - (ay + t * dy)) ** 2);
    }
    if (dist > maxDist) { maxDist = dist; maxIdx = i; }
  }
  if (maxDist > tolerance) {
    const left = simplifyLine(points.slice(0, maxIdx + 1), tolerance);
    const right = simplifyLine(points.slice(maxIdx), tolerance);
    return left.slice(0, -1).concat(right);
  }
  return [points[0], points[points.length - 1]];
}

function simplifyGeometry(geojson, tolerance) {
  function simplifyRing(ring) {
    const simplified = simplifyLine(ring, tolerance);
    return simplified.length >= 4 ? simplified : ring; // polygon rings need >= 4 points
  }
  function simplifyCoords(coords, depth) {
    if (depth === 0) return simplifyRing(coords);
    return coords.map(c => simplifyCoords(c, depth - 1));
  }
  const depth = geojson.type === 'MultiPolygon' ? 2 : geojson.type === 'Polygon' ? 1 : 0;
  return { type: geojson.type, coordinates: simplifyCoords(geojson.coordinates, depth) };
}

// Map Wine Australia state codes to our region names
const STATE_TO_REGION = {
  'SA': 'South Australia',
  'VIC': 'Victoria',
  'NSW': 'New South Wales',
  'WA': 'Western Australia',
  'TAS': 'Tasmania',
  'QLD': null,  // No existing region — use catch-all
  'ACT': null,
};

// Name normalization for matching: Wine Australia uses slightly different names
const NAME_MAP = {
  'Mclaren Vale': 'McLaren Vale',  // Capitalize correctly
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('=== Wine Australia GI Import ===');
  if (DRY_RUN) console.log('[DRY RUN MODE]');

  // -----------------------------------------------------------------------
  // Phase 0: Load GeoJSON data
  // -----------------------------------------------------------------------
  console.log('\n--- Phase 0: Loading data ---');

  const regionsGeoJSON = JSON.parse(readFileSync('data/geo/wine_australia_regions.geojson', 'utf8'));
  const subregionsGeoJSON = JSON.parse(readFileSync('data/geo/wine_australia_subregions.geojson', 'utf8'));

  console.log(`  Regions: ${regionsGeoJSON.features.length} features`);
  console.log(`  Subregions: ${subregionsGeoJSON.features.length} features`);

  // Combine regions + subregions into a single list
  const allGIs = [];

  for (const f of regionsGeoJSON.features) {
    const name = NAME_MAP[f.properties.GI_NAME] || f.properties.GI_NAME;
    allGIs.push({
      name,
      giNumber: f.properties.GI_NUMBER,
      giType: f.properties.GI_TYPE,         // "region"
      yearRegistered: f.properties.YEAR_REGISTERED || f.properties.YEAR_REGISTER,
      state: f.properties.STATE,
      giUrl: f.properties.GI_URL,
      areaKm2: f.properties.GI_AREA_KM2,
      altitudeLow: f.properties.ALTITUDE_LOW,
      altitudeHigh: f.properties.ALTITUDE_HIGH,
      vineAreaHa: f.properties.VINE_AREA_HA,
      growDegreeDays: f.properties.GROW_DEGREE_DAYS_OCT_APR,
      meanJanTemp: f.properties.MEAN_JAN_TEMP,
      annualRain: f.properties.ANNUAL_RAIN_JULY_JUN,
      growSeasonRain: f.properties.GROW_SEASON_RAIN_OCT_APR,
      geometry: f.geometry,
    });
  }

  for (const f of subregionsGeoJSON.features) {
    const name = NAME_MAP[f.properties.GI_NAME] || f.properties.GI_NAME;
    allGIs.push({
      name,
      giNumber: f.properties.GI_NUMBER,
      giType: f.properties.GI_TYPE,         // "subregion"
      yearRegistered: f.properties.YEAR_REGISTERED || f.properties.YEAR_REGISTER,
      state: f.properties.STATE,
      giUrl: f.properties.GI_URL,
      areaKm2: f.properties.GI_AREA_KM2,
      vineAreaHa: f.properties.VINE_AREA_HA,
      geometry: f.geometry,
    });
  }

  console.log(`  Total GIs to process: ${allGIs.length}`);

  // -----------------------------------------------------------------------
  // Load DB reference data
  // -----------------------------------------------------------------------
  console.log('\n--- Loading DB reference data ---');

  const { data: countries } = await sb.from('countries').select('id, name, iso_code');
  const australia = countries.find(c => c.iso_code === 'AU');
  if (!australia) throw new Error('Australia not found in countries table');
  console.log(`  Australia ID: ${australia.id}`);

  // Load all Australian regions
  const { data: auRegions } = await sb.from('regions')
    .select('id, name, is_catch_all, country_id')
    .eq('country_id', australia.id);
  const regionByName = new Map();
  let catchAllRegion = null;
  for (const r of auRegions) {
    regionByName.set(r.name.toLowerCase(), r);
    if (r.is_catch_all) catchAllRegion = r;
  }
  console.log(`  ${auRegions.length} Australian regions loaded`);

  // Load existing Australian appellations (paginated)
  let existingApps = [];
  let offset = 0;
  const PAGE_SIZE = 1000;
  while (true) {
    const { data: page, error } = await sb.from('appellations')
      .select('id, name, slug, country_id, region_id, designation_type')
      .eq('country_id', australia.id)
      .range(offset, offset + PAGE_SIZE - 1);
    if (error) throw new Error(`Failed to fetch appellations: ${error.message}`);
    existingApps = existingApps.concat(page);
    if (page.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  console.log(`  ${existingApps.length} existing Australian appellations`);

  // Build lookup maps
  const appByName = new Map();
  const appBySlug = new Map();
  for (const app of existingApps) {
    appByName.set(app.name.toLowerCase(), app);
    appBySlug.set(app.slug, app);
  }

  // -----------------------------------------------------------------------
  // Phase 1: Match / Create Appellations
  // -----------------------------------------------------------------------
  const stats = { matched: 0, created: 0, boundariesUpdated: 0, errors: 0 };
  const giToAppId = new Map(); // giNumber -> appellation_id

  if (!BOUNDARIES_ONLY) {
    console.log('\n--- Phase 1: Matching appellations ---');

    for (const gi of allGIs) {
      // Try to match existing
      let app = appByName.get(gi.name.toLowerCase()) || appBySlug.get(slugify(gi.name));

      if (app) {
        giToAppId.set(gi.giNumber, app.id);
        stats.matched++;
        continue;
      }

      // Find region_id for this GI
      const stateRegionName = STATE_TO_REGION[gi.state];
      let regionId = catchAllRegion?.id;
      if (stateRegionName) {
        const stateRegion = regionByName.get(stateRegionName.toLowerCase());
        if (stateRegion) regionId = stateRegion.id;
      }

      // Compute centroid
      let lat = null, lng = null;
      if (gi.geometry) {
        const centroid = computeCentroid(gi.geometry);
        if (centroid) { lat = centroid.lat; lng = centroid.lng; }
      }

      const newApp = {
        name: gi.name,
        slug: slugify(gi.name),
        country_id: australia.id,
        region_id: regionId,
        designation_type: 'GI',
        latitude: lat ? Math.round(lat * 100000) / 100000 : null,
        longitude: lng ? Math.round(lng * 100000) / 100000 : null,
        hemisphere: 'south',
        growing_season_start_month: 10,   // Oct-Apr in southern hemisphere
        growing_season_end_month: 4,
        established_year: gi.yearRegistered || null,
        regulatory_url: gi.giUrl || null,
      };

      if (DRY_RUN) {
        console.log(`  [DRY] Would create: ${gi.name} (${gi.giType}, ${gi.state})`);
        stats.created++;
        continue;
      }

      const { data: inserted, error } = await sb.from('appellations')
        .insert(newApp)
        .select('id')
        .single();

      if (error) {
        if (error.code === '23505') {
          // Slug conflict — try with country suffix
          newApp.slug = slugify(`${gi.name}-australia`);
          const { data: retry, error: retryErr } = await sb.from('appellations')
            .insert(newApp)
            .select('id')
            .single();
          if (retryErr) {
            console.log(`  [ERROR] Creating ${gi.name}: ${retryErr.message}`);
            stats.errors++;
            continue;
          }
          giToAppId.set(gi.giNumber, retry.id);
          appByName.set(gi.name.toLowerCase(), { id: retry.id, ...newApp });
          appBySlug.set(newApp.slug, { id: retry.id, ...newApp });
        } else {
          console.log(`  [ERROR] Creating ${gi.name}: ${error.message}`);
          stats.errors++;
          continue;
        }
      } else {
        giToAppId.set(gi.giNumber, inserted.id);
        appByName.set(gi.name.toLowerCase(), { id: inserted.id, ...newApp });
        appBySlug.set(newApp.slug, { id: inserted.id, ...newApp });
      }

      stats.created++;
    }

    console.log(`\n  Phase 1 complete: ${stats.matched} matched, ${stats.created} created, ${stats.errors} errors`);
  }

  // -----------------------------------------------------------------------
  // Phase 2: Import Boundary Polygons
  // -----------------------------------------------------------------------
  console.log('\n--- Phase 2: Importing boundaries ---');

  let processed = 0;
  for (const gi of allGIs) {
    let appId = giToAppId.get(gi.giNumber);

    // For --boundaries-only mode, try lookup
    if (!appId) {
      const app = appByName.get(gi.name.toLowerCase()) || appBySlug.get(slugify(gi.name));
      if (app) appId = app.id;
    }

    if (!appId) continue;
    if (!gi.geometry) {
      console.log(`  [SKIP] No geometry for ${gi.name}`);
      continue;
    }

    try {
      let simplified = simplifyPrecision(gi.geometry);
      // Apply Douglas-Peucker if geometry is too large (>50K coords)
      const geoStr = JSON.stringify(simplified);
      if (geoStr.length > 1_000_000) {
        simplified = simplifyGeometry(simplified, 0.005); // ~500m tolerance
        console.log(`  [SIMPLIFY] ${gi.name}: ${geoStr.length} -> ${JSON.stringify(simplified).length} bytes`);
      }

      if (DRY_RUN) {
        const centroid = computeCentroid(simplified);
        console.log(`  [DRY] Would import boundary for ${gi.name} (${simplified.type}, centroid: ${centroid?.lat?.toFixed(2)}, ${centroid?.lng?.toFixed(2)})`);
        stats.boundariesUpdated++;
        processed++;
        continue;
      }

      const { error } = await sb.rpc('upsert_appellation_boundary', {
        p_appellation_id: appId,
        p_geojson: JSON.stringify(simplified),
        p_source_id: `wine-australia/${gi.giNumber}`,
        p_confidence: 'official',  // Official Wine Australia boundaries
      });

      if (error) {
        console.log(`  [ERROR] Boundary for ${gi.name}: ${error.message}`);
        stats.errors++;
      } else {
        stats.boundariesUpdated++;
      }

      processed++;
      if (processed % 20 === 0) {
        console.log(`  Processed ${processed}/${allGIs.length} boundaries...`);
        await sleep(100);
      }
    } catch (e) {
      console.log(`  [ERROR] Processing ${gi.name}: ${e.message}`);
      stats.errors++;
    }
  }

  // -----------------------------------------------------------------------
  // Summary
  // -----------------------------------------------------------------------
  console.log('\n=== Import Complete ===');
  console.log(`  Matched existing: ${stats.matched}`);
  console.log(`  Created new:      ${stats.created}`);
  console.log(`  Boundaries:       ${stats.boundariesUpdated} updated`);
  console.log(`  Errors:           ${stats.errors}`);
  if (DRY_RUN) console.log('\n  [DRY RUN - no changes made]');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
