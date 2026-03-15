#!/usr/bin/env node
/**
 * import_global_appellations.mjs
 *
 * Imports appellations for multiple countries from data/appellations_global.json.
 * Geocodes via Nominatim (centroids + optional boundary polygons).
 * Boundaries are imported where available with confidence 'geocoded'.
 *
 * Usage:
 *   node import_global_appellations.mjs              # full run
 *   node import_global_appellations.mjs --dry-run    # preview only
 *   node import_global_appellations.mjs --country=ZA # single country
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
const COUNTRY_FILTER = process.argv.find(a => a.startsWith('--country='))?.split('=')[1];

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

const TARGET_SIZE = 250_000;

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
    return simplified.length >= 4 ? simplified : ring;
  }
  function simplifyCoords(coords, depth) {
    if (depth === 0) return simplifyRing(coords);
    return coords.map(c => simplifyCoords(c, depth - 1));
  }
  const depth = geojson.type === 'MultiPolygon' ? 2 : geojson.type === 'Polygon' ? 1 : 0;
  return { type: geojson.type, coordinates: simplifyCoords(geojson.coordinates, depth) };
}

// ---------------------------------------------------------------------------
// Geocoding with optional polygon
// ---------------------------------------------------------------------------
async function geocode(query) {
  const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1&polygon_geojson=1`;
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'LoamWineApp/1.0 (neil@loam.wine)' }
    });
    const data = await res.json();
    if (data.length > 0) {
      const result = {
        lat: parseFloat(data[0].lat),
        lng: parseFloat(data[0].lon),
        boundary: null,
      };
      // Check for polygon/multipolygon boundary
      if (data[0].geojson) {
        const gj = data[0].geojson;
        if (gj.type === 'Polygon' || gj.type === 'MultiPolygon') {
          result.boundary = gj;
        }
      }
      return result;
    }
  } catch (e) {
    // Silently fail — will just use null centroid
  }
  return null;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('=== Global Appellation Import ===');
  if (DRY_RUN) console.log('[DRY RUN MODE]');
  if (COUNTRY_FILTER) console.log(`[FILTERING TO: ${COUNTRY_FILTER}]`);

  // Load appellation data
  const allData = JSON.parse(readFileSync('data/appellations_global.json', 'utf8'));

  // Load DB reference data
  console.log('\n--- Loading DB reference data ---');
  const PAGE_SIZE = 1000;

  // Countries
  const { data: countries } = await sb.from('countries').select('id, name, iso_code');
  const countryByCode = new Map();
  for (const c of countries) countryByCode.set(c.iso_code, c);

  // All regions (paginated)
  let allRegions = [];
  let from = 0;
  while (true) {
    const { data: batch } = await sb.from('regions').select('id, name, is_catch_all, country_id').range(from, from + PAGE_SIZE - 1);
    if (!batch || batch.length === 0) break;
    allRegions = allRegions.concat(batch);
    if (batch.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  // Index regions by country
  const regionIndex = new Map(); // country_id -> Map(name_lower -> region)
  const catchAllIndex = new Map(); // country_id -> catch-all region
  for (const r of allRegions) {
    if (!regionIndex.has(r.country_id)) regionIndex.set(r.country_id, new Map());
    regionIndex.get(r.country_id).set(r.name.toLowerCase(), r);
    if (r.is_catch_all) catchAllIndex.set(r.country_id, r);
  }

  // Load existing appellations (paginated)
  let existingApps = [];
  from = 0;
  while (true) {
    const { data: batch } = await sb.from('appellations').select('id, name, slug, country_id').range(from, from + PAGE_SIZE - 1);
    if (!batch || batch.length === 0) break;
    existingApps = existingApps.concat(batch);
    if (batch.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  // Index existing by country + name
  const appIndex = new Map(); // "country_id::name_lower" -> app
  const slugIndex = new Map(); // "country_id::slug" -> app
  for (const a of existingApps) {
    appIndex.set(`${a.country_id}::${a.name.toLowerCase()}`, a);
    slugIndex.set(`${a.country_id}::${a.slug}`, a);
  }

  console.log(`  ${countries.length} countries, ${allRegions.length} regions, ${existingApps.length} existing appellations`);

  // Process each country
  const globalStats = { matched: 0, created: 0, boundaries: 0, geocoded: 0, errors: 0 };

  const isoKeys = COUNTRY_FILTER ? [COUNTRY_FILTER] : Object.keys(allData);

  for (const iso of isoKeys) {
    const countryData = allData[iso];
    if (!countryData) { console.log(`\n[SKIP] No data for ${iso}`); continue; }

    const country = countryByCode.get(iso);
    if (!country) { console.log(`\n[SKIP] Country ${iso} not in DB`); continue; }

    const regions = regionIndex.get(country.id) || new Map();
    const catchAll = catchAllIndex.get(country.id);

    console.log(`\n=== ${country.name} (${iso}) — ${countryData.appellations.length} appellations ===`);

    for (const appDef of countryData.appellations) {
      // Check if exists
      const nameKey = `${country.id}::${appDef.name.toLowerCase()}`;
      const slugKey = `${country.id}::${slugify(appDef.name)}`;
      let existing = appIndex.get(nameKey) || slugIndex.get(slugKey);

      if (existing) {
        console.log(`  [MATCH] ${appDef.name}`);
        globalStats.matched++;
        continue;
      }

      // Find region_id
      let regionId = catchAll?.id;
      if (appDef.region) {
        const region = regions.get(appDef.region.toLowerCase());
        if (region) regionId = region.id;
      }

      // Geocode
      let lat = null, lng = null, boundary = null;
      if (appDef.geo) {
        const result = await geocode(appDef.geo);
        if (result) {
          lat = Math.round(result.lat * 100000) / 100000;
          lng = Math.round(result.lng * 100000) / 100000;
          boundary = result.boundary;
          globalStats.geocoded++;
        }
        await sleep(1100); // Nominatim rate limit
      }

      const newApp = {
        name: appDef.name,
        slug: slugify(appDef.name),
        country_id: country.id,
        region_id: regionId,
        designation_type: appDef.type,
        latitude: lat,
        longitude: lng,
        hemisphere: countryData.hemisphere,
        growing_season_start_month: countryData.growing_start,
        growing_season_end_month: countryData.growing_end,
      };

      if (DRY_RUN) {
        const hasBoundary = boundary ? ` +boundary(${boundary.type})` : '';
        console.log(`  [DRY] Would create: ${appDef.name} (${lat}, ${lng})${hasBoundary}`);
        globalStats.created++;
        continue;
      }

      // Insert appellation
      const { data: inserted, error } = await sb.from('appellations')
        .insert(newApp)
        .select('id')
        .single();

      if (error) {
        if (error.code === '23505') {
          // Slug conflict — try with country suffix
          newApp.slug = slugify(`${appDef.name}-${country.name}`);
          const { data: retry, error: retryErr } = await sb.from('appellations')
            .insert(newApp)
            .select('id')
            .single();
          if (retryErr) {
            console.log(`  [ERROR] ${appDef.name}: ${retryErr.message}`);
            globalStats.errors++;
            continue;
          }
          appIndex.set(nameKey, { id: retry.id, ...newApp });
          slugIndex.set(`${country.id}::${newApp.slug}`, { id: retry.id, ...newApp });

          // Try boundary import
          if (boundary && retry.id) {
            await importBoundary(retry.id, appDef.name, boundary, iso);
          }
        } else {
          console.log(`  [ERROR] ${appDef.name}: ${error.message}`);
          globalStats.errors++;
          continue;
        }
      } else {
        appIndex.set(nameKey, { id: inserted.id, ...newApp });
        slugIndex.set(slugKey, { id: inserted.id, ...newApp });

        // Try boundary import
        if (boundary && inserted.id) {
          await importBoundary(inserted.id, appDef.name, boundary, iso);
        }
      }

      globalStats.created++;
      const bStr = boundary ? ' +boundary' : '';
      console.log(`  Created: ${appDef.name} (${lat}, ${lng})${bStr}`);
    }
  }

  async function importBoundary(appId, name, boundary, iso) {
    try {
      // Simplify if needed
      let geoStr = JSON.stringify(boundary);
      const tolerances = [0.001, 0.002, 0.005, 0.01];
      let simplified = boundary;
      for (const tol of tolerances) {
        if (geoStr.length <= TARGET_SIZE) break;
        simplified = simplifyGeometry(simplified, tol);
        geoStr = JSON.stringify(simplified);
      }

      if (geoStr.length > TARGET_SIZE * 2) {
        // Too large even after simplification — skip
        return;
      }

      const { error } = await sb.rpc('upsert_appellation_boundary', {
        p_appellation_id: appId,
        p_geojson: geoStr,
        p_source_id: `nominatim/${iso}/${slugify(name)}`,
        p_confidence: 'geocoded',
      });

      if (!error) {
        globalStats.boundaries++;
      }
    } catch {
      // Don't block on boundary errors
    }
  }

  // Summary
  console.log('\n=== Import Complete ===');
  console.log(`  Matched existing: ${globalStats.matched}`);
  console.log(`  Created new:      ${globalStats.created}`);
  console.log(`  Geocoded:         ${globalStats.geocoded}`);
  console.log(`  Boundaries:       ${globalStats.boundaries}`);
  console.log(`  Errors:           ${globalStats.errors}`);
  if (DRY_RUN) console.log('\n  [DRY RUN - no changes made]');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
