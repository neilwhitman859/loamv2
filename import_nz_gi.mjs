#!/usr/bin/env node
/**
 * import_nz_gi.mjs
 *
 * Imports New Zealand GI (Geographical Indication) wine regions
 * from official IPONZ boundary data into the Loam database.
 *
 * Data source: Intellectual Property Office of New Zealand (IPONZ)
 * 18 registered GIs with official boundary polygons (Esri JSON format).
 * Plus 3 "enduring" GIs (New Zealand, North Island, South Island) without boundaries.
 *
 * Phases:
 *   Phase 0: Load Esri JSON files and convert to GeoJSON
 *   Phase 1: Match/create appellations
 *   Phase 2: Import boundary polygons
 *
 * Usage:
 *   node import_nz_gi.mjs              # full run
 *   node import_nz_gi.mjs --dry-run    # preview only
 *   node import_nz_gi.mjs --boundaries-only  # skip Phase 1
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, readdirSync, statSync } from 'fs';
import { join } from 'path';

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

function computeCentroid(coords) {
  let totalLat = 0, totalLng = 0, count = 0;
  function extract(c) {
    if (typeof c[0] === 'number') {
      totalLng += c[0];
      totalLat += c[1];
      count++;
    } else {
      c.forEach(x => extract(x));
    }
  }
  extract(coords);
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
    return simplified.length >= 4 ? simplified : ring;
  }
  function simplifyCoords(coords, depth) {
    if (depth === 0) return simplifyRing(coords);
    return coords.map(c => simplifyCoords(c, depth - 1));
  }
  const depth = geojson.type === 'MultiPolygon' ? 2 : geojson.type === 'Polygon' ? 1 : 0;
  return { type: geojson.type, coordinates: simplifyCoords(geojson.coordinates, depth) };
}

/**
 * Convert Esri JSON rings to GeoJSON Polygon or MultiPolygon.
 *
 * Esri JSON stores all rings in a flat array. Exterior rings are clockwise
 * (positive signed area), interior rings (holes) are counter-clockwise
 * (negative signed area). Each exterior ring starts a new polygon.
 *
 * GeoJSON convention is opposite: exterior=CCW, interior=CW.
 * We reverse ring winding for GeoJSON compliance.
 */
function esriRingsToGeoJSON(rings) {
  // Compute signed area to determine ring direction
  function signedArea(ring) {
    let area = 0;
    for (let i = 0; i < ring.length - 1; i++) {
      area += (ring[i + 1][0] - ring[i][0]) * (ring[i + 1][1] + ring[i][1]);
    }
    return area / 2;
  }

  // Group rings into polygons
  const polygons = [];
  let currentPolygon = null;

  for (const ring of rings) {
    const area = signedArea(ring);
    if (area >= 0) {
      // Clockwise in Esri = exterior ring → new polygon
      // Reverse for GeoJSON (CCW exterior)
      currentPolygon = [[...ring].reverse()];
      polygons.push(currentPolygon);
    } else {
      // Counter-clockwise in Esri = hole
      // Reverse for GeoJSON (CW hole)
      if (currentPolygon) {
        currentPolygon.push([...ring].reverse());
      } else {
        // Orphan hole — treat as exterior
        currentPolygon = [[...ring].reverse()];
        polygons.push(currentPolygon);
      }
    }
  }

  if (polygons.length === 0) return null;
  if (polygons.length === 1) {
    return { type: 'Polygon', coordinates: polygons[0] };
  }
  return { type: 'MultiPolygon', coordinates: polygons };
}

// Map GI names to existing DB regions
const GI_TO_REGION = {
  'marlborough': 'Marlborough',
  'hawke\'s bay': 'Hawke\'s Bay',
  'hawkes bay': 'Hawke\'s Bay',
  'central otago': 'Central Otago',
  'canterbury': 'Canterbury',
  'martinborough': 'Martinborough',
  'waiheke island': 'Waiheke Island',
  'wairarapa': 'Wairarapa',
  // These GIs don't map to existing regions — use catch-all
  'nelson': null,
  'gisborne': null,
  'auckland': null,
  'kumeu': null,
  'gladstone': null,
  'waipara valley': null,
  'north canterbury': null,
  'northland': null,
  'matakana': null,
  'waitaki valley north otago': null,
  'central hawke\'s bay': null,
  'central hawkes bay': null,
};

// Normalize GI names (IPONZ uses "X / Y" for alternate names)
function normalizeGIName(name) {
  // Take the first name before " / "
  const primary = name.split(' / ')[0].trim();
  return primary;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('=== New Zealand GI Import ===');
  if (DRY_RUN) console.log('[DRY RUN MODE]');

  // -----------------------------------------------------------------------
  // Phase 0: Load Esri JSON data and convert to GeoJSON
  // -----------------------------------------------------------------------
  console.log('\n--- Phase 0: Loading data ---');

  const nzGiDir = 'data/geo/nz_gi';
  const dirs = readdirSync(nzGiDir).filter(d => {
    try { return statSync(join(nzGiDir, d)).isDirectory(); }
    catch { return false; }
  });

  const allGIs = [];

  for (const dir of dirs) {
    const files = readdirSync(join(nzGiDir, dir));
    const jsonFile = files.find(f => f.endsWith('.json'));
    if (!jsonFile) {
      console.log(`  [SKIP] No JSON in ${dir}`);
      continue;
    }

    const fp = join(nzGiDir, dir, jsonFile);
    const data = JSON.parse(readFileSync(fp, 'utf8'));
    const feat = data.features[0];
    const attrs = feat.attributes;

    // Convert Esri JSON rings to GeoJSON
    const geojson = esriRingsToGeoJSON(feat.geometry.rings);
    if (!geojson) {
      console.log(`  [SKIP] No valid geometry in ${dir}`);
      continue;
    }

    const name = normalizeGIName(attrs.GI_Name || attrs.GIName);
    const totalPts = feat.geometry.rings.reduce((s, r) => s + r.length, 0);

    allGIs.push({
      name,
      ipNumber: attrs.IP_Number,
      registerLink: attrs.Registerlink,
      dirName: dir,
      geometry: geojson,
      totalPoints: totalPts,
    });

    console.log(`  Loaded: ${name} (${geojson.type}, ${totalPts} pts)`);
  }

  // Add enduring GIs without boundaries
  const enduringGIs = [
    { name: 'New Zealand', ipNumber: 'enduring-nz' },
    { name: 'North Island', ipNumber: 'enduring-north' },
    { name: 'South Island', ipNumber: 'enduring-south' },
  ];
  for (const gi of enduringGIs) {
    allGIs.push({ ...gi, geometry: null, totalPoints: 0 });
  }

  console.log(`\n  Total GIs: ${allGIs.length} (${allGIs.length - 3} with boundaries, 3 enduring)`);

  // -----------------------------------------------------------------------
  // Load DB reference data
  // -----------------------------------------------------------------------
  console.log('\n--- Loading DB reference data ---');

  const { data: countries } = await sb.from('countries').select('id, name, iso_code');
  const nz = countries.find(c => c.iso_code === 'NZ');
  if (!nz) throw new Error('New Zealand not found in countries table');
  console.log(`  New Zealand ID: ${nz.id}`);

  // Load NZ regions
  const { data: nzRegions } = await sb.from('regions')
    .select('id, name, is_catch_all, country_id')
    .eq('country_id', nz.id);
  const regionByName = new Map();
  let catchAllRegion = null;
  for (const r of nzRegions) {
    regionByName.set(r.name.toLowerCase(), r);
    if (r.is_catch_all) catchAllRegion = r;
  }
  console.log(`  ${nzRegions.length} NZ regions loaded`);

  // Load existing NZ appellations
  const { data: existingApps, error: appErr } = await sb.from('appellations')
    .select('id, name, slug, country_id, region_id')
    .eq('country_id', nz.id);
  if (appErr) throw new Error(`Failed to fetch appellations: ${appErr.message}`);
  console.log(`  ${existingApps.length} existing NZ appellations`);

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
  const giToAppId = new Map();

  if (!BOUNDARIES_ONLY) {
    console.log('\n--- Phase 1: Matching appellations ---');

    for (const gi of allGIs) {
      // Try to match existing
      const nameLower = gi.name.toLowerCase();
      let app = appByName.get(nameLower) || appBySlug.get(slugify(gi.name));

      if (app) {
        giToAppId.set(gi.ipNumber, app.id);
        stats.matched++;
        continue;
      }

      // Find region_id
      const regionName = GI_TO_REGION[nameLower];
      let regionId = catchAllRegion?.id;
      if (regionName) {
        const region = regionByName.get(regionName.toLowerCase());
        if (region) regionId = region.id;
      }

      // Compute centroid from geometry
      let lat = null, lng = null;
      if (gi.geometry) {
        const centroid = computeCentroid(gi.geometry.coordinates);
        if (centroid) { lat = centroid.lat; lng = centroid.lng; }
      }

      const newApp = {
        name: gi.name,
        slug: slugify(gi.name),
        country_id: nz.id,
        region_id: regionId,
        designation_type: 'GI',
        latitude: lat ? Math.round(lat * 100000) / 100000 : null,
        longitude: lng ? Math.round(lng * 100000) / 100000 : null,
        hemisphere: 'south',
        growing_season_start_month: 10,   // Oct-Apr in southern hemisphere
        growing_season_end_month: 4,
        regulatory_url: gi.registerLink || null,
      };

      if (DRY_RUN) {
        console.log(`  [DRY] Would create: ${gi.name}`);
        stats.created++;
        continue;
      }

      const { data: inserted, error } = await sb.from('appellations')
        .insert(newApp)
        .select('id')
        .single();

      if (error) {
        if (error.code === '23505') {
          newApp.slug = slugify(`${gi.name}-new-zealand`);
          const { data: retry, error: retryErr } = await sb.from('appellations')
            .insert(newApp)
            .select('id')
            .single();
          if (retryErr) {
            console.log(`  [ERROR] Creating ${gi.name}: ${retryErr.message}`);
            stats.errors++;
            continue;
          }
          giToAppId.set(gi.ipNumber, retry.id);
          appByName.set(nameLower, { id: retry.id, ...newApp });
          appBySlug.set(newApp.slug, { id: retry.id, ...newApp });
        } else {
          console.log(`  [ERROR] Creating ${gi.name}: ${error.message}`);
          stats.errors++;
          continue;
        }
      } else {
        giToAppId.set(gi.ipNumber, inserted.id);
        appByName.set(nameLower, { id: inserted.id, ...newApp });
        appBySlug.set(newApp.slug, { id: inserted.id, ...newApp });
      }

      stats.created++;
      console.log(`  Created: ${gi.name} (${newApp.slug})`);
    }

    console.log(`\n  Phase 1: ${stats.matched} matched, ${stats.created} created, ${stats.errors} errors`);
  }

  // -----------------------------------------------------------------------
  // Phase 2: Import Boundary Polygons
  // -----------------------------------------------------------------------
  console.log('\n--- Phase 2: Importing boundaries ---');

  const TARGET_SIZE = 250_000; // 250KB target

  let processed = 0;
  for (const gi of allGIs) {
    if (!gi.geometry) continue;

    let appId = giToAppId.get(gi.ipNumber);
    if (!appId) {
      const app = appByName.get(gi.name.toLowerCase()) || appBySlug.get(slugify(gi.name));
      if (app) appId = app.id;
    }
    if (!appId) {
      console.log(`  [SKIP] No appellation match for ${gi.name}`);
      continue;
    }

    try {
      // Round precision first
      let simplified = simplifyPrecision(gi.geometry);
      let geoStr = JSON.stringify(simplified);

      // Apply progressive Douglas-Peucker if too large
      const tolerances = [0.001, 0.002, 0.005, 0.01];
      for (const tol of tolerances) {
        if (geoStr.length <= TARGET_SIZE) break;
        simplified = simplifyGeometry(simplified, tol);
        const newStr = JSON.stringify(simplified);
        console.log(`  [SIMPLIFY] ${gi.name}: ${geoStr.length} -> ${newStr.length} bytes (tol=${tol})`);
        geoStr = newStr;
      }

      if (geoStr.length > TARGET_SIZE) {
        console.log(`  [WARN] ${gi.name} still ${(geoStr.length/1024).toFixed(0)}KB after max simplification`);
      }

      if (DRY_RUN) {
        const centroid = computeCentroid(simplified.coordinates);
        console.log(`  [DRY] Would import boundary for ${gi.name} (${simplified.type}, ${(geoStr.length/1024).toFixed(0)}KB, centroid: ${centroid?.lat?.toFixed(2)}, ${centroid?.lng?.toFixed(2)})`);
        stats.boundariesUpdated++;
        processed++;
        continue;
      }

      const { error } = await sb.rpc('upsert_appellation_boundary', {
        p_appellation_id: appId,
        p_geojson: geoStr,
        p_source_id: `iponz/${gi.ipNumber}`,
        p_confidence: 'official',
      });

      if (error) {
        console.log(`  [ERROR] Boundary for ${gi.name}: ${error.message}`);
        stats.errors++;
      } else {
        stats.boundariesUpdated++;
        console.log(`  Imported: ${gi.name} (${(geoStr.length/1024).toFixed(0)}KB)`);
      }

      processed++;
      if (processed % 5 === 0) {
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
  console.log(`  Boundaries:       ${stats.boundariesUpdated} imported`);
  console.log(`  Errors:           ${stats.errors}`);
  if (DRY_RUN) console.log('\n  [DRY RUN - no changes made]');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
