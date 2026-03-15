#!/usr/bin/env node
/**
 * fix_au_zone_boundaries.mjs
 *
 * Fixes the failed Australian zone boundary polygon import from populate_au_containment.mjs.
 * The original script called `upsert_appellation_boundary` with `p_boundary_geojson`
 * but the actual function signature uses `p_geojson`.
 *
 * Reads data/geo/wine_australia_zones.geojson, matches each zone feature to an existing
 * zone appellation in the DB, and calls the RPC with the correct parameter names.
 *
 * Usage:
 *   node scripts/fix_au_zone_boundaries.mjs --dry-run   # preview only
 *   node scripts/fix_au_zone_boundaries.mjs              # full run
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

// Load .env
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envLines = readFileSync(envPath, 'utf8').split('\n');
const env = {};
for (const l of envLines) {
  const m = l.replace(/\r/g, '').match(/^(\w+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE);

const DRY_RUN = process.argv.includes('--dry-run');

function slugify(s) {
  return s.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

// Round coordinates to 5 decimal places (~1m precision)
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

async function main() {
  console.log(`\n=== Fix AU Zone Boundaries ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

  // 1. Get Australia country ID
  const { data: auCountry, error: countryErr } = await sb
    .from('countries').select('id').eq('iso_code', 'AU').single();
  if (countryErr) throw countryErr;
  const AU_COUNTRY_ID = auCountry.id;

  // 2. Load existing AU zone appellations
  const { data: zoneApps, error: appErr } = await sb
    .from('appellations')
    .select('id, name, classification_level')
    .eq('country_id', AU_COUNTRY_ID)
    .eq('classification_level', 'zone');
  if (appErr) throw appErr;

  const appByName = {};
  for (const a of zoneApps) {
    appByName[a.name] = a;
  }
  console.log(`Found ${zoneApps.length} AU zone appellations in DB`);

  // 3. Check which already have boundaries
  const zoneIds = zoneApps.map(a => a.id);
  const { data: existingBounds, error: boundErr } = await sb
    .from('geographic_boundaries')
    .select('appellation_id, boundary')
    .in('appellation_id', zoneIds);
  if (boundErr) throw boundErr;

  const hasPolygon = new Set();
  for (const gb of (existingBounds || [])) {
    if (gb.boundary) hasPolygon.add(gb.appellation_id);
  }
  console.log(`Zones already with boundary polygon: ${hasPolygon.size}`);

  // 4. Load zone GeoJSON
  const geoPath = new URL('../data/geo/wine_australia_zones.geojson', import.meta.url)
    .pathname.replace(/^\/([A-Z]:)/, '$1');
  const zonesGeo = JSON.parse(readFileSync(geoPath, 'utf8'));
  console.log(`GeoJSON features: ${zonesGeo.features.length}\n`);

  // 5. Match and import
  let imported = 0;
  let skippedAlready = 0;
  let skippedNoMatch = 0;

  for (const feature of zonesGeo.features) {
    const giName = feature.properties.GI_NAME;
    const giNumber = feature.properties.GI_NUMBER;
    const geom = feature.geometry;

    const app = appByName[giName];
    if (!app) {
      console.log(`  SKIP (no DB match): ${giName}`);
      skippedNoMatch++;
      continue;
    }

    if (hasPolygon.has(app.id)) {
      console.log(`  SKIP (already has polygon): ${giName}`);
      skippedAlready++;
      continue;
    }

    // Simplify: round precision, then Douglas-Peucker if still > 1MB
    let simplified = simplifyPrecision(geom);
    let geojsonStr = JSON.stringify(simplified);
    const origSizeKB = (geojsonStr.length / 1024).toFixed(1);

    if (geojsonStr.length > 1_000_000) {
      simplified = simplifyGeometry(simplified, 0.005); // ~500m tolerance
      geojsonStr = JSON.stringify(simplified);
      console.log(`  [SIMPLIFY] ${giName}: ${origSizeKB} KB -> ${(geojsonStr.length / 1024).toFixed(1)} KB`);
    }

    const sizeKB = (geojsonStr.length / 1024).toFixed(1);
    const sourceId = `wine-australia-zone/${slugify(giName)}`;

    console.log(`  ${DRY_RUN ? '[dry-run] ' : ''}Import: ${giName} (${simplified.type}, ${sizeKB} KB) -> ${app.id}`);

    if (!DRY_RUN) {
      const { error: rpcErr } = await sb.rpc('upsert_appellation_boundary', {
        p_appellation_id: app.id,
        p_geojson: geojsonStr,       // FIXED: was p_boundary_geojson
        p_source_id: sourceId,
        p_confidence: 'official',
      });

      if (rpcErr) {
        console.log(`    ERROR: ${rpcErr.message}`);
        continue;
      }
    }
    imported++;
  }

  console.log(`\n--- Summary ---`);
  console.log(`Imported: ${imported}`);
  console.log(`Skipped (already has polygon): ${skippedAlready}`);
  console.log(`Skipped (no DB match): ${skippedNoMatch}`);
  console.log(`Total features processed: ${zonesGeo.features.length}`);
  console.log(`\nDone!`);
}

main().catch(e => { console.error(e); process.exit(1); });
