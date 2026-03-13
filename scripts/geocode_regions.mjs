#!/usr/bin/env node
/**
 * geocode_regions.mjs
 *
 * Phase 2C: Fetch boundaries for remaining regions without geographic data
 * using Nominatim (OpenStreetMap) API. Queries admin boundaries (provinces,
 * cantons, prefectures) and wine region locations.
 *
 * For regions that ARE administrative units → polygon boundary (approximate confidence)
 * For wine-concept regions → centroid or small admin boundary (geocoded confidence)
 *
 * Uses data/region_nominatim_queries.json for query overrides.
 * Respects Nominatim rate limit (1 req/sec).
 *
 * Usage:
 *   node scripts/geocode_regions.mjs --dry-run   # Preview queries (default)
 *   node scripts/geocode_regions.mjs --apply      # Actually geocode and insert
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

// ── Load .env ───────────────────────────────────────────────
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
for (const line of envContent.split('\n')) {
  const trimmed = line.replace(/\r/g, '').trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx === -1) continue;
  const key = trimmed.slice(0, eqIdx);
  const val = trimmed.slice(eqIdx + 1);
  if (!process.env[key]) process.env[key] = val;
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

const APPLY = process.argv.includes('--apply');
const MODE = APPLY ? 'APPLY' : 'DRY RUN';

// ── Load query overrides ────────────────────────────────────
const queriesPath = new URL('../data/region_nominatim_queries.json', import.meta.url)
  .pathname.replace(/^\/([A-Z]:)/, '$1');
const { overrides } = JSON.parse(readFileSync(queriesPath, 'utf8'));

// ── Nominatim helpers ───────────────────────────────────────
const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search';
const USER_AGENT = 'Loam/1.0 (wine-intelligence-platform)';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function nominatimSearch(query, withPolygon = true) {
  const params = new URLSearchParams({
    q: query,
    format: 'jsonv2',
    limit: '1',
    ...(withPolygon ? { polygon_geojson: '1' } : {}),
  });

  const url = `${NOMINATIM_URL}?${params}`;
  const res = await fetch(url, {
    headers: { 'User-Agent': USER_AGENT },
  });

  if (!res.ok) {
    throw new Error(`Nominatim ${res.status}: ${await res.text()}`);
  }

  const results = await res.json();
  return results.length > 0 ? results[0] : null;
}

/**
 * Simplify GeoJSON geometry to stay under 250KB.
 * Uses coordinate precision rounding + Douglas-Peucker if needed.
 */
function simplifyPrecision(geojson) {
  function roundCoords(coords) {
    if (typeof coords[0] === 'number') {
      return [Math.round(coords[0] * 100000) / 100000, Math.round(coords[1] * 100000) / 100000];
    }
    return coords.map(c => roundCoords(c));
  }
  return { type: geojson.type, coordinates: roundCoords(geojson.coordinates) };
}

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
  if (depth === 0) return geojson;
  return { type: geojson.type, coordinates: simplifyCoords(geojson.coordinates, depth) };
}

function prepareGeometry(geojson) {
  let simplified = simplifyPrecision(geojson);
  let str = JSON.stringify(simplified);

  // Progressive simplification to stay under 250KB
  const tolerances = [0.001, 0.005, 0.01, 0.05];
  for (const tol of tolerances) {
    if (str.length <= 256_000) break;
    simplified = simplifyGeometry(simplified, tol);
    str = JSON.stringify(simplified);
  }

  return { geojson: simplified, sizeKB: (str.length / 1024).toFixed(1) };
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log(`\n=== Geocode Regions — Phase 2C (${MODE}) ===\n`);

  // 1. Get all named regions without boundaries
  const { data: regions, error: regErr } = await sb
    .from('regions')
    .select('id, slug, name, parent_id, country_id, countries!inner(name, iso_code)')
    .is('deleted_at', null)
    .eq('is_catch_all', false)
    .order('name');
  if (regErr) throw regErr;

  // Get existing region boundaries
  const { data: existingBounds, error: boundErr } = await sb
    .from('geographic_boundaries')
    .select('region_id')
    .not('region_id', 'is', null);
  if (boundErr) throw boundErr;

  const hasRegionBoundary = new Set(existingBounds.map(b => b.region_id));
  const remaining = regions.filter(r => !hasRegionBoundary.has(r.id));

  console.log(`Total named regions: ${regions.length}`);
  console.log(`Already have boundaries: ${hasRegionBoundary.size}`);
  console.log(`Remaining to geocode: ${remaining.length}\n`);

  // 2. Process each region
  let polygonCount = 0;
  let centroidCount = 0;
  let skippedCount = 0;
  let errorCount = 0;

  for (const region of remaining) {
    const slug = region.slug;
    const countryName = region.countries.name;
    const override = overrides[slug];
    const level = region.parent_id ? 'L2' : 'L1';

    // Check if should skip
    if (override?.skip) {
      console.log(`  SKIP: ${region.name} (${countryName}) — ${override.reason}`);
      skippedCount++;
      continue;
    }

    // Determine query
    const query = override?.query || `${region.name}, ${countryName}`;

    console.log(`  [${level}] ${region.name} (${countryName})`);
    console.log(`       Query: "${query}"`);

    if (!APPLY) {
      continue;
    }

    // Rate limit
    await sleep(1100);

    try {
      let result = await nominatimSearch(query, true);

      // Try fallback query if no result
      if (!result && override?.fallback) {
        console.log(`       Trying fallback: "${override.fallback}"`);
        await sleep(1100);
        result = await nominatimSearch(override.fallback, true);
      }

      if (!result) {
        console.log(`       ❌ No results found`);
        errorCount++;
        continue;
      }

      const lat = parseFloat(result.lat);
      const lng = parseFloat(result.lon);
      const geojson = result.geojson;
      const osmType = result.osm_type;
      const osmId = result.osm_id;
      const sourceId = `nominatim/${osmType}/${osmId}`;

      // Determine confidence
      const isPolygon = geojson &&
        (geojson.type === 'Polygon' || geojson.type === 'MultiPolygon');
      const confidence = isPolygon ? 'approximate' : 'geocoded';

      if (isPolygon) {
        const { geojson: simplified, sizeKB } = prepareGeometry(geojson);

        console.log(`       ✅ Polygon (${simplified.type}, ${sizeKB} KB) → ${confidence}`);

        const { error: rpcErr } = await sb.rpc('upsert_region_boundary', {
          p_region_id: region.id,
          p_geojson: JSON.stringify(simplified),
          p_source_id: sourceId,
          p_confidence: confidence,
        });

        if (rpcErr) {
          console.log(`       ❌ RPC error: ${rpcErr.message}`);
          errorCount++;
          continue;
        }
        polygonCount++;
      } else {
        console.log(`       📍 Centroid only (${lat.toFixed(4)}, ${lng.toFixed(4)}) → geocoded`);

        const { error: rpcErr } = await sb.rpc('upsert_region_boundary', {
          p_region_id: region.id,
          p_lat: lat,
          p_lng: lng,
          p_source_id: sourceId,
          p_confidence: 'geocoded',
        });

        if (rpcErr) {
          console.log(`       ❌ RPC error: ${rpcErr.message}`);
          errorCount++;
          continue;
        }
        centroidCount++;
      }
    } catch (err) {
      console.log(`       ❌ Error: ${err.message}`);
      errorCount++;
    }
  }

  console.log(`\n=== Summary ===`);
  console.log(`Polygon boundaries: ${polygonCount}`);
  console.log(`Centroid only: ${centroidCount}`);
  console.log(`Skipped: ${skippedCount}`);
  console.log(`Errors: ${errorCount}`);
  console.log(`Total processed: ${polygonCount + centroidCount + skippedCount + errorCount}`);
  console.log(`\nDone!`);
}

main().catch(e => { console.error(e); process.exit(1); });
