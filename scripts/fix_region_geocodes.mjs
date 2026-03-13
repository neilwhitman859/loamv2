#!/usr/bin/env node
/**
 * fix_region_geocodes.mjs
 *
 * Fixes specific geocoding issues from Phase 2C:
 * 1. Swiss cantons that failed with English "Canton of" queries → retry with local names
 * 2. Argentine provinces with wrong centroids → retry with Spanish "Provincia de" queries
 * 3. Simplify oversized polygons
 *
 * Usage:
 *   node scripts/fix_region_geocodes.mjs --dry-run
 *   node scripts/fix_region_geocodes.mjs --apply
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

const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search';
const USER_AGENT = 'Loam/1.0 (wine-intelligence-platform)';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function nominatimSearch(query) {
  const params = new URLSearchParams({
    q: query, format: 'jsonv2', limit: '1', polygon_geojson: '1',
  });
  const res = await fetch(`${NOMINATIM_URL}?${params}`, {
    headers: { 'User-Agent': USER_AGENT },
  });
  if (!res.ok) throw new Error(`Nominatim ${res.status}`);
  const results = await res.json();
  return results[0] || null;
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

// Targeted retries: slug → [query1, query2, ...]
const RETRIES = {
  // Swiss cantons — local-language names
  'fribourg': ['Fribourg, Schweiz', 'Kanton Freiburg'],
  'graubunden': ['Graubünden, Schweiz', 'Kanton Graubünden'],
  'luzern': ['Luzern, Schweiz', 'Kanton Luzern'],
  'schaffhausen': ['Schaffhausen, Schweiz', 'Kanton Schaffhausen'],
  'st-gallen': ['Sankt Gallen, Schweiz', 'St. Gallen, Switzerland'],
  'thurgau': ['Thurgau, Schweiz', 'Kanton Thurgau'],
  'ticino': ['Ticino, Svizzera', 'Canton Ticino, Switzerland'],
  'valais': ['Valais, Suisse', 'Wallis, Schweiz'],
  // Argentine provinces with wrong results
  'buenos-aires': ['Provincia de Buenos Aires, Argentina'],
  'salta': ['Provincia de Salta, Argentina'],
  'san-juan': ['Provincia de San Juan, Argentina'],
  'la-rioja': ['Provincia de La Rioja, Argentina'],
  'cordoba': ['Provincia de Córdoba, Argentina'],
  'patagonia': ['Provincia de Río Negro, Argentina'],
};

async function main() {
  console.log(`\n=== Fix Region Geocodes (${MODE}) ===\n`);

  // Get region IDs for the slugs we need to fix
  const slugs = Object.keys(RETRIES);
  const { data: regions, error: regErr } = await sb
    .from('regions')
    .select('id, slug, name')
    .in('slug', slugs)
    .is('deleted_at', null);
  if (regErr) throw regErr;

  const regionBySlug = {};
  for (const r of regions) regionBySlug[r.slug] = r;

  let fixed = 0;
  let failed = 0;

  for (const [slug, queries] of Object.entries(RETRIES)) {
    const region = regionBySlug[slug];
    if (!region) {
      console.log(`  ⚠️ Region not found: ${slug}`);
      continue;
    }

    console.log(`  ${region.name} (${slug})`);

    if (!APPLY) {
      console.log(`    Would try: ${queries.join(' → ')}`);
      continue;
    }

    let result = null;
    for (const query of queries) {
      await sleep(1100);
      console.log(`    Trying: "${query}"`);
      result = await nominatimSearch(query);
      if (result) break;
    }

    if (!result) {
      console.log(`    ❌ All queries failed`);
      failed++;
      continue;
    }

    const lat = parseFloat(result.lat);
    const lng = parseFloat(result.lon);
    const geojson = result.geojson;
    const osmType = result.osm_type;
    const osmId = result.osm_id;
    const sourceId = `nominatim/${osmType}/${osmId}`;
    const isPolygon = geojson && (geojson.type === 'Polygon' || geojson.type === 'MultiPolygon');

    if (isPolygon) {
      const simplified = simplifyPrecision(geojson);
      const sizeKB = (JSON.stringify(simplified).length / 1024).toFixed(1);
      const confidence = 'approximate';
      console.log(`    ✅ Polygon (${simplified.type}, ${sizeKB} KB)`);

      // Delete existing bad entry first
      await sb.from('geographic_boundaries')
        .delete()
        .eq('region_id', region.id);

      const { error: rpcErr } = await sb.rpc('upsert_region_boundary', {
        p_region_id: region.id,
        p_geojson: JSON.stringify(simplified),
        p_source_id: sourceId,
        p_confidence: confidence,
      });
      if (rpcErr) { console.log(`    ❌ RPC: ${rpcErr.message}`); failed++; continue; }
    } else {
      console.log(`    📍 Centroid (${lat.toFixed(4)}, ${lng.toFixed(4)})`);

      await sb.from('geographic_boundaries')
        .delete()
        .eq('region_id', region.id);

      const { error: rpcErr } = await sb.rpc('upsert_region_boundary', {
        p_region_id: region.id,
        p_lat: lat,
        p_lng: lng,
        p_source_id: sourceId,
        p_confidence: 'geocoded',
      });
      if (rpcErr) { console.log(`    ❌ RPC: ${rpcErr.message}`); failed++; continue; }
    }
    fixed++;
  }

  console.log(`\n=== Summary ===`);
  console.log(`Fixed: ${fixed}`);
  console.log(`Failed: ${failed}`);
  console.log(`\nDone!`);
}

main().catch(e => { console.error(e); process.exit(1); });
