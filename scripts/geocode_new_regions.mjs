#!/usr/bin/env node
/**
 * geocode_new_regions.mjs
 *
 * Targeted Nominatim geocoding for 5 newly created regions:
 * - Niagara Peninsula (Canada)
 * - Okanagan Valley (Canada)
 * - Klein Karoo (South Africa)
 * - Olifants River (South Africa)
 * - Scotland (UK)
 *
 * Usage:
 *   node scripts/geocode_new_regions.mjs --dry-run
 *   node scripts/geocode_new_regions.mjs --apply
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

// Targeted queries for each region
const REGIONS = [
  {
    slug: 'niagara-peninsula-region',
    queries: ['Niagara Peninsula, Ontario, Canada', 'Niagara Region, Ontario, Canada'],
  },
  {
    slug: 'okanagan-valley-region',
    queries: ['Okanagan Valley, British Columbia, Canada', 'Regional District of Central Okanagan, British Columbia'],
  },
  {
    slug: 'klein-karoo-region',
    queries: ['Klein Karoo, South Africa', 'Little Karoo, South Africa', 'Klein Karoo, Western Cape'],
  },
  {
    slug: 'olifants-river-region',
    queries: ['Olifants River Valley, South Africa', 'Citrusdal, Western Cape, South Africa'],
  },
  {
    slug: 'scotland',
    queries: ['Scotland, United Kingdom', 'Scotland'],
  },
];

async function main() {
  console.log(`\n=== Geocode New Regions (${MODE}) ===\n`);

  // Get region IDs
  const slugs = REGIONS.map(r => r.slug);
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

  for (const { slug, queries } of REGIONS) {
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
      if (result) {
        const isPolygon = result.geojson &&
          (result.geojson.type === 'Polygon' || result.geojson.type === 'MultiPolygon');
        if (isPolygon) break; // Found a polygon, use it
        console.log(`    Got centroid only, trying next query...`);
        // Keep this result as fallback but try for a polygon
      }
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
      console.log(`    ✅ Polygon (${simplified.type}, ${sizeKB} KB)`);

      const { error: rpcErr } = await sb.rpc('upsert_region_boundary', {
        p_region_id: region.id,
        p_geojson: JSON.stringify(simplified),
        p_source_id: sourceId,
        p_confidence: 'approximate',
      });
      if (rpcErr) { console.log(`    ❌ RPC: ${rpcErr.message}`); failed++; continue; }
    } else {
      console.log(`    📍 Centroid (${lat.toFixed(4)}, ${lng.toFixed(4)})`);

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
