#!/usr/bin/env node
/**
 * fetch_region_boundaries.mjs
 * Geocodes + fetches polygon boundaries for all regions via Nominatim.
 * Uses upsert_region_boundary RPC to insert/update geographic_boundaries.
 *
 * Usage:
 *   node fetch_region_boundaries.mjs              # live run
 *   node fetch_region_boundaries.mjs --dry-run    # preview only
 *   node fetch_region_boundaries.mjs --limit 10   # process first N
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const env = Object.fromEntries(
  readFileSync('frontend/.env', 'utf8')
    .split('\n')
    .filter(l => l.includes('='))
    .map(l => { const [k, ...v] = l.split('='); return [k.trim(), v.join('=').trim()] })
);

const supabase = createClient(env.VITE_SUPABASE_URL, env.VITE_SUPABASE_ANON_KEY);

const DRY_RUN = process.argv.includes('--dry-run');
const limitIdx = process.argv.indexOf('--limit');
const LIMIT = limitIdx !== -1 ? parseInt(process.argv[limitIdx + 1]) : Infinity;

// Haversine distance in km
function haversine(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

// Compute centroid from GeoJSON polygon
function computeCentroid(geojson) {
  let totalLat = 0, totalLng = 0, count = 0;
  function extractCoords(coords) {
    if (typeof coords[0] === 'number') {
      totalLng += coords[0];
      totalLat += coords[1];
      count++;
    } else {
      coords.forEach(c => extractCoords(c));
    }
  }
  if (geojson.type === 'GeometryCollection') {
    geojson.geometries.forEach(g => extractCoords(g.coordinates));
  } else {
    extractCoords(geojson.coordinates);
  }
  return count > 0 ? { lat: totalLat / count, lng: totalLng / count } : null;
}

async function nominatimSearch(query, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&polygon_geojson=1&limit=1`;
    const res = await fetch(url, {
      headers: { 'User-Agent': 'LoamWineApp/1.0 (contact@loam.wine)' }
    });
    if (res.status === 429) {
      const wait = 5000 * (attempt + 1);
      process.stdout.write(`[rate-limited, waiting ${wait/1000}s] `);
      await sleep(wait);
      continue;
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }
  throw new Error('Rate limited after retries');
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  console.log(`\n=== Region Boundary Fetcher ===`);
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}`);

  // Fetch all regions with their country name and existing boundary status
  const { data: regions, error } = await supabase
    .from('regions')
    .select('id, name, country:countries(id, name)')
    .order('name');

  if (error) { console.error('Failed to fetch regions:', error); process.exit(1); }

  // Get existing boundary rows for regions
  const { data: existing } = await supabase
    .from('geographic_boundaries')
    .select('id, region_id')
    .not('region_id', 'is', null);

  const existingMap = new Map((existing || []).map(e => [e.region_id, e.id]));

  // Get average appellation centroids per region for validation
  const centroidMap = new Map();
  const { data: bpData } = await supabase.rpc('get_boundary_points');
  if (bpData) {
    // Group appellation points by region_name + country_name
    const regionPoints = {};
    for (const pt of bpData) {
      if (pt.entity_type !== 'appellation' || !pt.region_name || !pt.lat || !pt.lng) continue;
      const key = `${pt.region_name}|${pt.country_name}`;
      if (!regionPoints[key]) regionPoints[key] = { sumLat: 0, sumLng: 0, count: 0 };
      regionPoints[key].sumLat += pt.lat;
      regionPoints[key].sumLng += pt.lng;
      regionPoints[key].count++;
    }
    // Map back to region IDs
    for (const r of regions) {
      const key = `${r.name}|${r.country?.name}`;
      if (regionPoints[key]) {
        const p = regionPoints[key];
        centroidMap.set(r.id, { lat: p.sumLat / p.count, lng: p.sumLng / p.count });
      }
    }
    console.log(`Computed expected centroids for ${centroidMap.size} regions from appellation data`);
  }

  // Filter to regions without polygon boundaries (keep those that are centroid-only or missing entirely)
  const todo = regions.filter(r => {
    // Skip regions where name == country name (catch-all regions)
    if (r.name === r.country?.name) return false;
    return true;
  });

  // Further filter: only those without existing boundaries, OR existing but we want to try polygons
  const toProcess = todo.filter(r => !existingMap.has(r.id));

  console.log(`Total regions: ${regions.length}`);
  console.log(`Skipping ${regions.length - todo.length} catch-all regions (name == country)`);
  console.log(`Already have boundaries: ${existingMap.size}`);
  console.log(`To process: ${toProcess.length}`);

  const limit = Math.min(toProcess.length, LIMIT);
  let success = 0, failed = 0, stored = 0;
  const countryCounts = {};

  for (let i = 0; i < limit; i++) {
    const r = toProcess[i];
    const countryName = r.country?.name || 'Unknown';
    process.stdout.write(`[${i+1}/${limit}] ${countryName}/${r.name}... `);

    await sleep(3000); // Rate limit (conservative to avoid 429s after heavy usage)

    // Build search queries - try multiple strategies
    const queries = [
      `${r.name}, ${countryName}`,
      r.name
    ];

    let found = false;
    for (const query of queries) {
      try {
        const results = await nominatimSearch(query);
        if (!results.length) continue;

        const result = results[0];
        const lat = parseFloat(result.lat);
        const lng = parseFloat(result.lon);
        const geojson = result.geojson;
        const hasPolygon = geojson && ['Polygon', 'MultiPolygon'].includes(geojson.type);

        // Validate: check if result is near the expected location (if we have appellation centroids)
        const expected = centroidMap.get(r.id);
        if (expected) {
          const dist = haversine(lat, lng, expected.lat, expected.lng);
          if (dist > 200) { // Regions can be larger, so 200km threshold
            process.stdout.write(`✗ too far (${Math.round(dist)}km, query: "${query}")\n `);
            continue;
          }
        }

        if (hasPolygon) {
          const centroid = computeCentroid(geojson);
          if (centroid && expected) {
            const polyDist = haversine(centroid.lat, centroid.lng, expected.lat, expected.lng);
            if (polyDist > 300) {
              process.stdout.write(`✗ polygon too far (${Math.round(polyDist)}km)\n `);
              continue;
            }
          }
        }

        if (!DRY_RUN) {
          if (hasPolygon) {
            const { error: rpcError } = await supabase.rpc('upsert_region_boundary', {
              p_region_id: r.id,
              p_geojson: JSON.stringify(geojson),
              p_source_id: `nominatim/${result.osm_type}/${result.osm_id}`,
              p_confidence: 'approximate'
            });
            if (rpcError) {
              console.log(`✗ DB error: ${rpcError.message}`);
              failed++;
              found = true;
              break;
            }
          } else {
            // Centroid only
            const { error: rpcError } = await supabase.rpc('upsert_region_boundary', {
              p_region_id: r.id,
              p_lat: lat,
              p_lng: lng,
              p_source_id: `nominatim/${result.osm_type}/${result.osm_id}`,
              p_confidence: 'geocoded'
            });
            if (rpcError) {
              console.log(`✗ DB error: ${rpcError.message}`);
              failed++;
              found = true;
              break;
            }
          }
        }

        const typeLabel = hasPolygon ? `${geojson.type}` : 'centroid';
        console.log(`✓ ${typeLabel} (query: "${query}")`);
        success++;
        stored++;
        countryCounts[countryName] = (countryCounts[countryName] || 0) + 1;
        found = true;
        break;

      } catch (e) {
        process.stdout.write(`✗ error: ${e.message}\n `);
        await sleep(2000);
      }
    }

    if (!found) {
      console.log(`✗ no result found`);
      failed++;
    }
  }

  console.log(`\n=== Summary ===`);
  console.log(`Success: ${success}  Failed: ${failed}`);
  console.log(`\nStored (${stored}):`);
  Object.entries(countryCounts)
    .sort((a, b) => b[1] - a[1])
    .forEach(([c, n]) => console.log(`  ${c}: ${n}`));
}

main().catch(console.error);
