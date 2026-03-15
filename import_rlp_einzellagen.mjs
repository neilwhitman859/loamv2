#!/usr/bin/env node
/**
 * import_rlp_einzellagen.mjs
 *
 * Imports German RLP (Rheinland-Pfalz) Einzellagen vineyard boundaries
 * from the ldproxy OGC API into the Loam database.
 *
 * Data source: ldproxy (Datenlizenz Deutschland 2.0)
 * 1,585 vineyards across 6 RLP wine regions (Mosel, Nahe, Pfalz, Rheinhessen, Ahr, Mittelrhein)
 *
 * Appellations are LOCKED — no new appellations are created.
 * Only matches existing DB Einzellagen by name and adds boundary polygons.
 *
 * Phases:
 *   Phase 0: Fetch all features from ldproxy API (paginated)
 *   Phase 1: Load DB Einzellagen and build lookup maps
 *   Phase 2: Match API features → DB appellations, import boundaries
 *   Phase 3: Report match/skip/error counts
 *
 * Usage:
 *   node import_rlp_einzellagen.mjs              # full run
 *   node import_rlp_einzellagen.mjs --dry-run    # preview matches only
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, writeFileSync, existsSync } from 'fs';

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
const LDPROXY_BASE = 'https://demo.ldproxy.net/vineyards/collections/vineyards/items';
const PAGE_SIZE = 1000;
const CACHE_FILE = 'data/rlp_vineyards.json';

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

/** Normalize German text for matching: ß→ss, strip parenthetical qualifiers */
function normalizeDE(s) {
  return s.toLowerCase()
    .replace(/ß/g, 'ss')
    .replace(/\s*\(mosel\)/gi, '')
    .replace(/\s*\(nahe\)/gi, '')
    .replace(/\s*\(ahr\)/gi, '')
    .replace(/a\.d\.weinstr\.?/gi, 'an der Weinstraße'.toLowerCase())
    .replace(/a\.\s*d\.\s*w\.?/gi, 'an der weinstraße')
    .replace(/a\.\s*sand/gi, 'am sand')
    .trim();
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
// Phase 0: Fetch from ldproxy API
// ---------------------------------------------------------------------------
async function fetchAllFeatures() {
  // Use cached file if it exists
  if (existsSync(CACHE_FILE)) {
    console.log(`  Loading cached data from ${CACHE_FILE}`);
    return JSON.parse(readFileSync(CACHE_FILE, 'utf8'));
  }

  console.log(`  Fetching from ${LDPROXY_BASE}...`);
  const allFeatures = [];
  let offset = 0;

  while (true) {
    const url = `${LDPROXY_BASE}?f=json&limit=${PAGE_SIZE}&offset=${offset}`;
    console.log(`  Fetching offset=${offset}...`);
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} at offset ${offset}`);
    const data = await res.json();

    const features = data.features || [];
    allFeatures.push(...features);
    console.log(`  Got ${features.length} features (total: ${allFeatures.length})`);

    if (features.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await sleep(1000); // Be polite to the API
  }

  // Cache for reproducibility
  writeFileSync(CACHE_FILE, JSON.stringify(allFeatures, null, 2));
  console.log(`  Cached ${allFeatures.length} features to ${CACHE_FILE}`);
  return allFeatures;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('=== RLP Einzellagen Boundary Import ===');
  if (DRY_RUN) console.log('[DRY RUN MODE]');

  // Phase 0: Fetch API data
  console.log('\n--- Phase 0: Fetching API data ---');
  const features = await fetchAllFeatures();
  console.log(`  Total features: ${features.length}`);

  // Phase 1: Load DB reference data
  console.log('\n--- Phase 1: Loading DB Einzellagen ---');

  const { data: countries } = await sb.from('countries').select('id, iso_code').eq('iso_code', 'DE');
  const germany = countries?.[0];
  if (!germany) throw new Error('Germany not found');

  // Paginated fetch of all DE Einzellagen
  let existingApps = [];
  let offset = 0;
  while (true) {
    const { data: page, error } = await sb.from('appellations')
      .select('id, name, slug')
      .eq('country_id', germany.id)
      .eq('classification_level', 'einzellage')
      .is('deleted_at', null)
      .range(offset, offset + PAGE_SIZE - 1);
    if (error) throw new Error(`Failed to fetch appellations: ${error.message}`);
    existingApps = existingApps.concat(page);
    if (page.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  console.log(`  ${existingApps.length} DB Einzellagen loaded`);

  // Build lookup maps
  const appByName = new Map();      // lowercase exact name → app
  const appByNorm = new Map();      // normalized name → app
  const appBySlug = new Map();      // slug → app
  const matchedAppIds = new Set();  // track which DB apps got matched

  for (const app of existingApps) {
    appByName.set(app.name.toLowerCase(), app);
    appByNorm.set(normalizeDE(app.name), app);
    appBySlug.set(app.slug, app);
  }

  // Phase 2: Match & import boundaries
  console.log('\n--- Phase 2: Matching & importing boundaries ---');

  const stats = { matched: 0, skipped: 0, boundaries: 0, errors: 0 };
  const unmatched = [];
  let processed = 0;

  for (const feature of features) {
    const props = feature.properties;
    const name = props.name;
    const village = props.village;
    const cadastral = props.cadastraldistrict;
    const geometry = feature.geometry;

    if (!geometry) {
      console.log(`  [SKIP] No geometry: ${name}`);
      stats.skipped++;
      continue;
    }

    // Try matching strategies in order
    let app = null;

    // Strategy 1: "name, cadastraldistrict" exact match
    if (cadastral) {
      app = appByName.get(`${name}, ${cadastral}`.toLowerCase());
    }

    // Strategy 2: "name, village" exact match
    if (!app && village) {
      app = appByName.get(`${name}, ${village}`.toLowerCase());
    }

    // Strategy 3: Normalized match with cadastraldistrict
    if (!app && cadastral) {
      app = appByNorm.get(normalizeDE(`${name}, ${cadastral}`));
    }

    // Strategy 4: Normalized match with village
    if (!app && village) {
      app = appByNorm.get(normalizeDE(`${name}, ${village}`));
    }

    // Strategy 5: Slug-based match with cadastraldistrict
    if (!app && cadastral) {
      app = appBySlug.get(slugify(`${name} ${cadastral}`));
    }

    // Strategy 6: Slug-based match with village
    if (!app && village) {
      app = appBySlug.get(slugify(`${name} ${village}`));
    }

    if (!app) {
      unmatched.push({ name, village, cadastral, region: props.region, subregion: props.subregion });
      stats.skipped++;
      continue;
    }

    stats.matched++;
    matchedAppIds.add(app.id);

    // Simplify polygon
    let simplified = simplifyPrecision(geometry);
    const geoStr = JSON.stringify(simplified);
    if (geoStr.length > 250_000) {
      for (const tol of [0.001, 0.002, 0.005, 0.01]) {
        simplified = simplifyGeometry(simplified, tol);
        if (JSON.stringify(simplified).length <= 250_000) break;
      }
      console.log(`  [SIMPLIFY] ${name}: ${geoStr.length} -> ${JSON.stringify(simplified).length} bytes`);
    }

    // Import boundary
    const sourceId = `ldproxy-rlp/${feature.id || `${slugify(name)}-${slugify(cadastral || village || '')}`}`;

    if (DRY_RUN) {
      stats.boundaries++;
      processed++;
      continue;
    }

    const { error } = await sb.rpc('upsert_appellation_boundary', {
      p_appellation_id: app.id,
      p_geojson: JSON.stringify(simplified),
      p_source_id: sourceId,
      p_confidence: 'official',
    });

    if (error) {
      console.log(`  [ERROR] Boundary for ${app.name}: ${error.message}`);
      stats.errors++;
    } else {
      stats.boundaries++;
    }

    processed++;
    if (processed % 50 === 0) {
      console.log(`  Processed ${processed}/${features.length} (${stats.matched} matched, ${stats.skipped} skipped)...`);
    }
    if (processed % 20 === 0) await sleep(50);
  }

  // Phase 3: Report
  console.log('\n=== Results ===');
  console.log(`  API features:     ${features.length}`);
  console.log(`  Matched:          ${stats.matched}`);
  console.log(`  Boundaries added: ${stats.boundaries}`);
  console.log(`  Skipped:          ${stats.skipped}`);
  console.log(`  Errors:           ${stats.errors}`);
  console.log(`  Match rate:       ${(stats.matched / features.length * 100).toFixed(1)}%`);

  if (unmatched.length > 0) {
    console.log(`\n--- Unmatched API features (${unmatched.length}) ---`);
    for (const u of unmatched.slice(0, 50)) {
      console.log(`  ${u.name}, ${u.cadastral || u.village} (${u.region} / ${u.subregion})`);
    }
    if (unmatched.length > 50) {
      console.log(`  ... and ${unmatched.length - 50} more`);
    }
  }

  const unmatchedDB = existingApps.filter(a => !matchedAppIds.has(a.id));
  if (unmatchedDB.length > 0) {
    console.log(`\n--- DB Einzellagen without RLP match (${unmatchedDB.length}) ---`);
    for (const a of unmatchedDB.slice(0, 20)) {
      console.log(`  ${a.name}`);
    }
    if (unmatchedDB.length > 20) {
      console.log(`  ... and ${unmatchedDB.length - 20} more`);
    }
  }

  if (DRY_RUN) console.log('\n[DRY RUN - no changes made]');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
