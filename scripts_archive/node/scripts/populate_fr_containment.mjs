#!/usr/bin/env node
/**
 * populate_fr_containment.mjs
 *
 * Derives the French AOC hierarchy from spatial containment of existing
 * boundary polygons (Eurac municipality-level data) in PostGIS.
 *
 * Approach:
 *   1. Query PostGIS for all pairs where ST_Contains(parent, child)
 *      with parent area strictly larger than child area
 *   2. Remove reciprocal containment (same-boundary appellations like
 *      Bordeaux ↔ Crémant de Bordeaux, quality tiers, Eurac artifacts)
 *   3. Prune transitive ancestors to get direct parents only
 *   4. Insert into appellation_containment with source='spatially_derived'
 *
 * Limitations of Eurac municipality-level boundaries:
 *   - Grand cru / premier cru in same commune appear as same-size → skipped
 *   - Some sub-appellations spanning part of a commune may not show containment
 *   - This gives correct regional→sub-regional→communal hierarchy
 *   - Parcel-level accuracy would require INAO shapefile (future improvement)
 *
 * Usage:
 *   node scripts/populate_fr_containment.mjs              # full run
 *   node scripts/populate_fr_containment.mjs --dry-run    # preview only
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

// ---------------------------------------------------------------------------
// .env loading
// ---------------------------------------------------------------------------
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envLines = readFileSync(envPath, 'utf8').split('\n');
const env = {};
for (const l of envLines) {
  const m = l.replace(/\r/g, '').match(/^(\w+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE);

const DRY_RUN = process.argv.includes('--dry-run');

// ---------------------------------------------------------------------------
// Step 1: Fetch all spatial containment pairs from PostGIS
// ---------------------------------------------------------------------------
async function fetchContainmentPairs() {
  console.log('Querying PostGIS for spatial containment pairs...');
  console.log('(This may take a minute — computing ST_Contains for 361 × 361 pairs)');

  // We use the Supabase management API raw SQL via RPC
  // Since we can't run raw SQL from supabase-js, we'll use a paginated approach:
  // First get all French appellations with boundaries, then compute containment in batches

  // Get all French appellations with their boundary areas
  const { data: frApps, error: frErr } = await sb.rpc('execute_raw_sql', {
    query: `
      SELECT a.id, a.name,
             extensions.ST_Area(gb.boundary::extensions.geography)::float8 as area_m2
      FROM appellations a
      JOIN countries c ON a.country_id = c.id
      JOIN geographic_boundaries gb ON gb.appellation_id = a.id
      WHERE c.iso_code = 'FR' AND a.deleted_at IS NULL AND gb.boundary IS NOT NULL
      ORDER BY area_m2 DESC
    `
  });

  if (frErr) {
    // RPC doesn't exist, fall back to direct approach
    console.log('RPC not available, using direct spatial query via management API...');
    return await fetchContainmentViaDirectSQL();
  }

  return frApps;
}

// Fetch containment pairs using the Supabase SQL endpoint directly
async function fetchContainmentViaDirectSQL() {
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/`;

  // Since we can't use raw SQL from supabase-js client,
  // we'll use fetch against the PostgREST endpoint with a DB function
  // Instead, let's create a simpler approach: use the Supabase SQL API

  const sqlUrl = `${env.SUPABASE_URL}/pg`;
  const response = await fetch(sqlUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE}`,
      'apikey': env.SUPABASE_SERVICE_ROLE,
    },
    body: JSON.stringify({
      query: `
        WITH fr_bounds AS (
          SELECT a.id, a.name, gb.boundary,
                 extensions.ST_Area(gb.boundary::extensions.geography) as area_m2
          FROM appellations a
          JOIN countries c ON a.country_id = c.id
          JOIN geographic_boundaries gb ON gb.appellation_id = a.id
          WHERE c.iso_code = 'FR' AND a.deleted_at IS NULL AND gb.boundary IS NOT NULL
        )
        SELECT p.id as parent_id, p.name as parent_name,
               c.id as child_id, c.name as child_name,
               p.area_m2 as parent_area, c.area_m2 as child_area
        FROM fr_bounds p
        JOIN fr_bounds c ON p.id != c.id
        WHERE extensions.ST_Contains(p.boundary, c.boundary)
          AND p.area_m2 > c.area_m2 * 1.01
        ORDER BY p.area_m2 DESC, c.area_m2 DESC
      `
    })
  });

  if (response.ok) {
    const data = await response.json();
    return data;
  }

  throw new Error(`SQL API failed: ${response.status} ${await response.text()}`);
}

// ---------------------------------------------------------------------------
// Step 2: Prune transitive ancestors (in JavaScript)
// ---------------------------------------------------------------------------
function pruneTransitive(pairs) {
  console.log(`\nPruning transitive ancestors from ${pairs.length} pairs...`);

  // Build adjacency maps
  // childToParents: child_id -> Set of parent_ids
  // parentToChildren: parent_id -> Set of child_ids
  const childToParents = new Map();
  const pairMap = new Map(); // key -> pair object

  for (const p of pairs) {
    if (!childToParents.has(p.child_id)) childToParents.set(p.child_id, new Set());
    childToParents.get(p.child_id).add(p.parent_id);
    pairMap.set(`${p.parent_id}|${p.child_id}`, p);
  }

  // For each child, find its direct parents by removing transitive ones
  // P is transitive for C if there exists M where:
  //   P contains M AND M contains C (and P != M != C)
  const directPairs = [];
  let prunedCount = 0;

  for (const [childId, parentIds] of childToParents) {
    const parents = [...parentIds];

    for (const parentId of parents) {
      let isTransitive = false;

      // Check if any other parent of C is itself a child of P
      for (const otherParentId of parents) {
        if (otherParentId === parentId) continue;

        // Is P a parent of otherParent? (Does P contain otherParent?)
        const otherParents = childToParents.get(otherParentId);
        if (otherParents && otherParents.has(parentId)) {
          // P contains otherParent, and otherParent contains C
          // So P->C is transitive through otherParent
          isTransitive = true;
          break;
        }
      }

      if (!isTransitive) {
        directPairs.push(pairMap.get(`${parentId}|${childId}`));
      } else {
        prunedCount++;
      }
    }
  }

  console.log(`Pruned ${prunedCount} transitive relationships`);
  console.log(`Direct parent-child relationships: ${directPairs.length}`);

  return directPairs;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== France AOC Spatial Containment ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

  // Try to load pre-computed pairs from file (if saved by previous run)
  let pairs;
  try {
    const cached = readFileSync(new URL('../data/fr_containment_raw.json', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1'), 'utf8');
    pairs = JSON.parse(cached);
    console.log(`Loaded ${pairs.length} pre-computed containment pairs from cache`);
  } catch {
    // Need to compute from PostGIS
    pairs = await fetchContainmentPairs();
    console.log(`Fetched ${pairs.length} containment pairs from PostGIS`);
  }

  // Prune transitive relationships
  const directPairs = pruneTransitive(pairs);

  // Count multi-parent appellations
  const childParentCount = {};
  for (const r of directPairs) {
    childParentCount[r.child_name] = (childParentCount[r.child_name] || 0) + 1;
  }
  const multiParent = Object.entries(childParentCount).filter(([, c]) => c > 1);
  if (multiParent.length > 0) {
    console.log(`\nMulti-parent appellations (${multiParent.length}):`);
    for (const [name, count] of multiParent.slice(0, 10)) {
      const parents = directPairs.filter(r => r.child_name === name).map(r => r.parent_name);
      console.log(`  ${name} -> ${parents.join(' + ')} (${count} parents)`);
    }
    if (multiParent.length > 10) console.log(`  ... and ${multiParent.length - 10} more`);
  }

  // Show sample hierarchy
  console.log('\nSample hierarchies:');
  for (const region of ['Bordeaux', 'Bourgogne', 'Côtes du Rhône', 'Languedoc', 'Champagne', 'Alsace / Vin d\'Alsace']) {
    const children = directPairs.filter(r => r.parent_name === region);
    if (children.length > 0) {
      console.log(`  ${region} (${children.length} children): ${children.map(r => r.child_name).sort().slice(0, 8).join(', ')}${children.length > 8 ? '...' : ''}`);
    }
  }

  // Show depth analysis
  console.log('\nHierarchy depth analysis:');
  const topLevel = new Set(directPairs.map(r => r.parent_name));
  const childSet = new Set(directPairs.map(r => r.child_name));
  const roots = [...topLevel].filter(n => !childSet.has(n));
  console.log(`  Root appellations (no parent): ${roots.length}`);
  console.log(`    ${roots.sort().join(', ')}`);

  const leaves = [...childSet].filter(n => !topLevel.has(n));
  console.log(`  Leaf appellations (no children): ${leaves.length}`);

  if (DRY_RUN) {
    console.log(`\n=== DRY RUN SUMMARY ===`);
    console.log(`Direct containment relationships: ${directPairs.length}`);
    console.log(`Source: spatially_derived (Eurac municipality boundaries)`);
    console.log(`\n[DRY RUN] No changes made.`);
    return;
  }

  // Insert into appellation_containment
  const { data: existing, error: existErr } = await sb
    .from('appellation_containment')
    .select('parent_id, child_id');
  if (existErr) throw existErr;

  const existingSet = new Set((existing || []).map(r => `${r.parent_id}|${r.child_id}`));
  const toInsert = directPairs
    .filter(r => !existingSet.has(`${r.parent_id}|${r.child_id}`))
    .map(r => ({
      parent_id: r.parent_id,
      child_id: r.child_id,
      source: 'spatially_derived',
    }));

  if (toInsert.length === 0) {
    console.log('\nAll relationships already exist in DB. Nothing to insert.');
    return;
  }

  console.log(`\nInserting ${toInsert.length} new containment rows (source: spatially_derived)...`);

  const BATCH_SIZE = 500;
  let inserted = 0;
  for (let i = 0; i < toInsert.length; i += BATCH_SIZE) {
    const batch = toInsert.slice(i, i + BATCH_SIZE);
    const { error } = await sb.from('appellation_containment').insert(batch);
    if (error) {
      console.error(`Error inserting batch ${Math.floor(i / BATCH_SIZE) + 1}:`, error);
      throw error;
    }
    inserted += batch.length;
    console.log(`  Inserted ${inserted}/${toInsert.length}`);
  }

  console.log(`\nDone! Inserted ${inserted} French AOC containment relationships.`);
}

main().catch(e => { console.error(e); process.exit(1); });
