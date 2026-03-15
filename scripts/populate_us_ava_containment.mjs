#!/usr/bin/env node
/**
 * populate_us_ava_containment.mjs
 *
 * Parses UC Davis AVA GeoJSON `within`/`contains` fields to populate
 * the appellation_containment junction table with direct parent-child
 * relationships for all 276+ US AVAs.
 *
 * Key logic:
 *   - The `within` field lists ALL ancestors (transitive closure), not just direct parents
 *   - We compute direct parents by pruning transitive ancestors
 *   - 5 AVAs have 2 direct parents (DAG cases: Los Carneros, etc.)
 *   - 5 pairs of overlapping AVAs (reciprocal containment) are skipped
 *   - 4 data quality fixes applied to the `within` field
 *
 * Usage:
 *   node scripts/populate_us_ava_containment.mjs              # full run
 *   node scripts/populate_us_ava_containment.mjs --dry-run    # preview only
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
// Data quality fixes for the UC Davis `within` field
// ---------------------------------------------------------------------------
// These are pipe-delimiter bugs where AVA names containing spaces were split
const WITHIN_FIXES = {
  // AVA name -> corrected `within` string
  'Lake Wisconsin': within => within.replace('Upper Mississippi|River Valley', 'Upper Mississippi River Valley'),
  'Wild Horse Valley': within => within.replace('Solano County|Green Valley', 'Solano County Green Valley'),
  'Green Valley of Russian River Valley': within => within.replace('Northern Sonoma Valley', 'Northern Sonoma'),
  'West Sonoma Coast': within => within.replace('Sonoma Coast, North Coast', 'Sonoma Coast|North Coast'),
};

// ---------------------------------------------------------------------------
// Overlapping AVA pairs — these have reciprocal containment in the data
// but are NOT hierarchical. Skip these relationships.
// ---------------------------------------------------------------------------
const OVERLAP_PAIRS = new Set([
  'Sonoma Coast|Northern Sonoma',
  'Northern Sonoma|Sonoma Coast',
  'Russian River Valley|Alexander Valley',
  'Alexander Valley|Russian River Valley',
  'Mendocino Ridge|Anderson Valley',
  'Anderson Valley|Mendocino Ridge',
  'Alexander Valley|Pine Mountain-Cloverdale Peak',
  'Pine Mountain-Cloverdale Peak|Alexander Valley',
  'Dry Creek Valley|Rockpile',
  'Rockpile|Dry Creek Valley',
]);

function isOverlap(parentName, childName) {
  return OVERLAP_PAIRS.has(`${parentName}|${childName}`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== US AVA Containment Import ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

  // 1. Load GeoJSON
  const geojsonPath = new URL('../avas_ucdavis.geojson', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
  const geojson = JSON.parse(readFileSync(geojsonPath, 'utf8'));
  console.log(`Loaded ${geojson.features.length} AVAs from GeoJSON`);

  // 2. Build GeoJSON lookup by name
  const geoByName = {};
  for (const f of geojson.features) {
    geoByName[f.properties.name] = f.properties;
  }

  // 3. Load all US appellations from DB
  const usAppellations = [];
  let page = 0;
  const PAGE_SIZE = 1000;
  while (true) {
    const { data, error } = await sb
      .from('appellations')
      .select('id, name, designation_type, country_id')
      .eq('designation_type', 'AVA')
      .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);
    if (error) throw error;
    usAppellations.push(...data);
    if (data.length < PAGE_SIZE) break;
    page++;
  }
  console.log(`Loaded ${usAppellations.length} US AVAs from DB`);

  // 4. Build DB lookup: name -> id (case-insensitive)
  const dbByName = {};
  const dbByNameLower = {};
  for (const a of usAppellations) {
    dbByName[a.name] = a.id;
    dbByNameLower[a.name.toLowerCase()] = a.id;
  }

  // Name mapping: GeoJSON name -> DB name (for mismatches)
  const GEO_TO_DB_NAME = {
    'Mt. Veeder': 'Mount Veeder',
    'Moon Mountain District Sonoma County': 'Moon Mountain District',
    'San Luis Obispo Coast': 'San Luis Obispo',
    'San Benito': 'San Benito County',
    'Contra Costa': 'Contra Costa County',
  };

  function resolveDbId(geoName) {
    // Direct match
    if (dbByName[geoName]) return { id: dbByName[geoName], matchedName: geoName };
    // Name mapping
    const mapped = GEO_TO_DB_NAME[geoName];
    if (mapped && dbByName[mapped]) return { id: dbByName[mapped], matchedName: mapped };
    // Case-insensitive
    const lower = geoName.toLowerCase();
    if (dbByNameLower[lower]) return { id: dbByNameLower[lower], matchedName: geoName };
    return null;
  }

  // 5. Parse `within` fields and compute direct parents
  const directRelationships = []; // { parentName, childName, parentId, childId }
  const unmatched = [];
  let skippedOverlaps = 0;

  for (const f of geojson.features) {
    const props = f.properties;
    let withinStr = props.within;
    if (withinStr === null || withinStr === undefined) continue;

    // Apply data quality fixes
    if (WITHIN_FIXES[props.name]) {
      withinStr = WITHIN_FIXES[props.name](withinStr);
    }

    // Parse ancestor list
    const ancestors = withinStr.split('|').map(s => s.trim()).filter(Boolean);
    if (ancestors.length === 0) continue;

    // Compute direct parents: prune transitive ancestors
    // An ancestor A is transitive (not direct) if another ancestor B is itself within A
    const directParents = ancestors.filter(ancestor => {
      // Check if this ancestor is reachable through another ancestor
      for (const otherAncestor of ancestors) {
        if (otherAncestor === ancestor) continue;
        // Is otherAncestor within ancestor? (making ancestor transitive for this child)
        const otherProps = geoByName[otherAncestor];
        if (otherProps && otherProps.within) {
          let otherWithin = otherProps.within;
          if (WITHIN_FIXES[otherAncestor]) {
            otherWithin = WITHIN_FIXES[otherAncestor](otherWithin);
          }
          const otherAncestors = otherWithin.split('|').map(s => s.trim());
          if (otherAncestors.includes(ancestor)) {
            // ancestor is reachable through otherAncestor -> it's transitive
            return false;
          }
        }
      }
      return true;
    });

    // Resolve child ID
    const childResolved = resolveDbId(props.name);
    if (!childResolved) {
      unmatched.push({ name: props.name, type: 'child' });
      continue;
    }

    // Create relationships for each direct parent
    for (const parentName of directParents) {
      // Skip overlapping pairs
      if (isOverlap(parentName, props.name)) {
        skippedOverlaps++;
        continue;
      }

      const parentResolved = resolveDbId(parentName);
      if (!parentResolved) {
        unmatched.push({ name: parentName, type: 'parent', referencedBy: props.name });
        continue;
      }

      directRelationships.push({
        parentName: parentResolved.matchedName,
        childName: childResolved.matchedName,
        parentId: parentResolved.id,
        childId: childResolved.id,
      });
    }
  }

  // 6. Deduplicate (in case of name mapping collisions)
  const seen = new Set();
  const uniqueRelationships = directRelationships.filter(r => {
    const key = `${r.parentId}|${r.childId}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // 7. Report
  console.log(`\nDirect parent-child relationships: ${uniqueRelationships.length}`);
  console.log(`Skipped overlapping pairs: ${skippedOverlaps}`);
  if (unmatched.length > 0) {
    console.log(`\nUnmatched names (${unmatched.length}):`);
    for (const u of unmatched) {
      console.log(`  [${u.type}] ${u.name}${u.referencedBy ? ` (referenced by ${u.referencedBy})` : ''}`);
    }
  }

  // Count multi-parent AVAs
  const childParentCount = {};
  for (const r of uniqueRelationships) {
    childParentCount[r.childName] = (childParentCount[r.childName] || 0) + 1;
  }
  const multiParent = Object.entries(childParentCount).filter(([, c]) => c > 1);
  if (multiParent.length > 0) {
    console.log(`\nMulti-parent AVAs (DAG cases):`);
    for (const [name, count] of multiParent) {
      const parents = uniqueRelationships.filter(r => r.childName === name).map(r => r.parentName);
      console.log(`  ${name} -> ${parents.join(' + ')} (${count} parents)`);
    }
  }

  // Show some sample relationships
  console.log(`\nSample relationships:`);
  const napaChildren = uniqueRelationships.filter(r => r.parentName === 'Napa Valley');
  console.log(`  Napa Valley has ${napaChildren.length} children: ${napaChildren.map(r => r.childName).sort().join(', ')}`);

  if (DRY_RUN) {
    console.log('\n[DRY RUN] No changes made.');
    return;
  }

  // 8. Check for existing rows and insert
  const { data: existing, error: existErr } = await sb
    .from('appellation_containment')
    .select('parent_id, child_id');
  if (existErr) throw existErr;

  const existingSet = new Set((existing || []).map(r => `${r.parent_id}|${r.child_id}`));
  const toInsert = uniqueRelationships.filter(r => !existingSet.has(`${r.parentId}|${r.childId}`));

  if (toInsert.length === 0) {
    console.log('\nAll relationships already exist in DB. Nothing to insert.');
    return;
  }

  console.log(`\nInserting ${toInsert.length} new containment rows...`);

  // Batch insert (Supabase handles up to ~1000 at a time)
  const BATCH_SIZE = 500;
  let inserted = 0;
  for (let i = 0; i < toInsert.length; i += BATCH_SIZE) {
    const batch = toInsert.slice(i, i + BATCH_SIZE).map(r => ({
      parent_id: r.parentId,
      child_id: r.childId,
      source: 'explicit',
    }));
    const { error } = await sb.from('appellation_containment').insert(batch);
    if (error) {
      console.error(`Error inserting batch ${i / BATCH_SIZE + 1}:`, error);
      throw error;
    }
    inserted += batch.length;
    console.log(`  Inserted ${inserted}/${toInsert.length}`);
  }

  console.log(`\nDone! Inserted ${inserted} US AVA containment relationships.`);
}

main().catch(e => { console.error(e); process.exit(1); });
