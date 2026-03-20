#!/usr/bin/env node
/**
 * populate_ch_containment.mjs
 *
 * Imports the Swiss wine AOC hierarchy into Loam:
 *   1. Soft-deletes invalid entries (Trois Lacs — marketing term, not an AOC)
 *   2. Updates existing canton-level appellations with classification_level
 *   3. Creates missing canton-level AOCs (German-speaking Switzerland cantons,
 *      Three Lakes cantons: Fribourg, Bern)
 *   4. Creates sub-cantonal AOCs (Vaud's 8 AOCs including 2 Grand Crus)
 *   5. Populates appellation_containment with the nesting hierarchy
 *
 * Swiss AOC structure:
 *   - Each of the 26 cantons can have its own AOC (cantonal level)
 *   - Sub-cantonal AOCs exist within some cantons (notably Vaud)
 *   - Grand Cru is canton-defined, not federal (Vaud has 2 official Grand Cru AOCs)
 *   - "Six wine regions" (Valais, Vaud, Geneva, Ticino, Three Lakes,
 *     German-speaking Switzerland) are marketing groupings, NOT AOCs
 *
 * Sources:
 *   - Swiss Wine Promotion (swisswine.com) — 6 regions, canton structure
 *   - Office des Vins Vaudois (ovv.ch) — 8 Vaud AOCs
 *   - SAWIS/BLW federal wine regulation framework
 *   - artisanswiss.com — Grand Cru classification analysis
 *
 * Usage:
 *   node scripts/populate_ch_containment.mjs              # full run
 *   node scripts/populate_ch_containment.mjs --dry-run    # preview only
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

function slugify(s) {
  return s.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

// ---------------------------------------------------------------------------
// Swiss AOC Hierarchy
// ---------------------------------------------------------------------------
// Switzerland has ~26 cantons; each wine-producing canton has its own AOC.
// The federal system recognises AOC/DOC at cantonal level.
// Sub-cantonal AOCs exist where cantons have legislated them (notably Vaud).
//
// We model:
//   canton-level AOC  →  sub-cantonal AOC  →  grand_cru (where applicable)
//
// "Six wine regions" are marketing groupings used for classification_level
// context but NOT modeled as appellations.

// ---------------------------------------------------------------------------
// Entries to soft-delete (not real AOCs)
// ---------------------------------------------------------------------------
const SOFT_DELETE = [
  'Trois Lacs',   // Marketing term for Three Lakes region, not an AOC
  // 'Zürichsee' — user mentioned but not found in DB
];

// ---------------------------------------------------------------------------
// Canton-level AOCs
// ---------------------------------------------------------------------------
// Each entry: { name, region (Loam region name), designation_type, hectares (approx) }
// Only cantons with confirmed official AOC wine production are included.
//
// The 6 major cantons (Valais, Vaud, Genève, Ticino, Neuchâtel, + Fribourg/Bern)
// plus the ~17 German-speaking cantons.
//
// Note: Neuchâtel already exists in DB. Genève exists as "Genève".
// Fribourg and Bern are "Three Lakes" cantons with their own AOCs.

const CANTON_AOCS = {
  // --- Already in DB (will get classification_level set) ---
  'Valais':     { region: 'Valais',      existsInDb: true },
  'Vaud':       { region: 'Vaud',        existsInDb: true },
  'Genève':     { region: 'Geneva',      existsInDb: true },
  'Ticino':     { region: 'Ticino',      existsInDb: true },
  'Neuchâtel':  { region: 'Switzerland', existsInDb: true },

  // --- Three Lakes cantons (new) ---
  'Fribourg':   { region: 'Switzerland' },
  'Bern':       { region: 'Switzerland' },

  // --- German-speaking Switzerland cantons (new) ---
  // Major wine-producing cantons (>100 ha)
  'Zürich':           { region: 'Switzerland' },
  'Schaffhausen':     { region: 'Switzerland' },  // ~490 ha
  'Aargau':           { region: 'Switzerland' },   // ~400 ha
  'Graubünden':       { region: 'Switzerland' },   // ~410 ha
  'Thurgau':          { region: 'Switzerland' },   // ~270 ha
  'St. Gallen':       { region: 'Switzerland' },   // ~220 ha
  'Basel-Landschaft': { region: 'Switzerland' },   // ~105 ha (combined Basel)
  'Luzern':           { region: 'Switzerland' },

  // Smaller wine-producing cantons (confirmed AOC status)
  'Schwyz':                   { region: 'Switzerland' },
  'Zug':                      { region: 'Switzerland' },
  'Basel-Stadt':              { region: 'Switzerland' },
  'Glarus':                   { region: 'Switzerland' },  // ~2 ha, smallest
  'Obwalden':                 { region: 'Switzerland' },
  'Nidwalden':                { region: 'Switzerland' },
  'Appenzell Ausserrhoden':   { region: 'Switzerland' },
  'Appenzell Innerrhoden':    { region: 'Switzerland' },
  'Uri':                      { region: 'Switzerland' },
};

// ---------------------------------------------------------------------------
// Sub-cantonal AOCs (Vaud)
// ---------------------------------------------------------------------------
// Vaud is the only canton with formally legislated sub-cantonal AOCs.
// The 8 Vaud AOCs are organized into 4 geographic zones, but the zones
// themselves are NOT separate AOCs.
//
// Hierarchy:
//   Vaud (canton AOC)
//     ├── La Côte AOC
//     ├── Lavaux AOC
//     │     ├── Calamin Grand Cru AOC  (16 ha, within Lavaux)
//     │     └── Dézaley Grand Cru AOC  (54 ha, within Lavaux)
//     ├── Chablais AOC
//     ├── Bonvillars AOC
//     ├── Côtes de l'Orbe AOC
//     └── Vully AOC  (shared with Fribourg, but the AOC is Vaud-legislated)

const VAUD_SUB_AOCS = [
  { name: 'La Côte',          level: 'aoc',       parent: 'Vaud' },
  { name: 'Lavaux',           level: 'aoc',       parent: 'Vaud' },
  { name: 'Chablais',         level: 'aoc',       parent: 'Vaud' },
  { name: 'Bonvillars',       level: 'aoc',       parent: 'Vaud' },
  { name: 'Côtes de l\'Orbe', level: 'aoc',       parent: 'Vaud' },
  { name: 'Vully',            level: 'aoc',       parent: 'Vaud' },
  // Grand Crus within Lavaux
  { name: 'Calamin',          level: 'grand_cru', parent: 'Lavaux' },
  { name: 'Dézaley',          level: 'grand_cru', parent: 'Lavaux' },
];

// ---------------------------------------------------------------------------
// Three Lakes sub-AOCs
// ---------------------------------------------------------------------------
// The Three Lakes region has 4 AOCs across 3 cantons:
//   - AOC Neuchâtel (canton of Neuchâtel) — already exists
//   - AOC Vully (shared Fribourg/Vaud) — modeled under Vaud above
//   - AOC Cheyres (Fribourg)
//   - AOC Lac de Bienne (Bern, also known as Bielersee)
//
// Cheyres and Lac de Bienne are sub-cantonal AOCs under Fribourg and Bern.

const THREE_LAKES_SUB_AOCS = [
  { name: 'Cheyres',        level: 'aoc', parent: 'Fribourg' },
  { name: 'Lac de Bienne',  level: 'aoc', parent: 'Bern' },
];

// ---------------------------------------------------------------------------
// Notes on entries NOT included (conservative approach):
// ---------------------------------------------------------------------------
// - Geneva sub-regions (Mandement, Entre-Arve-et-Rhône, Entre-Arve-et-Lac):
//   Geographic areas, not formally separate AOCs. Geneva has 1 AOC + Premier Cru.
// - Geneva Premier Cru communes (Choully, Satigny, Peissy, etc.):
//   These are commune-level Premier Cru designations within Geneva AOC,
//   not separate AOCs. Could be added later if needed.
// - Ticino sub-regions (Sopraceneri, Sottoceneri, Mendrisiotto):
//   Geographic descriptors, not separate DOC appellations.
// - Valais Grand Cru communes (Vétroz, Fully, Salquenen, etc.):
//   Commune-level Grand Cru designations, not separate AOCs.
//   Each commune defines its own Grand Cru rules. Could be added later.
// - Jura canton: Sometimes grouped with Three Lakes but has minimal
//   wine production and uncertain AOC status. Omitted for now.


// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== Switzerland AOC Containment Import ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

  // 1. Get Switzerland country ID
  const { data: chCountry } = await sb
    .from('countries').select('id').eq('iso_code', 'CH').single();
  const CH_COUNTRY_ID = chCountry.id;
  console.log(`CH country ID: ${CH_COUNTRY_ID}`);

  // 2. Load existing CH regions (for region_id assignment)
  const { data: existingRegions } = await sb
    .from('regions')
    .select('id, name')
    .eq('country_id', CH_COUNTRY_ID);
  const regionByName = {};
  for (const r of existingRegions) {
    regionByName[r.name] = r.id;
  }
  console.log(`CH regions: ${existingRegions.map(r => r.name).join(', ')}`);

  // 3. Load existing CH appellations
  const { data: existingApps } = await sb
    .from('appellations')
    .select('id, name, slug, classification_level, deleted_at')
    .eq('country_id', CH_COUNTRY_ID);
  const appByName = {};
  for (const a of existingApps) {
    appByName[a.name] = a;
  }
  console.log(`Existing CH appellations: ${existingApps.length} (${existingApps.map(a => a.name).join(', ')})`);

  // 4. Soft-delete invalid entries
  console.log(`\n--- Soft-delete invalid entries ---`);
  for (const name of SOFT_DELETE) {
    const app = appByName[name];
    if (!app) {
      console.log(`  "${name}" not found in DB — skipping`);
      continue;
    }
    if (app.deleted_at) {
      console.log(`  "${name}" already soft-deleted`);
      continue;
    }
    console.log(`  Soft-deleting "${name}" (id: ${app.id})`);
    if (!DRY_RUN) {
      const { error } = await sb
        .from('appellations')
        .update({ deleted_at: new Date().toISOString() })
        .eq('id', app.id);
      if (error) console.log(`    Error: ${error.message}`);
      else console.log(`    Done`);
    }
  }

  // 5. Create/update canton-level AOCs
  console.log(`\n--- Canton-level AOCs ---`);
  const cantonToCreate = [];
  const cantonLevelUpdates = [];

  for (const [name, info] of Object.entries(CANTON_AOCS)) {
    const regionId = regionByName[info.region];
    if (!regionId) {
      console.log(`  Warning: region "${info.region}" not found for ${name}`);
      continue;
    }

    const existing = appByName[name];
    if (existing) {
      // Update classification_level if needed
      if (existing.classification_level !== 'canton') {
        cantonLevelUpdates.push({ id: existing.id, name });
      }
      // Un-soft-delete if it was deleted
      if (existing.deleted_at) {
        console.log(`  "${name}" is soft-deleted, will restore`);
        if (!DRY_RUN) {
          await sb.from('appellations').update({ deleted_at: null }).eq('id', existing.id);
        }
      }
    } else {
      cantonToCreate.push({
        name,
        slug: slugify(`${name}-switzerland`),
        designation_type: 'AOC',
        classification_level: 'canton',
        country_id: CH_COUNTRY_ID,
        region_id: regionId,
        hemisphere: 'north',
      });
    }
  }

  console.log(`Canton AOCs to create: ${cantonToCreate.length}`);
  if (cantonToCreate.length > 0) {
    console.log(`  ${cantonToCreate.map(c => c.name).join(', ')}`);
  }
  console.log(`Canton classification_level updates: ${cantonLevelUpdates.length}`);
  if (cantonLevelUpdates.length > 0) {
    console.log(`  ${cantonLevelUpdates.map(u => u.name).join(', ')}`);
  }

  if (!DRY_RUN && cantonToCreate.length > 0) {
    const { data: created, error } = await sb
      .from('appellations')
      .insert(cantonToCreate)
      .select('id, name');
    if (error) throw error;
    for (const c of created) {
      appByName[c.name] = { id: c.id, name: c.name, classification_level: 'canton' };
    }
    console.log(`  Created ${created.length} canton appellations`);
  } else if (DRY_RUN && cantonToCreate.length > 0) {
    // Assign fake IDs for dry run containment preview
    for (const c of cantonToCreate) {
      appByName[c.name] = { id: `dry-${slugify(c.name)}`, name: c.name, classification_level: 'canton' };
    }
  }

  if (!DRY_RUN && cantonLevelUpdates.length > 0) {
    for (const u of cantonLevelUpdates) {
      const { error } = await sb
        .from('appellations')
        .update({ classification_level: 'canton' })
        .eq('id', u.id);
      if (error) console.log(`  Warning: failed to update ${u.name}: ${error.message}`);
    }
    console.log(`  Updated ${cantonLevelUpdates.length} classification levels`);
  }

  // Reflect classification_level updates in appByName for containment display
  for (const u of cantonLevelUpdates) {
    if (appByName[u.name]) appByName[u.name].classification_level = 'canton';
  }

  // 6. Create sub-cantonal AOCs (Vaud + Three Lakes)
  console.log(`\n--- Sub-cantonal AOCs ---`);
  const allSubAocs = [...VAUD_SUB_AOCS, ...THREE_LAKES_SUB_AOCS];
  const subToCreate = [];

  for (const sub of allSubAocs) {
    if (appByName[sub.name]) {
      console.log(`  "${sub.name}" already exists`);
      // Update classification if needed
      const existing = appByName[sub.name];
      if (existing.classification_level !== sub.level) {
        console.log(`    Updating classification: ${existing.classification_level} → ${sub.level}`);
        if (!DRY_RUN) {
          await sb.from('appellations')
            .update({ classification_level: sub.level })
            .eq('id', existing.id);
        }
      }
      continue;
    }

    // Determine region_id from parent canton
    const parentCanton = CANTON_AOCS[sub.parent] || {};
    // For Lavaux sub-aocs, parent is 'Lavaux' which is itself under Vaud
    let regionName = parentCanton.region;
    if (!regionName) {
      // sub.parent is a sub-AOC itself (e.g., Lavaux), find the canton
      const grandParent = allSubAocs.find(s => s.name === sub.parent);
      if (grandParent) {
        regionName = (CANTON_AOCS[grandParent.parent] || {}).region;
      }
    }
    const regionId = regionByName[regionName || 'Switzerland'];

    subToCreate.push({
      name: sub.name,
      slug: slugify(`${sub.name}-switzerland`),
      designation_type: 'AOC',
      classification_level: sub.level,
      country_id: CH_COUNTRY_ID,
      region_id: regionId,
      hemisphere: 'north',
    });
  }

  console.log(`Sub-cantonal AOCs to create: ${subToCreate.length}`);
  if (subToCreate.length > 0) {
    console.log(`  ${subToCreate.map(c => c.name).join(', ')}`);
  }

  if (!DRY_RUN && subToCreate.length > 0) {
    const { data: created, error } = await sb
      .from('appellations')
      .insert(subToCreate)
      .select('id, name');
    if (error) throw error;
    for (const c of created) {
      const sub = allSubAocs.find(s => s.name === c.name);
      appByName[c.name] = { id: c.id, name: c.name, classification_level: sub?.level };
    }
    console.log(`  Created ${created.length} sub-cantonal appellations`);
  } else if (DRY_RUN && subToCreate.length > 0) {
    for (const c of subToCreate) {
      const sub = allSubAocs.find(s => s.name === c.name);
      appByName[c.name] = { id: `dry-${slugify(c.name)}`, name: c.name, classification_level: sub?.level };
    }
  }

  // 7. Build containment relationships
  console.log(`\n--- Containment relationships ---`);
  const containmentRows = [];

  function addContainment(parentName, childName) {
    const parent = appByName[parentName];
    const child = appByName[childName];
    if (!parent) { console.log(`  Warning: parent not in DB: ${parentName}`); return; }
    if (!child) { console.log(`  Warning: child not in DB: ${childName}`); return; }
    // Skip soft-deleted
    if (parent.deleted_at || child.deleted_at) return;
    containmentRows.push({
      parent_id: parent.id,
      child_id: child.id,
      source: 'explicit',
      _parentName: parentName,
      _childName: childName,
    });
  }

  // Canton → sub-cantonal AOC relationships
  for (const sub of allSubAocs) {
    addContainment(sub.parent, sub.name);
  }

  // Also: Vully is shared between Vaud and Fribourg
  // We already have Vaud → Vully; also add Fribourg → Vully
  addContainment('Fribourg', 'Vully');

  // Deduplicate
  const seen = new Set();
  const uniqueRows = containmentRows.filter(r => {
    const key = `${r.parent_id}|${r.child_id}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  console.log(`\nContainment relationships: ${uniqueRows.length}`);
  console.log(`\n  Canton → sub-cantonal AOC:`);
  for (const r of uniqueRows.filter(r => {
    const parentApp = appByName[r._parentName];
    return parentApp && parentApp.classification_level === 'canton';
  })) {
    console.log(`    ${r._parentName} → ${r._childName}`);
  }

  console.log(`\n  Sub-AOC → Grand Cru:`);
  for (const r of uniqueRows.filter(r => {
    const childApp = appByName[r._childName];
    return childApp && childApp.classification_level === 'grand_cru';
  })) {
    console.log(`    ${r._parentName} → ${r._childName}`);
  }

  if (DRY_RUN) {
    // Summary
    console.log(`\n=== DRY RUN SUMMARY ===`);
    console.log(`Soft-deletes: ${SOFT_DELETE.filter(n => appByName[n] && !appByName[n].deleted_at).length}`);
    console.log(`Canton AOCs to create: ${cantonToCreate.length}`);
    console.log(`Canton classification_level updates: ${cantonLevelUpdates.length}`);
    console.log(`Sub-cantonal AOCs to create: ${subToCreate.length}`);
    console.log(`Containment rows to insert: ${uniqueRows.length}`);
    console.log(`\nTotal new appellations: ${cantonToCreate.length + subToCreate.length}`);
    console.log(`\n[DRY RUN] No changes made.`);
    return;
  }

  // 8. Insert containment rows
  const { data: existing, error: existErr } = await sb
    .from('appellation_containment')
    .select('parent_id, child_id');
  if (existErr) throw existErr;

  const existingSet = new Set((existing || []).map(r => `${r.parent_id}|${r.child_id}`));
  const toInsert = uniqueRows
    .filter(r => !existingSet.has(`${r.parent_id}|${r.child_id}`))
    .map(r => ({
      parent_id: r.parent_id,
      child_id: r.child_id,
      source: r.source,
    }));

  if (toInsert.length === 0) {
    console.log('\nAll containment relationships already exist. Nothing to insert.');
  } else {
    console.log(`\nInserting ${toInsert.length} new containment rows...`);
    const { error } = await sb.from('appellation_containment').insert(toInsert);
    if (error) throw error;
    console.log(`  Inserted ${toInsert.length} containment rows`);
  }

  console.log('\nDone!');
}

main().catch(e => { console.error(e); process.exit(1); });
