#!/usr/bin/env node
/**
 * populate_za_containment.mjs
 *
 * Imports the complete South Africa Wine of Origin (WO) hierarchy into Loam:
 *   1. Creates missing GU, Region, District, and Ward appellations
 *   2. Sets classification_level on all SA WO appellations
 *   3. Populates appellation_containment with the full SAWIS nesting
 *
 * Hierarchy levels (SAWIS):
 *   Geographical Unit (GU) → Region → District → Ward
 *
 * Source: SAWIS / TopWineSA official Wine of Origin hierarchy
 *
 * Usage:
 *   node scripts/populate_za_containment.mjs              # full run
 *   node scripts/populate_za_containment.mjs --dry-run    # preview only
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
// Complete SAWIS Wine of Origin Hierarchy
// ---------------------------------------------------------------------------
// Structure: GU → { regions: { regionName: { districts: { districtName: [wards] }, standaloneWards: [wards] } }, standaloneDistricts: { districtName: [wards] }, standaloneWards: [wards] }

const HIERARCHY = {
  'Western Cape': {
    regions: {
      'Breede River Valley': {
        districts: {
          'Breedekloof': ['Goudini', 'Slanghoek'],
          'Robertson': [
            'Agterkliphoogte', 'Ashton', 'Boesmansrivier', 'Bonnievale',
            'Eilandia', 'Goedemoed', 'Goree', 'Goudmyn', 'Hoopsrivier',
            'Klaasvoogds', 'Le Chasseur', 'McGregor', 'Vinkrivier', 'Zandrivier',
          ],
          'Worcester': [
            'Hex River Valley', 'Keeromsberg', 'Moordkuil', 'Nuy',
            'Rooikrans', 'Scherpenheuwel', 'Stettyn',
          ],
        },
        standaloneWards: [],
      },
      'Cape South Coast': {
        districts: {
          'Cape Agulhas': ['Elim'],
          'Elgin': [],
          'Lower Duivenhoks River': ['Napier'],
          'Overberg': ['Elandskloof', 'Greyton', 'Klein River', 'Theewater'],
          'Plettenberg Bay': ['Still Bay East'],
          'Swellendam': ['Buffeljags', 'Malgas', 'Stormsvlei'],
          'Walker Bay': [
            'Bot River', 'Hemel-en-Aarde Valley', 'Upper Hemel-en-Aarde Valley',
            'Hemel-en-Aarde Ridge', "Sunday's Glen", 'Springfontein Rim',
            'Stanford Foothills',
          ],
        },
        standaloneWards: ['Herbertsdale'],
      },
      'Coastal Region': {
        districts: {
          'Cape Town': ['Constantia', 'Durbanville', 'Hout Bay', 'Philadelphia'],
          'Darling': ['Groenekloof'],
          'Franschhoek': [],
          'Lutzville Valley': ['Koekenaap'],
          'Paarl': ['Agter-Paarl', 'Simonsberg-Paarl', 'Voor-Paardeberg'],
          'Stellenbosch': [
            'Banghoek', 'Bottelary', 'Devon Valley', 'Jonkershoek Valley',
            'Papegaaiberg', 'Polkadraai Hills', 'Simonsberg-Stellenbosch',
            'Vlottenburg',
          ],
          'Swartland': [
            'Malmesbury', 'Paardeberg', 'Paardeberg South', 'Piket-Bo-Berg',
            'Porseleinberg', 'Riebeekberg', 'Riebeeksrivier',
          ],
          'Tulbagh': [],
          'Wellington': ['Blouvlei', 'Bovlei', 'Groenberg', 'Limietberg', 'Mid-Berg River'],
        },
        standaloneWards: ['Bamboes Bay', 'Lamberts Bay', 'St Helena Bay'],
      },
      'Klein Karoo': {
        districts: {
          'Calitzdorp': ['Groenfontein'],
          'Langeberg-Garcia': ['Montagu', 'Outeniqua', 'Tradouw', 'Tradouw Highlands', 'Upper Langkloof'],
        },
        standaloneWards: ['Cango Valley', 'Koo Plateau'],
      },
      'Olifants River': {
        districts: {
          'Citrusdal Mountain': ['Piekenierskloof'],
          'Citrusdal Valley': ['Spruitdrift', 'Vredendal'],
        },
        standaloneWards: [],
      },
    },
    standaloneDistricts: {
      'Ceres Plateau': ['Ceres'],
      'Prince Albert': ['Kweekvallei', 'Prince Albert Valley', 'Swartberg'],
    },
    standaloneWards: ['Cederberg', 'Leipoldtville-Sandveld', 'Nieuwoudtville'],
  },

  'Northern Cape': {
    regions: {
      'Karoo-Hoogland': {
        districts: {
          'Sutherland-Karoo': [],
          'Central Orange River': ['Groblershoop', 'Grootdrink', 'Kakamas', 'Keimoes', 'Upington'],
        },
        standaloneWards: [],
      },
    },
    standaloneDistricts: {
      'Douglas': [],
    },
    standaloneWards: ['Hartswater', 'Prieska'],
  },

  'Eastern Cape': {
    regions: {},
    standaloneDistricts: {},
    standaloneWards: ['St Francis Bay'],
  },

  'KwaZulu-Natal': {
    regions: {},
    standaloneDistricts: {
      'Central Drakensberg': [],
      'Lions River': [],
    },
    standaloneWards: [],
  },

  'Free State': {
    regions: {},
    standaloneDistricts: {},
    standaloneWards: ['Rietrivier FS'],
  },
};

// Ward with no GU at all
const NO_GU_WARDS = ['Lanseria'];

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== South Africa WO Containment Import ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

  // 1. Get ZA country ID and catch-all region
  const { data: zaCountry } = await sb
    .from('countries').select('id').eq('iso_code', 'ZA').single();
  const ZA_COUNTRY_ID = zaCountry.id;
  console.log(`ZA country ID: ${ZA_COUNTRY_ID}`);

  const { data: zaRegions } = await sb
    .from('regions')
    .select('id, name')
    .eq('country_id', ZA_COUNTRY_ID);
  const catchAllRegion = zaRegions.find(r => r.name === 'South Africa');
  const ZA_REGION_ID = catchAllRegion.id;
  console.log(`Catch-all region ID: ${ZA_REGION_ID}`);

  // 2. Load existing ZA appellations
  const existingApps = [];
  let page = 0;
  const PAGE_SIZE = 1000;
  while (true) {
    const { data, error } = await sb
      .from('appellations')
      .select('id, name, slug, classification_level')
      .eq('country_id', ZA_COUNTRY_ID)
      .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);
    if (error) throw error;
    existingApps.push(...data);
    if (data.length < PAGE_SIZE) break;
    page++;
  }
  console.log(`Existing ZA appellations: ${existingApps.length}`);

  const appByName = {};
  for (const a of existingApps) {
    appByName[a.name] = a;
  }

  // 3. Collect ALL names from hierarchy and determine their classification_level
  const allEntries = []; // { name, level }

  // GUs
  for (const guName of Object.keys(HIERARCHY)) {
    allEntries.push({ name: guName, level: 'geographical_unit' });
  }

  for (const [, guData] of Object.entries(HIERARCHY)) {
    // Regions
    for (const regionName of Object.keys(guData.regions || {})) {
      allEntries.push({ name: regionName, level: 'region' });
      const regionData = guData.regions[regionName];

      // Districts within regions
      for (const [districtName, wards] of Object.entries(regionData.districts || {})) {
        allEntries.push({ name: districtName, level: 'district' });
        for (const ward of wards) {
          allEntries.push({ name: ward, level: 'ward' });
        }
      }
      // Standalone wards within regions
      for (const ward of (regionData.standaloneWards || [])) {
        allEntries.push({ name: ward, level: 'ward' });
      }
    }

    // Standalone districts (no region)
    for (const [districtName, wards] of Object.entries(guData.standaloneDistricts || {})) {
      allEntries.push({ name: districtName, level: 'district' });
      for (const ward of wards) {
        allEntries.push({ name: ward, level: 'ward' });
      }
    }

    // Standalone wards (no region or district)
    for (const ward of (guData.standaloneWards || [])) {
      allEntries.push({ name: ward, level: 'ward' });
    }
  }

  // No-GU wards
  for (const ward of NO_GU_WARDS) {
    allEntries.push({ name: ward, level: 'ward' });
  }

  // Deduplicate (some wards might share names — shouldn't happen here, but safety)
  const entryByName = {};
  for (const e of allEntries) {
    entryByName[e.name] = e;
  }

  console.log(`Total entries in hierarchy: ${Object.keys(entryByName).length}`);
  console.log(`  GUs: ${allEntries.filter(e => e.level === 'geographical_unit').length}`);
  console.log(`  Regions: ${allEntries.filter(e => e.level === 'region').length}`);
  console.log(`  Districts: ${new Set(allEntries.filter(e => e.level === 'district').map(e => e.name)).size}`);
  console.log(`  Wards: ${new Set(allEntries.filter(e => e.level === 'ward').map(e => e.name)).size}`);

  // 4. Identify entries to create
  const toCreate = [];
  for (const [name, entry] of Object.entries(entryByName)) {
    if (!appByName[name]) {
      toCreate.push({
        name,
        slug: slugify(`${name}-south-africa`),
        designation_type: 'WO',
        classification_level: entry.level,
        country_id: ZA_COUNTRY_ID,
        region_id: ZA_REGION_ID,
        hemisphere: 'south',
      });
    }
  }

  console.log(`\nAppellations to create: ${toCreate.length}`);
  if (toCreate.length > 0) {
    const byLevel = {};
    for (const c of toCreate) {
      byLevel[c.classification_level] = byLevel[c.classification_level] || [];
      byLevel[c.classification_level].push(c.name);
    }
    for (const [level, names] of Object.entries(byLevel)) {
      console.log(`  ${level} (${names.length}): ${names.sort().join(', ')}`);
    }
  }

  // 5. Create missing appellations
  if (!DRY_RUN && toCreate.length > 0) {
    const BATCH_SIZE = 500;
    for (let i = 0; i < toCreate.length; i += BATCH_SIZE) {
      const batch = toCreate.slice(i, i + BATCH_SIZE);
      const { data: created, error } = await sb
        .from('appellations')
        .insert(batch)
        .select('id, name');
      if (error) throw error;
      for (const c of created) {
        appByName[c.name] = { id: c.id, name: c.name };
      }
      console.log(`  Created batch ${Math.floor(i / BATCH_SIZE) + 1}: ${created.length} appellations`);
    }
  } else if (DRY_RUN && toCreate.length > 0) {
    // Assign fake IDs for dry run containment counting
    for (const c of toCreate) {
      appByName[c.name] = { id: `dry-run-${slugify(c.name)}`, name: c.name };
    }
  }

  // 6. Set classification_level on ALL entries (existing + new)
  const levelUpdates = [];
  for (const [name, entry] of Object.entries(entryByName)) {
    const app = appByName[name];
    if (!app) {
      console.log(`  Warning: ${name} not found in DB (should have been created)`);
      continue;
    }
    if (app.classification_level !== entry.level) {
      levelUpdates.push({ id: app.id, name, level: entry.level });
    }
  }

  console.log(`\nClassification level updates needed: ${levelUpdates.length}`);
  if (levelUpdates.length > 0) {
    const summary = {};
    for (const u of levelUpdates) {
      summary[u.level] = (summary[u.level] || 0) + 1;
    }
    console.log(`  ${JSON.stringify(summary)}`);
  }

  if (!DRY_RUN && levelUpdates.length > 0) {
    for (const u of levelUpdates) {
      const { error } = await sb
        .from('appellations')
        .update({ classification_level: u.level })
        .eq('id', u.id);
      if (error) console.log(`  Warning: failed to update ${u.name}: ${error.message}`);
    }
    console.log(`  Updated ${levelUpdates.length} classification levels`);
  }

  // 7. Build containment relationships
  const containmentRows = [];

  function addContainment(parentName, childName) {
    const parent = appByName[parentName];
    const child = appByName[childName];
    if (!parent) { console.log(`  Warning: parent not in DB: ${parentName}`); return; }
    if (!child) { console.log(`  Warning: child not in DB: ${childName}`); return; }
    containmentRows.push({
      parent_id: parent.id,
      child_id: child.id,
      source: 'explicit',
      _parentName: parentName,
      _childName: childName,
    });
  }

  for (const [guName, guData] of Object.entries(HIERARCHY)) {
    // GU → Region
    for (const regionName of Object.keys(guData.regions || {})) {
      addContainment(guName, regionName);
      const regionData = guData.regions[regionName];

      // Region → District
      for (const [districtName, wards] of Object.entries(regionData.districts || {})) {
        addContainment(regionName, districtName);

        // District → Ward
        for (const ward of wards) {
          addContainment(districtName, ward);
        }
      }

      // Region → standalone Ward (no district)
      for (const ward of (regionData.standaloneWards || [])) {
        addContainment(regionName, ward);
      }
    }

    // GU → standalone District (no region)
    for (const [districtName, wards] of Object.entries(guData.standaloneDistricts || {})) {
      addContainment(guName, districtName);

      // District → Ward
      for (const ward of wards) {
        addContainment(districtName, ward);
      }
    }

    // GU → standalone Ward (no region or district)
    for (const ward of (guData.standaloneWards || [])) {
      addContainment(guName, ward);
    }
  }

  // Note: Lanseria (no GU) gets no containment relationships

  // Deduplicate
  const seen = new Set();
  const uniqueRows = containmentRows.filter(r => {
    const key = `${r.parent_id}|${r.child_id}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  console.log(`\nContainment relationships: ${uniqueRows.length}`);

  // Show summary by type
  const guRegion = uniqueRows.filter(r => {
    const pLevel = entryByName[r._parentName]?.level;
    const cLevel = entryByName[r._childName]?.level;
    return pLevel === 'geographical_unit' && cLevel === 'region';
  });
  const guDistrict = uniqueRows.filter(r => {
    const pLevel = entryByName[r._parentName]?.level;
    const cLevel = entryByName[r._childName]?.level;
    return pLevel === 'geographical_unit' && cLevel === 'district';
  });
  const guWard = uniqueRows.filter(r => {
    const pLevel = entryByName[r._parentName]?.level;
    const cLevel = entryByName[r._childName]?.level;
    return pLevel === 'geographical_unit' && cLevel === 'ward';
  });
  const regionDistrict = uniqueRows.filter(r => {
    const pLevel = entryByName[r._parentName]?.level;
    const cLevel = entryByName[r._childName]?.level;
    return pLevel === 'region' && cLevel === 'district';
  });
  const regionWard = uniqueRows.filter(r => {
    const pLevel = entryByName[r._parentName]?.level;
    const cLevel = entryByName[r._childName]?.level;
    return pLevel === 'region' && cLevel === 'ward';
  });
  const districtWard = uniqueRows.filter(r => {
    const pLevel = entryByName[r._parentName]?.level;
    const cLevel = entryByName[r._childName]?.level;
    return pLevel === 'district' && cLevel === 'ward';
  });

  console.log(`  GU → Region: ${guRegion.length}`);
  console.log(`  GU → District (standalone): ${guDistrict.length}`);
  console.log(`  GU → Ward (standalone): ${guWard.length}`);
  console.log(`  Region → District: ${regionDistrict.length}`);
  console.log(`  Region → Ward (standalone): ${regionWard.length}`);
  console.log(`  District → Ward: ${districtWard.length}`);

  // Show sample relationships
  console.log(`\nSample relationships:`);
  console.log(`  Western Cape children: ${uniqueRows.filter(r => r._parentName === 'Western Cape').map(r => r._childName).join(', ')}`);
  console.log(`  Coastal Region children: ${uniqueRows.filter(r => r._parentName === 'Coastal Region').map(r => r._childName).join(', ')}`);
  console.log(`  Stellenbosch children: ${uniqueRows.filter(r => r._parentName === 'Stellenbosch').map(r => r._childName).join(', ')}`);

  if (DRY_RUN) {
    console.log('\n[DRY RUN] No changes made.');
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
    console.log('\nAll relationships already exist in DB. Nothing to insert.');
    return;
  }

  console.log(`\nInserting ${toInsert.length} new containment rows...`);

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

  console.log(`\nDone! Inserted ${inserted} SA WO containment relationships.`);
}

main().catch(e => { console.error(e); process.exit(1); });
