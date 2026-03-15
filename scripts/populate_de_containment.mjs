#!/usr/bin/env node
/**
 * populate_de_containment.mjs
 *
 * Imports the German wine hierarchy from the RLP Weinbergsrolle API
 * (Rhineland-Palatinate vineyard register) into Loam.
 *
 * Data source: demo.ldproxy.net/vineyards (OGC API Features)
 * Covers 6 of 13 Anbaugebiete: Mosel, Nahe, Rheinhessen, Pfalz, Ahr, Mittelrhein
 *
 * 4-level hierarchy:
 *   Anbaugebiet (region) → Bereich (subregion) → Großlage (cluster) → Einzellage (vineyard)
 *
 * Creates:
 *   - Bereich appellations (subregions)
 *   - Großlage appellations (collective vineyard sites)
 *   - Einzellage appellations (individual vineyard sites)
 *   - Containment relationships at all levels
 *   - Sets classification_level on all German appellations
 *
 * Usage:
 *   node scripts/populate_de_containment.mjs              # full run
 *   node scripts/populate_de_containment.mjs --dry-run    # preview only
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, writeFileSync, existsSync } from 'fs';

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
// Fetch all vineyards from the RLP Weinbergsrolle API
// ---------------------------------------------------------------------------
const API_BASE = 'https://demo.ldproxy.net/vineyards/collections/vineyards/items';
const PAGE_SIZE = 200;
const CACHE_FILE = new URL('../data/de_vineyards_cache.json', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');

async function fetchAllVineyards() {
  // Check cache first
  if (existsSync(CACHE_FILE)) {
    const cached = JSON.parse(readFileSync(CACHE_FILE, 'utf8'));
    console.log(`Loaded ${cached.length} vineyards from cache`);
    return cached;
  }

  console.log('Fetching vineyards from RLP Weinbergsrolle API...');
  const allFeatures = [];
  let offset = 0;

  while (true) {
    const url = `${API_BASE}?limit=${PAGE_SIZE}&offset=${offset}&f=json`;
    console.log(`  Fetching offset ${offset}...`);
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`API error: ${resp.status} ${resp.statusText}`);
    const data = await resp.json();

    const features = data.features || [];
    allFeatures.push(...features);

    if (features.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;

    // Rate limit courtesy
    await new Promise(r => setTimeout(r, 500));
  }

  console.log(`Fetched ${allFeatures.length} vineyards total`);

  // Cache for re-runs
  writeFileSync(CACHE_FILE, JSON.stringify(allFeatures, null, 2));
  console.log(`Cached to ${CACHE_FILE}`);

  return allFeatures;
}

// ---------------------------------------------------------------------------
// Normalize subregion names (fix API data quality issues)
// ---------------------------------------------------------------------------
function normalizeSubregion(raw) {
  // "Ber. Mittelhaardt/Dt." (truncated) -> "Ber. Mittelhaardt/Dt. Weinstraße"
  if (raw === 'Ber. Mittelhaardt/Dt.') return 'Ber. Mittelhaardt/Dt. Weinstraße';
  return raw;
}

// ---------------------------------------------------------------------------
// Build hierarchy from vineyard features
// ---------------------------------------------------------------------------
function buildHierarchy(features) {
  // Extract properties from each feature
  const vineyards = features.map(f => ({
    name: f.properties.name,
    region: f.properties.region,
    subregion: normalizeSubregion(f.properties.subregion),
    cluster: f.properties.cluster,
    village: f.properties.village,
    area_ha: f.properties.area_ha,
  }));

  // Build unique hierarchy levels
  const regions = new Map();      // region name -> { bereiche }
  const bereiche = new Map();     // bereich name -> { region, grosslagen }
  const grosslagen = new Map();   // grosslage name -> { region, bereich, einzellagen }
  const einzellagen = new Map();  // einzellage key -> { name, region, bereich, grosslage, village }

  for (const v of vineyards) {
    const hasGrosslage = v.cluster && v.cluster !== '--';

    // Region
    if (!regions.has(v.region)) {
      regions.set(v.region, { bereiche: new Set() });
    }
    regions.get(v.region).bereiche.add(v.subregion);

    // Bereich
    if (!bereiche.has(v.subregion)) {
      bereiche.set(v.subregion, { region: v.region, grosslagen: new Set() });
    }
    if (hasGrosslage) {
      bereiche.get(v.subregion).grosslagen.add(v.cluster);
    }

    // Großlage (skip if cluster is "--" or empty)
    if (hasGrosslage) {
      if (!grosslagen.has(v.cluster)) {
        grosslagen.set(v.cluster, { region: v.region, bereich: v.subregion, einzellagen: new Set() });
      }
      grosslagen.get(v.cluster).einzellagen.add(v.name);
    }

    // Einzellage (use region+name as key to avoid name collisions across regions)
    const key = `${v.region}|${v.name}`;
    if (!einzellagen.has(key)) {
      einzellagen.set(key, {
        name: v.name,
        region: v.region,
        bereich: v.subregion,
        grosslage: hasGrosslage ? v.cluster : null,
        village: v.village,
      });
    }
  }

  return { regions, bereiche, grosslagen, einzellagen };
}

// ---------------------------------------------------------------------------
// Clean up Bereich names
// ---------------------------------------------------------------------------
function cleanBereichName(raw) {
  // "Bereich Saar" -> "Saar"
  // "Ber. Mittelhaardt/Dt. Weinstraße" -> "Mittelhaardt/Deutsche Weinstraße"
  // "Ber. Südl. Weinstraße" -> "Südliche Weinstraße"
  return raw
    .replace(/^Bereich\s+/, '')
    .replace(/^Ber\.\s+/, '')
    .replace('Mittelhaardt/Dt. Weinstraße', 'Mittelhaardt-Deutsche Weinstraße')
    .replace('Südl. Weinstraße', 'Südliche Weinstraße');
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== Germany Vineyard Hierarchy Import ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

  // 1. Fetch vineyard data
  const features = await fetchAllVineyards();

  // 2. Build hierarchy
  const { regions, bereiche, grosslagen, einzellagen } = buildHierarchy(features);

  console.log(`\nHierarchy summary:`);
  console.log(`  Regions (Anbaugebiete): ${regions.size}`);
  console.log(`  Bereiche (subregions): ${bereiche.size}`);
  console.log(`  Großlagen (clusters): ${grosslagen.size}`);
  console.log(`  Einzellagen (vineyards): ${einzellagen.size}`);

  // Print hierarchy tree
  console.log(`\nHierarchy tree:`);
  for (const [region, rData] of regions) {
    console.log(`  ${region} (${rData.bereiche.size} Bereiche)`);
    for (const b of rData.bereiche) {
      const bData = bereiche.get(b);
      console.log(`    ${cleanBereichName(b)} (${bData.grosslagen.size} Großlagen)`);
    }
  }

  // 3. Load DB context
  const { data: deCountry } = await sb
    .from('countries').select('id').eq('iso_code', 'DE').single();
  const DE_COUNTRY_ID = deCountry.id;

  // Load existing German appellations
  const { data: existingApps } = await sb
    .from('appellations')
    .select('id, name, slug, classification_level, designation_type')
    .eq('country_id', DE_COUNTRY_ID);

  const appByName = {};
  const appByNameLower = {};
  for (const a of existingApps) {
    appByName[a.name] = a;
    appByNameLower[a.name.toLowerCase()] = a;
  }
  console.log(`\nExisting German appellations: ${existingApps.length}`);

  // Load German regions
  const { data: deRegions } = await sb
    .from('regions')
    .select('id, name')
    .eq('country_id', DE_COUNTRY_ID);
  const regionByName = {};
  for (const r of deRegions) {
    regionByName[r.name] = r.id;
  }

  // 4. Match Anbaugebiete to existing DB entries
  console.log(`\n--- Matching Anbaugebiete ---`);
  const anbaugebieteMap = {}; // API region name -> DB appellation
  for (const [region] of regions) {
    const existing = appByName[region];
    if (existing) {
      anbaugebieteMap[region] = existing;
      // Update classification_level if not set
      if (existing.classification_level !== 'anbaugebiet') {
        console.log(`  ${region}: updating classification_level to 'anbaugebiet'`);
        if (!DRY_RUN) {
          await sb.from('appellations')
            .update({ classification_level: 'anbaugebiet' })
            .eq('id', existing.id);
        }
        existing.classification_level = 'anbaugebiet';
      } else {
        console.log(`  ${region}: matched (${existing.id})`);
      }
    } else {
      console.log(`  ${region}: NOT FOUND IN DB`);
    }
  }

  // Also update classification_level for the 7 non-covered Anbaugebiete
  const OTHER_ANBAUGEBIETE = ['Baden', 'Württemberg', 'Franken', 'Rheingau',
    'Hessische Bergstraße', 'Saale-Unstrut', 'Sachsen'];
  for (const name of OTHER_ANBAUGEBIETE) {
    const existing = appByName[name];
    if (existing && existing.classification_level !== 'anbaugebiet') {
      console.log(`  ${name}: updating classification_level to 'anbaugebiet'`);
      if (!DRY_RUN) {
        await sb.from('appellations')
          .update({ classification_level: 'anbaugebiet' })
          .eq('id', existing.id);
      }
    }
  }

  // Find the default region_id for German appellations
  const defaultRegionId = regionByName['Germany'] || regionByName['Deutschland']
    || (deRegions.length > 0 ? deRegions[0].id : null);

  // 5. Create Bereiche
  console.log(`\n--- Creating Bereiche ---`);
  const bereicheToCreate = [];
  const bereichMap = {}; // bereich raw name -> DB id

  for (const [rawName, bData] of bereiche) {
    const cleanName = cleanBereichName(rawName);
    const displayName = `${cleanName} (Bereich)`;

    // Check if already exists
    if (appByName[displayName] || appByName[cleanName]) {
      const existing = appByName[displayName] || appByName[cleanName];
      bereichMap[rawName] = existing.id;
      console.log(`  ${displayName}: already exists`);
      continue;
    }

    // Find the region_id from the parent Anbaugebiet's region
    const parentApp = anbaugebieteMap[bData.region];
    const regionId = defaultRegionId;

    bereicheToCreate.push({
      name: displayName,
      slug: slugify(`${cleanName}-bereich-germany`),
      designation_type: 'Qualitätswein',
      classification_level: 'bereich',
      country_id: DE_COUNTRY_ID,
      region_id: regionId,
      hemisphere: 'north',
      _rawBereichName: rawName,
    });
  }

  console.log(`Bereiche to create: ${bereicheToCreate.length}`);

  if (!DRY_RUN && bereicheToCreate.length > 0) {
    const insertData = bereicheToCreate.map(({ _rawBereichName, ...rest }) => rest);
    const { data: created, error } = await sb
      .from('appellations')
      .insert(insertData)
      .select('id, name');
    if (error) throw error;
    for (const c of created) {
      appByName[c.name] = { id: c.id, name: c.name, classification_level: 'bereich' };
      // Find the raw name
      const entry = bereicheToCreate.find(b => b.name === c.name);
      if (entry) bereichMap[entry._rawBereichName] = c.id;
    }
    console.log(`  Created ${created.length} Bereiche`);
  } else if (DRY_RUN) {
    for (const b of bereicheToCreate) {
      const fakeId = `dry-bereich-${slugify(b.name)}`;
      appByName[b.name] = { id: fakeId, name: b.name, classification_level: 'bereich' };
      bereichMap[b._rawBereichName] = fakeId;
    }
  }

  // 6. Create Großlagen
  console.log(`\n--- Creating Großlagen ---`);
  const grosslagenToCreate = [];
  const grosslageMap = {}; // grosslage name -> DB id

  for (const [name, gData] of grosslagen) {
    const displayName = `${name} (Großlage)`;

    if (appByName[displayName] || appByName[name]) {
      const existing = appByName[displayName] || appByName[name];
      grosslageMap[name] = existing.id;
      continue;
    }

    grosslagenToCreate.push({
      name: displayName,
      slug: slugify(`${name}-grosslage-germany`),
      designation_type: 'Qualitätswein',
      classification_level: 'grosslage',
      country_id: DE_COUNTRY_ID,
      region_id: defaultRegionId,
      hemisphere: 'north',
      _rawGrosslageName: name,
    });
  }

  console.log(`Großlagen to create: ${grosslagenToCreate.length}`);

  if (!DRY_RUN && grosslagenToCreate.length > 0) {
    // Batch insert (some might have long names)
    const BATCH_SIZE = 200;
    let totalCreated = 0;
    for (let i = 0; i < grosslagenToCreate.length; i += BATCH_SIZE) {
      const batch = grosslagenToCreate.slice(i, i + BATCH_SIZE)
        .map(({ _rawGrosslageName, ...rest }) => rest);
      const { data: created, error } = await sb
        .from('appellations')
        .insert(batch)
        .select('id, name');
      if (error) throw error;
      for (const c of created) {
        appByName[c.name] = { id: c.id, name: c.name, classification_level: 'grosslage' };
        const entry = grosslagenToCreate.find(g => g.name === c.name);
        if (entry) grosslageMap[entry._rawGrosslageName] = c.id;
      }
      totalCreated += created.length;
    }
    console.log(`  Created ${totalCreated} Großlagen`);
  } else if (DRY_RUN) {
    for (const g of grosslagenToCreate) {
      const fakeId = `dry-grosslage-${slugify(g.name)}`;
      appByName[g.name] = { id: fakeId, name: g.name, classification_level: 'grosslage' };
      grosslageMap[g._rawGrosslageName] = fakeId;
    }
  }

  // 7. Create Einzellagen
  console.log(`\n--- Creating Einzellagen ---`);
  const einzellagenToCreate = [];
  const einzellageMap = {}; // "region|name" -> DB id

  for (const [key, eData] of einzellagen) {
    // Check if this vineyard already exists in DB
    // Existing vineyards: Monzinger Niederberg, Uhlen variants
    const existing = appByName[eData.name];
    if (existing) {
      einzellageMap[key] = existing.id;
      // Update classification_level if needed
      if (existing.classification_level !== 'einzellage') {
        if (!DRY_RUN) {
          await sb.from('appellations')
            .update({ classification_level: 'einzellage' })
            .eq('id', existing.id);
        }
      }
      continue;
    }

    // Use village name for disambiguation: "Kupp, Wiltingen" pattern
    const displayName = eData.village ? `${eData.name}, ${eData.village}` : eData.name;

    einzellagenToCreate.push({
      name: displayName,
      slug: slugify(`${eData.name}-${eData.village || eData.region}-einzellage`),
      designation_type: 'Qualitätswein',
      classification_level: 'einzellage',
      country_id: DE_COUNTRY_ID,
      region_id: defaultRegionId,
      hemisphere: 'north',
      _key: key,
    });
  }

  console.log(`Einzellagen to create: ${einzellagenToCreate.length}`);

  if (!DRY_RUN && einzellagenToCreate.length > 0) {
    const BATCH_SIZE = 200;
    let totalCreated = 0;
    for (let i = 0; i < einzellagenToCreate.length; i += BATCH_SIZE) {
      const batch = einzellagenToCreate.slice(i, i + BATCH_SIZE)
        .map(({ _key, ...rest }) => rest);
      const { data: created, error } = await sb
        .from('appellations')
        .insert(batch)
        .select('id, name');
      if (error) {
        console.error(`Error inserting batch ${Math.floor(i / BATCH_SIZE) + 1}:`, error);
        // Try individual inserts to find the problematic row
        for (const row of batch) {
          const { data: single, error: sErr } = await sb
            .from('appellations')
            .insert(row)
            .select('id, name');
          if (sErr) {
            console.error(`  Failed: ${row.name} (${row.slug}): ${sErr.message}`);
          } else if (single && single[0]) {
            appByName[single[0].name] = { id: single[0].id, name: single[0].name, classification_level: 'einzellage' };
            const entry = einzellagenToCreate.find(e => e.name === single[0].name);
            if (entry) einzellageMap[entry._key] = single[0].id;
            totalCreated++;
          }
        }
        continue;
      }
      for (const c of created) {
        appByName[c.name] = { id: c.id, name: c.name, classification_level: 'einzellage' };
        const entry = einzellagenToCreate.find(e => e.name === c.name);
        if (entry) einzellageMap[entry._key] = c.id;
      }
      totalCreated += created.length;
      console.log(`  Inserted ${totalCreated}/${einzellagenToCreate.length} Einzellagen`);
    }
    console.log(`  Created ${totalCreated} Einzellagen total`);
  } else if (DRY_RUN) {
    for (const e of einzellagenToCreate) {
      const fakeId = `dry-einzellage-${slugify(e.name)}`;
      appByName[e.name] = { id: fakeId, name: e.name, classification_level: 'einzellage' };
      einzellageMap[e._key] = fakeId;
    }
  }

  // 8. Build containment relationships
  console.log(`\n--- Building containment relationships ---`);
  const containmentRows = [];

  // Anbaugebiet → Bereich
  for (const [rawName, bData] of bereiche) {
    const parentApp = anbaugebieteMap[bData.region];
    const cleanName = cleanBereichName(rawName);
    const displayName = `${cleanName} (Bereich)`;
    const childApp = appByName[displayName] || appByName[cleanName];

    if (parentApp && childApp) {
      containmentRows.push({
        parent_id: parentApp.id,
        child_id: childApp.id,
        source: 'explicit',
      });
    }
  }

  // Bereich → Großlage
  for (const [name, gData] of grosslagen) {
    const displayName = `${name} (Großlage)`;
    const parentApp = appByName[`${cleanBereichName(gData.bereich)} (Bereich)`]
      || appByName[cleanBereichName(gData.bereich)];
    const childApp = appByName[displayName] || appByName[name];

    if (parentApp && childApp) {
      containmentRows.push({
        parent_id: parentApp.id,
        child_id: childApp.id,
        source: 'explicit',
      });
    }
  }

  // Großlage → Einzellage (or Bereich → Einzellage for Ruwer vineyards with no Großlage)
  for (const [key, eData] of einzellagen) {
    const displayName = eData.village ? `${eData.name}, ${eData.village}` : eData.name;
    const childApp = appByName[displayName] || appByName[eData.name];

    let parentApp;
    if (eData.grosslage) {
      // Normal case: parent is the Großlage
      const grosslageDisplayName = `${eData.grosslage} (Großlage)`;
      parentApp = appByName[grosslageDisplayName] || appByName[eData.grosslage];
    } else {
      // No Großlage (e.g., Ruwer): parent is the Bereich directly
      const bereichDisplayName = `${cleanBereichName(eData.bereich)} (Bereich)`;
      parentApp = appByName[bereichDisplayName] || appByName[cleanBereichName(eData.bereich)];
    }

    if (parentApp && childApp) {
      containmentRows.push({
        parent_id: parentApp.id,
        child_id: childApp.id,
        source: 'explicit',
      });
    }
  }

  // Deduplicate
  const seen = new Set();
  const uniqueRows = containmentRows.filter(r => {
    const k = `${r.parent_id}|${r.child_id}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  console.log(`\nContainment relationships: ${uniqueRows.length}`);
  console.log(`  Anbaugebiet → Bereich: ${containmentRows.filter(r => {
    const p = Object.values(appByName).find(a => a.id === r.parent_id);
    return p && p.classification_level === 'anbaugebiet';
  }).length}`);

  if (DRY_RUN) {
    console.log(`\n=== DRY RUN SUMMARY ===`);
    console.log(`Bereiche to create: ${bereicheToCreate.length}`);
    console.log(`Großlagen to create: ${grosslagenToCreate.length}`);
    console.log(`Einzellagen to create: ${einzellagenToCreate.length}`);
    console.log(`Total new appellations: ${bereicheToCreate.length + grosslagenToCreate.length + einzellagenToCreate.length}`);
    console.log(`Containment rows: ${uniqueRows.length}`);
    console.log(`\n[DRY RUN] No changes made.`);
    return;
  }

  // 9. Insert containment rows
  const { data: existing, error: existErr } = await sb
    .from('appellation_containment')
    .select('parent_id, child_id');
  if (existErr) throw existErr;

  const existingSet = new Set((existing || []).map(r => `${r.parent_id}|${r.child_id}`));
  const toInsert = uniqueRows.filter(r => !existingSet.has(`${r.parent_id}|${r.child_id}`));

  if (toInsert.length === 0) {
    console.log('\nAll containment relationships already exist. Nothing to insert.');
  } else {
    console.log(`\nInserting ${toInsert.length} new containment rows...`);
    const BATCH_SIZE = 500;
    let inserted = 0;
    for (let i = 0; i < toInsert.length; i += BATCH_SIZE) {
      const batch = toInsert.slice(i, i + BATCH_SIZE);
      const { error } = await sb.from('appellation_containment').insert(batch);
      if (error) {
        console.error(`Error inserting batch:`, error);
        throw error;
      }
      inserted += batch.length;
      console.log(`  Inserted ${inserted}/${toInsert.length}`);
    }
    console.log(`  Done! Inserted ${inserted} containment rows.`);
  }

  console.log('\nDone!');
}

main().catch(e => { console.error(e); process.exit(1); });
