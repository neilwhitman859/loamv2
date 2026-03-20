#!/usr/bin/env node
/**
 * populate_au_containment.mjs
 *
 * Imports Australian wine GI hierarchy into Loam:
 *   1. Creates 28 zone-level appellations (currently missing from DB)
 *   2. Imports zone boundary polygons from wine_australia_zones.geojson
 *   3. Sets classification_level (zone/region/subregion) on all AU GIs
 *   4. Populates appellation_containment with Zone→Region→Subregion nesting
 *
 * Source: Winetitles.com.au hierarchy (mirrors official Wine Australia GI Register)
 *         Zone boundary polygons from Wine Australia Open Data Hub
 *
 * Usage:
 *   node scripts/populate_au_containment.mjs              # full run
 *   node scripts/populate_au_containment.mjs --dry-run    # preview only
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

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
// Complete Australian GI Hierarchy (from Winetitles / Wine Australia)
// ---------------------------------------------------------------------------
// Format: { zone: { regions: { regionName: [subregions] } } }
// Super-zones noted separately.
const HIERARCHY = {
  // --- South Australia ---
  'Barossa': {
    state: 'SA',
    regions: {
      'Barossa Valley': [],
      'Eden Valley': ['High Eden'],
    }
  },
  'Far North': {
    state: 'SA',
    regions: {
      'Southern Flinders Ranges': [],
    }
  },
  'Fleurieu': {
    state: 'SA',
    regions: {
      'Currency Creek': [],
      'Kangaroo Island': [],
      'Langhorne Creek': [],
      'McLaren Vale': [],
      'Southern Fleurieu': [],
    }
  },
  'Limestone Coast': {
    state: 'SA',
    regions: {
      'Coonawarra': [],
      'Mount Benson': [],
      'Mount Gambier': [],
      'Padthaway': [],
      'Robe': [],
      'Wrattonbully': [],
    }
  },
  'Lower Murray': {
    state: 'SA',
    regions: {
      'Riverland': [],
    }
  },
  'Mount Lofty Ranges': {
    state: 'SA',
    regions: {
      'Adelaide Hills': ['Lenswood', 'Piccadilly Valley'],
      'Adelaide Plains': [],
      'Clare Valley': [],
    }
  },
  'The Peninsulas': {
    state: 'SA',
    regions: {}
  },

  // --- New South Wales ---
  'Big Rivers': {
    state: 'NSW',
    regions: {
      'Murray Darling': [],  // multi-state (NSW + VIC)
      'Perricoota': [],
      'Riverina': [],
      'Swan Hill': [],  // multi-state (NSW + VIC)
    }
  },
  'Central Ranges': {
    state: 'NSW',
    regions: {
      'Cowra': [],
      'Mudgee': [],
      'Orange': [],
    }
  },
  'Hunter Valley': {
    state: 'NSW',
    regions: {
      'Hunter': ['Broke Fordwich', 'Pokolbin', 'Upper Hunter Valley'],
    }
  },
  'Northern Rivers': {
    state: 'NSW',
    regions: {
      'Hastings River': [],
    }
  },
  'Northern Slopes': {
    state: 'NSW',
    regions: {
      'New England Australia': [],
    }
  },
  'South Coast': {
    state: 'NSW',
    regions: {
      'Shoalhaven Coast': [],
      'Southern Highlands': [],
    }
  },
  'Southern New South Wales': {
    state: 'NSW',
    regions: {
      'Canberra District': [],
      'Gundagai': [],
      'Hilltops': [],
      'Tumbarumba': [],
    }
  },
  'Western Plains': {
    state: 'NSW',
    regions: {}
  },

  // --- Victoria ---
  'Central Victoria': {
    state: 'VIC',
    regions: {
      'Bendigo': [],
      'Goulburn Valley': ['Nagambie Lakes'],
      'Heathcote': [],
      'Strathbogie Ranges': [],
      'Upper Goulburn': [],
    }
  },
  'Gippsland': {
    state: 'VIC',
    regions: {}
  },
  'North East Victoria': {
    state: 'VIC',
    regions: {
      'Alpine Valleys': [],
      'Beechworth': [],
      'Glenrowan': [],
      'King Valley': [],
      'Rutherglen': [],
    }
  },
  'North West Victoria': {
    state: 'VIC',
    regions: {
      'Murray Darling': [],  // shared with Big Rivers (NSW)
      'Swan Hill': [],       // shared with Big Rivers (NSW)
    }
  },
  'Port Phillip': {
    state: 'VIC',
    regions: {
      'Geelong': [],
      'Macedon Ranges': [],
      'Mornington Peninsula': [],
      'Sunbury': [],
      'Yarra Valley': [],
    }
  },
  'Western Victoria': {
    state: 'VIC',
    regions: {
      'Grampians': ['Great Western'],
      'Henty': [],
      'Pyrenees': [],
    }
  },

  // --- Western Australia ---
  'Greater Perth': {
    state: 'WA',
    regions: {
      'Peel': [],
      'Perth Hills': [],
      'Swan District': ['Swan Valley'],
    }
  },
  'South West Australia': {
    state: 'WA',
    regions: {
      'Blackwood Valley': [],
      'Geographe': [],
      'Great Southern': ['Albany', 'Denmark', 'Frankland River', 'Mount Barker', 'Porongurup'],
      'Manjimup': [],
      'Margaret River': [],
      'Pemberton': [],
    }
  },
  'Central Western Australia': {
    state: 'WA',
    regions: {}
  },
  'Eastern Plains, Inland And North Of Western Australia': {
    state: 'WA',
    regions: {}
  },
  'West Australian South East Coastal': {
    state: 'WA',
    regions: {}
  },

  // --- Queensland ---
  'Queensland': {
    state: 'QLD',
    regions: {
      'Granite Belt': [],
      'South Burnett': [],
    }
  },

  // --- Tasmania ---
  'Tasmania': {
    state: 'TAS',
    regions: {}
  },
};

// Adelaide is a "super zone" containing Barossa + Fleurieu + Mount Lofty Ranges
// This is a DAG case: Adelaide zone contains 3 other zones
const SUPER_ZONES = {
  'Adelaide': ['Barossa', 'Fleurieu', 'Mount Lofty Ranges'],
};

// Multi-state regions: Murray Darling and Swan Hill appear in both NSW and VIC zones
// We store them under their first-listed zone (Big Rivers / NSW) and also
// create containment from the VIC zone (North West Victoria)
const MULTI_ZONE_REGIONS = {
  'Murray Darling': ['Big Rivers', 'North West Victoria'],
  'Swan Hill': ['Big Rivers', 'North West Victoria'],
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`\n=== Australia GI Containment Import ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

  // 1. Get Australia country ID
  const { data: auCountry } = await sb
    .from('countries').select('id').eq('iso_code', 'AU').single();
  const AU_COUNTRY_ID = auCountry.id;

  // 2. Load existing AU appellations
  const { data: existingApps } = await sb
    .from('appellations')
    .select('id, name, classification_level')
    .eq('country_id', AU_COUNTRY_ID);
  const appByName = {};
  for (const a of existingApps) {
    appByName[a.name] = a;
  }
  console.log(`Existing AU appellations: ${existingApps.length}`);

  // 3. Load existing AU regions (for region_id assignment)
  const { data: existingRegions } = await sb
    .from('regions')
    .select('id, name')
    .eq('country_id', AU_COUNTRY_ID);
  const regionByName = {};
  for (const r of existingRegions) {
    regionByName[r.name] = r.id;
  }

  // State -> region mapping
  const STATE_REGIONS = {
    'SA': regionByName['South Australia'],
    'NSW': regionByName['New South Wales'],
    'VIC': regionByName['Victoria'],
    'WA': regionByName['Western Australia'],
    'TAS': regionByName['Tasmania'],
    'QLD': regionByName['Australia'],  // catch-all for QLD
  };

  // 4. Load zone GeoJSON for boundary polygons and centroids
  const zonesGeo = JSON.parse(readFileSync(
    new URL('../data/geo/wine_australia_zones.geojson', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1'),
    'utf8'
  ));
  const zoneBoundaries = {};
  for (const f of zonesGeo.features) {
    zoneBoundaries[f.properties.GI_NAME] = f.geometry;
  }

  // 5. Create zone appellations (not yet in DB)
  const allZoneNames = [...Object.keys(HIERARCHY), ...Object.keys(SUPER_ZONES)];
  const zonesToCreate = [];
  for (const zoneName of allZoneNames) {
    if (appByName[zoneName]) {
      console.log(`  Zone already exists: ${zoneName}`);
      continue;
    }
    const zoneInfo = HIERARCHY[zoneName] || { state: 'SA' }; // Adelaide super-zone
    const regionId = STATE_REGIONS[zoneInfo.state] || regionByName['Australia'];

    // Compute centroid from geometry if available
    let lat = null, lng = null;
    const geom = zoneBoundaries[zoneName];
    if (geom) {
      // Rough centroid: average of all ring coordinates (iterative, no recursion)
      let sumLng = 0, sumLat = 0, count = 0;
      const rings = geom.type === 'Polygon'
        ? geom.coordinates
        : geom.type === 'MultiPolygon'
          ? geom.coordinates.flat()
          : [];
      for (const ring of rings) {
        for (const coord of ring) {
          sumLng += coord[0];
          sumLat += coord[1];
          count++;
        }
      }
      if (count > 0) {
        lng = sumLng / count;
        lat = sumLat / count;
      }
    }

    zonesToCreate.push({
      name: zoneName,
      slug: slugify(`${zoneName} australia`),
      designation_type: 'GI',
      classification_level: 'zone',
      country_id: AU_COUNTRY_ID,
      region_id: regionId,
      hemisphere: 'south',
      latitude: lat ? parseFloat(lat.toFixed(6)) : null,
      longitude: lng ? parseFloat(lng.toFixed(6)) : null,
    });
  }

  // Also create "Adelaide" super-zone if not exists
  if (!appByName['Adelaide'] && !zonesToCreate.find(z => z.name === 'Adelaide')) {
    zonesToCreate.push({
      name: 'Adelaide',
      slug: 'adelaide-australia',
      designation_type: 'GI',
      classification_level: 'zone',
      country_id: AU_COUNTRY_ID,
      region_id: STATE_REGIONS['SA'],
      hemisphere: 'south',
      latitude: -34.9285,
      longitude: 138.6007,
    });
  }

  console.log(`Zones to create: ${zonesToCreate.length}`);
  if (zonesToCreate.length > 0) {
    console.log(`  ${zonesToCreate.map(z => z.name).join(', ')}`);
  }

  if (!DRY_RUN && zonesToCreate.length > 0) {
    const { data: created, error } = await sb
      .from('appellations')
      .insert(zonesToCreate)
      .select('id, name');
    if (error) throw error;
    for (const c of created) {
      appByName[c.name] = { id: c.id, name: c.name, classification_level: 'zone' };
    }
    console.log(`  Created ${created.length} zone appellations`);
  }

  // 6. Import zone boundary polygons
  if (!DRY_RUN) {
    let boundaryCount = 0;
    for (const zoneName of allZoneNames) {
      const app = appByName[zoneName];
      if (!app) continue;
      const geom = zoneBoundaries[zoneName];
      if (!geom) continue;

      // Check if boundary already exists
      const { data: existBound } = await sb
        .from('geographic_boundaries')
        .select('id, boundary_polygon')
        .eq('appellation_id', app.id)
        .maybeSingle();

      if (existBound && existBound.boundary_polygon) continue;

      // Simplify large geometries
      const geojsonStr = JSON.stringify(geom);

      try {
        const { error: rpcErr } = await sb.rpc('upsert_appellation_boundary', {
          p_appellation_id: app.id,
          p_geojson: geojsonStr,
          p_source_id: `wine-australia-zone/${slugify(zoneName)}`,
          p_confidence: 'official',
        });
        if (rpcErr) {
          console.log(`  Warning: boundary import failed for ${zoneName}: ${rpcErr.message}`);
        } else {
          boundaryCount++;
        }
      } catch (e) {
        console.log(`  Warning: boundary import failed for ${zoneName}: ${e.message}`);
      }
    }
    console.log(`Imported ${boundaryCount} zone boundaries`);
  }

  // 7. Set classification_level on all AU appellations
  // Determine which are subregions (from the HIERARCHY data)
  const subregionNames = new Set();
  const regionNames = new Set();
  for (const [, zoneData] of Object.entries(HIERARCHY)) {
    for (const [regName, subs] of Object.entries(zoneData.regions)) {
      regionNames.add(regName);
      for (const sub of subs) {
        subregionNames.add(sub);
      }
    }
  }

  const levelUpdates = [];
  for (const [name, app] of Object.entries(appByName)) {
    let level = null;
    if (Object.keys(HIERARCHY).includes(name) || Object.keys(SUPER_ZONES).includes(name)) {
      level = 'zone';
    } else if (subregionNames.has(name)) {
      level = 'subregion';
    } else if (regionNames.has(name)) {
      level = 'region';
    }
    if (level && app.classification_level !== level) {
      levelUpdates.push({ id: app.id, name, level });
    }
  }

  console.log(`\nClassification level updates: ${levelUpdates.length}`);
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

  // 8. Build containment relationships
  const containmentRows = [];

  // Zone -> Region
  for (const [zoneName, zoneData] of Object.entries(HIERARCHY)) {
    const zoneApp = appByName[zoneName];
    if (!zoneApp) { console.log(`  Warning: zone not in DB: ${zoneName}`); continue; }

    for (const [regName, subs] of Object.entries(zoneData.regions)) {
      // Skip multi-zone regions in their secondary zone (handled below)
      if (MULTI_ZONE_REGIONS[regName] && MULTI_ZONE_REGIONS[regName][0] !== zoneName) continue;

      const regApp = appByName[regName];
      if (!regApp) { console.log(`  Warning: region not in DB: ${regName}`); continue; }

      containmentRows.push({
        parent_id: zoneApp.id,
        child_id: regApp.id,
        source: 'explicit',
      });

      // Region -> Subregion
      for (const subName of subs) {
        const subApp = appByName[subName];
        if (!subApp) { console.log(`  Warning: subregion not in DB: ${subName}`); continue; }
        containmentRows.push({
          parent_id: regApp.id,
          child_id: subApp.id,
          source: 'explicit',
        });
      }
    }
  }

  // Multi-zone regions: also parent them under their secondary zone
  for (const [regName, zones] of Object.entries(MULTI_ZONE_REGIONS)) {
    const regApp = appByName[regName];
    if (!regApp) continue;
    for (const zoneName of zones) {
      const zoneApp = appByName[zoneName];
      if (!zoneApp) continue;
      // Check if already added
      const exists = containmentRows.some(r => r.parent_id === zoneApp.id && r.child_id === regApp.id);
      if (!exists) {
        containmentRows.push({
          parent_id: zoneApp.id,
          child_id: regApp.id,
          source: 'explicit',
        });
      }
    }
  }

  // Super-zone -> Zone (Adelaide contains Barossa, Fleurieu, Mount Lofty Ranges)
  for (const [superZone, childZones] of Object.entries(SUPER_ZONES)) {
    const superApp = appByName[superZone];
    if (!superApp) { console.log(`  Warning: super-zone not in DB: ${superZone}`); continue; }
    for (const childZone of childZones) {
      const childApp = appByName[childZone];
      if (!childApp) { console.log(`  Warning: child zone not in DB: ${childZone}`); continue; }
      containmentRows.push({
        parent_id: superApp.id,
        child_id: childApp.id,
        source: 'explicit',
      });
    }
  }

  console.log(`\nContainment relationships to insert: ${containmentRows.length}`);

  if (!DRY_RUN && containmentRows.length > 0) {
    // Check for existing
    const { data: existing } = await sb
      .from('appellation_containment')
      .select('parent_id, child_id');
    const existingSet = new Set((existing || []).map(r => `${r.parent_id}|${r.child_id}`));
    const toInsert = containmentRows.filter(r => !existingSet.has(`${r.parent_id}|${r.child_id}`));

    if (toInsert.length > 0) {
      const { error } = await sb.from('appellation_containment').insert(toInsert);
      if (error) throw error;
      console.log(`  Inserted ${toInsert.length} containment rows`);
    } else {
      console.log(`  All relationships already exist`);
    }
  }

  console.log('\nDone!');
}

main().catch(e => { console.error(e); process.exit(1); });
