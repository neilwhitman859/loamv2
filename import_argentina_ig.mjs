#!/usr/bin/env node
/**
 * import_argentina_ig.mjs
 *
 * Imports Argentine wine Indicaciones Geográficas (IGs) and
 * Denominaciones de Origen Controladas (DOCs) into the Loam database.
 *
 * Data source: Instituto Nacional de Vitivinicultura (INV)
 * https://www.argentina.gob.ar/inv/proteccion-del-origen
 *
 * 2 DOCs + 34 IGs = 36 total appellations.
 * No official boundary GIS data available — geocoded via Nominatim.
 *
 * Usage:
 *   node import_argentina_ig.mjs              # full run
 *   node import_argentina_ig.mjs --dry-run    # preview only
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

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

// ---------------------------------------------------------------------------
// Argentine IG/DOC Data
// ---------------------------------------------------------------------------
// Source: INV (https://www.argentina.gob.ar/inv/proteccion-del-origen)
// Region mapping based on Argentine wine geography

const APPELLATIONS = [
  // DOCs (Denominación de Origen Controlada)
  { name: 'Luján de Cuyo', type: 'DOC', region: 'Mendoza', province: 'Mendoza' },
  { name: 'San Rafael', type: 'DOC', region: 'Mendoza', province: 'Mendoza' },

  // IGs (Indicación Geográfica) — Mendoza province
  { name: 'Lunlunta', type: 'IG', region: 'Mendoza', province: 'Mendoza' },
  { name: 'Russel', type: 'IG', region: 'Mendoza', province: 'Mendoza' },
  { name: 'Agrelo', type: 'IG', region: 'Mendoza', province: 'Mendoza' },
  { name: 'Barrancas', type: 'IG', region: 'Mendoza', province: 'Mendoza' },
  { name: 'Las Compuertas', type: 'IG', region: 'Mendoza', province: 'Mendoza' },
  { name: 'Desierto de Lavalle', type: 'IG', region: 'Mendoza', province: 'Mendoza' },
  { name: 'Distrito Medrano', type: 'IG', region: 'Mendoza', province: 'Mendoza' },
  { name: 'Reducción', type: 'IG', region: 'Mendoza', province: 'Mendoza' },

  // IGs — Uco Valley (Mendoza province, but mapped to Uco Valley region)
  { name: 'Valle de Tupungato', type: 'IG', region: 'Uco Valley', province: 'Mendoza' },
  { name: 'Vista Flores', type: 'IG', region: 'Uco Valley', province: 'Mendoza' },
  { name: 'Paraje Altamira', type: 'IG', region: 'Uco Valley', province: 'Mendoza' },
  { name: 'La Consulta', type: 'IG', region: 'Uco Valley', province: 'Mendoza' },
  { name: 'Los Chacayes', type: 'IG', region: 'Uco Valley', province: 'Mendoza' },
  { name: 'San Pablo', type: 'IG', region: 'Uco Valley', province: 'Mendoza' },
  { name: 'El Paraiso', type: 'IG', region: 'Uco Valley', province: 'Mendoza' },

  // IGs — Salta province
  { name: 'Valle de Cafayate', type: 'IG', region: 'Salta', province: 'Salta' },
  { name: 'Valle Calchaquí', type: 'IG', region: 'Salta', province: 'Salta' },

  // IGs — San Juan province
  { name: 'Valle del Pedernal', type: 'IG', region: 'San Juan', province: 'San Juan' },
  { name: 'Valle de Calingasta', type: 'IG', region: 'San Juan', province: 'San Juan' },
  { name: 'Barreal', type: 'IG', region: 'San Juan', province: 'San Juan' },
  { name: 'Valle de Zonda', type: 'IG', region: 'San Juan', province: 'San Juan' },
  { name: 'Pozo de los Algarrobos', type: 'IG', region: 'San Juan', province: 'San Juan' },
  { name: 'Pampa El Cepillo', type: 'IG', region: 'San Juan', province: 'San Juan' },

  // IGs — La Rioja province
  { name: 'Valles del Famatina', type: 'IG', region: 'La Rioja', province: 'La Rioja' },
  { name: 'Valle de Chañarmuyo', type: 'IG', region: 'La Rioja', province: 'La Rioja' },

  // IGs — Patagonia
  { name: 'Patagonia Argentina', type: 'IG', region: 'Patagonia', province: null },
  { name: 'Trevelin', type: 'IG', region: 'Patagonia', province: 'Chubut' },

  // IGs — Córdoba province
  { name: 'Colonia Caroya', type: 'IG', region: null, province: 'Córdoba' },

  // IGs — Buenos Aires province
  { name: 'Villa Ventana', type: 'IG', region: null, province: 'Buenos Aires' },
  { name: 'Chapadmalal', type: 'IG', region: null, province: 'Buenos Aires' },

  // IGs — Jujuy province
  { name: 'Quebrada de Humahuaca', type: 'IG', region: null, province: 'Jujuy' },

  // IGs — San Luis province
  { name: 'San Luis', type: 'IG', region: null, province: 'San Luis' },

  // IGs — Canota (Mendoza)
  { name: 'Canota', type: 'IG', region: 'Mendoza', province: 'Mendoza' },

  // IGs — Entre Ríos
  { name: 'Victoria', type: 'IG', region: null, province: 'Entre Ríos' },
];

// Nominatim geocoding queries — help locate places accurately
const GEOCODE_QUERIES = {
  'Luján de Cuyo': 'Luján de Cuyo, Mendoza, Argentina',
  'San Rafael': 'San Rafael, Mendoza, Argentina',
  'Lunlunta': 'Lunlunta, Mendoza, Argentina',
  'Russel': 'Russell, Maipú, Mendoza, Argentina',
  'Agrelo': 'Agrelo, Luján de Cuyo, Mendoza, Argentina',
  'Barrancas': 'Barrancas, Maipú, Mendoza, Argentina',
  'Las Compuertas': 'Las Compuertas, Luján de Cuyo, Mendoza, Argentina',
  'Desierto de Lavalle': 'Lavalle, Mendoza, Argentina',
  'Distrito Medrano': 'Medrano, Junín, Mendoza, Argentina',
  'Reducción': 'Reducción, Rivadavia, Mendoza, Argentina',
  'Valle de Tupungato': 'Tupungato, Mendoza, Argentina',
  'Vista Flores': 'Vista Flores, Tunuyán, Mendoza, Argentina',
  'Paraje Altamira': 'Altamira, San Carlos, Mendoza, Argentina',
  'La Consulta': 'La Consulta, San Carlos, Mendoza, Argentina',
  'Los Chacayes': 'Los Chacayes, Tunuyán, Mendoza, Argentina',
  'San Pablo': 'San Pablo, Tunuyán, Mendoza, Argentina',
  'El Paraiso': 'El Paraíso, Mendoza, Argentina',
  'Valle de Cafayate': 'Cafayate, Salta, Argentina',
  'Valle Calchaquí': 'Cachi, Salta, Argentina',  // Cachi is central to the Calchaquí valley
  'Valle del Pedernal': 'Pedernal, San Juan, Argentina',
  'Valle de Calingasta': 'Calingasta, San Juan, Argentina',
  'Barreal': 'Barreal, San Juan, Argentina',
  'Valle de Zonda': 'Zonda, San Juan, Argentina',
  'Pozo de los Algarrobos': 'Pozo de los Algarrobos, San Juan, Argentina',
  'Pampa El Cepillo': 'Caucete, San Juan, Argentina',
  'Valles del Famatina': 'Famatina, La Rioja, Argentina',
  'Valle de Chañarmuyo': 'Chañarmuyo, La Rioja, Argentina',
  'Patagonia Argentina': 'Patagonia, Argentina',
  'Trevelin': 'Trevelin, Chubut, Argentina',
  'Colonia Caroya': 'Colonia Caroya, Córdoba, Argentina',
  'Villa Ventana': 'Villa Ventana, Buenos Aires, Argentina',
  'Chapadmalal': 'Chapadmalal, Buenos Aires, Argentina',
  'Quebrada de Humahuaca': 'Quebrada de Humahuaca, Jujuy, Argentina',
  'San Luis': 'San Luis, Argentina',
  'Canota': 'Canota, Mendoza, Argentina',
  'Victoria': 'Victoria, Entre Ríos, Argentina',
};

// ---------------------------------------------------------------------------
// Geocoding
// ---------------------------------------------------------------------------
async function geocode(query) {
  const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1`;
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'LoamWineApp/1.0 (neil@loam.wine)' }
    });
    const data = await res.json();
    if (data.length > 0) {
      return {
        lat: parseFloat(data[0].lat),
        lng: parseFloat(data[0].lon),
      };
    }
  } catch (e) {
    console.log(`  [WARN] Geocode failed for "${query}": ${e.message}`);
  }
  return null;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('=== Argentina IG/DOC Import ===');
  console.log(`  ${APPELLATIONS.length} appellations (${APPELLATIONS.filter(a => a.type === 'DOC').length} DOC, ${APPELLATIONS.filter(a => a.type === 'IG').length} IG)`);
  if (DRY_RUN) console.log('[DRY RUN MODE]');

  // Load DB reference data
  console.log('\n--- Loading DB reference data ---');

  const { data: countries } = await sb.from('countries').select('id, name, iso_code');
  const argentina = countries.find(c => c.iso_code === 'AR');
  if (!argentina) throw new Error('Argentina not found in countries table');
  console.log(`  Argentina ID: ${argentina.id}`);

  const { data: arRegions } = await sb.from('regions')
    .select('id, name, is_catch_all, country_id')
    .eq('country_id', argentina.id);
  const regionByName = new Map();
  let catchAllRegion = null;
  for (const r of arRegions) {
    regionByName.set(r.name.toLowerCase(), r);
    if (r.is_catch_all) catchAllRegion = r;
  }
  console.log(`  ${arRegions.length} Argentine regions loaded`);

  // Check existing appellations
  const { data: existingApps } = await sb.from('appellations')
    .select('id, name, slug, country_id')
    .eq('country_id', argentina.id);
  console.log(`  ${existingApps.length} existing Argentine appellations`);

  const appByName = new Map();
  const appBySlug = new Map();
  for (const app of existingApps) {
    appByName.set(app.name.toLowerCase(), app);
    appBySlug.set(app.slug, app);
  }

  // -----------------------------------------------------------------------
  // Create appellations with geocoding
  // -----------------------------------------------------------------------
  console.log('\n--- Creating appellations ---');

  const stats = { matched: 0, created: 0, geocoded: 0, errors: 0 };

  for (const appDef of APPELLATIONS) {
    // Check if already exists
    let existing = appByName.get(appDef.name.toLowerCase()) || appBySlug.get(slugify(appDef.name));
    if (existing) {
      console.log(`  [MATCH] ${appDef.name}`);
      stats.matched++;
      continue;
    }

    // Geocode
    let lat = null, lng = null;
    const geoQuery = GEOCODE_QUERIES[appDef.name];
    if (geoQuery) {
      const coords = await geocode(geoQuery);
      if (coords) {
        lat = Math.round(coords.lat * 100000) / 100000;
        lng = Math.round(coords.lng * 100000) / 100000;
        stats.geocoded++;
      }
      await sleep(1100); // Nominatim rate limit
    }

    // Find region_id
    let regionId = catchAllRegion?.id;
    if (appDef.region) {
      const region = regionByName.get(appDef.region.toLowerCase());
      if (region) regionId = region.id;
    }

    const newApp = {
      name: appDef.name,
      slug: slugify(appDef.name),
      country_id: argentina.id,
      region_id: regionId,
      designation_type: appDef.type,
      latitude: lat,
      longitude: lng,
      hemisphere: 'south',
      growing_season_start_month: 10,
      growing_season_end_month: 4,
    };

    if (DRY_RUN) {
      console.log(`  [DRY] Would create: ${appDef.name} (${appDef.type}, ${appDef.region || 'catch-all'}, ${lat}, ${lng})`);
      stats.created++;
      continue;
    }

    const { data: inserted, error } = await sb.from('appellations')
      .insert(newApp)
      .select('id')
      .single();

    if (error) {
      if (error.code === '23505') {
        newApp.slug = slugify(`${appDef.name}-argentina`);
        const { data: retry, error: retryErr } = await sb.from('appellations')
          .insert(newApp)
          .select('id')
          .single();
        if (retryErr) {
          console.log(`  [ERROR] Creating ${appDef.name}: ${retryErr.message}`);
          stats.errors++;
          continue;
        }
        appByName.set(appDef.name.toLowerCase(), { id: retry.id, ...newApp });
        appBySlug.set(newApp.slug, { id: retry.id, ...newApp });
      } else {
        console.log(`  [ERROR] Creating ${appDef.name}: ${error.message}`);
        stats.errors++;
        continue;
      }
    } else {
      appByName.set(appDef.name.toLowerCase(), { id: inserted.id, ...newApp });
      appBySlug.set(newApp.slug, { id: inserted.id, ...newApp });
    }

    stats.created++;
    console.log(`  Created: ${appDef.name} (${appDef.type}, ${lat?.toFixed(2)}, ${lng?.toFixed(2)})`);
  }

  // -----------------------------------------------------------------------
  // Summary
  // -----------------------------------------------------------------------
  console.log('\n=== Import Complete ===');
  console.log(`  Matched existing: ${stats.matched}`);
  console.log(`  Created new:      ${stats.created}`);
  console.log(`  Geocoded:         ${stats.geocoded}`);
  console.log(`  Errors:           ${stats.errors}`);
  if (DRY_RUN) console.log('\n  [DRY RUN - no changes made]');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
