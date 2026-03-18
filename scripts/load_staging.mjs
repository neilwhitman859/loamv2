#!/usr/bin/env node
/**
 * load_staging.mjs — Loads raw catalog JSON files into per-source staging tables.
 *
 * Usage:
 *   node scripts/load_staging.mjs --source polaner
 *   node scripts/load_staging.mjs --source all
 *   node scripts/load_staging.mjs --source kl,skurnik,polaner
 *
 * Sources: polaner, kl, skurnik, winebow, empson, ec, last-bottle, best-wine-store, domestique
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// Manual .env parser (no dotenv dependency)
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
const vars = {};
for (const line of envContent.split('\n')) {
  const trimmed = line.replace(/\r/g, '').trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx === -1) continue;
  vars[trimmed.slice(0, eqIdx)] = trimmed.slice(eqIdx + 1);
}

const supabase = createClient(
  vars.SUPABASE_URL,
  vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY
);

const DATA_DIR = resolve('data/imports');
const BATCH_SIZE = 200;

// ─── Helpers ───────────────────────────────────────────────────────

function loadJSON(filename) {
  const path = resolve(DATA_DIR, filename);
  return JSON.parse(readFileSync(path, 'utf8'));
}

async function batchInsert(table, rows) {
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const { error } = await supabase.from(table).insert(batch);
    if (error) {
      console.error(`  Error inserting batch ${i}-${i + batch.length} into ${table}:`, error.message);
      // Try one-by-one for this batch to identify the problem row
      for (const row of batch) {
        const { error: rowErr } = await supabase.from(table).insert([row]);
        if (rowErr) {
          console.error(`  Row error:`, rowErr.message, '— title:', row.title || row.name || row.wine_name || 'unknown');
        } else {
          inserted++;
        }
      }
    } else {
      inserted += batch.length;
    }
  }
  return inserted;
}

function parseTagValue(tags, key) {
  if (!tags || !Array.isArray(tags)) return null;
  for (const tag of tags) {
    if (typeof tag === 'string' && tag.startsWith(key + ':')) {
      return tag.slice(key.length + 1).trim();
    }
  }
  return null;
}

function parseVintageFromTitle(title) {
  const match = title.match(/\b(19|20)\d{2}\b/);
  return match ? match[0] : null;
}

function parseProducerFromTitle(title) {
  // Simple heuristic: everything before the grape or region indicator
  // This is lossy — staging stores the raw title, matcher will do better
  return null; // Let the match engine handle this
}

// ─── Source Loaders ────────────────────────────────────────────────

async function loadPolaner() {
  console.log('\n=== Loading Polaner (1,680 wines) ===');
  const wines = loadJSON('polaner_catalog.json');
  const rows = wines.map(w => ({
    wp_id: String(w.wp_id),
    slug: w.slug,
    title: w.title,
    url: w.url,
    source_url: w._source,
    country: w.country || null,
    region: w.region || null,
    appellation: w.appellation || null,
    certifications: w.certifications || null,
  }));
  const count = await batchInsert('source_polaner', rows);
  console.log(`  Inserted ${count}/${wines.length} rows into source_polaner`);
  return count;
}

async function loadKermitLynch() {
  console.log('\n=== Loading Kermit Lynch (193 growers + 1,468 wines) ===');
  const catalog = loadJSON('kermit_lynch_catalog.json');

  // Growers first
  const growerRows = catalog.growers.map(g => ({
    kl_id: String(g.kl_id),
    name: g.name,
    slug: g.slug,
    country: g.country || null,
    region: g.region || null,
    farming: g.farming || null,
    winemaker: g.winemaker || null,
    founded_year: typeof g.founded_year === 'number' ? g.founded_year : null,
    website: g.website || null,
    location: g.location || null,
    annual_production: g.annual_production || null,
    viticulture_notes: g.viticulture_notes || null,
    about: g.about || null,
  }));
  const growerCount = await batchInsert('source_kermit_lynch_growers', growerRows);
  console.log(`  Inserted ${growerCount}/${catalog.growers.length} growers`);

  // Wines
  const wineRows = catalog.wines.map(w => ({
    kl_id: String(w.kl_id),
    sku: w.sku || null,
    wine_name: w.wine_name,
    grower_name: w.grower_name || null,
    grower_kl_id: w.grower_kl_id ? String(w.grower_kl_id) : null,
    country: w.country || null,
    region: w.region || null,
    wine_type: w.wine_type || null,
    blend: w.blend || null,
    soil: w.soil || null,
    vine_age: w.vine_age || null,
    vineyard_area: w.vineyard_area || null,
    vinification: w.vinification || null,
    farming: w.farming || null,
  }));
  const wineCount = await batchInsert('source_kermit_lynch', wineRows);
  console.log(`  Inserted ${wineCount}/${catalog.wines.length} wines`);
  return wineCount;
}

async function loadSkurnik() {
  console.log('\n=== Loading Skurnik (5,541 wines) ===');
  const wines = loadJSON('skurnik_catalog.json');
  const rows = wines.map(w => ({
    url: w.url || null,
    source_url: w._source || null,
    producer_slug: w.producer_slug || null,
    producer: w.producer || w.extra_fields?.producer || null,
    name: w.name,
    vintage: w.vintage || null,
    country: w.country || null,
    region: w.region || null,
    appellation: w.appellation || null,
    grape: w.grape || null,
    color: w.color || null,
    sku: w.sku || null,
    bottle_format: w.bottle_format || null,
    farming: w.farming || null,
    description: w.description || null,
    notes: w.notes || null,
    image_url: w.image_url || null,
    extra_fields: w.extra_fields || null,
  }));
  const count = await batchInsert('source_skurnik', rows);
  console.log(`  Inserted ${count}/${wines.length} rows into source_skurnik`);
  return count;
}

async function loadWinebow() {
  console.log('\n=== Loading Winebow (536 wines) ===');
  const wines = loadJSON('winebow_catalog.json');
  const rows = wines.map(w => {
    // Derive name from URL path if missing (e.g. /our-brands/el-enemigo/gran-enemigo-gualtallary/2022)
    let name = w.name;
    if (!name && w.url) {
      const parts = w.url.replace(/\/$/, '').split('/');
      // Skip the vintage at end if it's a year
      const last = parts[parts.length - 1];
      const namePart = /^\d{4}$/.test(last) ? parts[parts.length - 2] : last;
      name = namePart.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }
    if (!name) name = w.varietal_display || 'Unknown';
    return {
    url: w.url || null,
    source_url: w._source || null,
    brand_slug: w.brand_slug || null,
    producer: w.producer || null,
    name,
    varietal_display: w.varietal_display || null,
    vintage: w.vintage || null,
    appellation: w.appellation || null,
    vineyard: w.vineyard || null,
    vineyard_size: w.vineyard_size || null,
    soil: w.soil || null,
    training_method: w.training_method || null,
    elevation: w.elevation || null,
    vines_per_acre: w.vines_per_acre || null,
    yield_per_acre: w.yield_per_acre || null,
    exposure: w.exposure || null,
    production: w.production || null,
    grape: w.grape || null,
    maceration: w.maceration || null,
    malolactic: w.malolactic || null,
    aging_vessel_size: w.aging_vessel_size || null,
    oak_type: w.oak_type || null,
    ph: w.ph ? parseFloat(w.ph) : null,
    acidity: w.acidity ? parseFloat(w.acidity) : null,
    abv: w.abv ? parseFloat(w.abv) : null,
    residual_sugar: w.residual_sugar != null ? parseFloat(w.residual_sugar) : null,
    scores: w.scores || null,
    description: w.description || null,
    vineyard_description: w.vineyard_description || null,
  };});
  const count = await batchInsert('source_winebow', rows);
  console.log(`  Inserted ${count}/${wines.length} rows into source_winebow`);
  return count;
}

async function loadEmpson() {
  console.log('\n=== Loading Empson (279 wines) ===');
  const wines = loadJSON('empson_catalog.json');
  const rows = wines.map(w => ({
    url: w.url || null,
    source_url: w._source || null,
    name: w.name,
    producer: w.producer || null,
    producer_slug: w.producer_slug || null,
    grape: w.grape || null,
    fermentation_container: w.fermentation_container || null,
    fermentation_duration: w.fermentation_duration || null,
    fermentation_temp: w.fermentation_temp || null,
    yeast_type: w.yeast_type || null,
    maceration_duration: w.maceration_duration || null,
    maceration_technique: w.maceration_technique || null,
    malolactic: w.malolactic || null,
    aging_container: w.aging_container || null,
    aging_container_size: w.aging_container_size || null,
    aging_duration: w.aging_duration || null,
    oak_type: w.oak_type || null,
    closure: w.closure || null,
    vineyard_location: w.vineyard_location || null,
    soil: w.soil || null,
    training_method: w.training_method || null,
    altitude: w.altitude || null,
    vine_density: w.vine_density || null,
    exposure: w.exposure || null,
    vine_age: w.vine_age || null,
    vineyard_size: w.vineyard_size || null,
    yield: w.yield || null,
    tasting_notes: w.tasting_notes || null,
    serving_temp: w.serving_temp || null,
    food_pairings: w.food_pairings || null,
    aging_potential: w.aging_potential || null,
    abv: w.abv || null,
    winemaker: w.winemaker || null,
    description: w.description || null,
    production: w.production || null,
    harvest_time: w.harvest_time || null,
    bottling_period: w.bottling_period || null,
    first_vintage: w.first_vintage || null,
    extra_fields: w.extra_fields || null,
  }));
  const count = await batchInsert('source_empson', rows);
  console.log(`  Inserted ${count}/${wines.length} rows into source_empson`);
  return count;
}

async function loadEuropeanCellars() {
  console.log('\n=== Loading European Cellars (443 wines) ===');
  const wines = loadJSON('european_cellars_catalog.json');
  const rows = wines.map(w => {
    // Derive name from url_slug if missing
    let name = w.name;
    if (!name && w.url_slug) {
      name = w.url_slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }
    if (!name) name = w.grape || 'Unknown';
    return {
    url: w.url || null,
    source_url: w._source || null,
    producer: w.producer || null,
    name,
    color: w.color || null,
    certifications: w.certifications || null,
    appellation: w.appellation || null,
    grape: w.grape || null,
    vine_age: w.vine_age || null,
    farming: w.farming || null,
    soil: w.soil || null,
    altitude: w.altitude || null,
    vinification: w.vinification || null,
    aging: w.aging || null,
    scores: w.scores || null,
  };});
  const count = await batchInsert('source_european_cellars', rows);
  console.log(`  Inserted ${count}/${wines.length} rows into source_european_cellars`);
  return count;
}

async function loadLastBottle() {
  console.log('\n=== Loading Last Bottle (234 wines) ===');
  const products = loadJSON('last_bottle_raw.json');
  const rows = products
    .filter(p => p.product_type && (p.product_type.includes('Wine') || p.product_type.includes('Champagne')))
    .map(p => ({
      shopify_id: String(p.shopify_id),
      shopify_handle: p.handle || null,
      title: p.title,
      producer: null, // Will be parsed by match engine
      wine_name: null,
      country: null,
      region: null,
      appellation: null,
      color: p.product_type === 'Red Wine' ? 'red' : p.product_type === 'White Wine' ? 'white' : p.product_type === 'Rosé Wine' ? 'rose' : null,
      wine_type: p.product_type === 'Champagne' ? 'sparkling' : null,
      grape: null,
      vintage: parseVintageFromTitle(p.title),
      price_usd: p.price || null,
      compare_at_price_usd: p.compare_at_price || null,
      description: p.body_html || null,
      tags: p.tags || null,
      metadata: { product_type: p.product_type, vendor: p.vendor, created_at: p.created_at },
    }));
  const count = await batchInsert('source_last_bottle', rows);
  console.log(`  Inserted ${count}/${products.length} rows into source_last_bottle`);
  return count;
}

async function loadBestWineStore() {
  console.log('\n=== Loading Best Wine Store (752 wines) ===');
  const products = loadJSON('best_wine_store_raw.json');
  const rows = products.map(p => ({
    shopify_id: String(p.shopify_id),
    shopify_handle: p.handle || null,
    title: p.title,
    producer: p.vendor || null,
    wine_name: null,
    country: null,
    region: null,
    appellation: null,
    color: null,
    wine_type: p.product_type === 'Champagne' ? 'sparkling' : null,
    grape: null,
    vintage: parseVintageFromTitle(p.title),
    price_usd: p.price || null,
    description: p.body_html || null,
    tags: p.tags || null,
    metadata: { product_type: p.product_type, vendor: p.vendor, created_at: p.created_at },
  }));
  const count = await batchInsert('source_best_wine_store', rows);
  console.log(`  Inserted ${count}/${products.length} rows into source_best_wine_store`);
  return count;
}

async function loadDomestique() {
  console.log('\n=== Loading Domestique (245 wines) ===');
  const products = loadJSON('domestique_wine_raw.json');
  const rows = products.map(p => ({
    shopify_id: String(p.shopify_id),
    shopify_handle: p.handle || null,
    title: p.title,
    producer: p.vendor || null,
    wine_name: null,
    country: parseTagValue(p.tags, 'country'),
    region: parseTagValue(p.tags, 'region'),
    appellation: null,
    color: null,
    wine_type: parseTagValue(p.tags, 'type')?.toLowerCase() || null,
    grape: parseTagValue(p.tags, 'grape'),
    vintage: parseTagValue(p.tags, 'vintage') || parseVintageFromTitle(p.title),
    price_usd: p.price || null,
    description: p.body_html || null,
    tags: p.tags || null,
    metadata: { product_type: p.product_type, vendor: p.vendor, created_at: p.created_at, certified: parseTagValue(p.tags, 'certified') },
  }));
  const count = await batchInsert('source_domestique', rows);
  console.log(`  Inserted ${count}/${products.length} rows into source_domestique`);
  return count;
}

// ─── Main ──────────────────────────────────────────────────────────

const LOADERS = {
  polaner: loadPolaner,
  kl: loadKermitLynch,
  skurnik: loadSkurnik,
  winebow: loadWinebow,
  empson: loadEmpson,
  ec: loadEuropeanCellars,
  'last-bottle': loadLastBottle,
  'best-wine-store': loadBestWineStore,
  domestique: loadDomestique,
};

async function main() {
  const sourceArg = process.argv.find(a => a.startsWith('--source='))?.split('=')[1]
    || process.argv[process.argv.indexOf('--source') + 1];

  if (!sourceArg) {
    console.log('Usage: node scripts/load_staging.mjs --source <name|all>');
    console.log('Sources:', Object.keys(LOADERS).join(', '));
    process.exit(1);
  }

  const sources = sourceArg === 'all'
    ? Object.keys(LOADERS)
    : sourceArg.split(',').map(s => s.trim());

  let totalLoaded = 0;
  for (const source of sources) {
    if (!LOADERS[source]) {
      console.error(`Unknown source: ${source}`);
      continue;
    }
    try {
      const count = await LOADERS[source]();
      totalLoaded += count;
    } catch (err) {
      console.error(`Error loading ${source}:`, err.message);
    }
  }

  console.log(`\n=== Done. Total rows loaded: ${totalLoaded} ===`);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
