#!/usr/bin/env node
/**
 * load_new_staging.mjs — Load LCBO, Systembolaget, Polaner, FirstLeaf into staging tables
 */
import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
for (const line of envContent.split('\n')) {
  const trimmed = line.replace(/\r/g, '').trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx === -1) continue;
  if (!process.env[trimmed.slice(0, eqIdx)]) process.env[trimmed.slice(0, eqIdx)] = trimmed.slice(eqIdx + 1);
}
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY);

const BATCH = 500;
async function batchInsert(table, rows) {
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const { error } = await sb.from(table).insert(batch);
    if (error) console.error(`  ${table} batch ${Math.floor(i/BATCH)} error:`, error.message);
    else inserted += batch.length;
  }
  return inserted;
}

// ── LCBO ──
async function loadLCBO() {
  console.log('\n=== LCBO ===');
  const data = JSON.parse(readFileSync('data/imports/lcbo_catalog.json', 'utf-8'));
  console.log(`  ${data.length} wines from JSON`);

  const rows = data.map(w => ({
    sku: w.sku,
    name: w.name,
    upc: w.upc || null,
    producer: w.producer || null,
    country: w.country || null,
    region: w.region || null,
    abv: w.abv || null,
    price_cad_cents: w.price_cad_cents || null,
    category: w.category || null,
    description: w.description || null,
    volume_ml: w.volume_ml || null,
    is_vqa: w.is_vqa || false,
    is_kosher: w.is_kosher || false,
    updated_at_source: w.updated_at || null,
  }));

  const n = await batchInsert('source_lcbo', rows);
  console.log(`  Inserted ${n} rows`);
}

// ── Systembolaget ──
async function loadSystembolaget() {
  console.log('\n=== Systembolaget ===');
  const raw = JSON.parse(readFileSync('data/imports/systembolaget_raw.json', 'utf-8'));
  // Filter to wine only (categoryLevel1 = "Vin")
  const wines = raw.filter(p => (p.categoryLevel1 || '').toLowerCase() === 'vin');
  console.log(`  ${wines.length} wines from ${raw.length} total products`);

  const rows = wines.map(w => ({
    product_id: w.productId || null,
    product_number: w.productNumber || null,
    name_bold: w.productNameBold || null,
    name_thin: w.productNameThin || null,
    producer: w.producerName || null,
    country: w.country || null,
    origin_level1: w.originLevel1 || null,
    origin_level2: w.originLevel2 || null,
    category_level1: w.categoryLevel1 || null,
    category_level2: w.categoryLevel2 || null,
    color: w.color || null,
    grapes: w.grapes && w.grapes.length > 0 ? w.grapes : null,
    vintage: w.vintage ? String(w.vintage) : null,
    abv: w.alcoholPercentage || null,
    price_sek: w.price || null,
    volume_ml: w.volume || null,
    sugar_g_per_100ml: w.sugarContentGramPer100ml || null,
    taste_body: w.tasteClockBody || null,
    taste_sweetness: w.tasteClockSweetness || null,
    taste_fruitacid: w.tasteClockFruitacid || null,
    taste_bitterness: w.tasteClockBitter || null,
    taste_roughness: w.tasteClockRoughness || null,
    taste_smokiness: w.tasteClockSmokiness || null,
    is_organic: w.isOrganic || false,
    is_kosher: w.isKosher || false,
    is_ethical: w.isEthical || false,
    description: w.taste || null,
  }));

  const n = await batchInsert('source_systembolaget', rows);
  console.log(`  Inserted ${n} rows`);
}

// ── Polaner ──
async function loadPolaner() {
  console.log('\n=== Polaner ===');
  // Check if already loaded
  const { count } = await sb.from('source_polaner').select('id', { count: 'exact', head: true });
  if (count > 0) {
    console.log(`  Already has ${count} rows, skipping`);
    return;
  }

  const data = JSON.parse(readFileSync('data/imports/polaner_catalog.json', 'utf-8'));
  console.log(`  ${data.length} wines from JSON`);

  const rows = data.map(w => ({
    title: w.title || w.name || null,
    country: w.country || null,
    region: w.region || null,
    appellation: w.appellation || null,
    url: w.url || null,
    metadata: w.metadata || null,
  }));

  const n = await batchInsert('source_polaner', rows);
  console.log(`  Inserted ${n} rows`);
}

// ── FirstLeaf ──
async function loadFirstLeaf() {
  console.log('\n=== FirstLeaf ===');
  const data = JSON.parse(readFileSync('data/imports/firstleaf_catalog.json', 'utf-8'));
  console.log(`  ${data.length} products from JSON`);

  const rows = data.map(w => ({
    title: w.title || null,
    handle: w.handle || null,
    vendor: w.vendor || null,
    product_type: w.product_type || null,
    tags: w.tags && w.tags.length > 0 ? w.tags : null,
    price_usd: w.price ? parseFloat(w.price) : null,
    image_url: w.image?.src || null,
    metadata: w.metadata || null,
  }));

  const n = await batchInsert('source_firstleaf', rows);
  console.log(`  Inserted ${n} rows`);
}

// ── Main ──
async function main() {
  await loadLCBO();
  await loadSystembolaget();
  await loadPolaner();
  await loadFirstLeaf();
  console.log('\nDone!');
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
