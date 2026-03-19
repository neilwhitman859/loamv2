#!/usr/bin/env node
/**
 * Load UPC barcode sources into staging tables.
 * Sources: Open Food Facts, Horizon Beverage, WineDeals
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const envPath = resolve(process.cwd(), '.env');
const vars = {};
try {
  readFileSync(envPath, 'utf8').replace(/\r/g, '').split('\n').forEach(line => {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) vars[m[1].trim()] = m[2].trim().replace(/^["']|["']$/g, '');
  });
} catch {}

const supabase = createClient(vars.SUPABASE_URL, vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY);
const BATCH = 500;

async function loadBatches(table, records, conflict) {
  let inserted = 0, errors = 0;
  for (let i = 0; i < records.length; i += BATCH) {
    const batch = records.slice(i, i + BATCH);
    const { data, error } = await supabase
      .from(table)
      .upsert(batch, { onConflict: conflict, ignoreDuplicates: false })
      .select('id');
    if (error) { console.error(`  Error at ${i}: ${error.message}`); errors++; }
    else { inserted += data?.length || batch.length; }
    if ((i + BATCH) % 2000 === 0 || i + BATCH >= records.length) {
      process.stdout.write(`  ${Math.min(i + BATCH, records.length).toLocaleString()}/${records.length.toLocaleString()}\r`);
    }
  }
  return { inserted, errors };
}

// === Open Food Facts ===
console.log('=== Open Food Facts ===');
const offRaw = JSON.parse(readFileSync('data/imports/openfoodfacts_wines.json'));
const offWines = (offRaw.wines || offRaw).filter(w => w.barcode);
console.log(`Records: ${offWines.length}`);
const offBatch = offWines.map(r => ({
  barcode: r.barcode,
  name: r.name || null,
  brand: r.brand || null,
  country: r.countries || null,
  categories: r.categories || null,
  abv: r.abv ? parseFloat(r.abv) : null,
  color: r.color || null,
  origins: r.origins || null,
  labels: r.labels || null,
  quantity: r.quantity || null,
}));
const offResult = await loadBatches('source_openfoodfacts', offBatch, 'barcode');
console.log(`\n  Done: ${offResult.inserted} upserted, ${offResult.errors} errors\n`);

// === Horizon Beverage ===
console.log('=== Horizon Beverage ===');
const hzRaw = JSON.parse(readFileSync('data/imports/horizon_beverage_wines.json'));
const hzWines = (hzRaw.wines || hzRaw).filter(w => w.upc);
console.log(`Records: ${hzWines.length}`);
const hzBatch = hzWines.map(r => ({
  upc: r.upc,
  name: r.name || null,
  brand: r.producer || null,
  category: r.category || null,
  subcategory: r.style || null,
  country: r.country || null,
  region: r.region || null,
  varietal: r.grapes ? r.grapes.join(', ') : null,
  size: r.size_raw || null,
}));
const hzResult = await loadBatches('source_horizon', hzBatch, 'upc');
console.log(`\n  Done: ${hzResult.inserted} upserted, ${hzResult.errors} errors\n`);

// === WineDeals ===
console.log('=== WineDeals ===');
const wdRaw = JSON.parse(readFileSync('data/imports/winedeals_catalog.json'));
const wdWines = (Array.isArray(wdRaw) ? wdRaw : wdRaw.wines || []);
console.log(`Records: ${wdWines.length}`);
const wdBatch = wdWines.map(r => ({
  upc: r.upc || null,
  name: r.name || null,
  producer: r.producer || null,
  country: r.country || null,
  region: r.region || null,
  appellation: r.appellation || null,
  vintage: r.vintage || null,
  abv: r.abv ? parseFloat(String(r.abv).replace('%', '')) : null,
  color: r.color || null,
  price_usd: r.price ? parseFloat(String(r.price).replace('$', '')) : null,
  compare_at_price_usd: r.compare_at_price ? parseFloat(String(r.compare_at_price).replace('$', '')) : null,
  url: r.url || null,
  item_number: r.item_number || r.sku || null,
}));
const wdResult = await loadBatches('source_winedeals', wdBatch, 'item_number');
console.log(`\n  Done: ${wdResult.inserted} upserted, ${wdResult.errors} errors\n`);

// === Summary ===
console.log('=== UPC TOTALS ===');
for (const [table, label] of [['source_openfoodfacts', 'OFF'], ['source_horizon', 'Horizon'], ['source_winedeals', 'WineDeals']]) {
  const { count } = await supabase.from(table).select('*', { count: 'exact', head: true });
  console.log(`  ${label}: ${count?.toLocaleString()}`);
}
