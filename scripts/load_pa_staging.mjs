#!/usr/bin/env node
/**
 * load_pa_staging.mjs — Load PA PLCB wine catalog into source_pa staging table
 */
import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';
import XLSX from 'xlsx';

// Load .env
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

// Read xlsx
console.log('Reading PA catalog...');
const wb = XLSX.readFile('data/imports/pa_wine_catalog.xlsx');
const data = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
console.log(`  ${data.length} rows`);

// Skip cocktails/coolers — keep everything else (table wine, fortified, sparkling, dessert)
const wines = data.filter(r => {
  const g = (r['Group Name'] || '').toLowerCase();
  return g !== 'cocktails';
});
console.log(`  ${wines.length} wine rows (excl cocktails)`);

// Transform to staging format
const rows = wines.map(r => {
  // Collect all UPC values
  const upcKeys = Object.keys(r).filter(k => k === 'UPC' || k.startsWith('UPC_'));
  const upcs = upcKeys.map(k => r[k]).filter(Boolean).map(String);

  return {
    plcb_item: r['PLCB Item'] || null,
    item_description: r['Item Description'] || null,
    manufacturer_scc: r['Manufacturer SCC'] || null,
    group_name: r['Group Name'] || null,
    class_name: r['Class Name'] || null,
    volume: r['Liquid Volume'] || null,
    case_pack: r['Case Pack'] || null,
    retail_price: r['Current Regular Retail'] ? parseFloat(r['Current Regular Retail']) : null,
    proof: r['Proof'] || null,
    vintage: r['Vintage'] || null,
    brand_name: r['Brand Name'] || null,
    import_domestic: r['Import/Domestic'] || null,
    country: r['Country'] || null,
    region: r['Region'] || null,
    upcs: upcs.length > 0 ? upcs : null,
  };
});

// Insert in batches
const BATCH = 500;
let inserted = 0;
for (let i = 0; i < rows.length; i += BATCH) {
  const batch = rows.slice(i, i + BATCH);
  const { error } = await sb.from('source_pa').insert(batch);
  if (error) {
    console.error(`  Batch ${Math.floor(i/BATCH)} error:`, error.message);
  } else {
    inserted += batch.length;
  }
}

console.log(`\nInserted ${inserted} rows into source_pa`);
