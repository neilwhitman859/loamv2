#!/usr/bin/env node
/**
 * Audit metadata JSONB fields across producers, wines, and wine_vintages.
 * Identifies structured data that should be promoted to proper columns/table links.
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

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

const sb = createClient(vars.SUPABASE_URL, vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY);

async function fetchAll(table, columns = '*') {
  const all = [];
  let offset = 0;
  while (true) {
    const { data, error } = await sb.from(table).select(columns).range(offset, offset + 999);
    if (error) throw error;
    all.push(...data);
    if (data.length < 1000) break;
    offset += 1000;
  }
  return all;
}

async function main() {
  console.log('Auditing metadata fields...\n');

  // Producers
  const producers = await fetchAll('producers', 'id,name,metadata');
  const producerKeys = {};
  let producerWithMeta = 0;
  for (const p of producers) {
    if (!p.metadata || Object.keys(p.metadata).length === 0) continue;
    producerWithMeta++;
    for (const key of Object.keys(p.metadata)) {
      if (!producerKeys[key]) producerKeys[key] = { count: 0, examples: [] };
      producerKeys[key].count++;
      if (producerKeys[key].examples.length < 3) {
        producerKeys[key].examples.push({ producer: p.name, value: p.metadata[key] });
      }
    }
  }
  console.log(`PRODUCERS with metadata: ${producerWithMeta}/${producers.length}`);
  for (const [key, info] of Object.entries(producerKeys).sort((a, b) => b[1].count - a[1].count)) {
    console.log(`  ${key}: ${info.count} entries`);
    for (const ex of info.examples) {
      const val = typeof ex.value === 'string' ? ex.value.slice(0, 80) : JSON.stringify(ex.value).slice(0, 80);
      console.log(`    → ${ex.producer}: ${val}`);
    }
  }

  // Wines
  console.log('\n');
  const wines = await fetchAll('wines', 'id,name,metadata');
  const wineKeys = {};
  let wineWithMeta = 0;
  for (const w of wines) {
    if (!w.metadata || Object.keys(w.metadata).length === 0) continue;
    wineWithMeta++;
    for (const key of Object.keys(w.metadata)) {
      if (!wineKeys[key]) wineKeys[key] = { count: 0, examples: [] };
      wineKeys[key].count++;
      if (wineKeys[key].examples.length < 3) {
        wineKeys[key].examples.push({ wine: w.name, value: w.metadata[key] });
      }
    }
  }
  console.log(`WINES with metadata: ${wineWithMeta}/${wines.length}`);
  for (const [key, info] of Object.entries(wineKeys).sort((a, b) => b[1].count - a[1].count)) {
    console.log(`  ${key}: ${info.count} entries`);
    for (const ex of info.examples) {
      const val = typeof ex.value === 'string' ? ex.value.slice(0, 80) : JSON.stringify(ex.value).slice(0, 80);
      console.log(`    → ${ex.wine}: ${val}`);
    }
  }

  // Wine Vintages
  console.log('\n');
  const vintages = await fetchAll('wine_vintages', 'id,wine_id,vintage_year,metadata');
  const vintageKeys = {};
  let vintageWithMeta = 0;
  for (const v of vintages) {
    if (!v.metadata || Object.keys(v.metadata).length === 0) continue;
    vintageWithMeta++;
    for (const key of Object.keys(v.metadata)) {
      if (!vintageKeys[key]) vintageKeys[key] = { count: 0, examples: [] };
      vintageKeys[key].count++;
      if (vintageKeys[key].examples.length < 2) {
        vintageKeys[key].examples.push({ vintage_year: v.vintage_year, value: v.metadata[key] });
      }
    }
  }
  console.log(`WINE_VINTAGES with metadata: ${vintageWithMeta}/${vintages.length}`);
  for (const [key, info] of Object.entries(vintageKeys).sort((a, b) => b[1].count - a[1].count)) {
    console.log(`  ${key}: ${info.count} entries`);
    for (const ex of info.examples) {
      const val = typeof ex.value === 'string' ? ex.value.slice(0, 80) : JSON.stringify(ex.value).slice(0, 80);
      console.log(`    → vintage ${ex.vintage_year}: ${val}`);
    }
  }
}

main().catch(console.error);
