#!/usr/bin/env node
/**
 * Load TX TABC wine data into source_tabc staging table.
 * Source: data/imports/tx_tabc_wines.json (201K wines from Socrata API)
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
const BATCH_SIZE = 500;

console.log('=== TX TABC Staging Loader ===\n');

const raw = JSON.parse(readFileSync('data/imports/tx_tabc_wines.json', 'utf8'));
console.log(`Raw records: ${raw.length.toLocaleString()}`);

// Dedup by TTB number — keep richest record
const byTTB = new Map();
let noTTB = 0;
for (const r of raw) {
  const ttb = (r.ttb_number || '').trim();
  if (!ttb || !/^\d{10,}$/.test(ttb)) { noTTB++; continue; }

  const existing = byTTB.get(ttb);
  if (!existing || Object.keys(r).length > Object.keys(existing).length) {
    byTTB.set(ttb, r);
  }
}

const unique = [...byTTB.values()];
console.log(`Unique TTB numbers: ${unique.length.toLocaleString()}`);
console.log(`No valid TTB: ${noTTB.toLocaleString()}\n`);

let inserted = 0, errors = 0;
for (let i = 0; i < unique.length; i += BATCH_SIZE) {
  const batch = unique.slice(i, i + BATCH_SIZE).map(r => ({
    ttb_number: r.ttb_number.trim(),
    brand_name: r.brand_name || null,
    trade_name: r.trade_name || null,
    alcohol_content: r.alcohol_content_by_volume ? parseFloat(r.alcohol_content_by_volume) : null,
    approval_date: r.approval_date || null,
    tabc_certificate: r.tabc_certificate_number || null,
    permit_license: r.permit_license_number || null,
    product_type: r.type || 'WINE',
  }));

  const { data, error } = await supabase
    .from('source_tabc')
    .upsert(batch, { onConflict: 'ttb_number', ignoreDuplicates: false })
    .select('id');

  if (error) {
    console.error(`  Batch error at ${i}: ${error.message}`);
    errors++;
  } else {
    inserted += data?.length || batch.length;
  }

  if ((i + BATCH_SIZE) % 10000 === 0 || i + BATCH_SIZE >= unique.length) {
    process.stdout.write(`  ${Math.min(i + BATCH_SIZE, unique.length).toLocaleString()}/${unique.length.toLocaleString()} loaded\r`);
  }
}

console.log(`\n\nDone: ${inserted.toLocaleString()} upserted, ${errors} errors`);
const { count } = await supabase.from('source_tabc').select('*', { count: 'exact', head: true });
console.log(`DB total: ${count?.toLocaleString()}`);
