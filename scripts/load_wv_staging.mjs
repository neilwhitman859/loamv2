#!/usr/bin/env node
/**
 * Load WV ABCA wine data into source_wv_abca staging table.
 * Source: data/imports/wv_wines_list.json (55K wines from REST API)
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

console.log('=== WV ABCA Staging Loader ===\n');

// Try both possible filenames
let raw;
try { raw = JSON.parse(readFileSync('data/imports/wv_wines_list.json', 'utf8')); }
catch { raw = JSON.parse(readFileSync('data/imports/wv_abca_wines.json', 'utf8')); }
console.log(`Raw records: ${raw.length.toLocaleString()}`);

// Dedup by LabelID
const byLabel = new Map();
for (const r of raw) {
  if (!r.LabelID) continue;
  byLabel.set(r.LabelID, r);
}

const unique = [...byLabel.values()];
console.log(`Unique LabelIDs: ${unique.length.toLocaleString()}\n`);

let inserted = 0, errors = 0;
for (let i = 0; i < unique.length; i += BATCH_SIZE) {
  const batch = unique.slice(i, i + BATCH_SIZE).map(r => {
    // TTB field: sometimes numeric COLA, sometimes text
    const ttbRaw = (r.TTB || '').trim();
    const ttb = /^\d{10,}$/.test(ttbRaw) ? ttbRaw : null;

    return {
      label_id: r.LabelID,
      ttb: ttb,
      brand_name: r.BrandName || null,
      fanciful_name: ttb ? (r.FancifulName || null) : (r.FancifulName || ttbRaw || null),
      class_text: r.ClassText || null,
      alcohol_percentage: r.AlcoholPercentage || null,
      vintage: r.Vintage ? String(r.Vintage) : null,
      winery_name: r.WineryName || null,
    };
  });

  const { data, error } = await supabase
    .from('source_wv_abca')
    .upsert(batch, { onConflict: 'label_id', ignoreDuplicates: false })
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
const { count } = await supabase.from('source_wv_abca').select('*', { count: 'exact', head: true });
console.log(`DB total: ${count?.toLocaleString()}`);

// Stats
const { count: hasTTB } = await supabase.from('source_wv_abca').select('*', { count: 'exact', head: true }).not('ttb', 'is', null);
console.log(`With COLA: ${hasTTB?.toLocaleString()}`);
