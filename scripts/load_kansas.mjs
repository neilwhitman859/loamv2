/**
 * Load Kansas Active Brands JSON into source_kansas_brands staging table.
 * Source: data/imports/kansas_active_brands.json (24.6MB, 65K records)
 *
 * Usage: node scripts/load_kansas.mjs [--dry-run]
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

const envContent = readFileSync('.env', 'utf8');
const getEnv = (key) => envContent.match(new RegExp(`${key}=(.+)`))?.[1]?.trim();

const supabase = createClient(getEnv('SUPABASE_URL'), getEnv('SUPABASE_SERVICE_ROLE'));

const DRY_RUN = process.argv.includes('--dry-run');

// Field mapping from obfuscated Kansas JSON keys
function mapRecord(r) {
  return {
    cola_number: r.a?.trim() || null,
    ks_license: r.b?.trim() || null,
    brand_name: r.c?.trim() || null,
    fanciful_name: r.d?.trim() || null,
    product_type: r.e?.trim() || null,
    abv: r.f ? parseFloat(r.f) : null,
    pack_size: r.g ? parseInt(r.g) : null,
    container_size: r.h ? parseFloat(r.h) : null,
    container_unit: r.i?.trim() || null,
    vintage: r.j?.trim() || null,
    appellation: r.k?.trim() || null,
    expiration: parseKansasDate(r.l),
    unknown_m: r.m?.trim() || null,
    container_type: r.n?.trim() || null,
    flag_o: r.o?.trim() || null,
    flag_p: r.p?.trim() || null,
    distributor1: r.q?.trim() || null,
    distributor2: r.r?.trim() || null,
  };
}

function parseKansasDate(dateStr) {
  if (!dateStr || !dateStr.trim()) return null;
  // Format: MM/DD/YYYY
  const match = dateStr.trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) return null;
  return `${match[3]}-${match[1]}-${match[2]}`;
}

async function main() {
  console.log('Loading Kansas Active Brands...');
  const raw = JSON.parse(readFileSync('data/imports/kansas_active_brands.json', 'utf8'));
  console.log(`Total records: ${raw.length}`);

  // Filter to wine-type records only
  const wines = raw.filter(r => {
    const t = (r.e || '').trim();
    return t.includes('Wine') || t.includes('wine');
  });
  console.log(`Wine records: ${wines.length}`);

  const mapped = wines.map(mapRecord);

  if (DRY_RUN) {
    console.log('\n--- DRY RUN ---');
    console.log('Sample mapped records:');
    for (const r of mapped.slice(0, 5)) {
      console.log(JSON.stringify(r, null, 2));
    }
    console.log(`\nWould insert ${mapped.length} wine records.`);
    return;
  }

  // Clear existing data
  const { error: delErr } = await supabase.from('source_kansas_brands').delete().gte('id', 0);
  if (delErr) {
    console.error('Error clearing table:', delErr.message);
    return;
  }
  console.log('Cleared existing data.');

  // Batch insert (Supabase limit ~1000 rows per request)
  const BATCH = 500;
  let inserted = 0;
  for (let i = 0; i < mapped.length; i += BATCH) {
    const batch = mapped.slice(i, i + BATCH);
    const { error } = await supabase.from('source_kansas_brands').insert(batch);
    if (error) {
      console.error(`Error at batch ${i}: ${error.message}`);
      // Try row by row for this batch to find the problem
      let batchInserted = 0;
      for (const row of batch) {
        const { error: rowErr } = await supabase.from('source_kansas_brands').insert(row);
        if (rowErr) {
          console.error(`  Row error (${row.brand_name} / ${row.fanciful_name}): ${rowErr.message}`);
        } else {
          batchInserted++;
        }
      }
      inserted += batchInserted;
    } else {
      inserted += batch.length;
    }
    if ((i + BATCH) % 5000 === 0 || i + BATCH >= mapped.length) {
      console.log(`  ${Math.min(i + BATCH, mapped.length)}/${mapped.length} processed (${inserted} inserted)`);
    }
  }

  console.log(`\nDone. Inserted ${inserted} wine records into source_kansas_brands.`);

  // Quick stats
  const { count } = await supabase.from('source_kansas_brands').select('*', { count: 'exact', head: true });
  console.log(`Table row count: ${count}`);
}

main().catch(console.error);
