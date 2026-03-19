#!/usr/bin/env node
/**
 * Load parsed PRO Platform JSON files into source_pro_platform staging table.
 * Cross-state dedup in memory — one row per COLA with states[] array.
 *
 * Usage:
 *   node scripts/load_pro_staging.mjs                    # load all parsed states
 *   node scripts/load_pro_staging.mjs --state ar,co      # load specific states
 *   node scripts/load_pro_staging.mjs --dry-run           # count only, no insert
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, existsSync } from 'fs';
import { resolve } from 'path';

// Manual .env parser (no dotenv dependency)
const envPath = resolve(process.cwd(), '.env');
const vars = {};
try {
  readFileSync(envPath, 'utf8').replace(/\r/g, '').split('\n').forEach(line => {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) vars[m[1].trim()] = m[2].trim().replace(/^["']|["']$/g, '');
  });
} catch {}

const supabase = createClient(
  vars.SUPABASE_URL,
  vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY
);

const PRO_STATES = ['ar', 'co', 'il', 'ky', 'la', 'mn', 'nm', 'ny', 'oh', 'ok', 'sc', 'sd'];
const DATA_DIR = 'data/imports';
const BATCH_SIZE = 500;

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const stateArg = args.find(a => a.startsWith('--state'))
  ? (args[args.indexOf('--state') + 1] || args.find(a => a.startsWith('--state='))?.split('=')[1])?.split(',')
  : null;
const states = stateArg || PRO_STATES;

console.log('=== PRO Platform Staging Loader ===');
console.log(`States: ${states.map(s => s.toUpperCase()).join(', ')}`);
console.log(`Mode: ${dryRun ? 'DRY RUN' : 'LIVE INSERT'}\n`);

// Phase 1: Load all states into memory, merge by COLA
console.log('Phase 1: Cross-state merge in memory...');
const byCola = new Map();
let totalRaw = 0;

function fieldCount(rec) {
  return Object.values(rec).filter(v => v !== null && v !== '' && v !== undefined).length;
}

for (const state of states) {
  const filePath = `${DATA_DIR}/pro_${state}_parsed.json`;
  if (!existsSync(filePath)) {
    console.log(`  ${state.toUpperCase()}: FILE NOT FOUND — skipping`);
    continue;
  }

  const raw = JSON.parse(readFileSync(filePath, 'utf8'));
  const records = raw.records || raw;
  totalRaw += records.length;
  console.log(`  ${state.toUpperCase()}: ${records.length.toLocaleString()} records`);

  for (const r of records) {
    const cola = r.cola_number;
    if (!cola) continue;

    const existing = byCola.get(cola);
    if (existing) {
      // Add state to tracking array
      if (!existing.states.includes(state.toUpperCase())) {
        existing.states.push(state.toUpperCase());
      }
      // Merge distributors
      if (r.distributors) {
        for (const d of r.distributors) {
          if (!existing.distributors.includes(d)) existing.distributors.push(d);
        }
      }
      // Keep record with more filled fields
      if (fieldCount(r) > fieldCount(existing)) {
        const savedStates = existing.states;
        const savedDists = existing.distributors;
        Object.assign(existing, r);
        existing.states = savedStates;
        existing.distributors = savedDists;
      }
    } else {
      byCola.set(cola, {
        ...r,
        states: [state.toUpperCase()],
        distributors: r.distributors || [],
      });
    }
  }
}

const unique = [...byCola.values()];
console.log(`\n  Raw records: ${totalRaw.toLocaleString()}`);
console.log(`  Unique COLAs: ${unique.length.toLocaleString()}`);
console.log(`  Cross-state dedup: ${(totalRaw - unique.length).toLocaleString()} removed\n`);

if (dryRun) {
  // Show state distribution
  const stateCounts = {};
  unique.forEach(r => { const n = r.states.length; stateCounts[n] = (stateCounts[n] || 0) + 1; });
  console.log('State coverage:');
  Object.entries(stateCounts).sort((a, b) => a[0] - b[0]).forEach(([n, c]) => {
    console.log(`  In ${n} state(s): ${c.toLocaleString()}`);
  });
  process.exit(0);
}

// Phase 2: Insert into DB
console.log('Phase 2: Loading into source_pro_platform...');
let inserted = 0, errors = 0;

for (let i = 0; i < unique.length; i += BATCH_SIZE) {
  const batch = unique.slice(i, i + BATCH_SIZE).map(r => ({
    cola_number: r.cola_number,
    brand: r.brand || null,
    label_description: r.label_description || null,
    vintage: r.vintage || null,
    appellation: r.appellation || null,
    abv: r.abv || null,
    container_type: r.container_type || null,
    unit_size: r.unit_size || null,
    unit_measure: r.unit_measure || null,
    supplier_name: r.supplier_name || null,
    distributors: r.distributors || [],
    distributor_count: r.distributors?.length || 0,
    states: r.states,
  }));

  const { data, error } = await supabase
    .from('source_pro_platform')
    .upsert(batch, { onConflict: 'cola_number', ignoreDuplicates: false })
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

console.log(`\n\n=== TOTALS ===`);
console.log(`Raw records across ${states.length} states: ${totalRaw.toLocaleString()}`);
console.log(`Unique COLAs loaded: ${unique.length.toLocaleString()}`);
console.log(`Errors: ${errors}`);

const { count } = await supabase
  .from('source_pro_platform')
  .select('*', { count: 'exact', head: true });
console.log(`DB total: ${count?.toLocaleString() || 'unknown'}`);
