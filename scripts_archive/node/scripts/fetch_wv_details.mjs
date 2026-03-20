#!/usr/bin/env node
/**
 * fetch_wv_details.mjs — Batch fetch WV ABCA detail data for wine labels.
 *
 * The list endpoint only gives brand name, class, ABV, vintage, winery.
 * The detail endpoint adds: appellation, varietal, vineyard, origin, supplier DBA.
 *
 * Usage:
 *   node scripts/fetch_wv_details.mjs                # run (resume-safe)
 *   node scripts/fetch_wv_details.mjs --stats        # show progress
 *   node scripts/fetch_wv_details.mjs --limit 100    # fetch only 100 labels
 *   node scripts/fetch_wv_details.mjs --rate 2       # requests per second (default: 1)
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// ─── Env ──────────────────────────────────────────────────────────
const envPath = resolve(process.cwd(), '.env');
const vars = {};
try {
  readFileSync(envPath, 'utf8').replace(/\r/g, '').split('\n').forEach(line => {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) vars[m[1].trim()] = m[2].trim().replace(/^["']|["']$/g, '');
  });
} catch {}

const supabase = createClient(vars.SUPABASE_URL, vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY);

const WV_API_KEY = '2BB0C528-219F-49EE-A8B8-A5A2271BEF9D';
const WV_DETAIL_URL = 'https://api.wvabca.com/API.svc/GetWineLabelDetails';
const DB_BATCH_SIZE = 50; // Rows per DB update batch

// ─── Args ─────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const statsOnly = args.includes('--stats');
const limitArg = args.find(a => a.startsWith('--limit'));
const limit = limitArg ? parseInt(limitArg.includes('=') ? limitArg.split('=')[1] : args[args.indexOf('--limit') + 1]) : null;
const rateArg = args.find(a => a.startsWith('--rate'));
const reqPerSec = rateArg ? parseFloat(rateArg.includes('=') ? rateArg.split('=')[1] : args[args.indexOf('--rate') + 1]) : 1;
const delayMs = Math.floor(1000 / reqPerSec);

// ─── Stats ────────────────────────────────────────────────────────
async function showStats() {
  const { count: total } = await supabase.from('source_wv_abca').select('*', { count: 'exact', head: true });
  const { count: fetched } = await supabase.from('source_wv_abca').select('*', { count: 'exact', head: true }).not('detail_fetched_at', 'is', null);
  const { count: withAppellation } = await supabase.from('source_wv_abca').select('*', { count: 'exact', head: true }).not('appellation', 'is', null);
  const { count: withVarietal } = await supabase.from('source_wv_abca').select('*', { count: 'exact', head: true }).not('varietal', 'is', null);
  const remaining = total - fetched;
  const etaHours = (remaining / reqPerSec / 3600).toFixed(1);

  console.log(`source_wv_abca: ${total?.toLocaleString()} total`);
  console.log(`  Detail fetched: ${fetched?.toLocaleString()} (${remaining?.toLocaleString()} remaining, ~${etaHours}h at ${reqPerSec} req/s)`);
  console.log(`  With appellation: ${withAppellation?.toLocaleString()}`);
  console.log(`  With varietal: ${withVarietal?.toLocaleString()}`);
  return { total, fetched, remaining };
}

// ─── Ensure detail columns exist ──────────────────────────────────
// detail_fetched_at is our resume marker — if it's set, we already fetched this row
// (even if the detail endpoint returned no extra data)

if (statsOnly) {
  await showStats();
  process.exit(0);
}

// ─── Fetch unfetched label IDs ────────────────────────────────────
console.log('=== WV ABCA Detail Fetcher ===\n');
await showStats();

// Paginate through all unfetched rows (Supabase 1000-row limit)
let allRows = [];
let from = 0;
const PAGE = 1000;
const maxRows = limit || Infinity;

console.log('\nLoading unfetched label IDs...');
while (allRows.length < maxRows) {
  const fetchLimit = Math.min(PAGE, maxRows - allRows.length);
  const { data, error } = await supabase.from('source_wv_abca')
    .select('id, label_id')
    .is('detail_fetched_at', null)
    .order('label_id')
    .range(from, from + fetchLimit - 1);

  if (error) { console.error('Query error:', error.message); process.exit(1); }
  if (!data.length) break;
  allRows.push(...data);
  from += PAGE;
  if (data.length < fetchLimit) break;
}

if (!allRows.length) { console.log('\nAll details already fetched!'); process.exit(0); }
console.log(`Fetching details for ${allRows.length.toLocaleString()} labels at ${reqPerSec} req/s (delay: ${delayMs}ms)...\n`);

// ─── Fetch detail endpoint ────────────────────────────────────────
async function fetchDetail(labelId) {
  const url = `${WV_DETAIL_URL}?id=${labelId}`;
  const resp = await fetch(url, {
    headers: { 'api_key': WV_API_KEY },
  });

  if (!resp.ok) {
    if (resp.status === 429) throw new Error('RATE_LIMITED');
    throw new Error(`HTTP ${resp.status}`);
  }

  const data = await resp.json();
  return data;
}

function extractFields(detail) {
  // The detail endpoint may return different field name formats
  // Try common patterns
  const d = detail || {};
  return {
    appellation: d.Appellation || d.appellation || null,
    origin: d.Origin || d.origin || d.CountryOfOrigin || null,
    varietal: d.Varietal || d.varietal || d.GrapeVarietal || null,
    vineyard: d.Vineyard || d.vineyard || null,
    supplier_dba: d.SupplierDBA || d.supplierDba || d.ApplicantDBA || null,
  };
}

// ─── Main loop ────────────────────────────────────────────────────
let fetched = 0;
let errors = 0;
let withData = 0;
let updateBuffer = [];
const startTime = Date.now();

async function flushUpdates() {
  if (!updateBuffer.length) return;

  for (const update of updateBuffer) {
    const { error } = await supabase.from('source_wv_abca')
      .update(update.fields)
      .eq('id', update.id);

    if (error) {
      console.error(`\n  DB update error for label ${update.label_id}: ${error.message}`);
    }
  }
  updateBuffer = [];
}

for (let i = 0; i < allRows.length; i++) {
  const row = allRows[i];

  try {
    const detail = await fetchDetail(row.label_id);
    const fields = extractFields(detail);

    // Mark as fetched even if no extra data
    const updateFields = {
      detail_fetched_at: new Date().toISOString(),
    };
    if (fields.appellation) updateFields.appellation = fields.appellation;
    if (fields.origin) updateFields.origin = fields.origin;
    if (fields.varietal) updateFields.varietal = fields.varietal;
    if (fields.vineyard) updateFields.vineyard = fields.vineyard;
    if (fields.supplier_dba) updateFields.supplier_dba = fields.supplier_dba;

    if (fields.appellation || fields.varietal || fields.vineyard) withData++;

    updateBuffer.push({ id: row.id, label_id: row.label_id, fields: updateFields });
    fetched++;

    // Flush DB updates in batches
    if (updateBuffer.length >= DB_BATCH_SIZE) {
      await flushUpdates();
    }

    // Progress
    if (fetched % 100 === 0 || fetched === allRows.length) {
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = fetched / elapsed;
      const remaining = allRows.length - fetched;
      const etaMin = (remaining / rate / 60).toFixed(1);
      process.stdout.write(`\r  ${fetched.toLocaleString()}/${allRows.length.toLocaleString()} fetched | ${withData} with data | ${errors} errors | ${rate.toFixed(1)} req/s | ETA: ${etaMin}m`);
    }

    // Rate limiting
    await new Promise(r => setTimeout(r, delayMs));

  } catch (err) {
    if (err.message === 'RATE_LIMITED') {
      console.log('\n  Rate limited! Waiting 60s...');
      await new Promise(r => setTimeout(r, 60000));
      i--; // Retry
      continue;
    }
    errors++;
    // Mark as fetched to avoid infinite retry on persistent errors
    updateBuffer.push({
      id: row.id,
      label_id: row.label_id,
      fields: { detail_fetched_at: new Date().toISOString() }
    });

    if (errors <= 10) {
      console.error(`\n  Error fetching label ${row.label_id}: ${err.message}`);
    } else if (errors === 11) {
      console.error('\n  (suppressing further error messages)');
    }

    // If too many consecutive errors, something is wrong
    if (errors > 100 && fetched < 10) {
      console.error('\n\nToo many errors, aborting. Check API endpoint and key.');
      await flushUpdates();
      process.exit(1);
    }
  }
}

// Final flush
await flushUpdates();

console.log(`\n\n=== Done ===`);
console.log(`Fetched: ${fetched.toLocaleString()}, With data: ${withData}, Errors: ${errors}`);
const elapsed = (Date.now() - startTime) / 1000;
console.log(`Time: ${(elapsed / 60).toFixed(1)} minutes (${(fetched / elapsed).toFixed(1)} req/s avg)`);

await showStats();
