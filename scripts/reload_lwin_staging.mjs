#!/usr/bin/env node
/**
 * reload_lwin_staging.mjs — Reloads source_lwin staging table from data/lwin_database.csv.
 *
 * Filters to wine types only (Wine, Fortified Wine, Champagne), skips Deleted rows.
 *
 * Usage:
 *   node scripts/reload_lwin_staging.mjs
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import { resolve } from 'path';

// Manual .env parser (no dotenv dependency)
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
for (const line of envContent.split('\n')) {
  const trimmed = line.replace(/\r/g, '').trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx === -1) continue;
  const key = trimmed.slice(0, eqIdx);
  const val = trimmed.slice(eqIdx + 1);
  if (!process.env[key]) process.env[key] = val;
}

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY
);

const BATCH_SIZE = 500;
const ALLOWED_TYPES = new Set(['Wine', 'Fortified Wine', 'Champagne']);

function naVal(val) {
  if (!val || val === 'NA') return null;
  return val;
}

function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

function mapRow(headers, fields) {
  const row = {};
  for (let i = 0; i < headers.length; i++) {
    row[headers[i]] = fields[i] ?? '';
  }

  const lwin = row['LWIN'] || '';
  const type = row['TYPE'] || '';
  const status = row['STATUS'] || '';

  // Filter: only wine types, skip deleted
  if (!ALLOWED_TYPES.has(type)) return null;
  if (status === 'Deleted') return null;

  return {
    lwin: lwin,
    lwin_7: lwin.slice(0, 7) || null,
    lwin_11: lwin.length >= 11 ? lwin.slice(0, 11) : null,
    lwin_18: lwin.length >= 18 ? lwin : null,
    display_name: naVal(row['DISPLAY_NAME']),
    producer_name: naVal(row['PRODUCER_NAME']),
    wine_name: naVal(row['WINE']),
    country: naVal(row['COUNTRY']),
    region: naVal(row['REGION']),
    sub_region: naVal(row['SUB_REGION']),
    appellation: naVal(row['DESIGNATION']),
    colour: naVal(row['COLOUR']),
    wine_type: type,
    designation: naVal(row['DESIGNATION']),
    classification: naVal(row['CLASSIFICATION']),
    vintage: naVal(row['VINTAGE_CONFIG']),
  };
}

async function main() {
  const csvPath = resolve('data/lwin_database.csv');
  console.log(`Reading CSV from: ${csvPath}`);

  const rl = createInterface({
    input: createReadStream(csvPath, { encoding: 'utf-8' }),
    crlfDelay: Infinity,
  });

  let headers = null;
  let batch = [];
  let totalRead = 0;
  let totalInserted = 0;
  let totalSkipped = 0;
  let errors = 0;

  for await (const line of rl) {
    if (!headers) {
      headers = parseCSVLine(line);
      console.log(`CSV columns: ${headers.join(', ')}`);
      continue;
    }

    totalRead++;
    const fields = parseCSVLine(line);
    const mapped = mapRow(headers, fields);

    if (!mapped) {
      totalSkipped++;
      continue;
    }

    batch.push(mapped);

    if (batch.length >= BATCH_SIZE) {
      const { error } = await supabase.from('source_lwin').insert(batch);
      if (error) {
        console.error(`  Error at row ${totalRead}:`, error.message);
        errors++;
      } else {
        totalInserted += batch.length;
      }
      batch = [];

      if (totalRead % 10000 < BATCH_SIZE) {
        console.log(`  Progress: ${totalRead.toLocaleString()} read, ${totalInserted.toLocaleString()} inserted, ${totalSkipped.toLocaleString()} skipped`);
      }
    }
  }

  // Final batch
  if (batch.length > 0) {
    const { error } = await supabase.from('source_lwin').insert(batch);
    if (error) {
      console.error(`  Error on final batch:`, error.message);
      errors++;
    } else {
      totalInserted += batch.length;
    }
  }

  console.log(`\nDone.`);
  console.log(`  Total CSV rows read: ${totalRead.toLocaleString()}`);
  console.log(`  Inserted: ${totalInserted.toLocaleString()}`);
  console.log(`  Skipped (wrong type/deleted): ${totalSkipped.toLocaleString()}`);
  console.log(`  Batch errors: ${errors}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
