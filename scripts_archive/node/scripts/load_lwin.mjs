/**
 * Load LWIN database Excel into source_lwin staging table.
 * Source: data/LWINdatabase.xlsx (25MB, ~211K records)
 * Filters to: STATUS=Live, TYPE=Wine or Fortified Wine
 *
 * Usage: node scripts/load_lwin.mjs [--dry-run] [--include-fortified]
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';
import xlsx from 'xlsx';

const envContent = readFileSync('.env', 'utf8');
const getEnv = (key) => envContent.match(new RegExp(`${key}=(.+)`))?.[1]?.trim();
const supabase = createClient(getEnv('SUPABASE_URL'), getEnv('SUPABASE_SERVICE_ROLE'));

const DRY_RUN = process.argv.includes('--dry-run');
const INCLUDE_FORTIFIED = process.argv.includes('--include-fortified');

function mapRecord(r) {
  const lwin = String(r.LWIN);
  return {
    lwin: lwin,
    lwin_7: lwin.length >= 7 ? lwin.slice(0, 7) : lwin,
    lwin_11: null,  // Not in the base LWIN download (would be vintage-specific)
    lwin_18: null,  // Not in the base LWIN download (would be format-specific)
    display_name: clean(r.DISPLAY_NAME),
    producer_name: clean(r.PRODUCER_NAME),
    wine_name: clean(r.WINE),
    country: clean(r.COUNTRY),
    region: clean(r.REGION),
    sub_region: clean(r.SUB_REGION),
    appellation: clean(r.SITE) || clean(r.DESIGNATION),  // SITE often has appellation-level detail
    colour: clean(r.COLOUR),
    wine_type: clean(r.SUB_TYPE) || clean(r.TYPE),
    designation: clean(r.DESIGNATION),
    classification: clean(r.CLASSIFICATION),
    vintage: clean(r.VINTAGE_CONFIG),
  };
}

function clean(val) {
  if (val === undefined || val === null) return null;
  const s = String(val).trim();
  return (s === '' || s === 'NA' || s === 'N/A') ? null : s;
}

async function main() {
  console.log('Reading LWIN database...');
  const wb = xlsx.readFile('data/LWINdatabase.xlsx');
  const raw = xlsx.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
  console.log(`Total records: ${raw.length}`);

  // Filter to live wines
  const validTypes = ['Wine'];
  if (INCLUDE_FORTIFIED) validTypes.push('Fortified Wine');

  const wines = raw.filter(r => r.STATUS === 'Live' && validTypes.includes(r.TYPE));
  console.log(`Live wine records: ${wines.length} (${INCLUDE_FORTIFIED ? 'including' : 'excluding'} fortified)`);

  const mapped = wines.map(mapRecord);

  if (DRY_RUN) {
    console.log('\n--- DRY RUN ---');
    console.log('Sample records:');
    for (const r of mapped.slice(0, 5)) {
      console.log(JSON.stringify(r, null, 2));
    }

    // Stats
    const stats = {
      with_producer: mapped.filter(r => r.producer_name).length,
      with_wine: mapped.filter(r => r.wine_name).length,
      with_country: mapped.filter(r => r.country).length,
      with_region: mapped.filter(r => r.region).length,
      with_sub_region: mapped.filter(r => r.sub_region).length,
      with_colour: mapped.filter(r => r.colour).length,
      with_classification: mapped.filter(r => r.classification).length,
      with_designation: mapped.filter(r => r.designation).length,
    };
    console.log('\nField fill rates:');
    for (const [k, v] of Object.entries(stats)) {
      console.log(`  ${k}: ${v} (${(v / mapped.length * 100).toFixed(1)}%)`);
    }

    console.log(`\nWould insert ${mapped.length} records.`);
    return;
  }

  // Clear existing data
  console.log('Clearing existing source_lwin data...');
  // Delete in batches since there might be many rows
  let deleted = 0;
  while (true) {
    const { data, error } = await supabase.from('source_lwin').select('lwin').limit(5000);
    if (error) { console.error('Delete select error:', error.message); break; }
    if (!data || data.length === 0) break;
    const ids = data.map(r => r.lwin);
    const { error: delErr } = await supabase.from('source_lwin').delete().in('lwin', ids);
    if (delErr) { console.error('Delete error:', delErr.message); break; }
    deleted += ids.length;
    if (deleted % 10000 === 0) console.log(`  Deleted ${deleted} rows...`);
  }
  console.log(`Cleared ${deleted} existing rows.`);

  // Batch insert
  const BATCH = 500;
  let inserted = 0;
  let errors = 0;

  for (let i = 0; i < mapped.length; i += BATCH) {
    const batch = mapped.slice(i, i + BATCH);
    const { error } = await supabase.from('source_lwin').insert(batch);
    if (error) {
      // Try row by row
      for (const row of batch) {
        const { error: rowErr } = await supabase.from('source_lwin').insert(row);
        if (rowErr) {
          errors++;
          if (errors <= 5) console.error(`  Row error (${row.lwin} ${row.display_name}): ${rowErr.message}`);
        } else {
          inserted++;
        }
      }
    } else {
      inserted += batch.length;
    }
    if ((i + BATCH) % 10000 === 0 || i + BATCH >= mapped.length) {
      console.log(`  ${Math.min(i + BATCH, mapped.length)}/${mapped.length} processed (${inserted} inserted, ${errors} errors)`);
    }
  }

  console.log(`\nDone. Inserted ${inserted} records, ${errors} errors.`);

  // Verify
  const { count } = await supabase.from('source_lwin').select('*', { count: 'exact', head: true });
  console.log(`Table row count: ${count}`);
}

main().catch(console.error);
