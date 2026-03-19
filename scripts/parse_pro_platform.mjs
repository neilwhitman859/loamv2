#!/usr/bin/env node
/**
 * Parse PRO Platform XLSX exports from 12 US states.
 *
 * Processes one state at a time to avoid memory issues with large files.
 * Deduplicates by COLA within each state (rows duplicate per distributor).
 * Cross-state dedup happens at staging table level (upsert on cola_number).
 *
 * Wine filtering is NOT done here — that happens at match time
 * against TTB COLA wine records.
 *
 * Usage:
 *   node scripts/parse_pro_platform.mjs                    # parse all 12 states
 *   node scripts/parse_pro_platform.mjs --state ar         # parse one state
 *   node scripts/parse_pro_platform.mjs --state ar,co,il   # parse multiple
 *   node scripts/parse_pro_platform.mjs --stats            # stats only, no output
 */

import { writeFileSync, statSync, existsSync } from 'fs';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const XLSX = require('xlsx');

const PRO_STATES = ['ar', 'co', 'il', 'ky', 'la', 'mn', 'nm', 'ny', 'oh', 'ok', 'sc', 'sd'];

// Simple CSV line parser that handles quoted fields with commas
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') { current += '"'; i++; }
      else if (ch === '"') { inQuotes = false; }
      else { current += ch; }
    } else {
      if (ch === '"') { inQuotes = true; }
      else if (ch === ',') { result.push(current); current = ''; }
      else { current += ch; }
    }
  }
  result.push(current);
  return result;
}
const DATA_DIR = 'data/imports';

const args = process.argv.slice(2);
const statsOnly = args.includes('--stats');
const stateArg = args.find(a => a.startsWith('--state='))?.split('=')[1]?.split(',') ||
                 (args.includes('--state') ? args[args.indexOf('--state') + 1]?.split(',') : null);
const states = stateArg || PRO_STATES;

console.log('=== PRO Platform XLSX Parser ===');
console.log(`States: ${states.map(s => s.toUpperCase()).join(', ')}`);
console.log(`Mode: ${statsOnly ? 'STATS ONLY' : 'FULL PARSE — one JSON per state'}\n`);

const grandStats = { totalRaw: 0, totalUnique: 0, totalNoCola: 0 };

function parseRow(r, state) {
  const cola = (r['Tax Trade Bureau ID'] || '').toString().trim();
  if (!cola) return null;

  return {
    cola_number: cola,
    brand: (r['Brand Description'] || '').trim() || null,
    label_description: (r['Label Description'] || '').trim() || null,
    vintage: (r['Vintage'] || '').toString().trim() || null,
    appellation: (r['Appellation'] || '').trim() || null,
    abv: r['Percent Alcohol'] ? parseFloat(r['Percent Alcohol']) : null,
    container_type: (r['Container Type'] || '').trim() || null,
    unit_size: r['Unit Size'] ? parseFloat(r['Unit Size']) : null,
    unit_measure: (r['Unit Measure'] || '').trim() || null,
    supplier_name: (r['Supplier Name'] || '').trim() || null,
    distributor_name: (r['Distributor Name'] || '').trim() || null,
    approval_date: (r['Inception Date'] || r['Approval Date'] || '').toString().trim() || null,
    end_date: (r['End Date'] || '').toString().trim() || null,
    approval_number: (r['Approval Number'] || '').toString().trim() || null,
    status: (r['Status'] || '').trim() || null,
    state: state.toUpperCase(),
  };
}

function fieldCount(rec) {
  return Object.values(rec).filter(v => v !== null && v !== '').length;
}

for (const state of states) {
  // Find the XLSX file
  let filePath = `${DATA_DIR}/${state}_active_brands.xlsx`;
  if (!existsSync(filePath)) {
    filePath = `${DATA_DIR}/${state}_active_brands_wine.xlsx`;
    if (!existsSync(filePath)) {
      console.log(`  ${state.toUpperCase()}: FILE NOT FOUND — skipping`);
      continue;
    }
  }

  const fileSize = (statSync(filePath).size / 1024 / 1024).toFixed(1);
  process.stdout.write(`  ${state.toUpperCase()}: Reading ${fileSize} MB...`);
  const t0 = Date.now();

  // For large files (>20MB), use sheet_to_csv then parse — 10x faster than sheet_to_json
  const wb = XLSX.readFile(filePath);
  const ws = wb.Sheets[wb.SheetNames[0]];
  let rows;
  if (statSync(filePath).size > 20 * 1024 * 1024) {
    const csv = XLSX.utils.sheet_to_csv(ws);
    const lines = csv.split('\n');
    const headers = parseCSVLine(lines[0]);
    rows = [];
    for (let i = 1; i < lines.length; i++) {
      if (!lines[i].trim()) continue;
      const vals = parseCSVLine(lines[i]);
      const obj = {};
      headers.forEach((h, j) => { obj[h] = vals[j] || ''; });
      rows.push(obj);
    }
  } else {
    rows = XLSX.utils.sheet_to_json(ws);
  }
  const readTime = ((Date.now() - t0) / 1000).toFixed(1);
  process.stdout.write(` ${rows.length.toLocaleString()} rows (${readTime}s)\n`);

  // Dedup within state by COLA — keep richest record, collect all distributors
  const byCola = new Map();
  let noCola = 0;

  for (const r of rows) {
    const rec = parseRow(r, state);
    if (!rec) { noCola++; continue; }

    const existing = byCola.get(rec.cola_number);
    if (existing) {
      // Collect unique distributors
      if (rec.distributor_name && !existing.distributors.has(rec.distributor_name)) {
        existing.distributors.add(rec.distributor_name);
      }
      // Keep record with more filled fields
      if (fieldCount(rec) > fieldCount(existing.record)) {
        existing.record = rec;
      }
    } else {
      const distributors = new Set();
      if (rec.distributor_name) distributors.add(rec.distributor_name);
      byCola.set(rec.cola_number, { record: rec, distributors });
    }
  }

  // Build output array
  const unique = [];
  for (const [cola, entry] of byCola) {
    const rec = entry.record;
    rec.distributors = [...entry.distributors].sort();
    rec.distributor_count = entry.distributors.size;
    delete rec.distributor_name; // replaced by distributors array
    unique.push(rec);
  }

  // Stats
  const hasVintage = unique.filter(r => r.vintage).length;
  const hasAppellation = unique.filter(r => r.appellation).length;
  const hasAbv = unique.filter(r => r.abv !== null).length;

  grandStats.totalRaw += rows.length;
  grandStats.totalUnique += unique.length;
  grandStats.totalNoCola += noCola;

  console.log(`    Unique: ${unique.length.toLocaleString()} | Dedup: ${(rows.length - unique.length - noCola).toLocaleString()} removed | No COLA: ${noCola.toLocaleString()}`);
  console.log(`    Vintage: ${hasVintage.toLocaleString()} (${(hasVintage/unique.length*100).toFixed(0)}%) | Appellation: ${hasAppellation.toLocaleString()} (${(hasAppellation/unique.length*100).toFixed(0)}%) | ABV: ${hasAbv.toLocaleString()} (${(hasAbv/unique.length*100).toFixed(0)}%)`);

  if (!statsOnly) {
    const outPath = `${DATA_DIR}/pro_${state}_parsed.json`;
    const output = {
      metadata: {
        source: 'PRO Platform (Sovos ShipCompliant)',
        state: state.toUpperCase(),
        extracted_at: new Date().toISOString(),
        raw_rows: rows.length,
        unique_colas: unique.length,
        no_cola: noCola,
      },
      records: unique,
    };
    writeFileSync(outPath, JSON.stringify(output));
    const outSize = (statSync(outPath).size / 1024 / 1024).toFixed(1);
    console.log(`    Saved: ${outPath} (${outSize} MB)`);
  }

  // Free memory before next state
  byCola.clear();
  console.log('');
}

console.log('=== GRAND TOTALS ===');
console.log(`Raw rows: ${grandStats.totalRaw.toLocaleString()}`);
console.log(`Unique COLAs: ${grandStats.totalUnique.toLocaleString()}`);
console.log(`No COLA: ${grandStats.totalNoCola.toLocaleString()}`);
console.log(`Dedup ratio: ${((1 - grandStats.totalUnique / grandStats.totalRaw) * 100).toFixed(1)}%`);
console.log(`\nNote: cross-state dedup not shown here — happens at staging table load (upsert on cola_number).`);
