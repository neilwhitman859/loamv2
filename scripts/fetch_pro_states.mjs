#!/usr/bin/env node
/**
 * PRO Platform Multi-State Wine Brand Fetcher
 *
 * Fetches wine brand registrations from the PRO (Product Registration Online)
 * platform by Sovos/ShipCompliant, active in 11+ US states.
 *
 * Each record includes: TTB COLA number, ABV, vintage, appellation, brand name,
 * label description, distributor(s), unit size, container type, approval dates.
 *
 * API: POST {state}.productregistrationonline.com/Search/ActiveBrandSearch
 * Body: { draw: N, start: offset, length: pageSize }
 * Response: { Items: [...], TotalItems: N, MaxResults: 200 }
 *
 * Usage:
 *   node scripts/fetch_pro_states.mjs                        # Fetch ALL states
 *   node scripts/fetch_pro_states.mjs --state AR,CO,KY       # Specific states
 *   node scripts/fetch_pro_states.mjs --state CO --analyze    # Count only
 *   node scripts/fetch_pro_states.mjs --resume                # Resume
 *
 * Output: data/imports/pro_{state}_wines.json per state
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, '..', 'data', 'imports');
const CHECKPOINT_DIR = path.join(DATA_DIR, 'pro_checkpoints');

const PRO_STATES = {
  AR: { name: 'Arkansas' },
  CO: { name: 'Colorado' },
  KY: { name: 'Kentucky' },
  LA: { name: 'Louisiana' },
  MN: { name: 'Minnesota' },
  NM: { name: 'New Mexico' },
  NY: { name: 'New York' },
  OH: { name: 'Ohio' },
  OK: { name: 'Oklahoma' },
  SC: { name: 'South Carolina' },
  SD: { name: 'South Dakota' },
};

const PAGE_SIZE = 200;  // API max
const DELAY_MS = 1200;
const ANALYZE_ONLY = process.argv.includes('--analyze');
const RESUME = process.argv.includes('--resume');

const stateFlag = process.argv.includes('--state')
  ? process.argv[process.argv.indexOf('--state') + 1]
  : null;
const TARGET_STATES = stateFlag
  ? stateFlag.split(',').map(s => s.trim().toUpperCase())
  : Object.keys(PRO_STATES);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function pct(n, t) { return t ? `${((n/t)*100).toFixed(1)}%` : '0%'; }

async function fetchPage(apiUrl, start, draw) {
  const resp = await fetch(apiUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    },
    body: JSON.stringify({ draw, start, length: PAGE_SIZE }),
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

function saveCheckpoint(file, brands, nextStart) {
  if (!fs.existsSync(CHECKPOINT_DIR)) fs.mkdirSync(CHECKPOINT_DIR, { recursive: true });
  fs.writeFileSync(file, JSON.stringify({ brands, nextStart, saved_at: new Date().toISOString() }));
}

// Keep only useful fields to reduce file size
function slimBrand(b) {
  return {
    cola_number: b.ColaNumber || null,
    brand: b.BrandDescription || null,
    label: b.LabelDescription || null,
    licensee: b.LicenseeName || null,
    abv: b.ABV || null,
    vintage: b.Vintage || null,
    appellation: b.Appellation || null,
    unit_size: b.UnitSize || null,
    unit_of_measure: b.UnitOfMeasure || null,
    container_type: b.ContainerType || null,
    approval_date: b.ApprovalDate || null,
    expiration_date: b.ExpirationDateString || null,
    approval_number: b.ApprovalNumber || null,
    distributors: (b.Distributors || []).map(d => d.Name || d).filter(Boolean),
    origin: b.OriginName || null,
    item_number: b.ItemNumber || null,
  };
}

async function fetchState(stateCode) {
  const config = PRO_STATES[stateCode];
  if (!config) { console.log(`  Unknown state: ${stateCode}`); return null; }

  const apiUrl = `https://${stateCode.toLowerCase()}.productregistrationonline.com/Search/ActiveBrandSearch`;
  const outputFile = path.join(DATA_DIR, `pro_${stateCode.toLowerCase()}_wines.json`);
  const checkpointFile = path.join(CHECKPOINT_DIR, `${stateCode.toLowerCase()}.json`);

  console.log(`\n=== ${config.name} (${stateCode}) ===`);

  // Get total count
  let totalItems;
  try {
    const data = await fetchPage(apiUrl, 0, 1);
    totalItems = data.TotalItems;
    console.log(`  Total brands: ${totalItems?.toLocaleString() || 'unknown'}`);

    if (!totalItems) { console.log('  No data — skipping'); return null; }
    if (ANALYZE_ONLY) return { state: stateCode, name: config.name, total: totalItems };
  } catch (err) {
    console.error(`  Error: ${err.message} — skipping`);
    return null;
  }

  // Resume or start fresh
  let allBrands = [];
  let startOffset = 0;
  if (RESUME && fs.existsSync(checkpointFile)) {
    const cp = JSON.parse(fs.readFileSync(checkpointFile, 'utf8'));
    allBrands = cp.brands || [];
    startOffset = cp.nextStart || 0;
    console.log(`  Resuming from offset ${startOffset} (${allBrands.length} cached)`);
  }

  // Paginate
  const totalPages = Math.ceil(totalItems / PAGE_SIZE);
  const startPage = Math.floor(startOffset / PAGE_SIZE) + 1;
  console.log(`  Pages: ${startPage}-${totalPages} (${PAGE_SIZE}/page)`);

  let draw = startPage;
  let consecutiveEmpty = 0;
  let errors = 0;

  for (let offset = startOffset; offset < totalItems; offset += PAGE_SIZE) {
    draw++;
    try {
      const data = await fetchPage(apiUrl, offset, draw);
      const items = data.Items || [];

      if (items.length === 0) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= 3) { console.log('  3 consecutive empty pages — stopping'); break; }
        continue;
      }
      consecutiveEmpty = 0;

      allBrands.push(...items.map(slimBrand));

      const page = Math.floor(offset / PAGE_SIZE) + 1;
      if (page % 20 === 0 || page >= totalPages) {
        console.log(`  Page ${page}/${totalPages} — ${allBrands.length.toLocaleString()} brands`);
        saveCheckpoint(checkpointFile, allBrands, offset + PAGE_SIZE);
      }

      await sleep(DELAY_MS);

    } catch (err) {
      errors++;
      console.error(`  Offset ${offset}: ${err.message}`);
      if (errors >= 5) {
        console.error('  Too many errors — saving checkpoint and moving on');
        saveCheckpoint(checkpointFile, allBrands, offset);
        break;
      }
      await sleep(DELAY_MS * 5);
    }
  }

  // Deduplicate
  const seen = new Set();
  const deduped = allBrands.filter(b => {
    const key = b.cola_number || b.approval_number || JSON.stringify(b);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // Stats
  const stats = {
    total_from_api: totalItems,
    fetched: deduped.length,
    has_cola: deduped.filter(b => b.cola_number).length,
    has_abv: deduped.filter(b => b.abv).length,
    has_vintage: deduped.filter(b => b.vintage).length,
    has_appellation: deduped.filter(b => b.appellation).length,
  };

  console.log(`  Fetched: ${deduped.length.toLocaleString()} (deduped from ${allBrands.length.toLocaleString()})`);
  console.log(`  COLA: ${stats.has_cola.toLocaleString()} (${pct(stats.has_cola, deduped.length)})`);
  console.log(`  ABV: ${stats.has_abv.toLocaleString()} (${pct(stats.has_abv, deduped.length)})`);
  console.log(`  Vintage: ${stats.has_vintage.toLocaleString()} (${pct(stats.has_vintage, deduped.length)})`);
  console.log(`  Appellation: ${stats.has_appellation.toLocaleString()} (${pct(stats.has_appellation, deduped.length)})`);

  // Save
  const output = {
    metadata: {
      source: `PRO Platform — ${config.name} (${stateCode})`,
      url: apiUrl,
      fetched_at: new Date().toISOString(),
      stats,
    },
    brands: deduped,
  };

  fs.writeFileSync(outputFile, JSON.stringify(output, null, 2));
  const sizeMB = (fs.statSync(outputFile).size / 1024 / 1024).toFixed(1);
  console.log(`  Saved: ${outputFile} (${sizeMB} MB)`);

  if (fs.existsSync(checkpointFile)) fs.unlinkSync(checkpointFile);

  return { state: stateCode, name: config.name, ...stats };
}

async function main() {
  console.log('=== PRO Platform Multi-State Wine Brand Fetcher ===');
  console.log(`States: ${TARGET_STATES.join(', ')}`);
  console.log(`Mode: ${ANALYZE_ONLY ? 'ANALYZE' : 'FULL FETCH'}`);

  const results = [];
  let grandTotal = 0;

  for (const sc of TARGET_STATES) {
    const r = await fetchState(sc);
    if (r) { results.push(r); grandTotal += r.fetched || r.total || 0; }
    await sleep(2000);
  }

  console.log('\n=== GRAND SUMMARY ===');
  console.log(`States: ${results.length}/${TARGET_STATES.length}`);
  console.log(`Total wines: ${grandTotal.toLocaleString()}\n`);
  for (const r of results) {
    console.log(`  ${r.state} ${r.name}: ${(r.fetched || r.total || 0).toLocaleString()}`);
  }

  fs.writeFileSync(path.join(DATA_DIR, 'pro_states_summary.json'), JSON.stringify({
    fetched_at: new Date().toISOString(),
    mode: ANALYZE_ONLY ? 'analyze' : 'full',
    states: results,
    grand_total: grandTotal,
  }, null, 2));
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
