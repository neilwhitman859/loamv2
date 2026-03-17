#!/usr/bin/env node
/**
 * COLA Cloud API test round 2 — match against known wines
 * Uses ~10 of our remaining 495 requests
 */

import { readFileSync, writeFileSync } from 'fs';

const envContent = readFileSync('.env', 'utf8');
const API_KEY = envContent.match(/COLA_CLOUD_API_KEY=(.+)/)?.[1]?.trim();
const BASE = 'https://app.colacloud.us/api/v1';

let requestCount = 0;

async function apiGet(path, params = {}) {
  const url = new URL(`${BASE}${path}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) url.searchParams.set(k, v);
  }
  console.log(`  GET ${url.pathname}${url.search}`);
  const res = await fetch(url, {
    headers: { 'X-API-Key': API_KEY }
  });
  requestCount++;
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status}: ${text}`);
  }
  const remaining = res.headers.get('x-ratelimit-remaining');
  if (remaining) console.log(`  → Remaining: ${remaining}`);
  return res.json();
}

async function searchAndDetail(label, searchParams) {
  console.log(`\n=== ${label} ===`);
  const results = await apiGet('/colas', {
    product_type: 'wine',
    per_page: 5,
    approval_date_from: '2015-01-01',
    ...searchParams
  });
  console.log(`  Found: ${results.pagination?.total} COLAs`);

  if (results.data.length === 0) {
    console.log('  (no results)');
    return null;
  }

  // Show search results
  for (const c of results.data.slice(0, 3)) {
    console.log(`  [search] ${c.ttb_id}: ${c.brand_name} — ${c.product_name} | ${c.origin_name} | ABV ${c.abv}%`);
  }

  // Get detail on first result
  const detail = await apiGet(`/colas/${results.data[0].ttb_id}`);
  const d = detail.data;
  console.log(`  [detail] Grapes: ${JSON.stringify(d.grape_varietals)}`);
  console.log(`  [detail] Appellation: ${d.wine_appellation}`);
  console.log(`  [detail] Vintage: ${d.wine_vintage_year}`);
  console.log(`  [detail] Barcode: ${d.barcode_value} (${d.barcode_type})`);
  console.log(`  [detail] Designation: ${d.llm_wine_designation}`);
  console.log(`  [detail] Description: ${d.llm_product_description?.slice(0, 200)}`);
  console.log(`  [detail] Tasting: ${JSON.stringify(d.llm_tasting_note_flavors)}`);
  console.log(`  [detail] Permit: ${d.permit_number} | State: ${d.address_state}`);

  return detail.data;
}

async function main() {
  console.log('\n=== COLA Cloud Test Round 2: Known Wine Matching ===');

  const details = [];

  // Test known producers we have in DB
  // 1. Ridge Monte Bello (US domestic)
  details.push(await searchAndDetail('Ridge Monte Bello', {
    q: 'Ridge Monte Bello'
  }));

  // 2. López de Heredia (Spanish import)
  details.push(await searchAndDetail('López de Heredia', {
    q: 'Lopez de Heredia'
  }));

  // 3. Antinori Tignanello (Italian import)
  details.push(await searchAndDetail('Antinori Tignanello', {
    q: 'Tignanello'
  }));

  // 4. Louis Roederer Cristal (Champagne)
  details.push(await searchAndDetail('Louis Roederer Cristal', {
    q: 'Cristal Roederer'
  }));

  // 5. Château d'Yquem (Sauternes)
  details.push(await searchAndDetail("Château d'Yquem", {
    q: "Yquem"
  }));

  // Test barcode lookup endpoint
  console.log('\n=== Barcode Lookup Test ===');
  // Use the Bessin barcode from test 1
  const barcode = await apiGet('/barcodes/3554770154090');
  console.log(`Barcode lookup result:`);
  console.log(`  Total COLAs with this barcode: ${barcode.data?.total_colas}`);
  console.log(`  COLAs: ${JSON.stringify(barcode.data?.colas?.map(c => `${c.brand_name} ${c.product_name}`))}`);

  // Test: how many recent wine COLAs have grapes on detail?
  // Pull 5 detail records from different origins to check grape coverage
  console.log('\n=== Grape Coverage Spot Check (5 random details) ===');
  const randomSample = await apiGet('/colas', {
    product_type: 'wine',
    per_page: 100,
    approval_date_from: '2024-06-01',
    approval_date_to: '2024-12-31'
  });

  let grapeCount = 0, appellationCount = 0, vintageCount = 0, barcodeCount = 0;
  const detailChecks = randomSample.data.slice(0, 5);
  for (const c of detailChecks) {
    const det = await apiGet(`/colas/${c.ttb_id}`);
    const d = det.data;
    if (d.grape_varietals?.length > 0) grapeCount++;
    if (d.wine_appellation) appellationCount++;
    if (d.wine_vintage_year) vintageCount++;
    if (d.barcode_value) barcodeCount++;
    console.log(`  ${d.brand_name} ${d.product_name}: grapes=${JSON.stringify(d.grape_varietals)} | app=${d.wine_appellation} | vintage=${d.wine_vintage_year} | barcode=${d.barcode_value ? 'yes' : 'no'}`);
  }
  console.log(`\nDetail field coverage (5 samples):`);
  console.log(`  Grapes: ${grapeCount}/5`);
  console.log(`  Appellation: ${appellationCount}/5`);
  console.log(`  Vintage: ${vintageCount}/5`);
  console.log(`  Barcode: ${barcodeCount}/5`);

  // Save all details
  writeFileSync('data/imports/cola_cloud_test2.json', JSON.stringify({
    known_wine_details: details.filter(Boolean),
    barcode_lookup: barcode.data,
    grape_spot_check: detailChecks.map(c => c.ttb_id)
  }, null, 2));

  console.log(`\n✅ Saved to data/imports/cola_cloud_test2.json`);
  console.log(`Total requests this run: ${requestCount}`);
  console.log(`Estimated remaining: ${495 - requestCount}`);
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
