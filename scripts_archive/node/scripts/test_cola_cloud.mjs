#!/usr/bin/env node
/**
 * COLA Cloud API test — explore data quality on free tier
 * Uses 3-5 of our 500 monthly requests
 */

import { readFileSync, writeFileSync } from 'fs';

// Read .env manually
const envContent = readFileSync('.env', 'utf8');
const API_KEY = envContent.match(/COLA_CLOUD_API_KEY=(.+)/)?.[1]?.trim();
const BASE = 'https://app.colacloud.us/api/v1';

async function apiGet(path, params = {}) {
  const url = new URL(`${BASE}${path}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) url.searchParams.set(k, v);
  }
  console.log(`  GET ${url.pathname}${url.search}`);
  const res = await fetch(url, {
    headers: { 'X-API-Key': API_KEY }
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status}: ${text}`);
  }
  const remaining = res.headers.get('x-ratelimit-remaining');
  if (remaining) console.log(`  → Requests remaining: ${remaining}`);
  return res.json();
}

async function main() {
  console.log('\n=== COLA Cloud API Test ===\n');

  // Test 1: Get total wine COLA count (1 request)
  console.log('--- Test 1: Total wine COLA count ---');
  const wines = await apiGet('/colas', {
    product_type: 'wine',
    per_page: 1,
    approval_date_from: '2015-01-01',
    approval_date_to: '2026-12-31'
  });
  console.log(`Total wine COLAs: ${wines.pagination?.total?.toLocaleString()}`);
  console.log(`Pages: ${wines.pagination?.pages?.toLocaleString()}`);

  // Test 2: Get a page of wine COLAs to see real data (1 request)
  console.log('\n--- Test 2: Sample wine COLAs (100 records) ---');
  const sample = await apiGet('/colas', {
    product_type: 'wine',
    per_page: 100,
    approval_date_from: '2025-01-01'
  });

  // Analyze field coverage
  const fields = {
    brand_name: 0, product_name: 0, abv: 0,
    wine_appellation: 0, grape_varietals: 0,
    wine_vintage_year: 0, barcode_value: 0, barcode_type: 0,
    llm_category: 0, llm_category_path: 0,
    origin_name: 0, class_name: 0,
    llm_tasting_note_flavors: 0, llm_wine_designation: 0,
    llm_product_description: 0
  };

  for (const cola of sample.data) {
    for (const field of Object.keys(fields)) {
      const val = cola[field];
      if (val !== null && val !== undefined && val !== '' &&
          !(Array.isArray(val) && val.length === 0)) {
        fields[field]++;
      }
    }
  }

  console.log('\nField coverage (out of 100):');
  for (const [field, count] of Object.entries(fields)) {
    console.log(`  ${field}: ${count}%`);
  }

  // Show 3 sample records in detail
  console.log('\n--- Sample Records ---');
  for (let i = 0; i < Math.min(3, sample.data.length); i++) {
    const c = sample.data[i];
    console.log(`\n[${i + 1}] ${c.brand_name} — ${c.product_name}`);
    console.log(`    TTB ID: ${c.ttb_id}`);
    console.log(`    Class: ${c.class_name}`);
    console.log(`    Origin: ${c.origin_name} (${c.domestic_or_imported})`);
    console.log(`    ABV: ${c.abv}`);
    console.log(`    Volume: ${c.volume} ${c.volume_unit}`);
    console.log(`    LLM Category: ${c.llm_category_path}`);
    console.log(`    Image: ${c.main_image_url ? 'yes' : 'no'}`);
  }

  // Test 3: Get a detail record to see wine-specific fields (1 request)
  // Pick first record with a grape varietal
  const withGrapes = sample.data.find(c =>
    c.grape_varietals && c.grape_varietals.length > 0
  ) || sample.data.find(c =>
    c.llm_category_path?.includes('Wine')
  ) || sample.data[0];

  console.log(`\n--- Test 3: Detail for ${withGrapes.ttb_id} ---`);
  const detail = await apiGet(`/colas/${withGrapes.ttb_id}`);
  const d = detail.data;
  console.log(`  Brand: ${d.brand_name}`);
  console.log(`  Product: ${d.product_name}`);
  console.log(`  Grapes: ${JSON.stringify(d.grape_varietals)}`);
  console.log(`  Appellation: ${d.wine_appellation}`);
  console.log(`  Vintage: ${d.wine_vintage_year}`);
  console.log(`  ABV: ${d.abv}`);
  console.log(`  Barcode: ${d.barcode_value} (${d.barcode_type})`);
  console.log(`  QR URL: ${d.qrcode_url}`);
  console.log(`  LLM Category: ${d.llm_category_path}`);
  console.log(`  LLM Description: ${d.llm_product_description}`);
  console.log(`  LLM Tasting Flavors: ${JSON.stringify(d.llm_tasting_note_flavors)}`);
  console.log(`  LLM Designation: ${d.llm_wine_designation}`);
  console.log(`  Permit: ${d.permit_number}`);
  console.log(`  Address: ${d.address_state} ${d.address_zip_code}`);
  console.log(`  Images: ${d.images?.length}`);
  console.log(`  Barcodes: ${JSON.stringify(d.barcodes)}`);

  // Test 4: Search for a known producer to test matching (1 request)
  console.log('\n--- Test 4: Search for "Ridge" ---');
  const ridge = await apiGet('/colas', {
    product_type: 'wine',
    brand_name: 'Ridge',
    per_page: 10,
    approval_date_from: '2015-01-01'
  });
  console.log(`Ridge COLAs found: ${ridge.pagination?.total}`);
  for (const c of ridge.data.slice(0, 5)) {
    console.log(`  ${c.ttb_id}: ${c.brand_name} — ${c.product_name} (${c.abv}% ABV)`);
  }

  // Test 5: Search for "Chateau Margaux" to test imported wines (1 request)
  console.log('\n--- Test 5: Search for "Opus One" ---');
  const opus = await apiGet('/colas', {
    product_type: 'wine',
    q: 'Opus One',
    per_page: 10,
    approval_date_from: '2015-01-01'
  });
  console.log(`Opus One COLAs found: ${opus.pagination?.total}`);
  for (const c of opus.data.slice(0, 5)) {
    console.log(`  ${c.ttb_id}: ${c.brand_name} — ${c.product_name} | origin: ${c.origin_name} | ABV: ${c.abv}`);
  }

  // Save full sample for offline analysis
  writeFileSync('data/imports/cola_cloud_sample.json', JSON.stringify({
    sample: sample.data,
    ridge: ridge.data,
    opus: opus.data,
    detail: detail.data,
    total_wine_colas: wines.pagination?.total
  }, null, 2));
  console.log('\n✅ Full sample saved to data/imports/cola_cloud_sample.json');
  console.log(`\nTotal API requests used: 5 of 500`);
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
