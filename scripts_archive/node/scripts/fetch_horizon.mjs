#!/usr/bin/env node
/**
 * Horizon Beverage (Southern Glazer's) Wine Catalog Fetcher
 *
 * Source: horizonbeverage.com — MA/RI wholesale distributor
 * API:    POST /api/products/GetProducts (JSON, no auth)
 * Fields: producerName, productName, rawMaterialNames (grapes!),
 *         countryName, regionName, styleName, upc, size, caseSize
 *
 * Fetches all wine products from both MA and RI, deduplicates by UPC.
 * Output: data/imports/horizon_beverage_wines.json
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUTPUT_PATH = path.join(__dirname, '..', 'data', 'imports', 'horizon_beverage_wines.json');

const API_URL = 'https://www.horizonbeverage.com/api/products/GetProducts';
const PAGE_SIZE = 50; // API default
const DELAY_MS = 500; // polite delay between requests

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchPage(state, page) {
  const body = {
    Page: page,
    State: state,
    Category: 'Wine',
    Type: [],
    Style: [],
    Country: [],
    Region: [],
    Keywords: ''
  };

  const res = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body)
  });

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${state} page ${page}`);
  }

  return res.json();
}

async function fetchAllForState(state) {
  console.log(`\nFetching ${state} wines...`);

  // First request to get total
  const first = await fetchPage(state, 1);
  const totalItems = first.totalItems;
  const totalPages = first.totalPages;
  console.log(`  ${state}: ${totalItems} wines across ${totalPages} pages`);

  const allProducts = [...first.products];

  for (let page = 2; page <= totalPages; page++) {
    await sleep(DELAY_MS);

    try {
      const data = await fetchPage(state, page);
      allProducts.push(...data.products);

      if (page % 20 === 0 || page === totalPages) {
        console.log(`  ${state}: page ${page}/${totalPages} (${allProducts.length} products)`);
      }
    } catch (err) {
      console.error(`  ERROR on ${state} page ${page}: ${err.message}`);
      // Retry once after longer delay
      await sleep(3000);
      try {
        const data = await fetchPage(state, page);
        allProducts.push(...data.products);
      } catch (err2) {
        console.error(`  RETRY FAILED on ${state} page ${page}: ${err2.message}`);
      }
    }
  }

  console.log(`  ${state}: ${allProducts.length} total products fetched`);
  return allProducts;
}

function normalizeProduct(p) {
  return {
    state: p.stateName,
    item_number: p.itemNumber,
    category: p.productTypeName,
    style: p.styleName || null,
    producer: p.producerName || null,
    name: p.productName || null,
    grapes: p.rawMaterialNames
      ? p.rawMaterialNames.split(';').map(g => g.trim()).filter(Boolean)
      : [],
    country: p.countryName || null,
    region: p.regionName || null,
    size_ml: parseSize(p.size, p.sizeUnit),
    size_raw: p.size && p.sizeUnit ? `${p.size} ${p.sizeUnit}` : null,
    case_size: p.caseSize ? parseInt(p.caseSize) : null,
    upc: p.upc ? p.upc.trim() : null,
  };
}

function parseSize(size, unit) {
  if (!size || !unit) return null;
  const val = parseFloat(size);
  if (isNaN(val)) return null;
  if (unit === 'mL') return val;
  if (unit === 'L') return val * 1000;
  return val;
}

async function main() {
  console.log('=== Horizon Beverage Wine Catalog Fetcher ===');
  console.log(`API: ${API_URL}`);
  console.log(`Output: ${OUTPUT_PATH}`);

  // Fetch both states
  const maProducts = await fetchAllForState('MA');
  const riProducts = await fetchAllForState('RI');

  // Normalize all
  const maNorm = maProducts.map(normalizeProduct);
  const riNorm = riProducts.map(normalizeProduct);

  // Dedup by UPC (prefer MA entry since it has more products)
  const byUpc = new Map();
  const noUpc = [];

  for (const p of maNorm) {
    if (p.upc) {
      byUpc.set(p.upc, { ...p, states: ['MA'] });
    } else {
      noUpc.push({ ...p, states: ['MA'] });
    }
  }

  let riNew = 0;
  let riDup = 0;
  for (const p of riNorm) {
    if (p.upc && byUpc.has(p.upc)) {
      // Already have this wine from MA — note it's in both states
      byUpc.get(p.upc).states.push('RI');
      riDup++;
    } else if (p.upc) {
      byUpc.set(p.upc, { ...p, states: ['RI'] });
      riNew++;
    } else {
      noUpc.push({ ...p, states: ['RI'] });
    }
  }

  const allWines = [...byUpc.values(), ...noUpc];

  // Filter out non-wine styles
  const nonWineStyles = new Set(['Fruit', 'Sangria', 'Wine Cooler', 'Specialty']);
  const wines = allWines.filter(w => !nonWineStyles.has(w.style));
  const filtered = allWines.length - wines.length;

  // Stats
  const stats = {
    total: wines.length,
    ma_total: maNorm.length,
    ri_total: riNorm.length,
    ri_unique: riNew,
    ri_duplicate: riDup,
    no_upc: noUpc.length,
    filtered_non_wine: filtered,
    has_upc: wines.filter(w => w.upc).length,
    has_grapes: wines.filter(w => w.grapes.length > 0).length,
    has_country: wines.filter(w => w.country).length,
    has_region: wines.filter(w => w.region).length,
    styles: {},
    top_countries: {},
  };

  wines.forEach(w => {
    stats.styles[w.style || 'unknown'] = (stats.styles[w.style || 'unknown'] || 0) + 1;
    if (w.country) {
      stats.top_countries[w.country] = (stats.top_countries[w.country] || 0) + 1;
    }
  });

  // Sort countries by count
  stats.top_countries = Object.fromEntries(
    Object.entries(stats.top_countries).sort((a, b) => b[1] - a[1]).slice(0, 15)
  );

  console.log('\n=== RESULTS ===');
  console.log(`MA wines: ${stats.ma_total}`);
  console.log(`RI wines: ${stats.ri_total}`);
  console.log(`RI unique (not in MA): ${stats.ri_unique}`);
  console.log(`RI duplicates: ${stats.ri_duplicate}`);
  console.log(`Filtered non-wine: ${stats.filtered_non_wine}`);
  console.log(`Total unique wines: ${stats.total}`);
  console.log(`Has UPC: ${stats.has_upc}/${stats.total} (${(stats.has_upc/stats.total*100).toFixed(1)}%)`);
  console.log(`Has grapes: ${stats.has_grapes}/${stats.total} (${(stats.has_grapes/stats.total*100).toFixed(1)}%)`);
  console.log(`Has country: ${stats.has_country}/${stats.total} (${(stats.has_country/stats.total*100).toFixed(1)}%)`);
  console.log(`Has region: ${stats.has_region}/${stats.total} (${(stats.has_region/stats.total*100).toFixed(1)}%)`);
  console.log(`\nStyles:`, stats.styles);
  console.log(`\nTop countries:`, stats.top_countries);

  // Write output
  const output = {
    metadata: {
      source: 'Horizon Beverage (Southern Glazer\'s)',
      url: 'https://www.horizonbeverage.com/our-products',
      fetched_at: new Date().toISOString(),
      states: ['MA', 'RI'],
      stats
    },
    wines
  };

  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));
  console.log(`\nSaved to ${OUTPUT_PATH}`);
  console.log(`File size: ${(fs.statSync(OUTPUT_PATH).size / 1024 / 1024).toFixed(1)} MB`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
