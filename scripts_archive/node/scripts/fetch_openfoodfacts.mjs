#!/usr/bin/env node
/**
 * fetch_openfoodfacts.mjs — Fetch wine products from Open Food Facts
 *
 * Paginates through the OFF search API for wine categories,
 * filters for actual wines (with barcodes, alcohol content, etc.),
 * and saves structured data for staging import.
 *
 * Output: data/imports/openfoodfacts_wines.json
 *
 * Usage:
 *   node scripts/fetch_openfoodfacts.mjs
 *   node scripts/fetch_openfoodfacts.mjs --limit 500   # dev mode
 */
import { writeFileSync, existsSync, readFileSync } from 'fs';

const OUTPUT_FILE = 'data/imports/openfoodfacts_wines.json';
const PAGE_SIZE = 100; // max allowed by OFF API
const DELAY_MS = 2000; // be polite — OFF is volunteer-run
const USER_AGENT = 'LoamWineDB/1.0 (neil@loam.wine)'; // OFF asks for a user agent

const args = process.argv.slice(2);
const limitIdx = args.indexOf('--limit');
const LIMIT = limitIdx >= 0 ? parseInt(args[limitIdx + 1]) : Infinity;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Wine-related categories to search
const WINE_CATEGORIES = [
  'en:wines',
  'en:red-wines',
  'en:white-wines',
  'en:rose-wines',
  'en:sparkling-wines',
  'en:champagnes',
  'en:dessert-wines',
  'en:fortified-wines',
];

// Categories that indicate NOT a wine (false positives)
const EXCLUDE_PATTERNS = [
  'vinegar', 'vinaigre', 'juice', 'jus', 'jam', 'confiture',
  'marmelade', 'sauce', 'cooking', 'cuisine', 'non-alcoholic',
  'sans-alcool', 'alcohol-free', 'dealcoholized',
];

// Fields to request from OFF API
const FIELDS = [
  'code', 'product_name', 'product_name_en', 'product_name_fr',
  'brands', 'categories_tags', 'countries_tags', 'labels_tags',
  'quantity', 'alcohol_100g', 'alcohol_value', 'alcohol_unit',
  'origins', 'origins_tags', 'manufacturing_places',
  'nutriments', 'nutrition_grades_tags',
  'image_front_url', 'url',
  'states_tags', // completeness
].join(',');

function isActualWine(product) {
  const cats = (product.categories_tags || []).join(' ').toLowerCase();
  const name = (product.product_name || product.product_name_en || product.product_name_fr || '').toLowerCase();

  // Must have a barcode
  if (!product.code || product.code.length < 8) return false;

  // Exclude non-wine products
  for (const pat of EXCLUDE_PATTERNS) {
    if (cats.includes(pat) || name.includes(pat)) return false;
  }

  // Must have at least one wine category
  const hasWineCat = (product.categories_tags || []).some(c =>
    c.includes('wine') || c.includes('vin') || c.includes('champagne') ||
    c.includes('prosecco') || c.includes('cava') || c.includes('porto') ||
    c.includes('sherry') || c.includes('madeira')
  );
  if (!hasWineCat) return false;

  return true;
}

function extractWineData(p) {
  const name = p.product_name || p.product_name_en || p.product_name_fr || null;
  const nutriments = p.nutriments || {};

  // Extract alcohol
  let abv = null;
  if (p.alcohol_value) {
    abv = parseFloat(p.alcohol_value);
  } else if (p.alcohol_100g) {
    // alcohol_100g is in grams per 100ml; rough conversion: g / 0.789 = ml alcohol
    // then ml/100ml * 100 = %ABV. But OFF often stores it as % directly.
    abv = parseFloat(p.alcohol_100g);
  } else if (nutriments.alcohol_100g) {
    abv = parseFloat(nutriments.alcohol_100g);
  }

  // Extract country
  const countries = (p.countries_tags || [])
    .map(c => c.replace('en:', '').replace(/-/g, ' '))
    .map(c => c.charAt(0).toUpperCase() + c.slice(1));

  // Extract wine color from categories
  const cats = (p.categories_tags || []).join(',');
  let color = null;
  if (cats.includes('red-wine') || cats.includes('vins-rouges')) color = 'red';
  else if (cats.includes('white-wine') || cats.includes('vins-blancs')) color = 'white';
  else if (cats.includes('rose-wine') || cats.includes('vins-roses')) color = 'rose';

  // Extract wine type
  let wine_type = 'table';
  if (cats.includes('sparkling') || cats.includes('champagne') || cats.includes('prosecco') || cats.includes('cava') || cats.includes('cremant')) wine_type = 'sparkling';
  else if (cats.includes('dessert') || cats.includes('sweet-wine')) wine_type = 'dessert';
  else if (cats.includes('fortified') || cats.includes('porto') || cats.includes('sherry') || cats.includes('madeira')) wine_type = 'fortified';

  // Extract labels (organic, biodynamic, etc.)
  const labels = (p.labels_tags || []).map(l => l.replace('en:', '').replace(/-/g, ' '));

  return {
    barcode: p.code,
    name,
    brand: p.brands || null,
    countries,
    color,
    wine_type,
    abv: abv && abv > 0 ? abv : null,
    categories: p.categories_tags || [],
    labels,
    origins: p.origins || null,
    origins_tags: p.origins_tags || [],
    quantity: p.quantity || null,
    manufacturing_places: p.manufacturing_places || null,
    // Nutrition (useful for EU e-label data)
    energy_kcal: nutriments['energy-kcal_100g'] || null,
    sugars_g: nutriments.sugars_100g || null,
    off_url: p.url || `https://world.openfoodfacts.org/product/${p.code}`,
  };
}

async function fetchPage(category, page) {
  const url = `https://world.openfoodfacts.org/cgi/search.pl?` +
    `action=process&tagtype_0=categories&tag_contains_0=contains&tag_0=${category}` +
    `&fields=${FIELDS}&sort_by=unique_scans_n&page_size=${PAGE_SIZE}&page=${page}&json=1`;

  const resp = await fetch(url, {
    headers: { 'User-Agent': USER_AGENT },
  });

  if (!resp.ok) {
    console.error(`  HTTP ${resp.status} for ${category} page ${page}`);
    return null;
  }

  return resp.json();
}

async function main() {
  console.log('=== Open Food Facts Wine Fetcher ===\n');

  const allWines = new Map(); // barcode -> wine data (dedup)
  let totalFetched = 0;
  let totalFiltered = 0;

  for (const category of WINE_CATEGORIES) {
    console.log(`\nCategory: ${category}`);
    let page = 1;
    let hasMore = true;

    while (hasMore && allWines.size < LIMIT) {
      const data = await fetchPage(category, page);
      if (!data || !data.products || data.products.length === 0) {
        hasMore = false;
        break;
      }

      totalFetched += data.products.length;

      for (const p of data.products) {
        if (isActualWine(p)) {
          const wine = extractWineData(p);
          if (!allWines.has(wine.barcode)) {
            allWines.set(wine.barcode, wine);
          }
        } else {
          totalFiltered++;
        }
      }

      const totalCount = data.count || '?';
      if (page % 5 === 0 || data.products.length < PAGE_SIZE) {
        console.log(`  Page ${page} — ${allWines.size} wines (${totalCount} total in category)`);
      }

      if (data.products.length < PAGE_SIZE) {
        hasMore = false;
      }

      // OFF API caps at page 100 (10K results)
      if (page >= 100) {
        console.log(`  Hit page 100 limit for ${category}`);
        hasMore = false;
      }

      if (allWines.size >= LIMIT) break;

      page++;
      await sleep(DELAY_MS);
    }
  }

  // Save
  const wines = [...allWines.values()];
  writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));

  console.log(`\n=== Results ===`);
  console.log(`  Total API results: ${totalFetched}`);
  console.log(`  Filtered out: ${totalFiltered}`);
  console.log(`  Unique wines: ${wines.length}`);
  console.log(`  Saved to: ${OUTPUT_FILE}`);

  // Stats
  const stat = (label, fn) => {
    const n = wines.filter(fn).length;
    console.log(`  ${label}: ${n}/${wines.length} (${(n / wines.length * 100).toFixed(1)}%)`);
  };
  console.log('');
  stat('Has name', w => w.name);
  stat('Has brand', w => w.brand);
  stat('Has ABV', w => w.abv);
  stat('Has country', w => w.countries.length > 0);
  stat('Has color', w => w.color);
  stat('Has origins', w => w.origins);
  stat('Has labels', w => w.labels.length > 0);
  stat('Has nutrition', w => w.energy_kcal);

  // Country breakdown
  const countryCounts = {};
  wines.forEach(w => {
    (w.countries || []).forEach(c => {
      countryCounts[c] = (countryCounts[c] || 0) + 1;
    });
  });
  const topCountries = Object.entries(countryCounts).sort((a, b) => b[1] - a[1]).slice(0, 15);
  console.log('\n  Top countries:');
  topCountries.forEach(([c, n]) => console.log(`    ${c}: ${n}`));
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
