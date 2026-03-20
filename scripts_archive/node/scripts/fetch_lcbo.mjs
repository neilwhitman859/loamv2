#!/usr/bin/env node
/**
 * fetch_lcbo.mjs — Fetch LCBO wine catalog via GraphQL API
 *
 * Free API, no auth needed. Rate limit: 60 req/min.
 * Each product has UPC barcode, producer, country, region, ABV, price.
 *
 * Usage:
 *   node scripts/fetch_lcbo.mjs
 */
import { writeFileSync } from 'fs';

const API = 'https://api.lcbo.dev/graphql';
const CATEGORIES = ['red-wine', 'white-wine', 'rose-wine', 'sparkling-wine', 'champagne', 'fortified-wine', 'dessert-wine'];
const PAGE_SIZE = 50;

const QUERY = `
query FetchWines($category: String!, $after: String) {
  products(
    filters: { categorySlug: $category }
    pagination: { first: ${PAGE_SIZE}, after: $after }
  ) {
    totalCount
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        sku
        name
        upcNumber
        producerName
        countryOfManufacture
        regionName
        alcoholPercent
        priceInCents
        primaryCategory
        shortDescription
        unitVolumeMl
        sellingPackage
        isVqa
        isKosher
        isSeasonal
        updatedAt
      }
    }
  }
}`;

async function gql(query, variables) {
  const res = await fetch(API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
  const json = await res.json();
  if (json.errors) throw new Error(json.errors.map(e => e.message).join('; '));
  return json.data;
}

// Rate limit: 60/min = 1/sec
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  const allWines = [];

  for (const cat of CATEGORIES) {
    let after = null;
    let pageNum = 0;
    let totalForCat = 0;

    console.log(`\nFetching ${cat}...`);

    while (true) {
      const data = await gql(QUERY, { category: cat, after });
      const products = data.products;

      if (pageNum === 0) {
        totalForCat = products.totalCount;
        console.log(`  ${totalForCat} products`);
      }

      for (const edge of products.edges) {
        const n = edge.node;
        allWines.push({
          sku: n.sku,
          name: n.name,
          upc: n.upcNumber,
          producer: n.producerName,
          country: n.countryOfManufacture,
          region: n.regionName,
          abv: n.alcoholPercent,
          price_cad_cents: n.priceInCents,
          category: n.primaryCategory,
          description: n.shortDescription,
          volume_ml: n.unitVolumeMl,
          selling_package: n.sellingPackage,
          is_vqa: n.isVqa,
          is_kosher: n.isKosher,
          updated_at: n.updatedAt,
        });
      }

      pageNum++;
      if (!products.pageInfo.hasNextPage) break;
      after = products.pageInfo.endCursor;

      await sleep(1100); // Stay under 60/min
    }

    console.log(`  Fetched ${allWines.length} total so far`);
  }

  // Stats
  const hasUpc = allWines.filter(w => w.upc).length;
  const hasProducer = allWines.filter(w => w.producer).length;
  const hasAbv = allWines.filter(w => w.abv).length;
  const hasCountry = allWines.filter(w => w.country).length;
  const hasRegion = allWines.filter(w => w.region).length;

  console.log(`\n=== LCBO Fetch Complete ===`);
  console.log(`Total wines: ${allWines.length}`);
  console.log(`Has UPC: ${hasUpc} (${(hasUpc/allWines.length*100).toFixed(1)}%)`);
  console.log(`Has producer: ${hasProducer} (${(hasProducer/allWines.length*100).toFixed(1)}%)`);
  console.log(`Has ABV: ${hasAbv} (${(hasAbv/allWines.length*100).toFixed(1)}%)`);
  console.log(`Has country: ${hasCountry} (${(hasCountry/allWines.length*100).toFixed(1)}%)`);
  console.log(`Has region: ${hasRegion} (${(hasRegion/allWines.length*100).toFixed(1)}%)`);

  writeFileSync('data/imports/lcbo_catalog.json', JSON.stringify(allWines, null, 2));
  console.log(`\nSaved to data/imports/lcbo_catalog.json`);
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
