#!/usr/bin/env node
/**
 * fetch_shopify_catalog.mjs — Fetches all products from a Shopify store's JSON API
 *
 * Usage:
 *   node scripts/fetch_shopify_catalog.mjs <store-url> <output-file>
 *
 * Example:
 *   node scripts/fetch_shopify_catalog.mjs https://www.mysa.wine data/imports/mysa_wine_raw.json
 */

const storeUrl = process.argv[2];
const outputFile = process.argv[3];

if (!storeUrl || !outputFile) {
  console.error('Usage: node scripts/fetch_shopify_catalog.mjs <store-url> <output-file>');
  process.exit(1);
}

import { writeFileSync } from 'fs';

const baseUrl = storeUrl.replace(/\/$/, '');
const allProducts = [];
let page = 1;

console.log(`Fetching products from ${baseUrl}/products.json...`);

while (true) {
  const url = `${baseUrl}/products.json?limit=250&page=${page}`;
  console.log(`  Page ${page}...`);

  const resp = await fetch(url);
  if (!resp.ok) {
    console.error(`HTTP ${resp.status} on page ${page}`);
    break;
  }

  const data = await resp.json();
  if (!data.products || data.products.length === 0) break;

  for (const p of data.products) {
    allProducts.push({
      shopify_id: p.id,
      title: p.title,
      vendor: p.vendor,
      product_type: p.product_type,
      tags: typeof p.tags === 'string' ? p.tags.split(',').map(t => t.trim()) : (p.tags || []),
      body_html: p.body_html || '',
      price: p.variants?.[0]?.price ? parseFloat(p.variants[0].price) : null,
      compare_at_price: p.variants?.[0]?.compare_at_price ? parseFloat(p.variants[0].compare_at_price) : null,
      sku: p.variants?.[0]?.sku || null,
      barcode: p.variants?.[0]?.barcode || null,
      available: p.variants?.[0]?.available ?? null,
      created_at: p.created_at,
      updated_at: p.updated_at,
    });
  }

  console.log(`    Got ${data.products.length} products (total: ${allProducts.length})`);

  if (data.products.length < 250) break;
  page++;

  // Rate limiting — be polite
  await new Promise(r => setTimeout(r, 500));
}

writeFileSync(outputFile, JSON.stringify(allProducts, null, 2));
console.log(`\nSaved ${allProducts.length} products to ${outputFile}`);
