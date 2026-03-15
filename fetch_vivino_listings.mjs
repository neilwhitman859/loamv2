#!/usr/bin/env node
/**
 * fetch_vivino_listings.mjs
 *
 * Paginates through Vivino's explore API and saves raw wine listings to JSON.
 *
 * Usage:
 *   node fetch_vivino_listings.mjs --pages 42       # ~1,008 wines
 *   node fetch_vivino_listings.mjs --pages 5        # Test with 120 wines
 *   node fetch_vivino_listings.mjs --delay-ms 2000  # Slower requests
 */

const args = process.argv.slice(2);

function getArg(name, fallback) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : fallback;
}

const MAX_PAGES = parseInt(getArg("pages", "42"), 10);
const DELAY_MS = parseInt(getArg("delay-ms", "1500"), 10);
const OUTPUT_FILE = getArg("output", "vivino_listings.json");

const BASE_URL = "https://www.vivino.com/api/explore/explore";
const PARAMS = {
  country_code: "US",
  currency_code: "USD",
  min_rating: "1",
  order_by: "ratings_count",
  order: "desc",
  price_range_min: "0",
  price_range_max: "500",
};

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36";

import { writeFileSync } from "fs";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchPage(page) {
  const qs = new URLSearchParams({ ...PARAMS, page: String(page) });
  const url = `${BASE_URL}?${qs}`;
  const res = await fetch(url, {
    headers: { "User-Agent": USER_AGENT, Accept: "application/json" },
  });

  if (!res.ok) {
    throw new Error(`Page ${page}: HTTP ${res.status} ${res.statusText}`);
  }

  const data = await res.json();
  return data.explore_vintage;
}

function extractListing(match) {
  const v = match.vintage || {};
  const wine = v.wine || {};
  const winery = wine.winery || {};
  const region = wine.region || {};
  const country = region.country || {};
  const stats = v.statistics || {};
  const price = match.price || null;

  // Normalize price to per-bottle USD
  let pricePerBottle = null;
  let merchantName = null;
  let sourceUrl = null;
  let bottleQty = 1;
  if (price) {
    bottleQty = price.bottle_quantity || 1;
    pricePerBottle =
      price.amount != null ? Math.round((price.amount / bottleQty) * 100) / 100 : null;
    merchantName = price.merchant_name || null;
    sourceUrl = price.url || null;
  }

  // Extract vintage year from seo_name (e.g., "meiomi-pinot-noir-2021" → 2021)
  let vintageYear = null;
  if (v.year && v.year > 1900) {
    vintageYear = v.year;
  } else {
    const yearMatch = (v.seo_name || "").match(/-(\d{4})$/);
    if (yearMatch) vintageYear = parseInt(yearMatch[1], 10);
  }

  return {
    vivino_wine_id: wine.id,
    vivino_vintage_id: v.id,
    winery_name: winery.name || null,
    wine_name: wine.name || null,
    vintage_year: vintageYear,
    region_name: region.name || null,
    country_name: country.name || null,
    country_code: country.code || null,
    wine_type_id: wine.type_id, // 1=red, 2=white, 3=sparkling, 4=rosé, 7=dessert
    rating_average: stats.ratings_average || null,
    rating_count: stats.ratings_count || 0,
    price_usd: pricePerBottle,
    price_raw: price ? price.amount : null,
    bottle_quantity: bottleQty,
    merchant_name: merchantName,
    source_url: sourceUrl,
    is_natural: wine.is_natural || false,
  };
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log(`Fetching up to ${MAX_PAGES} pages (${MAX_PAGES * 24} wines) from Vivino...`);
  console.log(`Delay: ${DELAY_MS}ms between requests\n`);

  const allListings = [];
  let totalAvailable = null;
  let consecutiveErrors = 0;

  for (let page = 1; page <= MAX_PAGES; page++) {
    try {
      const result = await fetchPage(page);

      if (page === 1) {
        totalAvailable = result.records_matched;
        console.log(`Total wines available on Vivino: ${totalAvailable.toLocaleString()}`);
      }

      const matches = result.matches || [];
      if (matches.length === 0) {
        console.log(`Page ${page}: No more results. Stopping.`);
        break;
      }

      const listings = matches.map(extractListing);
      allListings.push(...listings);
      consecutiveErrors = 0;

      const withPrice = listings.filter((l) => l.price_usd != null).length;
      process.stdout.write(
        `  Page ${page}/${MAX_PAGES} — ${allListings.length} wines (${withPrice}/${listings.length} with price)\r`
      );

      if (page < MAX_PAGES) await sleep(DELAY_MS);
    } catch (err) {
      console.error(`\n  ERROR page ${page}: ${err.message}`);
      consecutiveErrors++;
      if (consecutiveErrors >= 3) {
        console.error("  3 consecutive errors — stopping.");
        break;
      }
      await sleep(DELAY_MS * 2);
    }
  }

  console.log(`\n\nFetched ${allListings.length} wine listings.`);

  // Stats
  const withPrice = allListings.filter((l) => l.price_usd != null);
  const prices = withPrice.map((l) => l.price_usd).sort((a, b) => a - b);
  console.log(`  With price: ${withPrice.length} (${Math.round((withPrice.length / allListings.length) * 100)}%)`);
  if (prices.length > 0) {
    console.log(
      `  Price range: $${prices[0]} — $${prices[prices.length - 1]}`
    );
    console.log(`  Median price: $${prices[Math.floor(prices.length / 2)]}`);
  }

  const countries = {};
  for (const l of allListings) {
    countries[l.country_name || "Unknown"] = (countries[l.country_name || "Unknown"] || 0) + 1;
  }
  console.log("\n  Top countries:");
  Object.entries(countries)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .forEach(([c, n]) => console.log(`    ${c}: ${n}`));

  // Write output
  writeFileSync(OUTPUT_FILE, JSON.stringify(allListings, null, 2));
  console.log(`\nSaved to ${OUTPUT_FILE}`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
