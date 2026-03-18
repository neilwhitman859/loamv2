#!/usr/bin/env node
/**
 * scrape_winedeals.mjs — Scrape winedeals.com wine catalog
 *
 * Two-pass approach:
 *   Pass 1: Paginate listing pages, collect all product URLs
 *   Pass 2: Visit each product page, extract structured attributes
 *
 * Saves progress to data/imports/winedeals_urls.json and winedeals_catalog.json
 * Resume-safe: skips already-scraped URLs on restart.
 *
 * Usage:
 *   node scripts/scrape_winedeals.mjs              # run both passes
 *   node scripts/scrape_winedeals.mjs --urls-only   # pass 1 only
 *   node scripts/scrape_winedeals.mjs --scrape-only  # pass 2 only (requires urls file)
 */
import puppeteer from 'puppeteer';
import { readFileSync, writeFileSync, existsSync } from 'fs';

const URLS_FILE = 'data/imports/winedeals_urls.json';
const CATALOG_FILE = 'data/imports/winedeals_catalog.json';
const BASE = 'https://www.winedeals.com';
const LISTING_URL = `${BASE}/wine.html?product_list_limit=15&p=`;
const DELAY_MS = 1500; // polite delay between requests

const args = process.argv.slice(2);
const urlsOnly = args.includes('--urls-only');
const scrapeOnly = args.includes('--scrape-only');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Pass 1: Collect product URLs ──
async function collectUrls(browser) {
  console.log('\n=== Pass 1: Collecting product URLs ===');
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36');

  const allUrls = new Set();
  let pageNum = 1;
  let totalPages = null;

  while (true) {
    const url = LISTING_URL + pageNum;
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    } catch (err) {
      console.error(`  Page ${pageNum} load error:`, err.message);
      break;
    }

    // Get total pages on first load
    if (totalPages === null) {
      totalPages = await page.evaluate(() => {
        const lastPage = [...document.querySelectorAll('.pages-items .item a')]
          .map(a => parseInt(a.textContent.trim().replace('Page', '').trim()))
          .filter(n => !isNaN(n))
          .sort((a, b) => b - a)[0];
        return lastPage || 1;
      });
      console.log(`  Total pages: ${totalPages}`);
    }

    // Extract product URLs
    const urls = await page.evaluate(() => {
      return [...document.querySelectorAll('a.product-item-link')]
        .map(a => a.href)
        .filter(h => h.includes('.html') && h.includes('/wine/'));
    });

    urls.forEach(u => allUrls.add(u));

    if (pageNum % 20 === 0 || pageNum === totalPages) {
      console.log(`  Page ${pageNum}/${totalPages} — ${allUrls.size} URLs collected`);
    }

    if (pageNum >= totalPages) break;
    pageNum++;
    await sleep(DELAY_MS);
  }

  await page.close();

  const urlList = [...allUrls];
  writeFileSync(URLS_FILE, JSON.stringify(urlList, null, 2));
  console.log(`  Saved ${urlList.length} URLs to ${URLS_FILE}`);
  return urlList;
}

// ── Pass 2: Scrape each product page ──
async function scrapeProducts(browser, urls) {
  console.log('\n=== Pass 2: Scraping product pages ===');

  // Load existing progress
  let catalog = [];
  const scraped = new Set();
  if (existsSync(CATALOG_FILE)) {
    catalog = JSON.parse(readFileSync(CATALOG_FILE, 'utf-8'));
    catalog.forEach(w => scraped.add(w.url));
    console.log(`  Resuming: ${catalog.length} already scraped`);
  }

  const remaining = urls.filter(u => !scraped.has(u));
  console.log(`  ${remaining.length} URLs to scrape`);

  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36');

  let count = 0;
  let errors = 0;

  for (const url of remaining) {
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

      const data = await page.evaluate(() => {
        // Product name and price
        const name = document.querySelector('.page-title span')?.textContent?.trim() || null;
        const price = document.querySelector('.price')?.textContent?.trim() || null;

        // Structured attributes from "More Information" table
        const attributes = {};
        const rows = document.querySelectorAll('.additional-attributes-wrapper table tbody tr');
        rows.forEach(row => {
          const label = row.querySelector('th')?.textContent?.trim();
          const value = row.querySelector('td')?.textContent?.trim();
          if (label && value) attributes[label] = value;
        });

        // Description
        const desc = document.querySelector('.product.attribute.description .value')?.textContent?.trim() || null;

        return { name, price, description: desc, attributes };
      });

      catalog.push({
        url,
        name: data.name,
        price: data.price,
        description: data.description,
        sku: data.attributes['SKU'] || null,
        upc: data.attributes['UPC'] || null,
        country: data.attributes['Country'] || null,
        region: data.attributes['Region'] || null,
        district: data.attributes['District'] || null,
        appellation: data.attributes['Appellation'] || null,
        abv: data.attributes['Proof/Alcohol by Volume'] || null,
        vintage: data.attributes['Vintage'] || null,
        grapes: data.attributes['Grape(s)'] || null,
        primary_grape: data.attributes['Primary Grape'] || null,
        brand: data.attributes['Wine/Spirit Brand'] || null,
        wine_type: data.attributes['Wine Type'] || null,
        color: data.attributes['Wine - Color'] || null,
        package_size: data.attributes['Package Size'] || null,
        product_type: data.attributes['Product Type'] || null,
        can_ship: data.attributes['Can it Be Shipped'] || null,
        awards: data.attributes['Awards'] || null,
        all_attributes: data.attributes,
      });

      count++;
    } catch (err) {
      console.error(`  Error scraping ${url}:`, err.message);
      errors++;
    }

    // Save progress every 100 wines
    if (count % 100 === 0) {
      writeFileSync(CATALOG_FILE, JSON.stringify(catalog, null, 2));
      console.log(`  ${count + catalog.length - remaining.length + errors}/${urls.length} — ${count} scraped, ${errors} errors`);
    }

    await sleep(DELAY_MS);
  }

  await page.close();

  // Final save
  writeFileSync(CATALOG_FILE, JSON.stringify(catalog, null, 2));
  console.log(`\n  Complete: ${catalog.length} total wines saved`);
  console.log(`  Errors: ${errors}`);

  // Stats
  const hasUpc = catalog.filter(w => w.upc).length;
  const hasGrapes = catalog.filter(w => w.grapes).length;
  const hasAbv = catalog.filter(w => w.abv).length;
  const hasVintage = catalog.filter(w => w.vintage).length;
  const hasCountry = catalog.filter(w => w.country).length;
  console.log(`\n  UPC: ${hasUpc}/${catalog.length} (${(hasUpc/catalog.length*100).toFixed(1)}%)`);
  console.log(`  Grapes: ${hasGrapes}/${catalog.length} (${(hasGrapes/catalog.length*100).toFixed(1)}%)`);
  console.log(`  ABV: ${hasAbv}/${catalog.length} (${(hasAbv/catalog.length*100).toFixed(1)}%)`);
  console.log(`  Vintage: ${hasVintage}/${catalog.length} (${(hasVintage/catalog.length*100).toFixed(1)}%)`);
  console.log(`  Country: ${hasCountry}/${catalog.length} (${(hasCountry/catalog.length*100).toFixed(1)}%)`);
}

// ── Main ──
async function main() {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  try {
    let urls;

    if (scrapeOnly) {
      if (!existsSync(URLS_FILE)) {
        console.error('No URLs file found. Run without --scrape-only first.');
        process.exit(1);
      }
      urls = JSON.parse(readFileSync(URLS_FILE, 'utf-8'));
      console.log(`Loaded ${urls.length} URLs from ${URLS_FILE}`);
    } else {
      urls = await collectUrls(browser);
      if (urlsOnly) {
        console.log('URLs collected. Run with --scrape-only to scrape products.');
        await browser.close();
        return;
      }
    }

    await scrapeProducts(browser, urls);
  } finally {
    await browser.close();
  }
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
