#!/usr/bin/env node
/**
 * Slow scraper for Total Wine Lexington Green store inventory.
 * Fetches server-rendered HTML pages, parses wine product cards,
 * and writes results to JSONL.
 *
 * Usage: node scrape_totalwine.mjs [startPage]
 */

import { writeFileSync, appendFileSync, existsSync, readFileSync } from 'fs';

const STORE_ID = '2102'; // Lexington Green, KY
const PAGE_SIZE = 120;
const BASE_URL = 'https://www.totalwine.com/wine/c/c0020';
const OUTPUT_FILE = 'totalwine_lexington_green.jsonl';
const PROGRESS_FILE = 'totalwine_progress.json';
const DELAY_MS = 20000; // 20 seconds between pages — go slow

// Resume support
function loadProgress() {
  if (existsSync(PROGRESS_FILE)) {
    return JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
  }
  return { lastPage: 0, totalProducts: 0 };
}

function saveProgress(page, totalProducts) {
  writeFileSync(PROGRESS_FILE, JSON.stringify({ lastPage: page, totalProducts, timestamp: new Date().toISOString() }));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Parse category path from product URL
function parseCategoriesFromUrl(url) {
  // URL format: /wine/[deals/gift-center/]<category>/<subcategory>/<type>/<product-slug>/p/<sku>
  const categories = [];
  if (!url) return categories;
  const cleaned = url.replace(/\?.*$/, '').replace(/^\/wine\//, '');
  const parts = cleaned.split('/');
  // Remove known non-category segments
  const skip = new Set(['deals', 'gift-center', 'p']);
  for (const part of parts) {
    if (skip.has(part)) continue;
    if (/^\d+$/.test(part)) continue; // SKU number
    if (part.includes('-p-')) continue;
    // Last segment before /p/ is the product slug, skip it
    const idx = parts.indexOf(part);
    if (idx === parts.length - 1 || (idx === parts.length - 3 && parts[parts.length - 2] === 'p')) continue;
    // Convert slug to readable: "champagne-sparkling-wine" -> "Champagne & Sparkling Wine"
    const readable = part.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
    categories.push(readable);
  }
  return categories;
}

// Parse wine products from HTML
function parseProducts(html, pageNum) {
  const products = [];

  // Extract article blocks - each product is in an <article> tag
  const articleRegex = /<article[^>]*>([\s\S]*?)<\/article>/gi;
  let match;

  while ((match = articleRegex.exec(html)) !== null) {
    const block = match[1];

    // Product name from h2 > a
    const nameMatch = block.match(/<h2[^>]*>[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/i);
    if (!nameMatch) continue;
    const name = nameMatch[1].replace(/<[^>]+>/g, '').trim();
    if (!name || name.length < 3) continue;

    // Product URL
    const hrefMatch = block.match(/<h2[^>]*>[\s\S]*?<a[^>]*href="([^"]*)"[^>]*>/i);
    const rawUrl = hrefMatch ? hrefMatch[1].replace(/&amp;/g, '&') : '';
    // Clean URL: just the path
    const url = rawUrl.split('?')[0];

    // SKU from data-sku attribute on add-to-cart button
    const skuMatch = block.match(/data-sku="([^"]+)"/);
    const sku = skuMatch ? skuMatch[1] : '';

    // Size - from span after product name in h2
    const sizeMatch = block.match(/(750ml|1\.5L|375ml|3L|1L|500ml|187ml|4\s*Pack)/i);
    const size = sizeMatch ? sizeMatch[0] : '';

    // Price - from the price div
    const priceMatch = block.match(/class="price[^"]*">\$([0-9]+\.?\d*)/);
    const price = priceMatch ? '$' + priceMatch[1] : '';
    // Fallback to any dollar amount
    const fallbackPrice = !price ? (block.match(/\$\d+\.\d{2}/) || [''])[0] : price;

    // Star rating - "X.X out of 5 stars"
    const starMatch = block.match(/([\d.]+)<!-- --> out of 5 stars/);
    const starRating = starMatch ? starMatch[1] : '';

    // Reviews count
    const revMatch = block.match(/([\d,]+)<!-- --> <span[^>]*>reviews?<\/span>/i);
    const reviews = revMatch ? revMatch[1].replace(/,/g, '') : '';

    // Winery Direct badge
    const wineryDirect = /directBanner/i.test(block) && block.includes('WINERY DIRECT') === false
      ? /WINERY DIRECT/i.test(block) : /WINERY DIRECT/i.test(block);

    // Categories from URL path
    const categories = parseCategoriesFromUrl(url);

    products.push({
      name, url, sku, size,
      price: price || fallbackPrice,
      starRating, reviews,
      wineryDirect: /directBanner(?!.*gridViewDirectBanner__cb5a8ac4"><\/div>)/.test(block) || /WINERY DIRECT/i.test(block),
      categories,
      page: pageNum
    });
  }

  return products;
}

// Fetch a single page
async function fetchPage(pageNum) {
  const url = `${BASE_URL}?page=${pageNum}&pageSize=${PAGE_SIZE}&aty=1,0,0,0`;

  const response = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Cookie': `STORE_ID=${STORE_ID}; shoppingMethod=INSTORE_PICKUP`
    }
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for page ${pageNum}`);
  }

  return await response.text();
}

// Main scraping loop
async function main() {
  const startPage = parseInt(process.argv[2]) || 1;
  const progress = loadProgress();
  const resumePage = Math.max(startPage, progress.lastPage + 1);

  console.log(`Starting from page ${resumePage} (${PAGE_SIZE} items/page)`);
  console.log(`Output: ${OUTPUT_FILE}`);
  console.log(`Delay: ${DELAY_MS}ms between pages\n`);

  // If starting fresh, clear the output file
  if (resumePage <= 1) {
    writeFileSync(OUTPUT_FILE, '');
  }

  let totalProducts = progress.totalProducts || 0;
  let totalPages = 43; // Will be updated from first page
  let consecutiveEmpty = 0;
  let retries = 0;

  for (let page = resumePage; page <= totalPages; page++) {
    try {
      console.log(`[${new Date().toLocaleTimeString()}] Fetching page ${page}/${totalPages}...`);

      const html = await fetchPage(page);

      // Try to get actual total from the HTML
      const totalMatch = html.match(/of\s+([\d,]+)\s*results/);
      if (totalMatch) {
        const total = parseInt(totalMatch[1].replace(/,/g, ''));
        totalPages = Math.ceil(total / PAGE_SIZE);
      }

      const products = parseProducts(html, page);
      console.log(`  Found ${products.length} wines (total so far: ${totalProducts + products.length})`);

      if (products.length === 0) {
        consecutiveEmpty++;
        console.log(`  WARNING: Empty page (${consecutiveEmpty} consecutive)`);
        if (consecutiveEmpty >= 3) {
          console.log('  3 consecutive empty pages — stopping.');
          break;
        }
      } else {
        consecutiveEmpty = 0;
      }

      // Append to JSONL
      const lines = products.map(p => JSON.stringify(p)).join('\n');
      if (lines) {
        appendFileSync(OUTPUT_FILE, lines + '\n');
      }

      totalProducts += products.length;
      saveProgress(page, totalProducts);

      // Log a sample
      if (products.length > 0) {
        const sample = products[0];
        console.log(`  Sample: "${sample.name}" — ${sample.price} (score: ${sample.score || 'n/a'})`);
      }

      // Slow delay between pages
      if (page < totalPages) {
        console.log(`  Waiting ${DELAY_MS / 1000}s...`);
        await sleep(DELAY_MS);
      }

    } catch (err) {
      console.error(`  ERROR on page ${page}: ${err.message}`);
      retries = (retries || 0) + 1;
      if (retries >= 3) {
        console.log(`  3 retries failed on page ${page} — waiting 2 minutes then continuing`);
        await sleep(120000);
        retries = 0;
        // Don't decrement — skip this page after extended wait
        continue;
      }
      console.log(`  Waiting 60s before retry (attempt ${retries}/3)...`);
      await sleep(60000);
      page--; // Retry same page
    }
  }

  console.log(`\nDone! ${totalProducts} total wines saved to ${OUTPUT_FILE}`);
}

main().catch(console.error);
