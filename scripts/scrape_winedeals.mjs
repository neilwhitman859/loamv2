#!/usr/bin/env node
/**
 * scrape_winedeals.mjs — Scrape winedeals.com wine catalog
 *
 * Two-pass approach:
 *   Pass 1: Paginate listing pages, collect all product URLs
 *   Pass 2: Visit each product page, extract structured attributes
 *
 * Captures: UPC, grapes (primary + all), ABV, scores with review text + critic + date,
 * food pairings, occasion pairings, compare-at price, producer, and all More Information fields.
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
const LISTING_URL = `${BASE}/wine.html?product_list_limit=44&p=`;
const DELAY_MS = 1500; // polite delay between requests

const args = process.argv.slice(2);
const urlsOnly = args.includes('--urls-only');
const scrapeOnly = args.includes('--scrape-only');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Pass 1: Collect product URLs ──
async function collectUrls(browser) {
  console.log('\n=== Pass 1: Collecting product URLs ===');
  const page = await browser.newPage();
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
  });
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36');

  const allUrls = new Set();
  let pageNum = 1;
  let totalPages = null;
  let consecutiveEmpty = 0;

  while (true) {
    const url = LISTING_URL + pageNum;
    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
      // Wait for product cards to render (Hyvä/Alpine.js theme)
      await page.waitForSelector('a.product-item-link', { timeout: 10000 }).catch(() => {});
    } catch (err) {
      console.error(`  Page ${pageNum} load error:`, err.message);
      consecutiveEmpty++;
      if (consecutiveEmpty >= 3) break;
      pageNum++;
      await sleep(DELAY_MS);
      continue;
    }

    // Get total pages on first load
    if (totalPages === null) {
      totalPages = await page.evaluate(() => {
        // Look for the last page number in pagination
        const pageLinks = document.querySelectorAll('ul.items.pages-items li a, .pages-items .item a');
        let max = 1;
        pageLinks.forEach(a => {
          const text = a.textContent.trim().replace('Page', '').trim();
          const num = parseInt(text);
          if (!isNaN(num) && num > max) max = num;
        });
        return max;
      });
      console.log(`  Total pages: ${totalPages}`);
    }

    // Extract product URLs — wine products only (exclude spirits, category pages)
    const urls = await page.evaluate(() => {
      return [...document.querySelectorAll('a.product-item-link')]
        .map(a => a.href)
        .filter(h => h.includes('.html') && (h.includes('/wine/') || h.includes('/spirits/')));
    });

    if (urls.length === 0) {
      consecutiveEmpty++;
      if (consecutiveEmpty >= 3) {
        console.log(`  3 consecutive empty pages at page ${pageNum}, stopping.`);
        break;
      }
    } else {
      consecutiveEmpty = 0;
    }

    urls.forEach(u => allUrls.add(u));

    if (pageNum % 10 === 0 || pageNum === totalPages) {
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
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
  });
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36');

  let count = 0;
  let errors = 0;

  for (const url of remaining) {
    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
      // Wait for the page title to render
      await page.waitForSelector('.page-title', { timeout: 10000 }).catch(() => {});

      const data = await page.evaluate(() => {
        const result = {};

        // ── Product name ──
        result.name = document.querySelector('.page-title span, .page-title')?.textContent?.trim() || null;

        // ── Prices ──
        const oldPrice = document.querySelector('.old-price .price');
        const finalPrice = document.querySelector('.final-price .price, .price-wrapper .price, .special-price .price');
        // Fallback: grab any .price element
        const anyPrice = document.querySelector('.price');
        result.compare_at_price = oldPrice?.textContent?.trim() || null;
        result.price = finalPrice?.textContent?.trim() || anyPrice?.textContent?.trim() || null;

        // ── Item number ──
        const itemText = document.body.innerText.match(/Item#:\s*(\d+)/);
        result.item_number = itemText ? itemText[1] : null;

        // ── Score badges near the top ──
        // Format: "WE90" or "SP90" or "RP90" etc in badge elements
        const badgeSection = document.querySelector('.product-info-main');
        if (badgeSection) {
          const badgeText = badgeSection.textContent;
          const badgeMatch = badgeText.match(/(WE|SP|RP|JS|WS|VI|JD)\s*(\d{2,3})\s*(?:pts?\.|points?)?/i);
          if (badgeMatch) {
            result.badge_publication = badgeMatch[1].toUpperCase();
            result.badge_score = parseInt(badgeMatch[2]);
          }
        }

        // ── Summary area: All Grapes, Food Pairings, Producer ──
        const infoMain = document.querySelector('.product-info-main');
        if (infoMain) {
          const text = infoMain.textContent;

          // All Grapes
          const allGrapesMatch = text.match(/All Grapes?:\s*([^\n]+)/i);
          result.all_grapes = allGrapesMatch ? allGrapesMatch[1].trim() : null;

          // Food pairings
          const foodMatch = text.match(/Food Pairings?:\s*([^\n]+)/i);
          result.food_pairings = foodMatch ? foodMatch[1].trim() : null;

          // Producer
          const producerLink = infoMain.querySelector('a[href*="brand"]');
          if (!producerLink) {
            const moreFrom = text.match(/More from this Producer:\s*(.+)/i);
            result.producer = moreFrom ? moreFrom[1].trim() : null;
          } else {
            result.producer = producerLink.textContent.trim();
          }
        }

        // ── Critical Acclaim section: scores with review text ──
        result.reviews = [];
        // Look for the Critical Acclaim heading and its sibling content
        const allElements = document.querySelectorAll('h2, h3, .critical-acclaim, [class*="critical"]');
        let criticalSection = null;
        allElements.forEach(el => {
          if (el.textContent.trim().toLowerCase().includes('critical acclaim')) {
            criticalSection = el.closest('section') || el.parentElement?.parentElement || el.parentElement;
          }
        });

        if (criticalSection) {
          // Each review is typically in a card/block
          // Pattern: "WE90 90 pts. / Wine Enthusiast (5/1/2024)" followed by review text
          const reviewBlocks = criticalSection.querySelectorAll('[class*="review"], [class*="rating"], [class*="acclaim"]');

          // If no specific review blocks, parse the whole section text
          const sectionText = criticalSection.textContent;

          // Match pattern: CODE SCORE pts. / PUBLICATION (DATE) ...review text... (Critic Name)
          const reviewRegex = /([A-Z]{2})\s*(\d{2,3})\s+(\d{2,3})\s+pts?\.\s*\/?\s*([^(]+?)\s*\((\d{1,2}\/\d{1,2}\/\d{4})\)\s*([\s\S]*?)(?=(?:[A-Z]{2}\s*\d{2,3}\s+\d{2,3}\s+pts?)|Average rating|$)/gi;
          let match;
          while ((match = reviewRegex.exec(sectionText)) !== null) {
            const reviewText = match[6].trim();
            // Extract critic name from end of review (in parentheses)
            const criticMatch = reviewText.match(/\(([A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\)\s*$/);
            result.reviews.push({
              publication_code: match[1].toUpperCase(),
              score: parseInt(match[3]),
              publication_name: match[4].trim(),
              review_date: match[5],
              review_text: criticMatch ? reviewText.replace(criticMatch[0], '').trim() : reviewText,
              critic: criticMatch ? criticMatch[1] : null,
            });
          }

          // Fallback: simpler pattern if the fancy regex didn't match
          if (result.reviews.length === 0) {
            const simpleRegex = /(\d{2,3})\s+pts?\.\s*\/?\s*([^(]+?)\s*\((\d{1,2}\/\d{1,2}\/\d{4})\)\s*([\s\S]*?)(?=\d{2,3}\s+pts?\.|Average rating|$)/gi;
            while ((match = simpleRegex.exec(sectionText)) !== null) {
              const reviewText = match[4].trim();
              const criticMatch = reviewText.match(/\(([A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\)\s*$/);
              result.reviews.push({
                publication_code: null,
                score: parseInt(match[1]),
                publication_name: match[2].trim(),
                review_date: match[3],
                review_text: criticMatch ? reviewText.replace(criticMatch[0], '').trim() : reviewText,
                critic: criticMatch ? criticMatch[1] : null,
              });
            }
          }
        }

        // ── Average rating ──
        const avgMatch = document.body.textContent.match(/Average rating of ([\d.]+) out of 100 based on (\d+) rating/);
        if (avgMatch) {
          result.avg_rating = parseFloat(avgMatch[1]);
          result.num_ratings = parseInt(avgMatch[2]);
        }

        // ── Description ──
        result.description = document.querySelector('.product.attribute.description .value')?.textContent?.trim() || null;

        // ── Structured attributes from "More Information" table ──
        const attributes = {};
        // Hyvä theme uses different table structures
        const tables = document.querySelectorAll('table');
        tables.forEach(table => {
          const rows = table.querySelectorAll('tr');
          rows.forEach(row => {
            const th = row.querySelector('th');
            const td = row.querySelector('td');
            if (th && td) {
              const label = th.textContent.trim();
              const value = td.textContent.trim();
              if (label && value) attributes[label] = value;
            }
          });
        });
        result.attributes = attributes;

        return result;
      });

      const attrs = data.attributes || {};

      catalog.push({
        url,
        name: data.name,
        item_number: data.item_number,
        price: data.price,
        compare_at_price: data.compare_at_price,
        description: data.description,
        producer: data.producer || attrs['Wine/Spirit Brand'] || null,
        sku: attrs['SKU'] || null,
        upc: attrs['UPC'] || null,
        country: attrs['Country'] || null,
        region: attrs['Region'] || null,
        district: attrs['District'] || null,
        appellation: attrs['Appellation'] || null,
        abv: attrs['Proof/Alcohol by Volume'] || null,
        vintage: attrs['Vintage'] || null,
        grapes: attrs['Grape(s)'] || null,
        primary_grape: attrs['Primary Grape'] || null,
        all_grapes: data.all_grapes || attrs['Grape(s)'] || null,
        brand: attrs['Wine/Spirit Brand'] || null,
        wine_type: attrs['Wine Type'] || null,
        color: attrs['Wine - Color'] || null,
        package_size: attrs['Package Size'] || null,
        product_type: attrs['Product Type'] || null,
        bottles_per_case: attrs['Bottles per Case'] || null,
        alternate_name: attrs['Alternate Name'] || null,
        can_ship: attrs['Can it Be Shipped'] || null,
        awards: attrs['Awards'] || null,
        food_pairing: data.food_pairings || attrs['Food Pairing'] || null,
        occasion_pairing: attrs['Occasion Pairing'] || null,
        spec_designation: attrs['Spec. Designation'] || null,
        dollar_sale: attrs['Dollar Sale (Y/N)'] || null,
        // Scores
        reviews: data.reviews || [],
        avg_rating: data.avg_rating || null,
        num_ratings: data.num_ratings || null,
        // All raw attributes for anything we missed
        all_attributes: attrs,
      });

      count++;
    } catch (err) {
      console.error(`  Error scraping ${url}:`, err.message);
      errors++;
    }

    // Save progress every 50 wines
    if (count % 50 === 0 && count > 0) {
      writeFileSync(CATALOG_FILE, JSON.stringify(catalog, null, 2));
      const total = catalog.length;
      const withScores = catalog.filter(w => w.reviews && w.reviews.length > 0).length;
      console.log(`  ${total}/${urls.length} — ${count} new, ${errors} errors, ${withScores} with scores`);
    }

    await sleep(DELAY_MS);
  }

  await page.close();

  // Final save
  writeFileSync(CATALOG_FILE, JSON.stringify(catalog, null, 2));
  console.log(`\n  Complete: ${catalog.length} total wines saved`);
  console.log(`  Errors: ${errors}`);

  // Stats
  const total = catalog.length;
  if (total === 0) return;
  const stat = (label, fn) => {
    const n = catalog.filter(fn).length;
    console.log(`  ${label}: ${n}/${total} (${(n/total*100).toFixed(1)}%)`);
  };
  console.log('');
  stat('UPC', w => w.upc);
  stat('Grapes', w => w.grapes || w.all_grapes);
  stat('ABV', w => w.abv);
  stat('Vintage', w => w.vintage);
  stat('Country', w => w.country);
  stat('Appellation', w => w.appellation);
  stat('Producer', w => w.producer);
  stat('Scores', w => w.reviews && w.reviews.length > 0);
  stat('Food pairing', w => w.food_pairing);
  stat('Compare-at price', w => w.compare_at_price);
  stat('Description', w => w.description);
}

// ── Main ──
async function main() {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled'],
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
