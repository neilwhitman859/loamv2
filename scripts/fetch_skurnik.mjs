#!/usr/bin/env node
/**
 * fetch_skurnik.mjs — Fetch Skurnik Wines catalog from sitemap + HTML scraping
 *
 * Skurnik uses WordPress with custom 'sku' post type.
 * SKU pages have structured label/value pairs + bullet lists.
 *
 * Strategy:
 *   1. Fetch all SKU URLs from 8 sitemaps (~7,500 URLs)
 *   2. Scrape each wine page for structured data
 *   3. Save to data/imports/skurnik_catalog.json
 *
 * Usage:
 *   node scripts/fetch_skurnik.mjs                  # Full fetch
 *   node scripts/fetch_skurnik.mjs --limit 50       # Test with 50 wines
 *   node scripts/fetch_skurnik.mjs --resume          # Resume from last position
 *
 * Output: data/imports/skurnik_catalog.json
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import https from 'https';

const OUTPUT_FILE = 'data/imports/skurnik_catalog.json';
const PROGRESS_FILE = 'data/imports/skurnik_progress.json';
const DELAY_MS = 2000; // 2 second delay between requests
const SITEMAP_COUNT = 8;

const args = process.argv.slice(2);
const LIMIT = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : Infinity;
const RESUME = args.includes('--resume');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' },
      timeout: 15000,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `https://${urlObj.hostname}${res.headers.location}`;
        return httpsGet(redirectUrl).then(resolve).catch(reject);
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body }));
    }).on('error', reject).on('timeout', () => reject(new Error('Timeout')));
  });
}

function decodeEntities(s) {
  return s
    .replace(/&#039;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&nbsp;/g, ' ')
    .replace(/&#8217;/g, "'")
    .replace(/&#8216;/g, "'")
    .replace(/&#8220;/g, '"')
    .replace(/&#8221;/g, '"')
    .replace(/&#8211;/g, '–')
    .replace(/&#8212;/g, '—')
    .replace(/&eacute;/g, 'é')
    .replace(/&egrave;/g, 'è')
    .replace(/&agrave;/g, 'à')
    .replace(/&uuml;/g, 'ü')
    .replace(/&ouml;/g, 'ö')
    .replace(/&oacute;/g, 'ó')
    .replace(/&ntilde;/g, 'ñ')
    .replace(/&ccedil;/g, 'ç')
    .replace(/&#\d+;/g, m => String.fromCharCode(parseInt(m.slice(2, -1))));
}

// ── Parse wine data from HTML ───────────────────────────────
function parseWinePage(html, url) {
  const wine = { url, _source: 'skurnik' };

  // Producer (h3 with link to /producer/)
  const producerMatch = html.match(/<h3><a href="[^"]*\/producer\/([^"\/]+)\/">([^<]+)<\/a><\/h3>/);
  if (producerMatch) {
    wine.producer_slug = producerMatch[1];
    wine.producer = decodeEntities(producerMatch[2].trim());
  }

  // Wine name (h1 with class sku_title)
  const titleMatch = html.match(/<h1 class="sku_title">([^<]+)<\/h1>/);
  if (titleMatch) {
    wine.name = decodeEntities(titleMatch[1].trim());
  }

  // Label image
  const imgMatch = html.match(/<img class="center-block img-responsive" src="([^"]+)"/);
  if (imgMatch) wine.image_url = imgMatch[1];

  // Structured details between SKU DETAILS START/END
  const detailsSection = html.match(/<!-- SKU DETAILS START -->([\s\S]*?)<!-- SKU DETAILS END -->/);
  if (detailsSection) {
    const details = detailsSection[1];

    // Extract all label/value pairs (label and desc divs may be separated by whitespace/newlines)
    const pairs = details.matchAll(/<div class="list-label[^"]*">([^<]+)<\/div>\s*\n?\s*<div class="list-desc[^"]*">(?:<a[^>]*>)?([^<]+)(?:<\/a>)?<\/div>/g);
    for (const m of pairs) {
      const label = m[1].replace(':', '').trim().toLowerCase();
      const value = decodeEntities(m[2].trim());

      switch (label) {
        case 'vintage': wine.vintage = value; break;
        case 'country': wine.country = value; break;
        case 'region': wine.region = value; break;
        case 'appellation': wine.appellation = value; break;
        case 'variety': wine.grape = value; break;
        case 'color': wine.color = value; break;
        case 'farming practice': wine.farming = value; break;
        case 'soil': wine.soil = value; break;
        default:
          if (!wine.extra_fields) wine.extra_fields = {};
          wine.extra_fields[label] = value;
      }
    }

    // SKU code from table
    const skuMatch = details.match(/<td>([A-Z]{2}-[A-Z]+-[\w-]+)<\/td>/);
    if (skuMatch) wine.sku = skuMatch[1];

    // Bottle format from table
    const formatMatch = details.match(/<td>(\d+\/\d+ml)<\/td>/);
    if (formatMatch) wine.bottle_format = formatMatch[1];
  }

  // Bullet list (winemaking notes) between POST CONTENT START/END
  const contentSection = html.match(/<!-- POST CONTENT START -->([\s\S]*?)<!-- POST CONTENT END -->/);
  if (contentSection) {
    const bullets = [];
    const listItems = contentSection[1].matchAll(/<li>([^<]+)<\/li>/g);
    for (const m of listItems) {
      bullets.push(decodeEntities(m[1].trim()));
    }
    if (bullets.length > 0) wine.notes = bullets;

    // Also grab any paragraph text
    const paras = contentSection[1].matchAll(/<p>([^<]+)<\/p>/g);
    const paraTexts = [];
    for (const m of paras) {
      const t = decodeEntities(m[1].trim());
      if (t.length > 10) paraTexts.push(t);
    }
    if (paraTexts.length > 0) wine.description = paraTexts.join('\n');
  }

  // Detect wine type from URL or data
  const urlSlug = url.split('/sku/')[1]?.replace(/\/$/, '') || '';
  wine.url_slug = urlSlug;

  // Determine if this is a wine (vs spirit/sake/vinegar/etc)
  const nonWineTerms = ['rum', 'vodka', 'gin', 'whisky', 'whiskey', 'tequila', 'mezcal',
    'brandy', 'cognac', 'armagnac', 'grappa', 'sake', 'shochu', 'vinegar', 'bitters',
    'liqueur', 'amaro', 'absinthe', 'aquavit', 'beer', 'cider', 'vermouth',
    'combo-pack', 'kombucha', 'non-alcoholic', 'soda', 'mixer', 'ice'];
  const isWine = !nonWineTerms.some(t => urlSlug.includes(t)) &&
    !!(wine.color || wine.grape || wine.appellation || wine.farming);
  wine.is_wine = isWine;

  return wine;
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  // Step 1: Collect all SKU URLs from sitemaps
  console.log('Step 1: Fetching SKU URLs from sitemaps...');
  let allUrls = [];

  if (RESUME && existsSync(PROGRESS_FILE)) {
    const progress = JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
    allUrls = progress.allUrls;
    console.log(`  Resumed with ${allUrls.length} URLs from progress file`);
  } else {
    for (let i = 1; i <= SITEMAP_COUNT; i++) {
      const suffix = i === 1 ? '' : i.toString();
      const url = `https://www.skurnik.com/sku-sitemap${suffix}.xml`;
      try {
        const res = await httpsGet(url);
        const urls = (res.body.match(/<loc>([^<]+)<\/loc>/g) || [])
          .map(u => u.replace(/<\/?loc>/g, ''));
        allUrls.push(...urls);
        console.log(`  ${url}: ${urls.length} URLs`);
      } catch (e) {
        console.error(`  Failed to fetch ${url}: ${e.message}`);
      }
      await sleep(500);
    }
    console.log(`  Total: ${allUrls.length} SKU URLs`);
  }

  // Filter out the base /sku/ page
  allUrls = allUrls.filter(u => u !== 'https://www.skurnik.com/sku/');

  // Step 2: Scrape each page
  const limit = Math.min(LIMIT, allUrls.length);
  console.log(`\nStep 2: Scraping ${limit} pages (delay: ${DELAY_MS}ms)...`);

  // Load existing results if resuming
  let wines = [];
  let startIdx = 0;
  if (RESUME && existsSync(OUTPUT_FILE)) {
    wines = JSON.parse(readFileSync(OUTPUT_FILE, 'utf8'));
    startIdx = wines.length;
    console.log(`  Resuming from index ${startIdx}`);
  }

  let wineCount = 0;
  let nonWineCount = 0;
  let errorCount = 0;

  for (let i = startIdx; i < limit; i++) {
    const url = allUrls[i];
    try {
      const res = await httpsGet(url);
      if (res.status !== 200) {
        console.log(`  [${i + 1}/${limit}] ${res.status}: ${url}`);
        errorCount++;
        continue;
      }

      const wine = parseWinePage(res.body, url);

      if (wine.is_wine) {
        wines.push(wine);
        wineCount++;
      } else {
        nonWineCount++;
      }

      if ((i + 1) % 50 === 0) {
        console.log(`  [${i + 1}/${limit}] wines: ${wineCount}, non-wine: ${nonWineCount}, errors: ${errorCount}`);
        // Save progress
        writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));
        writeFileSync(PROGRESS_FILE, JSON.stringify({ allUrls, lastIndex: i }));
      }
    } catch (e) {
      console.log(`  [${i + 1}/${limit}] Error: ${e.message} — ${url}`);
      errorCount++;
    }

    await sleep(DELAY_MS);
  }

  // Final save
  writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));
  console.log(`\n✅ Done. ${wines.length} wines saved to ${OUTPUT_FILE}`);
  console.log(`   ${nonWineCount} non-wine items skipped, ${errorCount} errors`);

  // Print sample
  if (wines.length > 0) {
    console.log('\nSample wine:');
    console.log(JSON.stringify(wines[0], null, 2));
  }

  // Stats
  const stats = {
    total: wines.length,
    withVintage: wines.filter(w => w.vintage).length,
    withGrape: wines.filter(w => w.grape).length,
    withAppellation: wines.filter(w => w.appellation).length,
    withRegion: wines.filter(w => w.region).length,
    withCountry: wines.filter(w => w.country).length,
    withFarming: wines.filter(w => w.farming).length,
    withNotes: wines.filter(w => w.notes && w.notes.length > 0).length,
    withSoil: wines.filter(w => w.soil).length,
    withSku: wines.filter(w => w.sku).length,
  };
  console.log('\nField coverage:');
  Object.entries(stats).forEach(([k, v]) => {
    const pct = stats.total > 0 ? ((v / stats.total) * 100).toFixed(1) : 0;
    console.log(`  ${k}: ${v} (${pct}%)`);
  });

  // Clean up progress file on completion
  if (limit >= allUrls.length && existsSync(PROGRESS_FILE)) {
    const { unlinkSync } = await import('fs');
    unlinkSync(PROGRESS_FILE);
  }
}

main().catch(console.error);
