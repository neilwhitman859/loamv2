#!/usr/bin/env node
/**
 * fetch_european_cellars.mjs — Fetch European Cellars (Eric Solomon) catalog
 *
 * WordPress site with 718 wine pages. Clean dt/dd structure for technical data.
 * Robots.txt requests 10-second crawl delay.
 *
 * Data fields per wine:
 *   - Wine name (h1), Producer (h3)
 *   - Technical: Appellation, Variety, Age of Vines, Farming, Soil, Altitude,
 *     Fermentation, Aging, Vineyard size
 *   - Location: Appellation, Proprietor, Winemaker, Size/Elevation
 *   - Scores: vintage-level ratings with publication codes (WA, JS, JD, etc.)
 *   - Wine type from CSS class (red, white, rose, sparkling)
 *   - Certifications from CSS class (organic, biodynamic, vegan)
 *
 * Usage:
 *   node scripts/fetch_european_cellars.mjs                 # Full fetch
 *   node scripts/fetch_european_cellars.mjs --limit 50      # Test with 50
 *   node scripts/fetch_european_cellars.mjs --resume         # Resume
 *
 * Output: data/imports/european_cellars_catalog.json
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import https from 'https';

const OUTPUT_FILE = 'data/imports/european_cellars_catalog.json';
const PROGRESS_FILE = 'data/imports/european_cellars_progress.json';
const DELAY_MS = 10000; // 10 seconds — respecting robots.txt crawl-delay
const BASE_URL = 'https://www.europeancellars.com';

const args = process.argv.slice(2);
const LIMIT = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : Infinity;
const RESUME = args.includes('--resume');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' },
      timeout: 20000,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${BASE_URL}${res.headers.location}`;
        res.resume();
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
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/&#8217;/g, "'")
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

function cleanText(html) {
  return decodeEntities(html.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim());
}

// ── Parse wine data from HTML ───────────────────────────────
function parseWinePage(html, url) {
  const wine = { url, _source: 'european_cellars' };

  // Wine name
  const titleMatch = html.match(/<h1[^>]*>([^<]+)<\/h1>/);
  if (titleMatch) wine.name = cleanText(titleMatch[1]);

  // Producer (in h3.producer-header, may contain an anchor)
  const producerMatch = html.match(/<h3 class="producer-header"[^>]*>(?:<a[^>]*>)?([^<]+)(?:<\/a>)?<\/h3>/);
  if (producerMatch) wine.producer = cleanText(producerMatch[1]);

  // Wine type from body class
  const typeMatch = html.match(/class="[^"]*wine-(red|white|rose|sparkling|dessert|cider)[^"]*"/);
  if (typeMatch) wine.color = typeMatch[1];

  // Certifications from body class
  const certs = [];
  if (html.includes('wine-certified-organic')) certs.push('certified_organic');
  if (html.includes('wine-biodynamic')) certs.push('biodynamic');
  if (html.includes('wine-vegan')) certs.push('vegan');
  if (certs.length > 0) wine.certifications = certs;

  // Technical information (dt/dd pairs)
  const techMatch = html.match(/<dl class="technical-information">([\s\S]*?)<\/dl>/);
  if (techMatch) {
    const dtdd = techMatch[1].matchAll(/<dt>([^<]+)<\/dt>\s*\n?\s*<dd>([\s\S]*?)<\/dd>/g);
    for (const m of dtdd) {
      const label = m[1].trim().toLowerCase();
      const value = cleanText(m[2]);
      if (!value) continue;

      switch (label) {
        case 'appellation': wine.appellation = value; break;
        case 'variety': wine.grape = value; break;
        case 'age of vines': wine.vine_age = value; break;
        case 'farming': wine.farming = value; break;
        case 'soil': wine.soil = value; break;
        case 'altitude': wine.altitude = value; break;
        case 'fermentation': wine.vinification = value; break;
        case 'aging': wine.aging = value; break;
        case 'vineyard size': wine.vineyard_size = value; break;
        case 'winemaker': wine.winemaker = value; break;
        case 'proprietor': wine.proprietor = value; break;
        default:
          if (!wine.extra_fields) wine.extra_fields = {};
          wine.extra_fields[label] = value;
      }
    }
  }

  // Location information
  const locMatch = html.match(/<dl class="location-information">([\s\S]*?)<\/dl>/);
  if (locMatch) {
    const entries = locMatch[1].matchAll(/<h6>([^<]+)<\/h6>\s*\n?\s*<dd>([\s\S]*?)<\/dd>/g);
    for (const m of entries) {
      const label = m[1].trim().toLowerCase();
      const value = cleanText(m[2]);
      if (!value) continue;

      switch (label) {
        case 'location': wine.location = value; break;
        case 'appellation': if (!wine.appellation) wine.appellation = value; break;
        case 'proprietor': wine.proprietor = value; break;
        case 'winemaker': wine.winemaker = value; break;
        case 'size / elevation': wine.size_elevation = value; break;
      }
    }
  }

  // Scores (h2 = score number, h6 = vintage + wine name)
  const ratingsSection = html.match(/Ratings &amp; Reviews([\s\S]*?)(?:Other Wines|Downloads|<footer)/);
  if (ratingsSection) {
    const scores = [];
    // Match score + vintage pairs
    const scorePairs = ratingsSection[1].matchAll(/<h2[^>]*>(\d{2,3})\+?<\/h2>\s*[\s\S]*?<h6[^>]*>([^<]+)<\/h6>/g);
    for (const sp of scorePairs) {
      const score = parseInt(sp[1]);
      const label = cleanText(sp[2]); // e.g., "2021 Jumilla"
      const vintageMatch = label.match(/^(\d{4})\s+(.+)/);

      const entry = { score };
      if (vintageMatch) {
        entry.vintage = vintageMatch[1];
        entry.wine_name = vintageMatch[2];
      } else {
        entry.label = label;
      }

      // Look for publication code in nearby text (WA, JS, JD, etc.)
      scores.push(entry);
    }
    if (scores.length > 0) wine.scores = scores;
  }

  // URL slug
  wine.url_slug = url.split('/wine/')[1]?.replace(/\/$/, '') || '';

  return wine;
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  // Step 1: Get wine URLs from sitemap
  console.log('Step 1: Fetching wine URLs from sitemap...');
  let allUrls = [];

  if (RESUME && existsSync(PROGRESS_FILE)) {
    const progress = JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
    allUrls = progress.allUrls;
    console.log(`  Resumed with ${allUrls.length} URLs from progress file`);
  } else {
    const smRes = await httpsGet(`${BASE_URL}/wp-sitemap-posts-wine-1.xml`);
    allUrls = (smRes.body.match(/<loc>([^<]+)<\/loc>/g) || [])
      .map(u => u.replace(/<\/?loc>/g, ''));
    console.log(`  ${allUrls.length} wine URLs from sitemap`);
  }

  // Step 2: Scrape each page
  const limit = Math.min(LIMIT, allUrls.length);
  console.log(`\nStep 2: Scraping ${limit} pages (delay: ${DELAY_MS}ms — respecting robots.txt)...`);

  let wines = [];
  let startIdx = 0;
  if (RESUME && existsSync(OUTPUT_FILE)) {
    wines = JSON.parse(readFileSync(OUTPUT_FILE, 'utf8'));
    startIdx = wines.length;
    console.log(`  Resuming from index ${startIdx}`);
  }

  let wineCount = 0;
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
      wines.push(wine);
      wineCount++;

      if ((i + 1) % 25 === 0) {
        console.log(`  [${i + 1}/${limit}] wines: ${wineCount}, errors: ${errorCount}`);
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
  console.log(`   ${errorCount} errors`);

  // Print sample
  if (wines.length > 0) {
    console.log('\nSample wine:');
    console.log(JSON.stringify(wines[0], null, 2));
  }

  // Stats
  const stats = {
    total: wines.length,
    withProducer: wines.filter(w => w.producer).length,
    withGrape: wines.filter(w => w.grape).length,
    withAppellation: wines.filter(w => w.appellation).length,
    withSoil: wines.filter(w => w.soil).length,
    withAltitude: wines.filter(w => w.altitude).length,
    withVineAge: wines.filter(w => w.vine_age).length,
    withFarming: wines.filter(w => w.farming).length,
    withVinification: wines.filter(w => w.vinification).length,
    withAging: wines.filter(w => w.aging).length,
    withScores: wines.filter(w => w.scores && w.scores.length > 0).length,
    withCerts: wines.filter(w => w.certifications?.length > 0).length,
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
