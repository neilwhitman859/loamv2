#!/usr/bin/env node
/**
 * fetch_winebow.mjs — Fetch Winebow catalog from sitemap + HTML scraping
 *
 * Winebow is a Drupal site with ~153 brand pages, each listing wines.
 * Wine detail pages (vintage-specific) have excellent structured data:
 *   - 19 Drupal Views fields (appellation, vineyard, soil, chemistry, etc.)
 *   - Acclaim/scores section with publication names and tasting notes
 *   - Producer about section
 *
 * Strategy:
 *   1. Get all brand URLs from sitemap (~153 brands)
 *   2. For each brand page, extract individual wine URLs
 *   3. For each wine URL, follow redirect to vintage page and scrape
 *   4. Save to data/imports/winebow_catalog.json
 *
 * Usage:
 *   node scripts/fetch_winebow.mjs                  # Full fetch
 *   node scripts/fetch_winebow.mjs --limit 50       # Test with 50 wines
 *   node scripts/fetch_winebow.mjs --resume          # Resume from last position
 *
 * Output: data/imports/winebow_catalog.json
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import https from 'https';

const OUTPUT_FILE = 'data/imports/winebow_catalog.json';
const PROGRESS_FILE = 'data/imports/winebow_progress.json';
const DELAY_MS = 2000; // 2 second delay between requests
const BASE_URL = 'https://www.winebow.com';

const args = process.argv.slice(2);
const LIMIT = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : Infinity;
const RESUME = args.includes('--resume');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpsGet(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
    https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' },
      timeout: 20000,
    }, (res) => {
      // Follow redirects (301, 302, meta-refresh)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${BASE_URL}${res.headers.location}`;
        // Consume the response body to free the socket
        res.resume();
        return httpsGet(redirectUrl, maxRedirects - 1).then(resolve).catch(reject);
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        // Check for meta-refresh redirect
        const metaRefresh = body.match(/<meta http-equiv="refresh" content="0;url='([^']+)'/);
        if (metaRefresh) {
          const redirectUrl = metaRefresh[1].startsWith('http')
            ? metaRefresh[1]
            : `${BASE_URL}${metaRefresh[1]}`;
          return httpsGet(redirectUrl, maxRedirects - 1).then(resolve).catch(reject);
        }
        resolve({ status: res.statusCode, body, finalUrl: url });
      });
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

// ── Parse wine data from vintage page HTML ─────────────────
function parseWinePage(html, url, brandSlug) {
  const wine = { url, _source: 'winebow', brand_slug: brandSlug };

  // Brand name (producer)
  const brandMatch = html.match(/vintage__(?:mobile-)?brand-name[^>]*>([^<]+)/);
  if (brandMatch) wine.producer = cleanText(brandMatch[1]);

  // Product name (wine)
  const prodMatch = html.match(/vintage__(?:mobile-)?product-name[^>]*>([^<]+)/);
  if (prodMatch) wine.name = cleanText(prodMatch[1]);

  // Varietal from mobile header
  const varietalMatch = html.match(/vintage__mobile-varietal[^>]*>\s*([\s\S]*?)<\/div>/);
  if (varietalMatch) wine.varietal_display = cleanText(varietalMatch[1]);

  // Vintage year from URL or year dropdown or Views field
  const yearMatch = url.match(/\/(\d{4})\/?$/);
  if (yearMatch) wine.vintage = yearMatch[1];
  // Also try the mobile year dropdown which shows the current vintage
  if (!wine.vintage) {
    const yearDropdown = html.match(/vintage__mobile-year-label[^>]*>([^<]*\d{4}[^<]*)/);
    if (yearDropdown) {
      const y = yearDropdown[1].match(/(\d{4})/);
      if (y) wine.vintage = y[1];
    }
  }
  // Also try Vintage text in header
  if (!wine.vintage) {
    const vintText = html.match(/Vintage[:\s]*(\d{4})/i);
    if (vintText) wine.vintage = vintText[1];
  }

  // ── Drupal Views fields ──────────────────────────────
  const viewFields = html.matchAll(/<div class="views-field views-field-([a-z0-9-]+)">\s*([\s\S]*?)<\/div>/g);
  for (const m of viewFields) {
    const field = m[1];
    let value = cleanText(m[2]);
    // Strip the label prefix (e.g., "Acidity: 8.35 g/L" → "8.35 g/L")
    value = value.replace(/^[^:]+:\s*/, '');
    if (!value) continue;

    switch (field) {
      case 'field-vintage-appellation': wine.appellation = value; break;
      case 'field-vintage-vineyard-name': wine.vineyard = value; break;
      case 'field-vintage-vineyard-size': wine.vineyard_size = value; break;
      case 'field-vintage-soil-composition': wine.soil = value; break;
      case 'field-vintage-training-method': wine.training_method = value; break;
      case 'field-vintage-elevation': wine.elevation = value; break;
      case 'field-vintage-vines-acre': wine.vines_per_acre = value; break;
      case 'field-vintage-yield-acre': wine.yield_per_acre = value; break;
      case 'field-vintage-exposure': wine.exposure = value; break;
      case 'field-vintage-bottles-produced': wine.production = value; break;
      case 'field-vintage-varietal-comp': wine.grape = value; break;
      case 'field-vintage-maceration': wine.maceration = value; break;
      case 'field-vintage-malolactic-ferm': wine.malolactic = value; break;
      case 'field-vintage-size-aging': wine.aging_vessel_size = value; break;
      case 'field-vintage-oak': wine.oak_type = value; break;
      case 'field-vintage-ph-level': wine.ph = value; break;
      case 'field-vintage-acidity': wine.acidity = value; break;
      case 'field-vintage-alcohol': wine.abv = value; break;
      case 'field-vintage-residual-sugar': wine.residual_sugar = value; break;
    }
  }

  // ── Scores ───────────────────────────────────────────
  const scores = [];
  const acclaimSection = html.match(/acclaim-container([\s\S]*?)(?=esg-section|<\/main|$)/);
  if (acclaimSection) {
    const slides = acclaimSection[1].split(/vintage__acclaim-slide/).slice(1);
    for (const slide of slides) {
      const score = {};
      const scoreMatch = slide.match(/rating-score[^>]*>(\d+)/);
      if (scoreMatch) score.score = parseInt(scoreMatch[1]);

      const pubMatch = slide.match(/acclaim-publication-name[^>]*>([^<]+)/);
      if (pubMatch) score.publication = cleanText(pubMatch[1]);

      const quoteMatch = slide.match(/acclaim-quote[^>]*>([\s\S]*?)<\/div>/);
      if (quoteMatch) score.note = cleanText(quoteMatch[1]);

      if (score.score || score.publication) scores.push(score);
    }
  }
  if (scores.length > 0) wine.scores = scores;

  // ── Tasting notes / description ──────────────────────
  const descMatch = html.match(/vintage__section-content[^>]*>([\s\S]*?)<\/div>/);
  if (descMatch) {
    const text = cleanText(descMatch[1]);
    if (text.length > 20) wine.description = text;
  }

  // ── Vineyard about section ───────────────────────────
  const aboutMatch = html.match(/vintage__about-content[^>]*>([\s\S]*?)<\/div>/);
  if (aboutMatch) {
    const text = cleanText(aboutMatch[1]);
    if (text.length > 20) wine.vineyard_description = text;
  }

  return wine;
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  // Step 1: Get brand URLs from sitemap
  console.log('Step 1: Fetching brand URLs from sitemap...');

  let brandUrls = [];
  let allWineUrls = [];

  if (RESUME && existsSync(PROGRESS_FILE)) {
    const progress = JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
    allWineUrls = progress.allWineUrls;
    console.log(`  Resumed with ${allWineUrls.length} wine URLs from progress file`);
  } else {
    const smRes = await httpsGet(`${BASE_URL}/sitemap.xml`);
    const smUrls = (smRes.body.match(/<loc>([^<]+)<\/loc>/g) || [])
      .map(u => u.replace(/<\/?loc>/g, ''));

    brandUrls = smUrls
      .filter(u => u.includes('/our-brands/') && !u.endsWith('/our-brands'))
      .map(u => u.replace(/^http:/, 'https:'));
    console.log(`  ${brandUrls.length} brand pages found`);

    // Step 2: For each brand, extract wine URLs
    console.log('\nStep 2: Collecting wine URLs from brand pages...');

    // Filter out obvious non-wine brands (spirits/beer)
    const spiritTerms = ['nardini', 'poli-distillery', 'four-pillars', 'diplomatico',
      'lustau', 'topo-chico', 'fever-tree', 'regans', 'bitter-truth',
      'combier', 'tempus-fugit', 'clear-creek', 'st-george-spirits'];

    for (let i = 0; i < brandUrls.length; i++) {
      const brandUrl = brandUrls[i];
      const brandSlug = brandUrl.split('/our-brands/')[1]?.replace(/\/$/, '');

      // Skip obvious spirits brands
      if (spiritTerms.some(t => brandSlug?.includes(t))) {
        console.log(`  [${i + 1}/${brandUrls.length}] Skipping spirits: ${brandSlug}`);
        continue;
      }

      try {
        const res = await httpsGet(brandUrl);
        if (res.status !== 200) {
          console.log(`  [${i + 1}/${brandUrls.length}] ${res.status}: ${brandUrl}`);
          continue;
        }

        // Extract wine URLs from brand page
        const wineLinks = [];
        const matches = res.body.matchAll(/<a href="(\/our-brands\/[^"]+\/[^"]+)"/g);
        for (const m of matches) {
          const wineUrl = `${BASE_URL}${m[1]}`;
          if (!wineLinks.includes(wineUrl)) wineLinks.push(wineUrl);
        }

        if (wineLinks.length > 0) {
          for (const wu of wineLinks) {
            allWineUrls.push({ url: wu, brandSlug });
          }
          console.log(`  [${i + 1}/${brandUrls.length}] ${brandSlug}: ${wineLinks.length} wines`);
        } else {
          console.log(`  [${i + 1}/${brandUrls.length}] ${brandSlug}: 0 wines (spirit/other?)`);
        }
      } catch (e) {
        console.log(`  [${i + 1}/${brandUrls.length}] Error: ${e.message} — ${brandUrl}`);
      }

      await sleep(1000);
    }
    console.log(`  Total: ${allWineUrls.length} wine URLs`);

    // Save progress
    writeFileSync(PROGRESS_FILE, JSON.stringify({ allWineUrls }));
  }

  // Step 3: Scrape each wine page
  const limit = Math.min(LIMIT, allWineUrls.length);
  console.log(`\nStep 3: Scraping ${limit} wine pages (delay: ${DELAY_MS}ms)...`);

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
    const { url, brandSlug } = allWineUrls[i];
    try {
      const res = await httpsGet(url);
      if (res.status !== 200) {
        console.log(`  [${i + 1}/${limit}] ${res.status}: ${url}`);
        errorCount++;
        continue;
      }

      const wine = parseWinePage(res.body, res.finalUrl || url, brandSlug);
      wines.push(wine);
      wineCount++;

      if ((i + 1) % 25 === 0) {
        console.log(`  [${i + 1}/${limit}] wines: ${wineCount}, errors: ${errorCount}`);
        writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));
        writeFileSync(PROGRESS_FILE, JSON.stringify({ allWineUrls, lastIndex: i }));
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
    withVintage: wines.filter(w => w.vintage).length,
    withGrape: wines.filter(w => w.grape).length,
    withAppellation: wines.filter(w => w.appellation).length,
    withSoil: wines.filter(w => w.soil).length,
    withAbv: wines.filter(w => w.abv).length,
    withAcidity: wines.filter(w => w.acidity).length,
    withRS: wines.filter(w => w.residual_sugar).length,
    withPH: wines.filter(w => w.ph).length,
    withProduction: wines.filter(w => w.production).length,
    withVineyard: wines.filter(w => w.vineyard).length,
    withScores: wines.filter(w => w.scores && w.scores.length > 0).length,
    withDescription: wines.filter(w => w.description).length,
  };
  console.log('\nField coverage:');
  Object.entries(stats).forEach(([k, v]) => {
    const pct = stats.total > 0 ? ((v / stats.total) * 100).toFixed(1) : 0;
    console.log(`  ${k}: ${v} (${pct}%)`);
  });

  // Clean up progress file on completion
  if (limit >= allWineUrls.length && existsSync(PROGRESS_FILE)) {
    const { unlinkSync } = await import('fs');
    unlinkSync(PROGRESS_FILE);
  }
}

main().catch(console.error);
