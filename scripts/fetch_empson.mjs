#!/usr/bin/env node
/**
 * fetch_empson.mjs — Fetch Empson & Co. Italian wine catalog
 *
 * WordPress site with ~279 wine pages. Excellent per-wine technical data.
 * Uses h5/p column pairs for 25+ structured fields.
 *
 * Fields: grape (100% breakdown), fermentation container/duration/temperature/yeast,
 *   aging containers/size/oak/duration, closure, vineyard location/size/soil/
 *   training/altitude/density/yield/exposure/vine age, harvest timing,
 *   production volume, tasting notes, food pairings, aging potential, ABV, winemaker
 *
 * Usage:
 *   node scripts/fetch_empson.mjs                 # Full fetch
 *   node scripts/fetch_empson.mjs --limit 50      # Test with 50
 *   node scripts/fetch_empson.mjs --resume         # Resume
 *
 * Output: data/imports/empson_catalog.json
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import https from 'https';

const OUTPUT_FILE = 'data/imports/empson_catalog.json';
const PROGRESS_FILE = 'data/imports/empson_progress.json';
const DELAY_MS = 3000; // 3 seconds between requests
const BASE_URL = 'https://www.empson.com';

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
  const wine = { url, _source: 'empson' };

  // Wine name (h1)
  const titleMatch = html.match(/<h1[^>]*>([^<]+)<\/h1>/);
  if (titleMatch) wine.name = cleanText(titleMatch[1]);

  // Producer (h2 in "You may also like from" section or standalone)
  // Also try the producer link on the page
  const producerLink = html.match(/<a[^>]*href="[^"]*\/wine_producer\/([^"\/]+)\/"[^>]*>([^<]+)<\/a>/);
  if (producerLink) {
    wine.producer = cleanText(producerLink[2]);
    wine.producer_slug = producerLink[1];
  } else {
    // Try h2 heading that's the producer
    const h2s = html.matchAll(/<h2[^>]*>([^<]+)<\/h2>/g);
    for (const m of h2s) {
      const text = cleanText(m[1]);
      if (text !== 'Most recent awards' && text.length > 1 && text.length < 100) {
        wine.producer = text;
        break;
      }
    }
  }

  // Extract h5 label + next sibling p value pairs
  const pairs = html.matchAll(/<h5>([^<]+)<\/h5>\s*<\/div>\s*<div[^>]*>\s*<p>([\s\S]*?)<\/p>/g);
  for (const m of pairs) {
    const label = m[1].replace(':', '').trim().toLowerCase();
    const value = cleanText(m[2]);
    if (!value) continue;

    switch (label) {
      case 'grape varieties': wine.grape = value; break;
      case 'fermentation container': wine.fermentation_container = value; break;
      case 'length of alcoholic fermentation': wine.fermentation_duration = value; break;
      case 'type of yeast': wine.yeast_type = value; break;
      case 'fermentation temperature': wine.fermentation_temp = value; break;
      case 'maceration technique': wine.maceration_technique = value; break;
      case 'length of maceration': wine.maceration_duration = value; break;
      case 'aging containers': wine.aging_container = value; break;
      case 'container size': wine.aging_container_size = value; break;
      case 'type of oak': wine.oak_type = value; break;
      case 'aging before bottling': wine.aging_duration = value; break;
      case 'closure': wine.closure = value; break;
      case 'vineyard location': wine.vineyard_location = value; break;
      case 'vineyard size': wine.vineyard_size = value; break;
      case 'soil composition': wine.soil = value; break;
      case 'vine training': wine.training_method = value; break;
      case 'altitude': wine.altitude = value; break;
      case 'vine density': wine.vine_density = value; break;
      case 'yield': wine.yield = value; break;
      case 'exposure': wine.exposure = value; break;
      case 'age of vines': wine.vine_age = value; break;
      case 'time of harvest': wine.harvest_time = value; break;
      case 'total yearly production (in bottles)': wine.production = value; break;
      case 'tasting notes': wine.tasting_notes = value; break;
      case 'food pairings': wine.food_pairings = value; break;
      case 'aging potential': wine.aging_potential = value; break;
      case 'alcohol': wine.abv = value; break;
      case 'winemaker': wine.winemaker = value; break;
      case 'malolactic fermentation': wine.malolactic = value; break;
      case 'bottling period': wine.bottling_period = value; break;
      case 'serving temperature': wine.serving_temp = value; break;
      case 'first vintage of this wine': wine.first_vintage = value; break;
      default:
        if (!wine.extra_fields) wine.extra_fields = {};
        wine.extra_fields[label] = value;
    }
  }

  // Description (main content paragraph)
  const descMatch = html.match(/<div class="col-md-8">\s*\n?\s*\n?\s*<div><p>([\s\S]*?)<\/p>/);
  if (descMatch) {
    const text = cleanText(descMatch[1]);
    if (text.length > 20) wine.description = text;
  }

  // Scores/awards section
  // Format: "2021 | Wine Enthusiast 94" — strip SVGs first
  const awardsSection = html.match(/Most recent awards([\s\S]*?)(?:You may also like|<footer)/);
  if (awardsSection) {
    const scores = [];
    // Strip SVGs and other noise
    const cleanAwards = awardsSection[1].replace(/<svg[\s\S]*?<\/svg>/g, '').replace(/<style[\s\S]*?<\/style>/g, '');
    // Pattern: "YEAR | Publication SCORE"
    const scoreEntries = cleanAwards.matchAll(/(\d{4})\s*\|\s*([A-Za-z][A-Za-z\s.]+?)\s+(\d{2,3})\b/g);
    for (const m of scoreEntries) {
      scores.push({
        vintage: m[1],
        publication: m[2].trim(),
        score: parseInt(m[3]),
      });
    }
    if (scores.length > 0) wine.scores = scores;
  }

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
    const smRes = await httpsGet(`${BASE_URL}/wine-sitemap.xml`);
    // Handle CDATA in loc tags
    allUrls = (smRes.body.match(/<loc>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/loc>/g) || [])
      .map(u => u.replace(/<\/?loc>/g, '').replace(/<!\[CDATA\[/g, '').replace(/\]\]>/g, ''));
    console.log(`  ${allUrls.length} wine URLs from sitemap`);
  }

  // Step 2: Scrape each page
  const limit = Math.min(LIMIT, allUrls.length);
  console.log(`\nStep 2: Scraping ${limit} pages (delay: ${DELAY_MS}ms)...`);

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
    withSoil: wines.filter(w => w.soil).length,
    withAltitude: wines.filter(w => w.altitude).length,
    withVineAge: wines.filter(w => w.vine_age).length,
    withAbv: wines.filter(w => w.abv).length,
    withProduction: wines.filter(w => w.production).length,
    withTastingNotes: wines.filter(w => w.tasting_notes).length,
    withFoodPairings: wines.filter(w => w.food_pairings).length,
    withWinemaker: wines.filter(w => w.winemaker).length,
    withFermentContainer: wines.filter(w => w.fermentation_container).length,
    withOakType: wines.filter(w => w.oak_type).length,
    withScores: wines.filter(w => w.scores && w.scores.length > 0).length,
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
