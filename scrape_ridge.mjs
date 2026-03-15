#!/usr/bin/env node
/**
 * scrape_ridge.mjs
 *
 * Scrapes Ridge Vineyards (ridgewine.com) wine catalog and detail pages.
 * Outputs structured JSONL for review before DB insertion.
 *
 * Phase 1: Crawl catalog pages → collect all wine URLs
 * Phase 2: Fetch each detail page → parse structured data
 * Phase 3: Write to JSONL
 *
 * Usage:
 *   node scrape_ridge.mjs                    # Full scrape
 *   node scrape_ridge.mjs --resume           # Resume from checkpoint
 *   node scrape_ridge.mjs --detail-only      # Skip catalog, use existing URLs
 *   node scrape_ridge.mjs --insert           # Insert JSONL data into DB
 */

import { readFileSync, writeFileSync, appendFileSync, existsSync } from 'fs';
import { createClient } from '@supabase/supabase-js';
import { randomUUID } from 'crypto';

// ── Load .env ───────────────────────────────────────────────
const envPath = new URL('.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
for (const line of envContent.split('\n')) {
  const trimmed = line.replace(/\r/g, '').trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx === -1) continue;
  const key = trimmed.slice(0, eqIdx);
  const val = trimmed.slice(eqIdx + 1);
  if (!process.env[key]) process.env[key] = val;
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── Config ──────────────────────────────────────────────────
const BASE_URL = 'https://www.ridgewine.com';
const CATALOG_URL = `${BASE_URL}/wines/`;
const OUTPUT_FILE = 'ridge_wines.jsonl';
const URLS_FILE = 'ridge_urls.json';
const PROGRESS_FILE = 'ridge_progress.json';
const DELAY_MS = 4000; // 4 seconds between detail pages
const CATALOG_DELAY_MS = 2000; // 2 seconds between catalog pages

const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36';

// ── CLI args ────────────────────────────────────────────────
const args = process.argv.slice(2);
const RESUME = args.includes('--resume');
const DETAIL_ONLY = args.includes('--detail-only');
const INSERT_MODE = args.includes('--insert');

// ── Helpers ─────────────────────────────────────────────────
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalize(s) {
  return s
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function slugify(s) {
  return s
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function loadProgress() {
  if (existsSync(PROGRESS_FILE)) {
    return JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
  }
  return { lastDetailIndex: -1, catalogDone: false };
}

function saveProgress(data) {
  writeFileSync(PROGRESS_FILE, JSON.stringify({ ...data, timestamp: new Date().toISOString() }));
}

async function fetchPage(url, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await fetch(url, {
        headers: {
          'User-Agent': USER_AGENT,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
        },
        redirect: 'follow',
      });
      if (!res.ok) {
        console.warn(`  HTTP ${res.status} for ${url} (attempt ${attempt + 1})`);
        if (res.status === 429) {
          await sleep(30000);
          continue;
        }
        if (res.status === 404) return null;
        await sleep(10000);
        continue;
      }
      return await res.text();
    } catch (err) {
      console.warn(`  Fetch error for ${url}: ${err.message} (attempt ${attempt + 1})`);
      await sleep(10000);
    }
  }
  console.error(`  Failed after ${retries} attempts: ${url}`);
  return null;
}

// ── Phase 1: Catalog Discovery ──────────────────────────────
async function discoverWineUrls() {
  console.log('Phase 1: Discovering wine URLs from catalog...');
  const allUrls = [];
  let pageNo = 1;

  while (true) {
    const url = pageNo === 1 ? CATALOG_URL : `${CATALOG_URL}?pageNo=${pageNo}`;
    process.stdout.write(`  Page ${pageNo}...`);
    const html = await fetchPage(url);

    if (!html) {
      console.log(' empty response, stopping.');
      break;
    }

    // Extract wine URLs from data attributes: <div ... data="https://www.ridgewine.com/wines/2025-alder-springs-falanghina/">
    const urlRegex = /class="wineItem[^"]*"[^>]*data="([^"]+)"/g;
    const pageUrls = [];
    let match;
    while ((match = urlRegex.exec(html)) !== null) {
      pageUrls.push(match[1]);
    }

    // Also try: data="..." ... class="wineItem" (attribute order may vary)
    const urlRegex2 = /data="(https:\/\/www\.ridgewine\.com\/wines\/[^"]+)"[^>]*class="wineItem/g;
    while ((match = urlRegex2.exec(html)) !== null) {
      if (!pageUrls.includes(match[1])) pageUrls.push(match[1]);
    }

    if (pageUrls.length === 0) {
      console.log(' no wines found, stopping.');
      break;
    }

    console.log(` ${pageUrls.length} wines`);
    allUrls.push(...pageUrls);

    // Check for next page
    if (!html.includes('Next Page')) {
      console.log('  No "Next Page" link — last page reached.');
      break;
    }

    pageNo++;
    await sleep(CATALOG_DELAY_MS);
  }

  console.log(`\nDiscovered ${allUrls.length} wine URLs across ${pageNo} pages`);
  writeFileSync(URLS_FILE, JSON.stringify(allUrls, null, 2));
  return allUrls;
}

// ── Phase 2: Detail Page Parsing ────────────────────────────

function parseGrapeComposition(text) {
  // Parse "64% Cabernet Sauvignon, 31% Merlot, 5% Petit Verdot"
  const grapes = [];
  const regex = /(\d+)%\s+([^,]+)/g;
  let m;
  while ((m = regex.exec(text)) !== null) {
    grapes.push({
      percentage: parseInt(m[1]),
      grape: m[2].trim(),
    });
  }
  return grapes;
}

function parseScores(html) {
  // HTML uses &#8211; (en dash entity) or – for the dash between score and critic
  // Normalize to simple dash first
  const normalized = html.replace(/&#8211;/g, '–').replace(/&ndash;/g, '–');

  // Parse: <b>100 Points</b> – Jim Gordon, <em>JamesSuckling.com</em>
  // Also: <b>96 Points</b> – <em>Robert Parker Wine Advocate</em>
  // Also: <b>97 Points + Cellar Selection</b> – Matt Kettmann, <em>Wine Enthusiast</em>
  const scores = [];
  const scoreRegex = /<b>(\d+)\s*Points?\s*(?:\+\s*([^<]+))?<\/b>\s*–\s*(?:([^,<]+),\s*)?<em>\s*([^<]+)<\/em>/gi;
  let m;
  while ((m = scoreRegex.exec(normalized)) !== null) {
    scores.push({
      score: parseInt(m[1]),
      designation: m[2] ? m[2].trim() : null,
      critic: m[3] ? m[3].trim() : null,
      publication: m[4].trim(),
    });
  }

  // Also handle: <b>100 Points</b> – Wilfred Wong (no publication em tag)
  const scoreRegex2 = /<b>(\d+)\s*Points?\s*(?:\+\s*([^<]+))?<\/b>\s*–\s*([^<\n]+?)(?:<br|<\/p|$)/gi;
  while ((m = scoreRegex2.exec(normalized)) !== null) {
    const critic = m[3].trim().replace(/,\s*$/, '');
    // Skip if already captured (has <em> tag)
    if (scores.some(s => s.score === parseInt(m[1]) && (s.critic === critic || s.publication === critic))) continue;
    scores.push({
      score: parseInt(m[1]),
      designation: m[2] ? m[2].trim() : null,
      critic: critic,
      publication: null,
    });
  }

  return scores;
}

function parseWinemakerNotes(text) {
  // Remove author initials at end like "TG (4/23)"
  return text.replace(/\s*[A-Z]{1,3}\s*\(\d+\/\d+\)\s*$/, '').trim();
}

function parseVintageNotes(text) {
  return text.replace(/\s*[A-Z]{1,3}\s*\(\d+\/\d+\)\s*$/, '').trim();
}

function parseWinemaking(text) {
  const data = {};
  // Harvest Dates: 1 – 24 September
  const harvestMatch = text.match(/Harvest Dates?:\s*(.+?)(?:\n|$)/i);
  if (harvestMatch) data.harvestDates = harvestMatch[1].trim();

  // Grapes: Average Brix 24.4˚
  const brixMatch = text.match(/(?:Average\s+)?Brix[:\s]*(\d+\.?\d*)/i);
  if (brixMatch) data.brix = parseFloat(brixMatch[1]);

  // TA: 6.90 g/L
  const taMatch = text.match(/TA:\s*(\d+\.?\d*)\s*g\/L/i);
  if (taMatch) data.ta = parseFloat(taMatch[1]);

  // pH: 3.46
  const phMatch = text.match(/pH:\s*(\d+\.?\d*)/i);
  if (phMatch) data.ph = parseFloat(phMatch[1]);

  // Barrels: 100% new oak barrels: 92% American oak, 8% French oak
  const barrelMatch = text.match(/Barrels?:\s*(.+?)(?:\n|$)/i);
  if (barrelMatch) data.barrels = barrelMatch[1].trim();

  // Aging: Nineteen months in barrel
  const agingMatch = text.match(/Aging:\s*(.+?)(?:\n|$)/i);
  if (agingMatch) data.aging = agingMatch[1].trim();

  // New oak percentage
  const newOakMatch = text.match(/(\d+)%\s*new\s*oak/i);
  if (newOakMatch) data.newOakPct = parseInt(newOakMatch[1]);

  // Fermentation info
  const fermMatch = text.match(/Fermentation:\s*(.+?)(?:\n|$)/i);
  if (fermMatch) data.fermentation = fermMatch[1].trim();

  // Selection
  const selMatch = text.match(/Selection:\s*(.+?)(?:\n|$)/i);
  if (selMatch) data.selection = selMatch[1].trim();

  // Cases produced
  const casesMatch = text.match(/(\d[\d,]*)\s*cases?\s*(?:produced|made|bottled)/i);
  if (casesMatch) data.casesProduced = parseInt(casesMatch[1].replace(/,/g, ''));

  // Full text for metadata
  data.fullText = text.trim();

  return data;
}

function parseGrowingSeason(text) {
  const data = {};
  const rainfallMatch = text.match(/Rainfall:\s*(.+?)(?:\n|$)/i);
  if (rainfallMatch) data.rainfall = rainfallMatch[1].trim();

  const bloomMatch = text.match(/Bloom:\s*(.+?)(?:\n|$)/i);
  if (bloomMatch) data.bloom = bloomMatch[1].trim();

  const weatherMatch = text.match(/Weather:\s*(.+)/is);
  if (weatherMatch) data.weather = weatherMatch[1].trim();

  data.fullText = text.trim();
  return data;
}

function parsePrice(html) {
  // Look for price in cart widget: Item Price$56.00
  const priceMatch = html.match(/Item\s*Price\s*\$(\d+\.?\d*)/i);
  if (priceMatch) return parseFloat(priceMatch[1]);

  // Also try: $XX.XX / 750 ml
  const priceMatch2 = html.match(/\$(\d+\.?\d*)\s*\/?\s*750\s*ml/i);
  if (priceMatch2) return parseFloat(priceMatch2[1]);

  // Generic price
  const priceMatch3 = html.match(/class="[^"]*price[^"]*"[^>]*>\s*\$(\d+\.?\d*)/i);
  if (priceMatch3) return parseFloat(priceMatch3[1]);

  return null;
}

function parseDetailPage(html, url) {
  const data = {
    url,
    title: null,
    vintage: null,
    wineName: null,
    grapes: [],
    scores: [],
    vineyard: null,
    appellation: null,
    abv: null,
    price: null,
    winemakerNotes: null,
    vintageNotes: null,
    history: null,
    growingSeason: null,
    winemaking: null,
    membersOnly: false,
  };

  // Title: <h1 class="membersOverlay-title">2021 Monte Bello</h1>
  // OR:    <h1>2023 Geyserville</h1> (no class for non-members wines)
  // Fallback to <title> tag
  let titleMatch = html.match(/<h1[^>]*class="membersOverlay-title"[^>]*>([^<]+)<\/h1>/i);
  if (!titleMatch) {
    // Try any h1 inside the pageContent area
    titleMatch = html.match(/<h1[^>]*>(\d{4}\s+[^<]+)<\/h1>/i);
  }
  if (!titleMatch) {
    // Fallback to page title: "2023 Geyserville - Ridge Vineyards"
    const pageTitleMatch = html.match(/<title>([^<]+)\s*-\s*Ridge Vineyards<\/title>/i);
    if (pageTitleMatch) titleMatch = [null, pageTitleMatch[1].trim()];
  }
  if (titleMatch) {
    data.title = titleMatch[1].trim();
    // Parse vintage year and wine name from title
    const yearMatch = data.title.match(/^(\d{4})\s+(.+)$/);
    if (yearMatch) {
      data.vintage = parseInt(yearMatch[1]);
      data.wineName = yearMatch[2].trim();
    } else {
      data.wineName = data.title;
    }
  }

  // Members-only badge
  if (html.includes('membersOverlay-left') && html.includes('MEMBERS')) {
    data.membersOnly = true;
  }

  // Wine Information section — grape composition
  const wineInfoMatch = html.match(/<div class="wineInfo">([\s\S]*?)<\/div>/i);
  if (wineInfoMatch) {
    const infoHtml = wineInfoMatch[1];

    // First <p> is usually grape composition
    const firstP = infoHtml.match(/<p>([^<]*?%[^<]*?)<\/p>/);
    if (firstP) {
      data.grapes = parseGrapeComposition(firstP[1]);
    }

    // Scores
    data.scores = parseScores(infoHtml);
  }

  // Labeled rows: Vintage, Vineyard, Appellation, ABV
  const rowRegex = /<div class="row">\s*<div class="wineInfo">\s*<h3[^>]*>([^<]+)<\/h3>\s*<p>([^<]*)<\/p>/gi;
  // Simpler approach: look for strong/h3 followed by text in rows
  const fieldRegex = /<h3[^>]*>\s*(Vintage|Vineyard|Appellation|Alcohol By Volume|Price|Drinking Window)\s*<\/h3>\s*(?:<p>)?\s*([^<]+)/gi;
  let m;
  while ((m = fieldRegex.exec(html)) !== null) {
    const field = m[1].trim();
    const value = m[2].trim();
    switch (field) {
      case 'Vintage': data.vintage = data.vintage || parseInt(value); break;
      case 'Vineyard': data.vineyard = value; break;
      case 'Appellation': data.appellation = value; break;
      case 'Alcohol By Volume': data.abv = parseFloat(value.replace('%', '')); break;
      case 'Price': data.price = parseFloat(value.replace(/[^0-9.]/g, '')); break;
      case 'Drinking Window': data.drinkingWindow = value; break;
    }
  }

  // Accordion sections — structure:
  // <div class="accordion" tabindex="0" data-enter-to-click>
  //   <h3>Section Title <i class="fa fa-chevron-down"></i></h3>
  //   <div class="accordion-content">...content...</div>
  // </div>
  const accordionRegex = /<div class="accordion"[^>]*>\s*<h3[^>]*>([\s\S]*?)<\/h3>\s*<div class="accordion-content"[^>]*>([\s\S]*?)<\/div>\s*<\/div>/gi;
  while ((m = accordionRegex.exec(html)) !== null) {
    const section = m[1].replace(/<[^>]+>/g, '').trim();
    const content = m[2].replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();

    switch (section) {
      case 'Winemaker Tasting Notes':
        data.winemakerNotes = parseWinemakerNotes(content);
        break;
      case 'Vintage Notes':
        data.vintageNotes = parseVintageNotes(content);
        break;
      case 'History':
        data.history = content.substring(0, 2000);
        break;
      case 'Growing Season':
        data.growingSeason = parseGrowingSeason(content);
        break;
      case 'Winemaking':
        data.winemaking = parseWinemaking(content);
        break;
      case 'Food Pairings':
        // Usually just links to recipes, but capture if there's content
        if (content && !content.includes('See all food pairing')) {
          data.foodPairings = content;
        }
        break;
    }
  }

  // Price from cart widget
  if (!data.price) {
    data.price = parsePrice(html);
  }

  return data;
}

// ── Phase 3: DB Insertion ───────────────────────────────────

// Normalize grape name: strip organic/vineyard suffixes, decode HTML entities, fix typos
function normalizeGrapeName(name) {
  let n = name
    .replace(/&#8211;/g, '–')
    .replace(/&#038;/g, '&')
    .replace(/&#8217;/g, "'")
    .replace(/\s*–\s*Organically Grown/i, '')
    .replace(/\s*–\s*Picchetti.*$/i, '')
    .replace(/\s*–\s*Jimsomare.*$/i, '')
    .replace(/\s*\(Mourv[eè]dre\)/i, '')
    .replace(/\s*\(mourvèdre\)/i, '')
    .replace(/\s*\(Primitivo\)/i, '')
    .replace(/\s+\d+%.*$/i, '') // strip trailing percentage info
    .trim();
  return n;
}

// Grape name mapping: Ridge name → DB grape name
const GRAPE_ALIASES = {
  'carignane': 'Carignan',
  'carignan': 'Carignan',
  'mataro': 'Mourvèdre',
  'mataró': 'Mourvèdre',
  'mourvèdre': 'Mourvèdre',
  'mourvedre': 'Mourvèdre',
  'mataro (mourvedre)': 'Mourvèdre',
  'mataro (mourvèdre)': 'Mourvèdre',
  'mataró (mourvèdre)': 'Mourvèdre',
  'petit verdot': 'Petit Verdot',
  'petit verdo': 'Petit Verdot',
  'petite verdot': 'Petit Verdot',
  'petire sirah': 'Petite Sirah',
  'petite sirah': 'Petite Sirah',
  'alicante bouschet': 'Alicante Bouschet',
  'alicante bouchet': 'Alicante Bouschet',
  'alicante': 'Alicante Bouschet',
  'cabernet sauvignon': 'Cabernet Sauvignon',
  'cabernet franc': 'Cabernet Franc',
  'franc': 'Cabernet Franc',
  'grenache blanc': 'Grenache Blanc',
  'grenache': 'Grenache',
  'chenin blanc': 'Chenin Blanc',
  'chardonnay': 'Chardonnay',
  'zinfandel': 'Zinfandel',
  'syrah': 'Syrah',
  'merlot': 'Merlot',
  'pinot noir': 'Pinot Noir',
  'primitivo': 'Primitivo',
  'gamay noir': 'Gamay',
  'gamay': 'Gamay',
  'cinsaut': 'Cinsaut',
  'counoise': 'Counoise',
  'falanghina': 'Falanghina',
  'valdiguié': 'Valdiguie',
  'valdiguie': 'Valdiguie',
  'teroldego': 'Teroldego',
  'viognier': 'Viognier',
  'malbec': 'Malbec',
  'barbera': 'Barbera',
  'sangiovese': 'Sangiovese',
  'roussanne': 'Roussanne',
  'picpoul': 'Picpoul',
  'vermentino': 'Vermentino',
  'semillon': 'Sémillon',
  'sémillon': 'Sémillon',
  'muscadelle': 'Muscadelle',
  'ruby cabernet': 'Ruby Cabernet',
  'charbono': 'Charbono',
  'peloursin': 'Peloursin',
  'grand noir': 'Grand Noir',
  'lenoir': 'Lenoir',
  'palomino': 'Palomino',
  'black malvoisie': 'Cinsaut', // Black Malvoisie is Cinsaut in California
  'burger': 'Burger',
};

// Varietal category classification based on primary grape
function classifyVarietal(grapes, wineName) {
  if (!grapes || grapes.length === 0) {
    // Try to infer from wine name
    const name = wineName.toLowerCase();
    if (name.includes('zinfandel')) return 'Zinfandel';
    if (name.includes('cabernet sauvignon')) return 'Cabernet Sauvignon';
    if (name.includes('cabernet franc')) return 'Cabernet Franc';
    if (name.includes('chardonnay')) return 'Chardonnay';
    if (name.includes('petite sirah')) return 'Petite Sirah';
    if (name.includes('syrah')) return 'Syrah';
    if (name.includes('pinot noir')) return 'Pinot Noir';
    if (name.includes('grenache blanc')) return 'Grenache Blanc';
    if (name.includes('grenache')) return 'Grenache';
    if (name.includes('merlot')) return 'Merlot';
    if (name.includes('primitivo')) return 'Primitivo';
    if (name.includes('gamay')) return 'Gamay';
    if (name.includes('falanghina')) return 'Falanghina';
    if (name.includes('valdiguié') || name.includes('valdiguie')) return 'Valdiguie';
    if (name.includes('chenin blanc')) return 'Chenin Blanc';
    if (name.includes('teroldego')) return 'Teroldego';
    if (name.includes('rosé') || name.includes('rose')) return 'Rosé Blend';
    if (name.includes('blanc')) return 'White Blend';
    return 'Red Blend';
  }

  const primary = grapes[0];
  const primaryName = primary.grape.toLowerCase();
  const pct = primary.percentage;

  // If single grape ≥75%, use that as varietal
  if (pct >= 75) {
    if (primaryName.includes('zinfandel')) return 'Zinfandel';
    if (primaryName.includes('cabernet sauvignon')) return 'Cabernet Sauvignon';
    if (primaryName.includes('cabernet franc')) return 'Cabernet Franc';
    if (primaryName.includes('chardonnay')) return 'Chardonnay';
    if (primaryName.includes('petite sirah')) return 'Petite Sirah';
    if (primaryName.includes('syrah')) return 'Syrah';
    if (primaryName.includes('merlot')) return 'Merlot';
    if (primaryName.includes('pinot noir')) return 'Pinot Noir';
    if (primaryName.includes('grenache blanc')) return 'Grenache Blanc';
    if (primaryName.includes('grenache')) return 'Grenache';
  }

  // Bordeaux varieties blend → Bordeaux Blend
  const bordeauxGrapes = ['cabernet sauvignon', 'merlot', 'cabernet franc', 'petit verdot', 'malbec'];
  const hasBordeaux = grapes.some(g => bordeauxGrapes.some(bg => g.grape.toLowerCase().includes(bg)));
  if (hasBordeaux && grapes.every(g => bordeauxGrapes.some(bg => g.grape.toLowerCase().includes(bg)))) {
    return 'Bordeaux Blend';
  }

  // Rhône varieties
  const rhoneGrapes = ['syrah', 'grenache', 'mourvèdre', 'mourvedre', 'mataro', 'viognier'];
  if (grapes.some(g => rhoneGrapes.some(rg => g.grape.toLowerCase().includes(rg)))) {
    if (primaryName.includes('syrah') && pct >= 50) return 'Syrah';
    if (primaryName.includes('grenache') && pct >= 50) return 'Grenache';
    return 'Rhône Blend';
  }

  // Zinfandel blends (Ridge's specialty)
  if (primaryName.includes('zinfandel') && pct >= 50) return 'Zinfandel';

  // White wines
  const whiteGrapes = ['chardonnay', 'chenin blanc', 'grenache blanc', 'falanghina', 'viognier'];
  if (whiteGrapes.some(wg => primaryName.includes(wg))) {
    if (pct >= 75) {
      if (primaryName.includes('chardonnay')) return 'Chardonnay';
      if (primaryName.includes('grenache blanc')) return 'Grenache Blanc';
    }
    return 'White Blend';
  }

  // Rosé
  if (wineName.toLowerCase().includes('rosé') || wineName.toLowerCase().includes('rose')) {
    return 'Rosé Blend';
  }

  // Default to Red Blend
  return 'Red Blend';
}

async function fetchAll(table, columns = '*', filter = {}, batchSize = 1000) {
  const all = [];
  let offset = 0;
  while (true) {
    let query = sb.from(table).select(columns).range(offset, offset + batchSize - 1);
    for (const [k, v] of Object.entries(filter)) {
      query = query.eq(k, v);
    }
    const { data, error } = await query;
    if (error) throw new Error(`fetchAll ${table}: ${error.message}`);
    all.push(...data);
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return all;
}

async function insertData() {
  console.log('\n=== Phase 3: DB Insertion ===\n');

  if (!existsSync(OUTPUT_FILE)) {
    console.error(`No ${OUTPUT_FILE} found. Run scraper first.`);
    process.exit(1);
  }

  const lines = readFileSync(OUTPUT_FILE, 'utf8').trim().split('\n');
  const wines = lines.map(l => JSON.parse(l)).filter(w => w.wineName);
  console.log(`Loaded ${wines.length} wines from ${OUTPUT_FILE}`);

  // ── Reference Data ──
  console.log('\nLoading reference data...');

  // Country: United States
  const { data: [usCountry] } = await sb.from('countries').select('id').ilike('name', '%United States%').limit(1);
  const countryId = usCountry.id;
  console.log(`  US country: ${countryId}`);

  // Region: California
  const { data: [caRegion] } = await sb.from('regions').select('id').ilike('name', '%California%').limit(1);
  const regionId = caRegion.id;
  console.log(`  California region: ${regionId}`);

  // Appellations
  const appellations = await fetchAll('appellations', 'id,name');
  const appellationMap = new Map(appellations.map(a => [a.name.toLowerCase(), a.id]));
  console.log(`  Appellations: ${appellations.length}`);

  // Grapes
  const grapes = await fetchAll('grapes', 'id,name');
  const grapeMap = new Map(grapes.map(g => [g.name.toLowerCase(), g.id]));
  console.log(`  Grapes: ${grapes.length}`);

  // Varietal categories
  const varietals = await fetchAll('varietal_categories', 'id,name,slug');
  const varietalMap = new Map(varietals.map(v => [v.name.toLowerCase(), v.id]));
  // Also map by slug
  for (const v of varietals) varietalMap.set(v.slug, v.id);
  console.log(`  Varietal categories: ${varietals.length}`);

  // Source types — create "winery_website" if missing
  let { data: sourceTypes } = await sb.from('source_types').select('id,slug');
  let winerySourceId = sourceTypes.find(s => s.slug === 'winery-website')?.id;
  if (!winerySourceId) {
    const { data: newST } = await sb.from('source_types').insert({
      slug: 'winery-website',
      name: 'Winery Website',
      description: 'Data sourced directly from the winery/producer website',
      reliability_tier: 1,
    }).select('id').single();
    winerySourceId = newST.id;
    console.log(`  Created source_type "winery-website": ${winerySourceId}`);
  } else {
    console.log(`  Source type "winery-website": ${winerySourceId}`);
  }

  // Publications — load existing, create new ones as needed
  const publications = await fetchAll('publications', 'id,name,slug');
  const pubMap = new Map(publications.map(p => [p.name.toLowerCase(), p.id]));
  // Also map by slug
  for (const p of publications) {
    pubMap.set(p.slug, p.id);
  }
  // Ridge publication name aliases → existing publication names
  const PUB_ALIASES = {
    'jamessuckling.com': 'james suckling',
    'robert parker wine advocate': 'wine advocate',
    'the wine advocate': 'wine advocate',
    'rober parker wine advocate': 'wine advocate', // typo on site
    'vinous media': 'vinous',
    'vinous': 'vinous',
    'wine spectator': 'wine spectator',
    'wine specator': 'wine spectator', // typo on site
    'winespectator': 'wine spectator',
    'wine enthusiast': 'wine enthusiast',
    'wineenthusiast.com': 'wine enthusiast',
    'decanter': 'decanter',
    'decanter.com': 'decanter',
    'decanter magazine': 'decanter',
    'owen bargreen': 'owenbargreen.com',
    'owen bargreen.com': 'owenbargreen.com',
    'owen bargeen': 'owenbargreen.com', // typo on site
  };
  // Decode HTML entities in publication names
  function decodePubName(name) {
    return name
      .replace(/&#038;/g, '&')
      .replace(/&#8217;/g, "'")
      .replace(/&#8211;/g, '–')
      .trim();
  }
  // Register aliases in pubMap
  for (const [alias, canonical] of Object.entries(PUB_ALIASES)) {
    const id = pubMap.get(canonical);
    if (id) pubMap.set(alias, id);
  }

  // ── Create Producer ──
  console.log('\nCreating Ridge Vineyards producer...');
  const producerId = randomUUID();
  const { error: prodErr } = await sb.from('producers').insert({
    id: producerId,
    slug: 'ridge-vineyards',
    name: 'Ridge Vineyards',
    name_normalized: normalize('Ridge Vineyards'),
    country_id: countryId,
    website_url: 'https://www.ridgewine.com',
    year_established: 1962,
    metadata: {
      winemaking_philosophy: 'Pre-industrial winemaking: native yeasts, natural malolactic, air-dried American oak, minimum effective sulfur',
      regions: ['Santa Cruz Mountains', 'Sonoma County', 'Paso Robles'],
    },
  });
  if (prodErr) {
    console.error('  Producer insert error:', prodErr.message);
    // Try to find existing
    const { data: existing } = await sb.from('producers').select('id').eq('slug', 'ridge-vineyards').single();
    if (existing) {
      console.log('  Using existing producer:', existing.id);
      // Continue with existing ID
    } else {
      process.exit(1);
    }
  } else {
    console.log(`  Producer created: ${producerId}`);
  }

  // Use whatever producer ID we have
  const { data: prodRow } = await sb.from('producers').select('id').eq('slug', 'ridge-vineyards').single();
  const finalProducerId = prodRow.id;

  // ── Group wines by name (not vintage) ──
  // Multiple vintages of the same wine should share a wine record
  const winesByName = new Map();
  for (const w of wines) {
    const key = w.wineName;
    if (!winesByName.has(key)) {
      winesByName.set(key, []);
    }
    winesByName.get(key).push(w);
  }
  console.log(`\n${winesByName.size} unique wine names, ${wines.length} total vintages`);

  // ── Create Wines ──
  console.log('\nCreating wine records...');
  const wineIdMap = new Map(); // wineName -> wine_id
  let wineCount = 0;

  for (const [wineName, vintages] of winesByName) {
    // Use data from the most recent vintage for wine-level fields
    const latest = vintages.sort((a, b) => (b.vintage || 0) - (a.vintage || 0))[0];

    // Resolve appellation (with aliases for name mismatches)
    const APPELLATION_ALIASES = {
      'moon mountain': 'moon mountain district',
      'santa cruz county': 'santa cruz mountains',
      'adelaida district': 'paso robles',
      'san louis obispo county': 'paso robles',
      'san luis obispo county': 'paso robles',
      'napa county': 'napa valley',
      'nap county': 'napa valley',
      'calistoga, napa valley': 'calistoga',
      'spring mountain': 'spring mountain district',
      'spring mountain, napa county': 'spring mountain district',
      'spring mountain, napa valley': 'spring mountain district',
      'howell mountain, napa county': 'howell mountain',
      'oakville, napa valley': 'oakville',
      'rutherford, napa valley': 'rutherford',
      'york creek, napa county': 'napa valley',
      'dry creek, sonoma county': 'dry creek valley',
      'dry creek valley, sonoma': 'dry creek valley',
      'dry creek, alexander, and russian river valleys': 'sonoma county',
      'the hills and bench land separating dry creek and alexander valleys': 'sonoma county',
      'sonoma': 'sonoma county',
      'foothills amador county': 'amador county',
      'amador foothills': 'amador county',
      'san francisco bay region': 'livermore valley',
    };
    const appName = latest.appellation || '';
    const appLower = appName.toLowerCase();
    const appellationId = appellationMap.get(appLower)
      || appellationMap.get(APPELLATION_ALIASES[appLower] || '')
      || null;
    if (!appellationId && appName) {
      console.warn(`    Unmapped appellation: "${appName}"`);
    }

    // Classify varietal
    const varietalName = classifyVarietal(latest.grapes, wineName);
    let varietalId = varietalMap.get(varietalName.toLowerCase());
    if (!varietalId) {
      // Try common mappings
      const fallbacks = {
        'gamay': 'red-blend', 'falanghina': 'white-blend', 'valdiguie': 'red-blend',
        'teroldego': 'red-blend', 'chenin blanc': 'white-blend', 'primitivo': 'zinfandel',
      };
      const fb = fallbacks[varietalName.toLowerCase()];
      if (fb) varietalId = varietalMap.get(fb);
      if (!varietalId) varietalId = varietalMap.get('red-blend') || varietalMap.get('red blend');
    }

    const wineId = randomUUID();
    const wineSlug = slugify(`ridge ${wineName}`);
    const { error } = await sb.from('wines').insert({
      id: wineId,
      slug: wineSlug,
      name: wineName,
      name_normalized: normalize(wineName),
      producer_id: finalProducerId,
      country_id: countryId,
      region_id: regionId,
      appellation_id: appellationId,
      varietal_category_id: varietalId,
      varietal_category_source: winerySourceId,
      food_pairings: latest.foodPairings || null,
      yeast_type: 'Native',
      metadata: {
        ridge_url_slug: latest.url ? new URL(latest.url).pathname : null,
      },
    });

    if (error) {
      console.error(`  Wine "${wineName}" error: ${error.message}`);
      continue;
    }

    wineIdMap.set(wineName, wineId);
    wineCount++;
    if (wineCount % 20 === 0) process.stdout.write(`  ${wineCount} wines...\r`);
  }
  console.log(`  Created ${wineCount} wines`);

  // ── Create Wine Grapes (per wine, from latest vintage) ──
  console.log('\nCreating grape compositions...');
  let grapeCount = 0;
  for (const [wineName, vintages] of winesByName) {
    const wineId = wineIdMap.get(wineName);
    if (!wineId) continue;

    const latest = vintages.sort((a, b) => (b.vintage || 0) - (a.vintage || 0))[0];
    if (!latest.grapes || latest.grapes.length === 0) continue;

    for (const g of latest.grapes) {
      const cleaned = normalizeGrapeName(g.grape);
      const grapeName = GRAPE_ALIASES[cleaned.toLowerCase()] || cleaned;
      const grapeId = grapeMap.get(grapeName.toLowerCase());
      if (!grapeId) {
        console.warn(`    Unknown grape: "${g.grape}" (normalized: "${grapeName}")`);
        continue;
      }

      const { error } = await sb.from('wine_grapes').insert({
        wine_id: wineId,
        grape_id: grapeId,
        percentage: g.percentage,
        percentage_source: winerySourceId,
      });
      if (error) {
        console.warn(`    Grape insert error for ${wineName}/${g.grape}: ${error.message}`);
      } else {
        grapeCount++;
      }
    }
  }
  console.log(`  Created ${grapeCount} grape entries`);

  // ── Create Wine Vintages ──
  console.log('\nCreating vintage records...');
  let vintageCount = 0;

  for (const w of wines) {
    const wineId = wineIdMap.get(w.wineName);
    if (!wineId) continue;

    // Parse winemaking data
    const wm = w.winemaking || {};
    const gs = w.growingSeason || {};

    // Build metadata from extra data
    const metadata = {};
    if (gs.rainfall) metadata.rainfall = gs.rainfall;
    if (gs.bloom) metadata.bloom = gs.bloom;
    if (gs.weather) metadata.growing_season_weather = gs.weather;
    if (wm.barrels) metadata.barrels = wm.barrels;
    if (wm.aging) metadata.aging = wm.aging;
    if (wm.fermentation) metadata.fermentation = wm.fermentation;
    if (wm.selection) metadata.selection = wm.selection;
    if (wm.fullText) metadata.winemaking_full = wm.fullText;
    if (w.history) metadata.history = w.history;
    if (w.membersOnly) metadata.members_only = true;

    // Parse oak aging months from text like "Nineteen months in barrel"
    let oakMonths = null;
    if (wm.aging) {
      const monthWords = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 'six': 6,
        'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10, 'eleven': 11, 'twelve': 12,
        'thirteen': 13, 'fourteen': 14, 'fifteen': 15, 'sixteen': 16, 'seventeen': 17,
        'eighteen': 18, 'nineteen': 19, 'twenty': 20, 'twenty-one': 21, 'twenty-two': 22,
        'twenty-three': 23, 'twenty-four': 24,
      };
      const agingLower = wm.aging.toLowerCase();
      for (const [word, num] of Object.entries(monthWords)) {
        if (agingLower.includes(word + ' month')) {
          oakMonths = num;
          break;
        }
      }
      const numMatch = wm.aging.match(/(\d+)\s*months?/i);
      if (numMatch) oakMonths = parseInt(numMatch[1]);
    }

    const { error } = await sb.from('wine_vintages').insert({
      wine_id: wineId,
      vintage_year: w.vintage,
      abv: w.abv || null,
      ph: wm.ph || null,
      ta_g_l: wm.ta || null,
      brix_at_harvest: wm.brix || null,
      duration_in_oak_months: oakMonths,
      new_oak_pct: wm.newOakPct || null,
      mlf: 'Natural',
      winemaker_notes: w.winemakerNotes || null,
      vintage_notes: w.vintageNotes || null,
      cases_produced: wm.casesProduced || null,
      release_price_usd: w.price || null,
      release_price_currency: w.price ? 'USD' : null,
      release_price_source: w.price ? winerySourceId : null,
      metadata: Object.keys(metadata).length > 0 ? metadata : {},
    });

    if (error) {
      console.error(`  Vintage ${w.wineName} ${w.vintage} error: ${error.message}`);
    } else {
      vintageCount++;
    }
    if (vintageCount % 50 === 0) process.stdout.write(`  ${vintageCount} vintages...\r`);
  }
  console.log(`  Created ${vintageCount} vintages`);

  // ── Create Wine Vintage Scores ──
  console.log('\nCreating score records...');
  let scoreCount = 0;

  for (const w of wines) {
    const wineId = wineIdMap.get(w.wineName);
    if (!wineId || !w.scores || w.scores.length === 0) continue;

    for (const s of w.scores) {
      // Resolve or create publication
      let pubId = null;
      if (s.publication) {
        const decoded = decodePubName(s.publication);
        const pubKey = decoded.toLowerCase().trim();
        pubId = pubMap.get(pubKey);
        if (!pubId) {
          // Create new publication
          const pubSlug = slugify(decoded);
          const { data: newPub, error: pubErr } = await sb.from('publications').insert({
            slug: pubSlug,
            name: decoded,
            type: 'critic_publication',
          }).select('id').single();
          if (pubErr) {
            // Might already exist
            const { data: existing } = await sb.from('publications').select('id').eq('slug', pubSlug).single();
            if (existing) {
              pubId = existing.id;
            } else {
              console.warn(`    Publication create error: ${pubErr.message}`);
            }
          } else {
            pubId = newPub.id;
            pubMap.set(pubKey, pubId);
            console.log(`    Created publication: "${s.publication}" (${pubId})`);
          }
        }
      }

      const { error } = await sb.from('wine_vintage_scores').insert({
        wine_id: wineId,
        vintage_year: w.vintage,
        score: s.score,
        score_scale: '100',
        publication_id: pubId,
        critic: s.critic || null,
        source_id: winerySourceId,
        url: w.url,
        discovered_at: new Date().toISOString(),
      });

      if (error) {
        // Likely duplicate
        if (!error.message.includes('duplicate')) {
          console.warn(`    Score error: ${error.message}`);
        }
      } else {
        scoreCount++;
      }
    }
  }
  console.log(`  Created ${scoreCount} scores`);

  // ── Summary ──
  console.log('\n========================================');
  console.log('   RIDGE VINEYARDS IMPORT COMPLETE');
  console.log('========================================');
  console.log(`  Producer: Ridge Vineyards (${finalProducerId})`);
  console.log(`  Wines: ${wineCount}`);
  console.log(`  Vintages: ${vintageCount}`);
  console.log(`  Scores: ${scoreCount}`);
  console.log(`  Grape entries: ${grapeCount}`);
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  if (INSERT_MODE) {
    await insertData();
    return;
  }

  // Phase 1: Discover URLs
  let urls;
  if (DETAIL_ONLY && existsSync(URLS_FILE)) {
    urls = JSON.parse(readFileSync(URLS_FILE, 'utf8'));
    console.log(`Loaded ${urls.length} URLs from ${URLS_FILE}`);
  } else {
    urls = await discoverWineUrls();
  }

  // Phase 2: Scrape detail pages
  const progress = RESUME ? loadProgress() : { lastDetailIndex: -1, catalogDone: true };
  const startIdx = progress.lastDetailIndex + 1;

  if (startIdx > 0) {
    console.log(`\nResuming from index ${startIdx} (${urls.length - startIdx} remaining)`);
  }

  console.log(`\nPhase 2: Scraping ${urls.length - startIdx} detail pages...`);
  let scraped = 0;
  let failed = 0;

  for (let i = startIdx; i < urls.length; i++) {
    const url = urls[i];
    process.stdout.write(`  [${i + 1}/${urls.length}] ${url.split('/wines/')[1] || url}...`);

    const html = await fetchPage(url);
    if (!html) {
      console.log(' FAILED');
      failed++;
      saveProgress({ lastDetailIndex: i, catalogDone: true });
      await sleep(DELAY_MS);
      continue;
    }

    const data = parseDetailPage(html, url);
    if (!data.wineName) {
      console.log(' NO TITLE');
      failed++;
    } else {
      appendFileSync(OUTPUT_FILE, JSON.stringify(data) + '\n');
      scraped++;
      console.log(` ${data.vintage} ${data.wineName} (${data.grapes.length} grapes, ${data.scores.length} scores)`);
    }

    saveProgress({ lastDetailIndex: i, catalogDone: true });

    if (i < urls.length - 1) {
      await sleep(DELAY_MS);
    }
  }

  console.log(`\n========================================`);
  console.log(`  RIDGE SCRAPE COMPLETE`);
  console.log(`========================================`);
  console.log(`  Total URLs: ${urls.length}`);
  console.log(`  Scraped: ${scraped}`);
  console.log(`  Failed: ${failed}`);
  console.log(`  Output: ${OUTPUT_FILE}`);
}

main().catch(console.error);
