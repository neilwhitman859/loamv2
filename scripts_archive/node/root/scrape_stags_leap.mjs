#!/usr/bin/env node
/**
 * scrape_stags_leap.mjs
 *
 * Scrapes Stag's Leap Wine Cellars (stagsleapwinecellars.com) wine catalog.
 * Three data sources:
 *   1. Product pages (current wines) — technical data, tasting notes
 *   2. Past-vintages pages (historical) — blend, ABV, pH, TA, aging, tasting notes
 *   3. Wine-acclaim page — critic scores across all wines/vintages
 *
 * Usage:
 *   node scrape_stags_leap.mjs                    # Full scrape
 *   node scrape_stags_leap.mjs --resume           # Resume from checkpoint
 *   node scrape_stags_leap.mjs --insert           # Insert JSONL data into DB
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
const BASE_URL = 'https://www.stagsleapwinecellars.com';
const SITEMAP_URL = `${BASE_URL}/product-sitemap.xml`;
const ACCLAIM_URL = `${BASE_URL}/wine-acclaim/`;
const PAST_VINTAGE_SLUGS = [
  'cask-23-cabernet-sauvignon',
  's-l-v-cabernet-sauvignon',
  'fay-cabernet-sauvignon',
  'artemis-cabernet-sauvignon',
];
const OUTPUT_FILE = 'stags_leap_wines.jsonl';
const SCORES_FILE = 'stags_leap_scores.jsonl';
const URLS_FILE = 'stags_leap_urls.json';
const PROGRESS_FILE = 'stags_leap_progress.json';
const DELAY_MS = 10000; // 10 seconds per robots.txt crawl-delay

const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36';

// ── CLI args ────────────────────────────────────────────────
const args = process.argv.slice(2);
const RESUME = args.includes('--resume');
const INSERT_MODE = args.includes('--insert');

// ── Helpers ─────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function normalize(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

function slugify(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

function decodeEntities(s) {
  return s.replace(/&#8217;/g, "'").replace(/&#8216;/g, "'")
    .replace(/&#8211;/g, '–').replace(/&#8212;/g, '—')
    .replace(/&#038;/g, '&').replace(/&amp;/g, '&')
    .replace(/&nbsp;/g, ' ').replace(/&#8230;/g, '…')
    .replace(/&rsquo;/g, "'").replace(/&lsquo;/g, "'")
    .replace(/&rdquo;/g, '"').replace(/&ldquo;/g, '"');
}

function stripHtml(s) {
  return decodeEntities(s.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
}

function loadProgress() {
  if (existsSync(PROGRESS_FILE)) return JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
  return { lastProductIndex: -1, productsDone: false, pastVintagesDone: false, acclaimDone: false };
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
        if (res.status === 429) { await sleep(30000); continue; }
        if (res.status === 404) return null;
        await sleep(10000); continue;
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

// ── Phase 1: Discover product URLs from sitemap ─────────────
async function discoverProductUrls() {
  console.log('Phase 1: Fetching product sitemap...');
  const xml = await fetchPage(SITEMAP_URL);
  if (!xml) { console.error('Failed to fetch sitemap'); process.exit(1); }

  const urlRegex = /<loc>([^<]+)<\/loc>/g;
  const allUrls = [];
  let m;
  while ((m = urlRegex.exec(xml)) !== null) {
    allUrls.push(m[1]);
  }
  console.log(`  Found ${allUrls.length} total sitemap URLs`);

  // Filter to wine products only
  const skipPatterns = [
    /1-5l/i, /magnum/i, /12pk/i, /boa-/i,
    /gift/i, /tasting/i, /experience/i, /event/i,
    /membership/i, /ice-pack/i, /club/i, /holiday/i,
    /shipping/i, /estate-visit/i, /virtual/i,
    /set$/i, /insert$/i, /pack$/i,
  ];

  const wineUrls = allUrls.filter(url => {
    const slug = (url.split('/product/')[1] || '').replace(/\/$/, '');
    if (!slug) return false;
    // Skip numeric-only slugs (Commerce7 internal IDs)
    if (/^\d+$/.test(slug)) return false;
    return !skipPatterns.some(p => p.test(slug));
  });

  console.log(`  Filtered to ${wineUrls.length} wine product URLs`);
  writeFileSync(URLS_FILE, JSON.stringify(wineUrls, null, 2));
  return wineUrls;
}

// ── Phase 2: Parse product detail pages ─────────────────────
function parseProductPage(html, url) {
  const data = {
    url,
    source: 'product',
    title: null,
    vintage: null,
    wineName: null,
    grapes: [],
    vineyard: null,
    appellation: null,
    abv: null,
    ph: null,
    ta: null,
    aging: null,
    tastingNotes: null,
    vintageNotes: null,
    winemakingNotes: null,
    aboutNotes: null,
  };

  // Title from <h1>
  const titleMatch = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
  if (titleMatch) {
    data.title = stripHtml(titleMatch[1]);
    const yearMatch = data.title.match(/^(\d{4})\s+(.+)$/);
    if (yearMatch) {
      data.vintage = parseInt(yearMatch[1]);
      data.wineName = yearMatch[2].trim();
    } else {
      data.wineName = data.title;
    }
  }

  // Analysis section: <div class="c7-product__analysis">
  // Contains label/value pairs: <div><div>Blend</div><div>100% Cab Sauv</div></div>
  const analysisMatch = html.match(/<div class="c7-product__analysis">([\s\S]*?)<\/div>\s*<!--analysis-->/i)
    || html.match(/<div class="c7-product__analysis">([\s\S]*?)<\/div>\s*<!--info-->/i);
  if (analysisMatch) {
    const analysisHtml = analysisMatch[1];
    // Parse all label/value div pairs
    const pairRegex = /<div>\s*<div>([^<]+)<\/div>\s*<div>([^<]+)<\/div>\s*<\/div>/gi;
    let m;
    while ((m = pairRegex.exec(analysisHtml)) !== null) {
      const label = m[1].trim();
      const value = stripHtml(m[2]);
      switch (label) {
        case 'Blend': data.grapes = parseGrapeComposition(value); break;
        case 'Aging': data.aging = value; break;
        case 'Alcohol': data.abv = parseFloat(value.replace('%', '')) || null; break;
        case 'TA': {
          const v = value.match(/([\d.]+)/);
          if (v) data.ta = parseFloat(v[1]);
          break;
        }
        case 'pH': {
          const v = value.match(/([\d.]+)/);
          if (v) data.ph = parseFloat(v[1]);
          break;
        }
        case 'Appellation': data.appellation = value; break;
        case 'Vineyard': case 'Vineyards': data.vineyard = value; break;
      }
    }
  }

  // Content sections: <h2><strong>SECTION NAME</strong></h2> followed by <p> content
  // Sections: ABOUT X, VINEYARDS & WINEMAKING, VINTAGE, TASTING NOTES
  const sectionRegex = /<h2[^>]*>\s*(?:<strong>)?\s*(ABOUT[^<]*|VINEYARDS?\s*(?:&amp;|&)\s*WINEMAKING|VINTAGE|TASTING\s+NOTES?)\s*(?:<\/strong>)?\s*<\/h2>\s*([\s\S]*?)(?=<h2[^>]*>|<div class="c7-|<footer|<section class="(?!product))/gi;
  let m;
  while ((m = sectionRegex.exec(html)) !== null) {
    const section = stripHtml(m[1]).toUpperCase();
    const content = stripHtml(m[2]).substring(0, 2000);
    if (content.length < 20) continue;

    if (section.startsWith('ABOUT')) {
      data.aboutNotes = content;
    } else if (section.includes('WINEMAKING')) {
      data.winemakingNotes = content;
    } else if (section === 'VINTAGE') {
      data.vintageNotes = content;
    } else if (section.includes('TASTING')) {
      data.tastingNotes = content;
    }
  }

  // Fallback appellation from page content
  if (!data.appellation) {
    const appMatch = html.match(/(?:Stags? Leap District|Napa Valley|Oak Knoll District|Atlas Peak|Coombsville)/i);
    if (appMatch) data.appellation = appMatch[0];
  }

  return data;
}

function parseGrapeComposition(text) {
  const grapes = [];
  // "98% Cabernet Sauvignon, 1.5% Cabernet Franc, 0.5% Petit Verdot"
  // Also: "100% Cabernet Sauvignon"
  const regex = /([\d.]+)%\s+([^,]+)/g;
  let m;
  while ((m = regex.exec(text)) !== null) {
    grapes.push({
      percentage: parseFloat(m[1]),
      grape: m[2].trim(),
    });
  }
  return grapes;
}

// ── Phase 3: Parse past-vintages pages ──────────────────────
function parsePastVintagesPage(html, wineName) {
  const vintages = [];
  const seen = new Set();

  // Page structure: pairs of <h2>YYYY</h2> and <h2>YYYY Wine Name</h2>
  // followed by content including vintage-analysis div and tasting notes.
  // Split by the YYYY-only h2 headings to get each vintage section.
  const sections = html.split(/<h2[^>]*>\s*(\d{4})\s*<\/h2>/i);

  // sections[0] is before first year, then alternating: [year, content, year, content, ...]
  for (let i = 1; i < sections.length; i += 2) {
    const vintage = parseInt(sections[i]);
    const content = sections[i + 1] || '';
    if (seen.has(vintage)) continue;
    seen.add(vintage);

    const data = {
      source: 'past_vintages',
      vintage,
      wineName,
      grapes: [],
      abv: null,
      ph: null,
      ta: null,
      aging: null,
      tastingNotes: null,
      appellation: null,
    };

    // Parse vintage-analysis div (same label/value structure as product pages)
    const analysisMatch = content.match(/<div class="vintage-analysis">([\s\S]*?)<\/div>\s*<!--analysis-->/i);
    if (analysisMatch) {
      const analysisHtml = analysisMatch[1];
      const pairRegex = /<div>\s*<div>([^<]+)<\/div>\s*<div>([^<]+)<\/div>\s*<\/div>/gi;
      let m;
      while ((m = pairRegex.exec(analysisHtml)) !== null) {
        const label = m[1].trim();
        const value = stripHtml(m[2]);
        switch (label) {
          case 'Blend': data.grapes = parseGrapeComposition(value); break;
          case 'Aging': data.aging = value; break;
          case 'Alcohol': data.abv = parseFloat(value.replace('%', '')) || null; break;
          case 'TA': {
            const v = value.match(/([\d.]+)/);
            if (v) data.ta = parseFloat(v[1]);
            break;
          }
          case 'pH': {
            const v = value.match(/([\d.]+)/);
            if (v) data.ph = parseFloat(v[1]);
            break;
          }
        }
      }
    }

    // Appellation from content
    const appMatch = content.match(/<h[23][^>]*>\s*(?:<strong>)?\s*(Napa Valley|Stags? Leap District)\s*(?:<\/strong>)?\s*<\/h[23]>/i);
    if (appMatch) data.appellation = stripHtml(appMatch[1]);

    // Tasting notes — look for substantial <p> content that isn't boilerplate
    const paragraphs = [...content.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi)];
    for (const p of paragraphs) {
      const text = stripHtml(p[1]);
      // Skip short, boilerplate, or navigation text
      if (text.length < 40) continue;
      if (text.match(/^(?:Not for Purchase|View Tech Sheet|The story of)/i)) continue;
      // This is likely a tasting note
      data.tastingNotes = text.substring(0, 2000);
      break;
    }

    // Only add if we have meaningful data
    if (data.grapes.length > 0 || data.tastingNotes || data.abv) {
      vintages.push(data);
    }
  }

  return vintages;
}

// ── Phase 4: Parse wine-acclaim page for scores ─────────────
function parseAcclaimPage(html) {
  const scores = [];

  // Scores are in items with wine name, vintage, score, publication
  // Pattern varies, but typically:
  // Wine name, vintage year, score points, publication name
  const itemRegex = /wine-acclaim__results__item[\s\S]*?<\/div>\s*<\/div>/gi;
  const items = html.match(itemRegex) || [];

  // Simpler approach: find all score entries with score + wine + vintage + publication
  // Try to parse structured data directly
  const scoreBlockRegex = /<(?:div|li|article)[^>]*class="[^"]*acclaim[^"]*"[^>]*>([\s\S]*?)<\/(?:div|li|article)>/gi;
  let m;

  // Fallback: just extract all score patterns from the full page
  // Pattern: "XX Points" or "XX/100" near wine names
  const fullText = stripHtml(html);

  // Look for patterns like: "WINE NAME VINTAGE SCORE Publication"
  const wineNames = ['CASK 23', 'S.L.V.', 'SLV', 'FAY', 'ARTEMIS', 'AVETA', 'KARIA', 'ARCADIA',
    'ARMILLARY', 'Heart of FAY', 'DANIKA', 'CELLARIUS', 'BATTUELLO', 'SODA CANYON', 'CHASE CREEK'];
  const publications = ['Wine Enthusiast', 'Wine Spectator', 'Wine Advocate', 'The Wine Advocate',
    'JamesSuckling.com', 'James Suckling', 'Jeb Dunnuck', 'JebDunnuck.com', 'Vinous',
    'Vinous Media', 'Wine & Spirits', 'Decanter', 'Robert Parker'];

  // Parse HTML more carefully — each acclaim item typically has structured content
  // Look for score number pattern + surrounding context
  const scorePattern = /(\d{2,3})\s*(?:Points?|pts?|\/100)/gi;
  while ((m = scorePattern.exec(html)) !== null) {
    const score = parseInt(m[1]);
    if (score < 80 || score > 100) continue;

    // Get surrounding context (500 chars before and after)
    const start = Math.max(0, m.index - 500);
    const end = Math.min(html.length, m.index + 500);
    const context = stripHtml(html.substring(start, end));

    // Try to find wine name in context
    let wine = null;
    for (const wn of wineNames) {
      if (context.includes(wn)) {
        wine = wn;
        break;
      }
    }

    // Try to find vintage year
    const vintageMatch = context.match(/\b(19[7-9]\d|20[0-2]\d)\b/);
    const vintage = vintageMatch ? parseInt(vintageMatch[1]) : null;

    // Try to find publication
    let pub = null;
    for (const p of publications) {
      if (context.toLowerCase().includes(p.toLowerCase())) {
        pub = p;
        break;
      }
    }

    if (wine && vintage && pub) {
      // Check for duplicates
      const isDup = scores.some(s => s.wine === wine && s.vintage === vintage && s.score === score && s.publication === pub);
      if (!isDup) {
        scores.push({ wine, vintage, score, publication: pub });
      }
    }
  }

  return scores;
}

// ── Main Scrape Flow ────────────────────────────────────────
async function scrape() {
  // Phase 1: Discover URLs
  let urls;
  if (existsSync(URLS_FILE)) {
    urls = JSON.parse(readFileSync(URLS_FILE, 'utf8'));
    console.log(`Loaded ${urls.length} URLs from ${URLS_FILE}`);
  } else {
    urls = await discoverProductUrls();
  }

  const progress = RESUME ? loadProgress() : { lastProductIndex: -1, productsDone: false, pastVintagesDone: false, acclaimDone: false };

  // Phase 2: Scrape product pages
  if (!progress.productsDone) {
    const startIdx = progress.lastProductIndex + 1;
    console.log(`\nPhase 2: Scraping ${urls.length - startIdx} product pages (10s delay per robots.txt)...`);

    for (let i = startIdx; i < urls.length; i++) {
      const url = urls[i];
      const slug = url.split('/product/')[1]?.replace(/\/$/, '') || url;
      process.stdout.write(`  [${i + 1}/${urls.length}] ${slug}...`);

      const html = await fetchPage(url);
      if (!html) {
        console.log(' FAILED');
        saveProgress({ ...progress, lastProductIndex: i });
        await sleep(DELAY_MS);
        continue;
      }

      const data = parseProductPage(html, url);
      if (!data.wineName) {
        console.log(' NO TITLE');
      } else {
        appendFileSync(OUTPUT_FILE, JSON.stringify(data) + '\n');
        console.log(` ${data.vintage || 'NV'} ${data.wineName} (${data.grapes.length} grapes, ABV:${data.abv || '-'}, pH:${data.ph || '-'})`);
      }

      saveProgress({ ...progress, lastProductIndex: i });
      if (i < urls.length - 1) await sleep(DELAY_MS);
    }

    progress.productsDone = true;
    saveProgress(progress);
    console.log('  Product pages done.');
  }

  // Phase 3: Scrape past-vintages pages
  if (!progress.pastVintagesDone) {
    console.log('\nPhase 3: Scraping past-vintages pages...');

    const wineNameMap = {
      'cask-23-cabernet-sauvignon': 'CASK 23 Cabernet Sauvignon',
      's-l-v-cabernet-sauvignon': 'S.L.V. Cabernet Sauvignon',
      'fay-cabernet-sauvignon': 'FAY Cabernet Sauvignon',
      'artemis-cabernet-sauvignon': 'ARTEMIS Cabernet Sauvignon',
    };

    for (const slug of PAST_VINTAGE_SLUGS) {
      const url = `${BASE_URL}/past-vintages/${slug}/`;
      const wineName = wineNameMap[slug] || slug;
      process.stdout.write(`  ${wineName}...`);

      const html = await fetchPage(url);
      if (!html) {
        console.log(' FAILED');
        await sleep(DELAY_MS);
        continue;
      }

      const vintages = parsePastVintagesPage(html, wineName);
      console.log(` ${vintages.length} vintages`);

      for (const v of vintages) {
        appendFileSync(OUTPUT_FILE, JSON.stringify(v) + '\n');
      }

      await sleep(DELAY_MS);
    }

    progress.pastVintagesDone = true;
    saveProgress(progress);
  }

  // Phase 4: Scrape wine-acclaim scores
  if (!progress.acclaimDone) {
    console.log('\nPhase 4: Scraping wine-acclaim page for scores...');
    const html = await fetchPage(ACCLAIM_URL);
    if (html) {
      const scores = parseAcclaimPage(html);
      console.log(`  Found ${scores.length} scores`);
      for (const s of scores) {
        appendFileSync(SCORES_FILE, JSON.stringify(s) + '\n');
      }
    } else {
      console.log('  FAILED to fetch acclaim page');
    }

    progress.acclaimDone = true;
    saveProgress(progress);
  }

  // Summary
  const outputLines = existsSync(OUTPUT_FILE) ? readFileSync(OUTPUT_FILE, 'utf8').trim().split('\n').length : 0;
  const scoreLines = existsSync(SCORES_FILE) ? readFileSync(SCORES_FILE, 'utf8').trim().split('\n').length : 0;

  console.log('\n========================================');
  console.log("  STAG'S LEAP SCRAPE COMPLETE");
  console.log('========================================');
  console.log(`  Wine entries: ${outputLines}`);
  console.log(`  Score entries: ${scoreLines}`);
  console.log(`  Output: ${OUTPUT_FILE}, ${SCORES_FILE}`);
}

// ── DB Insertion ────────────────────────────────────────────

const GRAPE_ALIASES = {
  'cabernet sauvignon': 'Cabernet Sauvignon',
  'cabernet franc': 'Cabernet Franc',
  'petit verdot': 'Petit Verdot',
  'merlot': 'Merlot',
  'malbec': 'Malbec',
  'chardonnay': 'Chardonnay',
  'sauvignon blanc': 'Sauvignon Blanc',
  'petite sirah': 'Petite Sirah',
  'syrah': 'Syrah',
};

function classifyVarietal(grapes, wineName) {
  const name = wineName.toLowerCase();

  if (grapes.length > 0) {
    const primary = grapes[0];
    const pct = primary.percentage;
    const grape = primary.grape.toLowerCase();

    if (pct >= 75) {
      if (grape.includes('cabernet sauvignon')) return 'Cabernet Sauvignon';
      if (grape.includes('cabernet franc')) return 'Cabernet Franc';
      if (grape.includes('chardonnay')) return 'Chardonnay';
      if (grape.includes('sauvignon blanc')) return 'Sauvignon Blanc';
      if (grape.includes('merlot')) return 'Merlot';
    }

    // Bordeaux blend check
    const bordeaux = ['cabernet sauvignon', 'merlot', 'cabernet franc', 'petit verdot', 'malbec'];
    if (grapes.every(g => bordeaux.some(bg => g.grape.toLowerCase().includes(bg)))) {
      return 'Bordeaux Blend';
    }
  }

  // Infer from name
  if (name.includes('cabernet sauvignon') || name.includes('cask 23') || name.includes('s.l.v') || name.includes('slv') || name.includes('fay') || name.includes('artemis') || name.includes('armillary')) return 'Cabernet Sauvignon';
  if (name.includes('cabernet franc')) return 'Cabernet Franc';
  if (name.includes('chardonnay')) return 'Chardonnay';
  if (name.includes('sauvignon blanc')) return 'Sauvignon Blanc';
  if (name.includes('merlot')) return 'Merlot';
  if (name.includes('petit verdot')) return 'Petit Verdot';
  if (name.includes('red blend')) return 'Red Blend';
  return 'Red Blend';
}

async function fetchAll(table, columns = '*', filter = {}, batchSize = 1000) {
  const all = [];
  let offset = 0;
  while (true) {
    let query = sb.from(table).select(columns).range(offset, offset + batchSize - 1);
    for (const [k, v] of Object.entries(filter)) query = query.eq(k, v);
    const { data, error } = await query;
    if (error) throw new Error(`fetchAll ${table}: ${error.message}`);
    all.push(...data);
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return all;
}

// Parse aging text for oak months: "20 months in 100% new French oak"
function parseOakMonths(aging) {
  if (!aging) return null;
  const m = aging.match(/(\d+)\s*months?/i);
  return m ? parseInt(m[1]) : null;
}

function parseNewOakPct(aging) {
  if (!aging) return null;
  const m = aging.match(/(\d+)%\s*new/i);
  return m ? parseInt(m[1]) : null;
}

async function insertData() {
  console.log("\n=== STAG'S LEAP DB INSERTION ===\n");

  if (!existsSync(OUTPUT_FILE)) {
    console.error(`No ${OUTPUT_FILE} found. Run scraper first.`);
    process.exit(1);
  }

  const lines = readFileSync(OUTPUT_FILE, 'utf8').trim().split('\n');
  let entries = lines.map(l => JSON.parse(l)).filter(e => e.wineName && e.vintage);

  // Data cleaning
  const skipNames = /event|vertical|exploration|test product|boxed set/i;
  entries = entries.filter(e => !skipNames.test(e.wineName));

  // Clean wine names: remove ", XX Points" suffix
  for (const e of entries) {
    e.wineName = e.wineName.replace(/,\s*\d+\s*Points?\s*$/i, '').trim();

    // Fix swapped pH/TA: pH should be 2.5-5.0, TA should be 0.3-1.5 g/100ml
    if (e.ph && e.ph < 2.0 && e.ta && e.ta > 2.0) {
      const tmp = e.ph;
      e.ph = e.ta;
      e.ta = tmp;
      console.log(`  Fixed swapped pH/TA for ${e.vintage} ${e.wineName}: pH=${e.ph}, TA=${e.ta}`);
    } else if (e.ph && e.ph < 2.0) {
      // pH is clearly TA, but no valid pH available
      e.ta = e.ta || e.ph;
      e.ph = null;
    }
  }
  console.log(`Loaded ${entries.length} wine entries (cleaned)`);

  // Load scores
  let scores = [];
  if (existsSync(SCORES_FILE)) {
    scores = readFileSync(SCORES_FILE, 'utf8').trim().split('\n').map(l => JSON.parse(l));
    console.log(`Loaded ${scores.length} score entries`);
  }

  // ── Reference Data ──
  console.log('\nLoading reference data...');

  const { data: [usCountry] } = await sb.from('countries').select('id').ilike('name', '%United States%').limit(1);
  const countryId = usCountry.id;

  const { data: [caRegion] } = await sb.from('regions').select('id').ilike('name', '%California%').limit(1);
  const regionId = caRegion.id;

  const appellations = await fetchAll('appellations', 'id,name');
  const appellationMap = new Map(appellations.map(a => [a.name.toLowerCase(), a.id]));

  const grapes = await fetchAll('grapes', 'id,name');
  const grapeMap = new Map(grapes.map(g => [g.name.toLowerCase(), g.id]));

  const varietals = await fetchAll('varietal_categories', 'id,name,slug');
  const varietalMap = new Map(varietals.map(v => [v.name.toLowerCase(), v.id]));
  for (const v of varietals) varietalMap.set(v.slug, v.id);

  // Source type
  let { data: sourceTypes } = await sb.from('source_types').select('id,slug');
  let winerySourceId = sourceTypes.find(s => s.slug === 'winery-website')?.id;
  if (!winerySourceId) {
    const { data: newST } = await sb.from('source_types').insert({
      slug: 'winery-website', name: 'Winery Website',
      description: 'Data sourced directly from the winery/producer website', reliability_tier: 1,
    }).select('id').single();
    winerySourceId = newST.id;
  }
  console.log(`  Source type "winery-website": ${winerySourceId}`);

  // Publications
  const publications = await fetchAll('publications', 'id,name,slug');
  const pubMap = new Map(publications.map(p => [p.name.toLowerCase(), p.id]));
  for (const p of publications) pubMap.set(p.slug, p.id);

  const PUB_ALIASES = {
    'wine enthusiast': 'wine enthusiast',
    'wine spectator': 'wine spectator',
    'the wine advocate': 'wine advocate',
    'wine advocate': 'wine advocate',
    'robert parker': 'wine advocate',
    'jamessuckling.com': 'james suckling',
    'james suckling': 'james suckling',
    'jeb dunnuck': 'jeb dunnuck',
    'jebdunnuck.com': 'jeb dunnuck',
    'vinous': 'vinous',
    'vinous media': 'vinous',
    'wine & spirits': 'wine & spirits',
    'decanter': 'decanter',
  };
  for (const [alias, canonical] of Object.entries(PUB_ALIASES)) {
    const id = pubMap.get(canonical);
    if (id) pubMap.set(alias, id);
  }

  // ── Create Producer ──
  console.log("\nCreating Stag's Leap Wine Cellars producer...");
  let producerId;
  const { data: existingProd } = await sb.from('producers').select('id').eq('slug', 'stags-leap-wine-cellars').single();
  if (existingProd) {
    producerId = existingProd.id;
    console.log(`  Using existing producer: ${producerId}`);
  } else {
    producerId = randomUUID();
    const { error: prodErr } = await sb.from('producers').insert({
      id: producerId,
      slug: 'stags-leap-wine-cellars',
      name: "Stag's Leap Wine Cellars",
      name_normalized: normalize("Stag's Leap Wine Cellars"),
      country_id: countryId,
      website_url: 'https://www.stagsleapwinecellars.com',
      year_established: 1970,
      metadata: {
        famous_for: '1976 Judgment of Paris winner (S.L.V. 1973)',
        appellations: ['Stags Leap District', 'Napa Valley'],
        winemaker: 'Marcus Notaro',
      },
    });
    if (prodErr) { console.error('Producer error:', prodErr.message); process.exit(1); }
    console.log(`  Created producer: ${producerId}`);
  }

  // ── Deduplicate entries: prefer product page over past-vintages ──
  // Group by wineName + vintage, prefer 'product' source
  const entryMap = new Map();
  for (const e of entries) {
    const key = `${normalizeWineName(e.wineName)}|${e.vintage}`;
    const existing = entryMap.get(key);
    if (!existing || (e.source === 'product' && existing.source !== 'product')) {
      entryMap.set(key, e);
    } else if (!existing.abv && e.abv) {
      // Merge missing data
      entryMap.set(key, { ...existing, ...Object.fromEntries(Object.entries(e).filter(([k,v]) => v !== null && !existing[k])) });
    }
  }
  const uniqueEntries = [...entryMap.values()];
  console.log(`\n${uniqueEntries.length} unique wine-vintage entries (after dedup)`);

  // ── Group by wine name (for wine-level records) ──
  const winesByName = new Map();
  for (const e of uniqueEntries) {
    const key = normalizeWineName(e.wineName);
    if (!winesByName.has(key)) winesByName.set(key, []);
    winesByName.get(key).push(e);
  }
  console.log(`${winesByName.size} unique wine names`);

  // ── Create Wines ──
  console.log('\nCreating wine records...');
  const wineIdMap = new Map(); // normalizedName -> wine_id
  let wineCount = 0;

  const APPELLATION_ALIASES = {
    'stags leap district': 'stags leap district',
    "stag's leap district": 'stags leap district',
    'napa valley': 'napa valley',
    'oak knoll district': 'oak knoll district of napa valley',
    'oak knoll district of napa valley': 'oak knoll district of napa valley',
    'atlas peak': 'atlas peak',
    'coombsville': 'coombsville',
  };

  for (const [normName, vintages] of winesByName) {
    const latest = vintages.sort((a, b) => (b.vintage || 0) - (a.vintage || 0))[0];
    const displayName = latest.wineName;

    // Resolve appellation
    const appName = (latest.appellation || '').toLowerCase().trim();
    const appLookup = APPELLATION_ALIASES[appName] || appName;
    const appellationId = appellationMap.get(appLookup) || null;

    // Classify varietal
    const varietalName = classifyVarietal(latest.grapes, displayName);
    let varietalId = varietalMap.get(varietalName.toLowerCase()) || varietalMap.get(slugify(varietalName));
    if (!varietalId) varietalId = varietalMap.get('red-blend') || varietalMap.get('red blend');

    const wineId = randomUUID();
    const wineSlug = slugify(`stags-leap-wc-${displayName}`);
    const { error } = await sb.from('wines').insert({
      id: wineId,
      slug: wineSlug,
      name: displayName,
      name_normalized: normalize(displayName),
      producer_id: producerId,
      country_id: countryId,
      region_id: regionId,
      appellation_id: appellationId,
      varietal_category_id: varietalId,
      varietal_category_source: winerySourceId,
      metadata: {
        stags_leap_url: latest.url || null,
      },
    });

    if (error) {
      console.error(`  Wine "${displayName}" error: ${error.message}`);
      continue;
    }
    wineIdMap.set(normName, wineId);
    wineCount++;
    if (wineCount % 10 === 0) process.stdout.write(`  ${wineCount} wines...\r`);
  }
  console.log(`  Created ${wineCount} wines`);

  // ── Create Wine Grapes ──
  console.log('\nCreating grape compositions...');
  let grapeCount = 0;
  for (const [normName, vintages] of winesByName) {
    const wineId = wineIdMap.get(normName);
    if (!wineId) continue;
    const latest = vintages.sort((a, b) => (b.vintage || 0) - (a.vintage || 0))[0];
    if (!latest.grapes || latest.grapes.length === 0) continue;

    for (const g of latest.grapes) {
      const grapeName = GRAPE_ALIASES[g.grape.toLowerCase()] || g.grape;
      const grapeId = grapeMap.get(grapeName.toLowerCase());
      if (!grapeId) {
        console.warn(`    Unknown grape: "${g.grape}"`);
        continue;
      }
      const { error } = await sb.from('wine_grapes').insert({
        wine_id: wineId, grape_id: grapeId, percentage: g.percentage, percentage_source: winerySourceId,
      });
      if (!error) grapeCount++;
    }
  }
  console.log(`  Created ${grapeCount} grape entries`);

  // ── Create Wine Vintages ──
  console.log('\nCreating vintage records...');
  let vintageCount = 0;

  for (const e of uniqueEntries) {
    const wineId = wineIdMap.get(normalizeWineName(e.wineName));
    if (!wineId || !e.vintage) continue;

    const metadata = {};
    if (e.winemakingNotes) metadata.winemaking = e.winemakingNotes;

    const { error } = await sb.from('wine_vintages').insert({
      wine_id: wineId,
      vintage_year: e.vintage,
      abv: e.abv || null,
      ph: e.ph || null,
      ta_g_l: e.ta ? e.ta * 10 : null, // convert g/100ml to g/L
      duration_in_oak_months: parseOakMonths(e.aging),
      new_oak_pct: parseNewOakPct(e.aging),
      winemaker_notes: e.tastingNotes || null,
      vintage_notes: e.vintageNotes || null,
      release_price_usd: null,
      metadata: Object.keys(metadata).length > 0 ? metadata : {},
    });

    if (error) {
      if (!error.message.includes('duplicate')) {
        console.error(`  Vintage ${e.wineName} ${e.vintage}: ${error.message}`);
      }
    } else {
      vintageCount++;
    }
    if (vintageCount % 20 === 0) process.stdout.write(`  ${vintageCount} vintages...\r`);
  }
  console.log(`  Created ${vintageCount} vintages`);

  // ── Create Scores ──
  console.log('\nCreating score records...');
  let scoreCount = 0;

  // Map acclaim wine names to our normalized names
  const acclaimNameMap = {
    'CASK 23': 'CASK 23 Cabernet Sauvignon',
    'S.L.V.': 'S.L.V. Cabernet Sauvignon',
    'SLV': 'S.L.V. Cabernet Sauvignon',
    'FAY': 'FAY Cabernet Sauvignon',
    'ARTEMIS': 'ARTEMIS Cabernet Sauvignon',
    'AVETA': 'AVETA Sauvignon Blanc',
    'KARIA': 'KARIA Chardonnay',
    'ARCADIA': 'ARCADIA Chardonnay',
    'ARMILLARY': 'ARMILLARY Cabernet Sauvignon',
    'DANIKA': 'DANIKA RANCH Sauvignon Blanc',
    'CELLARIUS': 'CELLARIUS Cabernet Sauvignon',
    'BATTUELLO': 'BATTUELLO Cabernet Sauvignon',
    'SODA CANYON': 'Soda Canyon Cabernet Sauvignon',
    'CHASE CREEK': 'Chase Creek Cabernet Sauvignon',
    'Heart of FAY': 'Heart of FAY Cabernet Sauvignon',
  };

  for (const s of scores) {
    const fullName = acclaimNameMap[s.wine] || s.wine;
    const wineId = wineIdMap.get(normalizeWineName(fullName));
    if (!wineId) {
      // Try partial match
      let found = false;
      for (const [normName, id] of wineIdMap) {
        if (normName.includes(normalizeWineName(s.wine))) {
          const pubKey = s.publication.toLowerCase().trim();
          const pubId = pubMap.get(pubKey) || pubMap.get(PUB_ALIASES[pubKey] || '');
          if (pubId) {
            const { error } = await sb.from('wine_vintage_scores').insert({
              wine_id: id, vintage_year: s.vintage, score: s.score, score_scale: '100',
              publication_id: pubId, source_id: winerySourceId,
              url: ACCLAIM_URL, discovered_at: new Date().toISOString(),
            });
            if (!error) scoreCount++;
            else if (!error.message.includes('duplicate')) console.warn(`  Score error: ${error.message}`);
          }
          found = true;
          break;
        }
      }
      if (!found) console.warn(`  No wine match for score: ${s.wine} ${s.vintage}`);
      continue;
    }

    const pubKey = s.publication.toLowerCase().trim();
    const pubId = pubMap.get(pubKey) || pubMap.get(PUB_ALIASES[pubKey] || '');
    if (!pubId) {
      console.warn(`  No publication match: "${s.publication}"`);
      continue;
    }

    const { error } = await sb.from('wine_vintage_scores').insert({
      wine_id: wineId, vintage_year: s.vintage, score: s.score, score_scale: '100',
      publication_id: pubId, source_id: winerySourceId,
      url: ACCLAIM_URL, discovered_at: new Date().toISOString(),
    });
    if (!error) scoreCount++;
    else if (!error.message.includes('duplicate')) console.warn(`  Score error: ${error.message}`);
  }
  console.log(`  Created ${scoreCount} scores`);

  // Summary
  console.log('\n========================================');
  console.log("   STAG'S LEAP WINE CELLARS IMPORT COMPLETE");
  console.log('========================================');
  console.log(`  Producer: Stag's Leap Wine Cellars (${producerId})`);
  console.log(`  Wines: ${wineCount}`);
  console.log(`  Vintages: ${vintageCount}`);
  console.log(`  Scores: ${scoreCount}`);
  console.log(`  Grape entries: ${grapeCount}`);
}

function normalizeWineName(name) {
  return name.replace(/\s+/g, ' ').trim().toLowerCase();
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  if (INSERT_MODE) {
    await insertData();
  } else {
    await scrape();
  }
}

main().catch(console.error);
