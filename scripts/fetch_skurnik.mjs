#!/usr/bin/env node
/**
 * fetch_skurnik.mjs — Fetch Skurnik Wines catalog via FacetWP REST API + detail scraping
 *
 * Two-phase approach:
 *   Phase 1: FacetWP API bulk listing (270 pages × 20 wines = ~5,394 wines)
 *     POST https://www.skurnik.com/wp-json/facetwp/v1/refresh
 *     Returns HTML template with wine cards: producer, name, SKU, vintage,
 *     country, region, appellation, variety, color, farming, image URL.
 *     Pre-filters to wines only (no spirits/sake/combos).
 *
 *   Phase 2: Individual SKU page scraping for enrichment detail
 *     Blend percentages, ABV, cases produced, soil, vineyard, fermentation,
 *     aging, reviews/scores with drinking windows, tech sheet PDFs.
 *
 * Usage:
 *   node scripts/fetch_skurnik.mjs                    # Full fetch (Phase 1 + Phase 2)
 *   node scripts/fetch_skurnik.mjs --phase1           # Phase 1 only (listing)
 *   node scripts/fetch_skurnik.mjs --phase2           # Phase 2 only (detail, requires Phase 1 done)
 *   node scripts/fetch_skurnik.mjs --limit 50         # Limit wines for testing
 *   node scripts/fetch_skurnik.mjs --resume            # Resume from last position
 *
 * Output: data/imports/skurnik_catalog.json
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import https from 'https';

const OUTPUT_FILE = 'data/imports/skurnik_catalog.json';
const PROGRESS_FILE = 'data/imports/skurnik_progress.json';
const API_DELAY_MS = 1500;  // 1.5s between API pages
const DETAIL_DELAY_MS = 2000; // 2s between detail page scrapes
const BASE_URL = 'https://www.skurnik.com';

const args = process.argv.slice(2);
const LIMIT = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : Infinity;
const RESUME = args.includes('--resume');
const PHASE1_ONLY = args.includes('--phase1');
const PHASE2_ONLY = args.includes('--phase2');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpsRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const reqOptions = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: options.method || 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ...options.headers,
      },
      timeout: 20000,
    };

    const req = https.request(reqOptions, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${BASE_URL}${res.headers.location}`;
        res.resume();
        return httpsRequest(redirectUrl).then(resolve).catch(reject);
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });

    if (options.body) req.write(options.body);
    req.end();
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

function cleanText(html) {
  return decodeEntities(html.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim());
}

// ── Phase 1: Parse wine cards from FacetWP API HTML template ─────
function parseListingCards(html) {
  const wines = [];

  // Split into individual card blocks (div.sku-list-item)
  const cardBlocks = html.split(/class="sku-list-item\b/).slice(1);

  for (const cardHtml of cardBlocks) {
    const wine = { _source: 'skurnik' };

    // Producer: <div class="producer"><a href="/producer/{slug}/">\n  Name  \n</a></div>
    const producerMatch = cardHtml.match(/href="[^"]*\/producer\/([^"\/]+)\/"[^>]*>\s*([\s\S]*?)\s*<\/a>/);
    if (producerMatch) {
      wine.producer_slug = producerMatch[1];
      wine.producer = decodeEntities(producerMatch[2].replace(/\s+/g, ' ').trim());
    }

    // Wine name: <div class="sku-title"><a href="/sku/{slug}/">Name</a>
    const titleMatch = cardHtml.match(/sku-title[^>]*><a href="([^"]+)"[^>]*>([^<]+)<\/a>/);
    if (titleMatch) {
      wine.url = titleMatch[1].startsWith('http') ? titleMatch[1] : `${BASE_URL}${titleMatch[1]}`;
      wine.url_slug = titleMatch[1].replace(/^.*\/sku\//, '').replace(/\/$/, '');
      wine.name = decodeEntities(titleMatch[2].trim());
    }

    // Image URL
    const imgMatch = cardHtml.match(/src="([^"]+\.(jpg|png|webp)[^"]*)"/i);
    if (imgMatch) wine.image_url = imgMatch[1];

    // All label/value pairs from list-label/list-desc divs
    const pairs = cardHtml.matchAll(/list-label[^"]*"[^>]*>([^<]+)<\/div>\s*(?:<\/div>)?\s*(?:<div[^>]*>)?\s*<div class="list-desc[^"]*"[^>]*>([^<]+)<\/div>/g);
    for (const m of pairs) {
      const label = m[1].replace(/[:#]/g, '').trim().toLowerCase();
      const value = decodeEntities(m[2].trim());
      if (!value) continue;

      switch (label) {
        case 'sku': wine.sku = value; break;
        case 'vintage': wine.vintage = value; break;
        case 'country': wine.country = value; break;
        case 'region': wine.region = value; break;
        case 'appellation': wine.appellation = value; break;
        case 'variety': wine.grape = value; break;
        case 'color': wine.color = value; break;
        case 'farming practice': wine.farming = value; break;
        default:
          if (!wine.extra_fields) wine.extra_fields = {};
          wine.extra_fields[label] = value;
      }
    }

    // Only include if we got at least a name
    if (wine.name) wines.push(wine);
  }

  return wines;
}

// ── Phase 1: Fetch all wines via FacetWP API ─────────────────────
async function fetchPhase1(existingWines, startPage) {
  console.log('Phase 1: Fetching wine catalog via FacetWP API...');

  const wines = existingWines || [];
  let page = startPage || 1;
  let totalPages = null;
  let totalRows = null;
  let consecutiveEmpty = 0;

  while (true) {
    if (wines.length >= LIMIT) {
      console.log(`  Reached limit of ${LIMIT} wines`);
      break;
    }

    const payload = JSON.stringify({
      action: 'facetwp_refresh',
      data: {
        facets: {
          color: [], country: [], region: [], appellation: [],
          producer: [], varietal: [], vintage: [],
          wine_farming_practice: [], kosher_type: [],
        },
        frozen_facets: {},
        http_params: { uri: 'portfolio-wine', lang: '' },
        template: 'our_wines_22',
        extras: { sort: 'default' },
        soft_refresh: 0,
        is_preload: 0,
        first_load: 0,
        paged: page,
      },
    });

    try {
      const res = await httpsRequest(`${BASE_URL}/wp-json/facetwp/v1/refresh`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Referer': `${BASE_URL}/portfolio-wine/`,
        },
        body: payload,
      });

      if (res.status !== 200) {
        console.log(`  Page ${page}: HTTP ${res.status}`);
        consecutiveEmpty++;
        if (consecutiveEmpty >= 3) break;
        page++;
        await sleep(API_DELAY_MS);
        continue;
      }

      const data = JSON.parse(res.body);

      // Get pagination info from settings
      if (data.settings?.pager) {
        totalPages = data.settings.pager.total_pages;
        totalRows = data.settings.pager.total_rows;
        if (page === (startPage || 1)) {
          console.log(`  Total: ${totalRows} wines across ${totalPages} pages`);
        }
      }

      // Parse wine cards from template HTML
      const template = data.template || '';
      const pageWines = parseListingCards(template);

      if (pageWines.length === 0) {
        // Try alternative parsing — the template HTML might have a different structure
        // Let's check what we got
        if (template.length < 100) {
          console.log(`  Page ${page}: empty template (${template.length} chars)`);
          consecutiveEmpty++;
          if (consecutiveEmpty >= 3) break;
        } else {
          // Template has content but our parser didn't find cards
          // Save a sample for debugging
          if (page <= 3) {
            writeFileSync(`data/imports/skurnik_debug_page${page}.html`, template);
            console.log(`  Page ${page}: ${template.length} chars but 0 wines parsed — saved debug HTML`);
          }
          consecutiveEmpty++;
          if (consecutiveEmpty >= 5) break;
        }
      } else {
        consecutiveEmpty = 0;
        wines.push(...pageWines);
      }

      console.log(`  [Page ${page}/${totalPages || '?'}] ${pageWines.length} wines (total: ${wines.length})`);

      // Save progress every 10 pages
      if (page % 10 === 0) {
        writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));
        writeFileSync(PROGRESS_FILE, JSON.stringify({ phase: 1, page, totalPages }));
      }

      if (totalPages && page >= totalPages) break;
      page++;
    } catch (e) {
      console.log(`  Page ${page}: Error — ${e.message}`);
      consecutiveEmpty++;
      if (consecutiveEmpty >= 3) break;
      page++;
    }

    await sleep(API_DELAY_MS);
  }

  // Final save
  writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));
  console.log(`\n  Phase 1 complete: ${wines.length} wines`);

  return wines;
}

// ── Phase 2: Parse detail page ───────────────────────────────────
function parseDetailPage(html, wine) {
  const detail = { ...wine };

  // Structured details between SKU DETAILS START/END
  const detailsSection = html.match(/<!-- SKU DETAILS START -->([\s\S]*?)<!-- SKU DETAILS END -->/);
  if (detailsSection) {
    const details = detailsSection[1];

    // Extract all label/value pairs
    const pairs = details.matchAll(/<div class="list-label[^"]*">([^<]+)<\/div>\s*\n?\s*<div class="list-desc[^"]*">(?:<a[^>]*>)?([^<]+)(?:<\/a>)?<\/div>/g);
    for (const m of pairs) {
      const label = m[1].replace(':', '').trim().toLowerCase();
      const value = decodeEntities(m[2].trim());

      switch (label) {
        case 'vintage': detail.vintage = value; break;
        case 'country': detail.country = value; break;
        case 'region': detail.region = value; break;
        case 'appellation': detail.appellation = value; break;
        case 'variety': detail.grape = value; break;
        case 'color': detail.color = value; break;
        case 'farming practice': detail.farming = value; break;
        case 'soil': detail.soil = value; break;
        case 'vineyard': detail.vineyard = value; break;
        default:
          if (!detail.extra_fields) detail.extra_fields = {};
          detail.extra_fields[label] = value;
      }
    }

    // Grape percentages (more detailed than listing variety)
    const grapeDetail = details.match(/grape[^>]*>([^<]*\d+%[^<]*)/i);
    if (grapeDetail) detail.grape_detail = decodeEntities(grapeDetail[1].trim());

    // ABV
    const abvMatch = details.match(/(\d{1,2}(?:\.\d+)?)\s*%\s*(?:ABV|alc)/i);
    if (abvMatch) detail.abv = abvMatch[1];

    // Cases produced
    const casesMatch = details.match(/(\d[\d,]+)\s*cases?\s*produced/i);
    if (casesMatch) detail.production = casesMatch[1].replace(/,/g, '');

    // SKU code
    const skuMatch = details.match(/<td>([A-Z]{2}-[A-Z]+-[\w-]+)<\/td>/);
    if (skuMatch) detail.sku = skuMatch[1];

    // Bottle format
    const formatMatch = details.match(/<td>(\d+\/\d+ml)<\/td>/);
    if (formatMatch) detail.bottle_format = formatMatch[1];
  }

  // Bullet list (winemaking notes)
  const contentSection = html.match(/<!-- POST CONTENT START -->([\s\S]*?)<!-- POST CONTENT END -->/);
  if (contentSection) {
    const bullets = [];
    const listItems = contentSection[1].matchAll(/<li>([\s\S]*?)<\/li>/g);
    for (const m of listItems) {
      bullets.push(cleanText(m[1]));
    }
    if (bullets.length > 0) detail.notes = bullets;

    const paras = contentSection[1].matchAll(/<p>([\s\S]*?)<\/p>/g);
    const paraTexts = [];
    for (const m of paras) {
      const t = cleanText(m[1]);
      if (t.length > 10) paraTexts.push(t);
    }
    if (paraTexts.length > 0) detail.description = paraTexts.join('\n');
  }

  // Reviews/scores — collapsible sections with tr_section_header/tr_section_content
  const scores = [];
  const reviewSections = html.matchAll(/tr_section_header[^>]*>([\s\S]*?)<\/div>\s*[\s\S]*?tr_section_content[^>]*>([\s\S]*?)<\/div>/g);
  for (const rs of reviewSections) {
    const header = cleanText(rs[1]);
    const content = cleanText(rs[2]);

    // Parse score from header (e.g., "Wine Advocate 95" or "James Suckling 93-95")
    const scoreMatch = header.match(/(.+?)\s+(\d{2,3})(?:\s*[-–]\s*(\d{2,3}))?\s*(?:pts?|points?)?$/i);
    if (scoreMatch) {
      const entry = {
        publication: scoreMatch[1].trim(),
        score: parseInt(scoreMatch[2]),
      };
      if (scoreMatch[3]) entry.score_high = parseInt(scoreMatch[3]);
      if (content.length > 10) entry.note = content;

      // Extract drinking window from note
      const dwMatch = content.match(/(?:drink|drinking)\s+(\d{4})\s*[-–]\s*(\d{4})/i);
      if (dwMatch) {
        entry.drinking_window_start = dwMatch[1];
        entry.drinking_window_end = dwMatch[2];
      }

      // Extract vintage from context
      const vintMatch = content.match(/\b((?:19|20)\d{2})\s+(?:vintage|harvest)/i);
      if (vintMatch) entry.review_vintage = vintMatch[1];

      scores.push(entry);
    }
  }
  if (scores.length > 0) detail.scores = scores;

  // Tech sheet PDF
  const pdfMatch = html.match(/href="([^"]+\.pdf)"/i);
  if (pdfMatch) detail.tech_sheet_url = pdfMatch[1];

  detail._detail_scraped = true;

  return detail;
}

// ── Phase 2: Enrich wines with detail pages ──────────────────────
async function fetchPhase2(wines, startIdx) {
  const limit = Math.min(LIMIT, wines.length);
  console.log(`\nPhase 2: Scraping ${limit} detail pages (delay: ${DETAIL_DELAY_MS}ms)...`);

  let enriched = 0;
  let skipped = 0;
  let errorCount = 0;

  for (let i = startIdx; i < limit; i++) {
    const wine = wines[i];

    // Skip if already scraped
    if (wine._detail_scraped) {
      skipped++;
      continue;
    }

    try {
      const res = await httpsRequest(wine.url);
      if (res.status !== 200) {
        console.log(`  [${i + 1}/${limit}] ${res.status}: ${wine.url}`);
        errorCount++;
        continue;
      }

      wines[i] = parseDetailPage(res.body, wine);
      enriched++;

      if ((i + 1) % 50 === 0) {
        console.log(`  [${i + 1}/${limit}] enriched: ${enriched}, skipped: ${skipped}, errors: ${errorCount}`);
        writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));
        writeFileSync(PROGRESS_FILE, JSON.stringify({ phase: 2, lastIndex: i }));
      }
    } catch (e) {
      console.log(`  [${i + 1}/${limit}] Error: ${e.message} — ${wine.url}`);
      errorCount++;
    }

    await sleep(DETAIL_DELAY_MS);
  }

  writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));
  console.log(`  Phase 2 complete: ${enriched} enriched, ${skipped} already done, ${errorCount} errors`);
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  let wines = [];
  let phase1StartPage = 1;
  let phase2StartIdx = 0;

  // Handle resume
  if (RESUME && existsSync(PROGRESS_FILE)) {
    const progress = JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
    if (existsSync(OUTPUT_FILE)) {
      wines = JSON.parse(readFileSync(OUTPUT_FILE, 'utf8'));
    }
    if (progress.phase === 1) {
      phase1StartPage = progress.page + 1;
      console.log(`Resuming Phase 1 from page ${phase1StartPage} (${wines.length} wines so far)`);
    } else if (progress.phase === 2) {
      phase2StartIdx = (progress.lastIndex || 0) + 1;
      console.log(`Resuming Phase 2 from index ${phase2StartIdx} (${wines.length} wines)`);
    }
  }

  // Phase 1: Bulk listing via API
  if (!PHASE2_ONLY) {
    if (RESUME && existsSync(PROGRESS_FILE)) {
      const progress = JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
      if (progress.phase === 2) {
        console.log('Phase 1 already complete, skipping to Phase 2');
      } else {
        wines = await fetchPhase1(wines, phase1StartPage);
      }
    } else {
      wines = await fetchPhase1([], 1);
    }
  } else {
    // Phase 2 only — load existing catalog
    if (existsSync(OUTPUT_FILE)) {
      wines = JSON.parse(readFileSync(OUTPUT_FILE, 'utf8'));
      console.log(`Loaded ${wines.length} wines from Phase 1`);
    } else {
      console.error('No catalog file found. Run Phase 1 first.');
      process.exit(1);
    }
  }

  // Phase 2: Detail enrichment
  if (!PHASE1_ONLY && wines.length > 0) {
    // Mark transition to Phase 2 in progress
    writeFileSync(PROGRESS_FILE, JSON.stringify({ phase: 2, lastIndex: phase2StartIdx - 1 }));
    await fetchPhase2(wines, phase2StartIdx);
  }

  // Final output
  console.log(`\n✅ Done. ${wines.length} wines saved to ${OUTPUT_FILE}`);

  // Print sample
  if (wines.length > 0) {
    console.log('\nSample wine:');
    console.log(JSON.stringify(wines[0], null, 2));
  }

  // Stats
  const stats = {
    total: wines.length,
    withProducer: wines.filter(w => w.producer).length,
    withVintage: wines.filter(w => w.vintage).length,
    withGrape: wines.filter(w => w.grape).length,
    withAppellation: wines.filter(w => w.appellation).length,
    withRegion: wines.filter(w => w.region).length,
    withCountry: wines.filter(w => w.country).length,
    withFarming: wines.filter(w => w.farming).length,
    withSoil: wines.filter(w => w.soil).length,
    withAbv: wines.filter(w => w.abv).length,
    withScores: wines.filter(w => w.scores?.length > 0).length,
    withNotes: wines.filter(w => w.notes?.length > 0).length,
    withDescription: wines.filter(w => w.description).length,
    withDetailScraped: wines.filter(w => w._detail_scraped).length,
  };
  console.log('\nField coverage:');
  Object.entries(stats).forEach(([k, v]) => {
    const pct = stats.total > 0 ? ((v / stats.total) * 100).toFixed(1) : 0;
    console.log(`  ${k}: ${v} (${pct}%)`);
  });

  // Country distribution
  const countryDist = {};
  for (const w of wines) {
    const c = w.country || 'unknown';
    countryDist[c] = (countryDist[c] || 0) + 1;
  }
  console.log('\nCountry distribution (top 10):');
  Object.entries(countryDist).sort((a, b) => b[1] - a[1]).slice(0, 10)
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));

  // Clean up progress file on completion
  if (!PHASE1_ONLY && existsSync(PROGRESS_FILE)) {
    const { unlinkSync } = await import('fs');
    unlinkSync(PROGRESS_FILE);
  }
}

main().catch(console.error);
