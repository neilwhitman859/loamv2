#!/usr/bin/env node
/**
 * scrape_tablas_creek.mjs
 *
 * Scrapes Tablas Creek Vineyard (tablascreek.com) wine catalog.
 * Data source: individual wine vintage pages with rich structured data.
 *
 * Usage:
 *   node scrape_tablas_creek.mjs                    # Full scrape
 *   node scrape_tablas_creek.mjs --resume           # Resume from checkpoint
 *   node scrape_tablas_creek.mjs --insert           # Insert JSONL data into DB
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
const OUTPUT_FILE = 'tablas_creek_wines.jsonl';
const URLS_FILE = 'tablas_creek_urls.json';
const PROGRESS_FILE = 'tablas_creek_progress.json';
const DELAY_MS = 3000;
const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36';

const args = process.argv.slice(2);
const RESUME = args.includes('--resume');
const INSERT_MODE = args.includes('--insert');

// ── Helpers ─────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function normalize(s) { return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim(); }
function slugify(s) { return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, ''); }
function stripHtml(s) {
  return s.replace(/&#8217;/g, "'").replace(/&#8216;/g, "'").replace(/&#8211;/g, '\u2013').replace(/&#8212;/g, '\u2014')
    .replace(/&#038;/g, '&').replace(/&amp;/g, '&').replace(/&nbsp;/g, ' ').replace(/&rsquo;/g, "'")
    .replace(/&lsquo;/g, "'").replace(/&rdquo;/g, '"').replace(/&ldquo;/g, '"').replace(/&eacute;/g, '\u00e9')
    .replace(/&egrave;/g, '\u00e8').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function loadProgress() {
  if (existsSync(PROGRESS_FILE)) return JSON.parse(readFileSync(PROGRESS_FILE, 'utf8'));
  return { lastIndex: -1 };
}
function saveProgress(data) {
  writeFileSync(PROGRESS_FILE, JSON.stringify({ ...data, timestamp: new Date().toISOString() }));
}

async function fetchPage(url, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': USER_AGENT, 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' },
        redirect: 'follow',
      });
      if (!res.ok) {
        if (res.status === 429) { await sleep(30000); continue; }
        if (res.status === 404) return null;
        await sleep(5000); continue;
      }
      return await res.text();
    } catch (err) { console.warn(`  Error: ${err.message}`); await sleep(5000); }
  }
  return null;
}

// ── Parse wine page ─────────────────────────────────────────
function parseGrapeComposition(text) {
  const grapes = [];
  const regex = /([\d.]+)%\s+([^,\n]+)/g;
  let m;
  while ((m = regex.exec(text)) !== null) {
    grapes.push({ percentage: parseFloat(m[1]), grape: m[2].trim() });
  }
  return grapes;
}

function parseWinePage(html, url) {
  const data = {
    url,
    title: null,
    vintage: null,
    wineName: null,
    grapes: [],
    appellation: null,
    abv: null,
    casesProduced: null,
    bottlingDate: null,
    blendingDate: null,
    aging: null,
    tastingNotes: null,
    productionNotes: null,
    certifications: [],
    foodPairings: [],
    scores: [],
  };

  // Title: <h1> or <title>
  const h1Match = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
  if (h1Match) {
    data.title = stripHtml(h1Match[1]);
    const yearMatch = data.title.match(/^(\d{4})\s+(.+)$/);
    if (yearMatch) {
      data.vintage = parseInt(yearMatch[1]);
      data.wineName = yearMatch[2].trim();
    } else {
      data.wineName = data.title;
    }
  }

  // Appellation: <h4>Appellation</h4> ... <li>...</li>
  const appSection = html.match(/<h4>Appellation<\/h4>\s*<ul>\s*([\s\S]*?)<\/ul>/i);
  if (appSection) {
    const liMatch = appSection[1].match(/<li>([\s\S]*?)<\/li>/i);
    if (liMatch) data.appellation = stripHtml(liMatch[1]);
  }

  // Technical Notes: <h4>Technical Notes</h4> <ul><li>ABV</li><li>Cases</li></ul>
  const techSection = html.match(/<h4>Technical Notes<\/h4>\s*<ul>([\s\S]*?)<\/ul>/i);
  if (techSection) {
    const lis = [...techSection[1].matchAll(/<li>([\s\S]*?)<\/li>/gi)];
    for (const li of lis) {
      const text = stripHtml(li[1]);
      const abvMatch = text.match(/([\d.]+)%\s*Alcohol/i);
      if (abvMatch) data.abv = parseFloat(abvMatch[1]);
      const casesMatch = text.match(/([\d,]+)\s*Cases/i);
      if (casesMatch) data.casesProduced = parseInt(casesMatch[1].replace(/,/g, ''));
    }
  }

  // Blend: <h4>Blend</h4> <ul><li>40% Mourvedre</li>...</ul>
  const blendSection = html.match(/<h4>Blend<\/h4>\s*<ul>([\s\S]*?)<\/ul>/i);
  if (blendSection) {
    const lis = [...blendSection[1].matchAll(/<li>([\s\S]*?)<\/li>/gi)];
    for (const li of lis) {
      const text = stripHtml(li[1]);
      const grapeMatch = text.match(/([\d.]+)%\s+(.+)/);
      if (grapeMatch) {
        data.grapes.push({ percentage: parseFloat(grapeMatch[1]), grape: grapeMatch[2].trim() });
      }
    }
  }

  // Certifications: <h4>Certifications</h4> <ul><li>...</li></ul>
  const certSection = html.match(/<h4>Certifications<\/h4>\s*<ul>([\s\S]*?)<\/ul>/i);
  if (certSection) {
    const lis = [...certSection[1].matchAll(/<li>([\s\S]*?)<\/li>/gi)];
    data.certifications = lis.map(li => stripHtml(li[1])).filter(t => t.length > 0);
  }

  // Food Pairings: <h4>Food Pairings</h4> <ul><li>...</li></ul>
  const fpSection = html.match(/<h4>Food Pairings<\/h4>\s*[\s\S]*?<ul>([\s\S]*?)<\/ul>/i);
  if (fpSection) {
    const lis = [...fpSection[1].matchAll(/<li>([\s\S]*?)<\/li>/gi)];
    data.foodPairings = lis.map(li => stripHtml(li[1])).filter(t => t.length > 0);
  }

  // Tasting Notes: <section class="wine_page__tasting_notes"> <h2>Tasting Notes</h2> <p>...</p>
  const tnSection = html.match(/wine_page__tasting_notes[\s\S]*?<p>([\s\S]*?)<\/p>/i);
  if (tnSection) {
    data.tastingNotes = stripHtml(tnSection[1]).substring(0, 2000);
  }

  // Production Notes: <section class="wine_page__production_notes"> ... <p>...</p>
  const pnSection = html.match(/wine_page__production_notes[\s\S]*?<p>([\s\S]*?)<\/p>/i);
  if (pnSection) {
    const pn = stripHtml(pnSection[1]).substring(0, 2000);
    data.productionNotes = pn;

    // Extract bottling date: "bottling in July 2024"
    const bottlingMatch = pn.match(/bottl(?:ing|ed)\s+in\s+(\w+\s+\d{4})/i);
    if (bottlingMatch) data.bottlingDate = bottlingMatch[1];

    // Extract blending date: "blended in June 2023"
    const blendingMatch = pn.match(/blended?\s+in\s+(\w+\s+\d{4})/i);
    if (blendingMatch) data.blendingDate = blendingMatch[1];

    // Extract aging: "aged in 1200-gallon foudre" or "aged in new French oak"
    const agingMatch = pn.match(/aged?\s+in\s+([^.]+?)(?:\s+before|\s+prior|\.|$)/i);
    if (agingMatch) data.aging = agingMatch[1].trim();
  }

  // Scores: Each score is in an <a> tag with text like "97 points; ...: Publication (Date)"
  const scoreRegex = /(\d{2,3})\s*points?[;:]\s*(?:"([^"]*)"[;:]\s*)?([^(]+?)\s*\(([^)]+)\)/gi;
  let m;
  while ((m = scoreRegex.exec(html)) !== null) {
    const score = parseInt(m[1]);
    if (score >= 80 && score <= 100) {
      data.scores.push({
        score,
        quote: m[2] || null,
        publication: m[3].trim().replace(/,\s*$/, ''),
        date: m[4].trim(),
      });
    }
  }

  return data;
}

// ── Scrape ──────────────────────────────────────────────────
async function scrape() {
  // Load URLs
  let urls;
  if (existsSync(URLS_FILE)) {
    urls = JSON.parse(readFileSync(URLS_FILE, 'utf8'));
    console.log(`Loaded ${urls.length} URLs from ${URLS_FILE}`);
  } else {
    console.error('Run URL discovery first: create tablas_creek_urls.json');
    process.exit(1);
  }

  // Filter out trailing-underscore duplicates and box wines
  urls = urls.filter(u => {
    const slug = u.split('/wines/')[1] || '';
    if (slug.endsWith('_')) return false;
    if (slug.endsWith('_2') || slug.endsWith('_1')) return false;
    if (slug.includes('_box')) return false;
    if (slug.includes('en_primeur')) return false;
    return true;
  });
  console.log(`Filtered to ${urls.length} URLs (removed duplicates/boxes/en primeur)`);

  const progress = RESUME ? loadProgress() : { lastIndex: -1 };
  const startIdx = progress.lastIndex + 1;
  console.log(`\nScraping ${urls.length - startIdx} pages (3s delay)...`);

  let scraped = 0, failed = 0;
  for (let i = startIdx; i < urls.length; i++) {
    const slug = urls[i].split('/wines/')[1] || urls[i];
    process.stdout.write(`  [${i + 1}/${urls.length}] ${slug}...`);

    const html = await fetchPage(urls[i]);
    if (!html) {
      console.log(' FAILED');
      failed++;
      saveProgress({ lastIndex: i });
      await sleep(DELAY_MS);
      continue;
    }

    const data = parseWinePage(html, urls[i]);
    if (!data.wineName) {
      console.log(' NO TITLE');
      failed++;
    } else {
      appendFileSync(OUTPUT_FILE, JSON.stringify(data) + '\n');
      scraped++;
      const cases = data.casesProduced ? `${data.casesProduced}cs` : '-';
      const bottled = data.bottlingDate || '-';
      console.log(` ${data.vintage} ${data.wineName} (${data.grapes.length}gr, ABV:${data.abv || '-'}, ${cases}, btl:${bottled}, ${data.scores.length}sc)`);
    }

    saveProgress({ lastIndex: i });
    if (i < urls.length - 1) await sleep(DELAY_MS);
  }

  console.log('\n========================================');
  console.log('  TABLAS CREEK SCRAPE COMPLETE');
  console.log('========================================');
  console.log(`  Scraped: ${scraped}`);
  console.log(`  Failed: ${failed}`);
  console.log(`  Output: ${OUTPUT_FILE}`);
}

// ── DB Insertion ────────────────────────────────────────────
const GRAPE_ALIASES = {
  'mourvedre': 'Mourv\u00e8dre', 'mourvèdre': 'Mourv\u00e8dre',
  'grenache': 'Grenache', 'grenache noir': 'Grenache', 'grenache blanc': 'Grenache Blanc',
  'syrah': 'Syrah', 'counoise': 'Counoise', 'cinsaut': 'Cinsaut',
  'roussanne': 'Roussanne', 'marsanne': 'Marsanne', 'viognier': 'Viognier',
  'vermentino': 'Vermentino', 'picpoul blanc': 'Picpoul', 'picpoul': 'Picpoul',
  'clairette blanche': 'Clairette', 'clairette': 'Clairette',
  'bourboulenc': 'Bourboulenc', 'picardan': 'Picardan',
  'tannat': 'Tannat', 'petit manseng': 'Petit Manseng',
  'cabernet sauvignon': 'Cabernet Sauvignon', 'pinot noir': 'Pinot Noir',
  'chardonnay': 'Chardonnay', 'vaccarese': 'Vaccar\u00e8se', 'vaccarèse': 'Vaccar\u00e8se',
  'muscardin': 'Muscardin', 'terret noir': 'Terret Noir',
};

function classifyVarietal(grapes, wineName) {
  const name = wineName.toLowerCase();

  // Check common Tablas Creek names
  if (name.includes('esprit de tablas') && !name.includes('blanc')) return 'Rh\u00f4ne Blend';
  if (name.includes('esprit de tablas') && name.includes('blanc')) return 'White Blend';
  if (name.includes('esprit de beaucastel') && !name.includes('blanc')) return 'Rh\u00f4ne Blend';
  if (name.includes('esprit de beaucastel') && name.includes('blanc')) return 'White Blend';
  if (name.includes('c\u00f4tes de tablas') || name.includes('cotes de tablas')) {
    return name.includes('blanc') ? 'White Blend' : 'Rh\u00f4ne Blend';
  }
  if (name.includes('patelin')) {
    if (name.includes('blanc')) return 'White Blend';
    if (name.includes('ros\u00e9') || name.includes('rose')) return 'Ros\u00e9 Blend';
    return 'Rh\u00f4ne Blend';
  }
  if (name.includes('panoplie')) return 'Rh\u00f4ne Blend';
  if (name.includes('en gobelet')) return 'Rh\u00f4ne Blend';
  if (name.includes('dianthus')) return 'Ros\u00e9 Blend';
  if (name.includes('ros\u00e9') || name.includes('rose')) return 'Ros\u00e9 Blend';

  if (grapes.length > 0) {
    const primary = grapes[0];
    if (primary.percentage >= 75) {
      const g = primary.grape.toLowerCase();
      if (g.includes('mourv')) return 'Mourv\u00e8dre';
      if (g.includes('grenache blanc')) return 'Grenache Blanc';
      if (g.includes('grenache')) return 'Grenache';
      if (g.includes('syrah')) return 'Syrah';
      if (g.includes('roussanne')) return 'Roussanne';
      if (g.includes('viognier')) return 'Viognier';
      if (g.includes('vermentino')) return 'Vermentino';
      if (g.includes('tannat')) return 'Tannat';
      if (g.includes('picpoul')) return 'Picpoul';
      if (g.includes('marsanne')) return 'Marsanne';
      if (g.includes('counoise')) return 'Counoise';
      if (g.includes('pinot noir')) return 'Pinot Noir';
      if (g.includes('cabernet')) return 'Cabernet Sauvignon';
      if (g.includes('chardonnay')) return 'Chardonnay';
      if (g.includes('petit manseng')) return 'Petit Manseng';
    }
  }

  // Default: check if white or red
  const whiteGrapes = ['roussanne', 'marsanne', 'viognier', 'grenache blanc', 'picpoul', 'vermentino', 'clairette', 'bourboulenc'];
  if (grapes.length > 0 && grapes.every(g => whiteGrapes.some(wg => g.grape.toLowerCase().includes(wg)))) return 'White Blend';
  if (name.includes('blanc')) return 'White Blend';
  return 'Rh\u00f4ne Blend';
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

function parseBottlingDate(dateStr) {
  if (!dateStr) return null;
  const months = { january:1, february:2, march:3, april:4, may:5, june:6, july:7, august:8, september:9, october:10, november:11, december:12 };
  const m = dateStr.match(/(\w+)\s+(\d{4})/);
  if (m) {
    const mo = months[m[1].toLowerCase()];
    if (mo) return `${m[2]}-${String(mo).padStart(2, '0')}-01`;
  }
  return null;
}

async function insertData() {
  console.log('\n=== TABLAS CREEK DB INSERTION ===\n');

  const lines = readFileSync(OUTPUT_FILE, 'utf8').trim().split('\n');
  let entries = lines.map(l => JSON.parse(l)).filter(e => e.wineName && e.vintage);
  console.log(`Loaded ${entries.length} wine entries`);

  // Reference data
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

  let { data: sourceTypes } = await sb.from('source_types').select('id,slug');
  const winerySourceId = sourceTypes.find(s => s.slug === 'winery-website')?.id;

  const publications = await fetchAll('publications', 'id,name,slug');
  const pubMap = new Map(publications.map(p => [p.name.toLowerCase(), p.id]));
  for (const p of publications) pubMap.set(p.slug, p.id);
  const PUB_ALIASES = {
    'wine advocate': 'wine advocate', 'the wine advocate': 'wine advocate', 'robert parker wine advocate': 'wine advocate',
    'vinous': 'vinous', 'vinous media': 'vinous',
    'jamessuckling.com': 'james suckling', 'james suckling': 'james suckling',
    'wine enthusiast': 'wine enthusiast', 'wine spectator': 'wine spectator',
    'jeb dunnuck': 'jeb dunnuck', 'jebdunnuck.com': 'jeb dunnuck',
    'decanter': 'decanter', 'wine & spirits': 'wine & spirits',
    'owen bargreen': 'owenbargreen.com',
  };
  for (const [alias, canonical] of Object.entries(PUB_ALIASES)) {
    const id = pubMap.get(canonical);
    if (id) pubMap.set(alias, id);
  }

  // Create producer
  let producerId;
  const { data: existingProd } = await sb.from('producers').select('id').eq('slug', 'tablas-creek-vineyard').single();
  if (existingProd) {
    producerId = existingProd.id;
    console.log(`Using existing producer: ${producerId}`);
  } else {
    producerId = randomUUID();
    const { error } = await sb.from('producers').insert({
      id: producerId, slug: 'tablas-creek-vineyard', name: 'Tablas Creek Vineyard',
      name_normalized: normalize('Tablas Creek Vineyard'), country_id: countryId,
      website_url: 'https://tablascreek.com', year_established: 1989,
      metadata: {
        partnership: 'Ch\u00e2teau de Beaucastel (Perrin family) and Robert Haas',
        appellations: ['Adelaida District', 'Paso Robles'],
        certifications: ['Regenerative Organic Certified', 'CCOF Organic'],
      },
    });
    if (error) { console.error('Producer error:', error.message); process.exit(1); }
    console.log(`Created producer: ${producerId}`);
  }

  // Dedup by wineName + vintage
  const entryMap = new Map();
  for (const e of entries) {
    const key = `${e.wineName.toLowerCase()}|${e.vintage}`;
    if (!entryMap.has(key)) entryMap.set(key, e);
  }
  const uniqueEntries = [...entryMap.values()];
  console.log(`${uniqueEntries.length} unique wine-vintage entries`);

  // Group by wine name
  const winesByName = new Map();
  for (const e of uniqueEntries) {
    const key = e.wineName.toLowerCase();
    if (!winesByName.has(key)) winesByName.set(key, []);
    winesByName.get(key).push(e);
  }
  console.log(`${winesByName.size} unique wine names`);

  // Appellation aliases
  const APP_ALIASES = {
    'adelaida district paso robles': 'adelaida district',
    'adelaida district, paso robles': 'adelaida district',
    'paso robles': 'paso robles',
    'paso robles estrella district': 'paso robles',
  };

  // Create wines
  console.log('\nCreating wine records...');
  const wineIdMap = new Map();
  let wineCount = 0;
  for (const [normName, vintages] of winesByName) {
    const latest = vintages.sort((a, b) => (b.vintage || 0) - (a.vintage || 0))[0];
    const appName = (latest.appellation || '').toLowerCase().trim();
    const appLookup = APP_ALIASES[appName] || appName;
    const appellationId = appellationMap.get(appLookup) || null;
    const varietalName = classifyVarietal(latest.grapes, latest.wineName);
    let varietalId = varietalMap.get(varietalName.toLowerCase()) || varietalMap.get(slugify(varietalName));
    if (!varietalId) varietalId = varietalMap.get('red-blend');

    const wineId = randomUUID();
    const wineSlug = slugify(`tablas-creek-${latest.wineName}`);
    const { error } = await sb.from('wines').insert({
      id: wineId, slug: wineSlug, name: latest.wineName, name_normalized: normalize(latest.wineName),
      producer_id: producerId, country_id: countryId, region_id: regionId,
      appellation_id: appellationId, varietal_category_id: varietalId, varietal_category_source: winerySourceId,
      food_pairings: latest.foodPairings.length > 0 ? latest.foodPairings.join(', ') : null,
      metadata: {
        certifications: latest.certifications,
        tablas_creek_url: latest.url,
      },
    });
    if (error) { console.error(`  Wine "${latest.wineName}": ${error.message}`); continue; }
    wineIdMap.set(normName, wineId);
    wineCount++;
  }
  console.log(`  Created ${wineCount} wines`);

  // Create grapes
  console.log('\nCreating grape compositions...');
  let grapeCount = 0;
  for (const [normName, vintages] of winesByName) {
    const wineId = wineIdMap.get(normName);
    if (!wineId) continue;
    const latest = vintages.sort((a, b) => (b.vintage || 0) - (a.vintage || 0))[0];
    for (const g of latest.grapes) {
      const grapeName = GRAPE_ALIASES[g.grape.toLowerCase()] || g.grape;
      let grapeId = grapeMap.get(grapeName.toLowerCase());
      if (!grapeId) {
        // Try to create the grape
        const { data: newGrape, error } = await sb.from('grapes').insert({
          name: grapeName, slug: slugify(grapeName), name_normalized: normalize(grapeName),
        }).select('id').single();
        if (newGrape) { grapeId = newGrape.id; grapeMap.set(grapeName.toLowerCase(), grapeId); console.log(`    Created grape: ${grapeName}`); }
        else if (error) { console.warn(`    Grape "${g.grape}": ${error.message}`); continue; }
      }
      const { error } = await sb.from('wine_grapes').insert({
        wine_id: wineId, grape_id: grapeId, percentage: g.percentage, percentage_source: winerySourceId,
      });
      if (!error) grapeCount++;
    }
  }
  console.log(`  Created ${grapeCount} grape entries`);

  // Create vintages
  console.log('\nCreating vintage records...');
  let vintageCount = 0;
  for (const e of uniqueEntries) {
    const wineId = wineIdMap.get(e.wineName.toLowerCase());
    if (!wineId) continue;

    const metadata = {};
    if (e.productionNotes) metadata.production_notes = e.productionNotes;
    if (e.blendingDate) metadata.blending_date = e.blendingDate;
    if (e.certifications.length > 0) metadata.certifications = e.certifications;

    const { error } = await sb.from('wine_vintages').insert({
      wine_id: wineId, vintage_year: e.vintage, abv: e.abv || null,
      cases_produced: e.casesProduced || null,
      bottling_date: parseBottlingDate(e.bottlingDate),
      winemaker_notes: e.tastingNotes || null, vintage_notes: e.productionNotes || null,
      release_price_usd: null,
      metadata: Object.keys(metadata).length > 0 ? metadata : {},
    });
    if (error) { if (!error.message.includes('duplicate')) console.error(`  ${e.vintage} ${e.wineName}: ${error.message}`); }
    else vintageCount++;
    if (vintageCount % 50 === 0) process.stdout.write(`  ${vintageCount} vintages...\r`);
  }
  console.log(`  Created ${vintageCount} vintages`);

  // Create scores
  console.log('\nCreating score records...');
  let scoreCount = 0;
  for (const e of uniqueEntries) {
    const wineId = wineIdMap.get(e.wineName.toLowerCase());
    if (!wineId || !e.scores) continue;
    for (const s of e.scores) {
      const pubKey = s.publication.toLowerCase().trim();
      let pubId = pubMap.get(pubKey) || pubMap.get(PUB_ALIASES[pubKey] || '');
      if (!pubId) {
        // Try creating
        const pubSlug = slugify(s.publication);
        const { data: newPub } = await sb.from('publications').insert({
          slug: pubSlug, name: s.publication, type: 'critic_publication',
        }).select('id').single();
        if (newPub) { pubId = newPub.id; pubMap.set(pubKey, pubId); console.log(`    Created pub: ${s.publication}`); }
        else {
          const { data: existing } = await sb.from('publications').select('id').eq('slug', pubSlug).single();
          if (existing) { pubId = existing.id; pubMap.set(pubKey, pubId); }
        }
      }
      if (!pubId) continue;
      const { error } = await sb.from('wine_vintage_scores').insert({
        wine_id: wineId, vintage_year: e.vintage, score: s.score, score_scale: '100',
        publication_id: pubId, source_id: winerySourceId, url: e.url, discovered_at: new Date().toISOString(),
      });
      if (!error) scoreCount++;
    }
  }
  console.log(`  Created ${scoreCount} scores`);

  console.log('\n========================================');
  console.log('   TABLAS CREEK IMPORT COMPLETE');
  console.log('========================================');
  console.log(`  Producer: Tablas Creek Vineyard (${producerId})`);
  console.log(`  Wines: ${wineCount}`);
  console.log(`  Vintages: ${vintageCount}`);
  console.log(`  Scores: ${scoreCount}`);
  console.log(`  Grape entries: ${grapeCount}`);
}

async function main() {
  if (INSERT_MODE) await insertData();
  else await scrape();
}
main().catch(console.error);
