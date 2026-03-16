#!/usr/bin/env node
/**
 * import_shopify_wines.mjs — Generic Shopify wine catalog importer
 *
 * Imports wines from any Shopify wine retailer's JSON catalog.
 * Handles multiple tag formats:
 *   - Flat tags: "Cabernet Sauvignon", "California", "usa"
 *   - Key:value tags: "country:Italy", "grape:nebbiolo", "region:Piedmont"
 *   - Operational tags: "freeship:6", "status:dailyoffer" (ignored)
 *
 * Usage:
 *   node scripts/import_shopify_wines.mjs <catalog-json> <source-name> [--dry-run] [--max-price=N]
 *
 * Examples:
 *   node scripts/import_shopify_wines.mjs data/imports/best_wine_store_raw.json "The Best Wine Store" --max-price=15
 *   node scripts/import_shopify_wines.mjs data/imports/domestique_wine_raw.json "Domestique Wine"
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';
import { randomUUID } from 'crypto';

// ── Load .env ───────────────────────────────────────────────
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
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

// ── CLI ─────────────────────────────────────────────────────
const args = process.argv.slice(2);
const jsonPath = args.find(a => !a.startsWith('--'));
const sourceName = args.filter(a => !a.startsWith('--'))[1];
const DRY_RUN = args.includes('--dry-run');
const maxPriceArg = args.find(a => a.startsWith('--max-price='));
const MAX_PRICE = maxPriceArg ? parseFloat(maxPriceArg.split('=')[1]) : Infinity;

if (!jsonPath || !sourceName) {
  console.error('Usage: node scripts/import_shopify_wines.mjs <catalog.json> "<Source Name>" [--dry-run] [--max-price=N]');
  process.exit(1);
}

const sourceSlug = sourceName.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

// ── Helpers ─────────────────────────────────────────────────
function normalize(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

function slugify(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
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

// ── Skip filters ────────────────────────────────────────────
function shouldSkip(product) {
  if (/Gift Card|T-Shirt|Cool-Pack|Shipping|Opener|Corkscrew|Glass Set/i.test(product.title)) return true;
  if (/^(TEST|DBG)/i.test(product.title)) return true;
  if (/DO NOT SELL/i.test(product.title)) return true;
  if (product.product_type && /Spirit|Accessory|Merch|Gift/i.test(product.product_type)) return true;
  return false;
}

// ── Tag parsing ─────────────────────────────────────────────
// Extract structured data from tags — handles both flat and key:value formats
function parseTags(tags) {
  const result = {
    grapes: [],
    countries: [],
    regions: [],
    color: null,
    vintage: null,
    isOrganic: false,
    isSparkling: false,
  };

  // Known flat-tag grapes (common varieties that appear as plain tags)
  const FLAT_GRAPES = new Set([
    'cabernet sauvignon', 'pinot noir', 'chardonnay', 'merlot', 'sauvignon blanc',
    'zinfandel', 'syrah', 'shiraz', 'malbec', 'pinot grigio', 'pinot gris',
    'riesling', 'gewurztraminer', 'grenache', 'tempranillo', 'sangiovese',
    'nebbiolo', 'barbera', 'primitivo', 'petit verdot', 'petite sirah',
    'cabernet franc', 'viognier', 'muscat', 'moscato', 'prosecco',
    'chenin blanc', 'verdejo', 'albarino', 'gruner veltliner',
    'mourvedre', 'carignan', 'gamay', 'verdicchio', 'trebbiano',
    'montepulciano', 'aglianico', 'nero d\'avola', 'lambrusco',
    'touriga nacional', 'red blend', 'white blend', 'bordeaux blend red',
    'fruit flavored', 'ribolla gialla', 'loureiro', 'elbling',
  ]);

  // Known flat-tag countries
  const FLAT_COUNTRIES = new Map([
    ['usa', 'United States'], ['france', 'France'], ['italy', 'Italy'],
    ['spain', 'Spain'], ['australia', 'Australia'], ['argentina', 'Argentina'],
    ['chile', 'Chile'], ['germany', 'Germany'], ['portugal', 'Portugal'],
    ['new zealand', 'New Zealand'], ['south africa', 'South Africa'],
    ['austria', 'Austria'], ['greece', 'Greece'], ['hungary', 'Hungary'],
  ]);

  // Known flat-tag regions
  const FLAT_REGIONS = new Set([
    'california', 'napa valley', 'sonoma county', 'sonoma coast', 'carneros',
    'paso robles', 'central coast', 'russian river valley', 'dry creek valley',
    'alexander valley', 'knights valley', 'mendocino', 'willamette valley',
    'columbia valley', 'walla walla valley', 'south eastern australia',
    'barossa valley', 'mclaren vale', 'clare valley', 'marlborough',
    'temecula valley', 'monterey', 'lodi', 'piedmont', 'tuscany',
    'sicily', 'veneto', 'puglia', 'bordeaux', 'burgundy', 'champagne',
    'rhone', 'loire', 'alsace', 'languedoc', 'provence', 'rioja',
    'stellenbosch', 'mendoza', 'uco valley', 'mosel', 'pfalz',
    'beaujolais', 'emilia-romagna', 'calabria', 'oregon', 'virginia',
  ]);

  for (const tag of tags) {
    const lower = tag.toLowerCase().trim();

    // Key:value format (Domestique style)
    if (lower.startsWith('country:')) {
      result.countries.push(tag.slice(8).trim());
      continue;
    }
    if (lower.startsWith('grape:')) {
      result.grapes.push(tag.slice(6).trim());
      continue;
    }
    if (lower.startsWith('region:')) {
      result.regions.push(tag.slice(7).trim());
      continue;
    }
    if (lower.startsWith('type:')) {
      const type = tag.slice(5).trim().toLowerCase();
      if (type === 'red') result.color = 'red';
      else if (type === 'white') result.color = 'white';
      else if (type === 'rose' || type === 'rosé') result.color = 'rose';
      else if (type === 'orange') result.color = 'orange';
      else if (type === 'sparkling') result.isSparkling = true;
      continue;
    }
    if (lower.startsWith('vintage:')) {
      result.vintage = parseInt(tag.slice(8).trim());
      continue;
    }
    if (lower.startsWith('certified:')) {
      if (tag.slice(10).trim().toLowerCase() === 'yes') result.isOrganic = true;
      continue;
    }

    // Skip operational tags
    if (/^(freeship|status|display|category|profile|staffpick|importer|past wine)/i.test(lower)) continue;
    // Skip price range tags
    if (/^(under-?\d+|\d+-\d+|over-?\d+)$/.test(lower)) continue;
    // Skip food pairing tags
    if (/^(beef|chicken|lamb|pork|salad|cheeses|crab|lobster|shellfish|cakes|cream|dessert|parmesan)/i.test(lower)) continue;
    if (/^(beef-venison|salad-green|shellfish-crab|cakes-and)/i.test(lower)) continue;
    // Skip format tags
    if (/^(mini|split|can|popular|constellation|dave phinney)$/i.test(lower)) continue;
    // Skip style tags
    if (/^(natty|glou glou|crowdpleaser|young|bold|stone|east|island|hybrid|lacave|pet nat|petnat)/i.test(lower)) continue;
    if (/^(fallwhite|staff pick|women-made)/i.test(lower)) continue;

    // Flat tag matching
    if (lower.startsWith('vintage ')) {
      const yr = parseInt(lower.replace('vintage ', ''));
      if (yr >= 1990 && yr <= 2030) result.vintage = yr;
      continue;
    }
    if (FLAT_COUNTRIES.has(lower)) {
      result.countries.push(FLAT_COUNTRIES.get(lower));
      continue;
    }
    if (FLAT_REGIONS.has(lower)) {
      result.regions.push(tag);
      continue;
    }
    if (FLAT_GRAPES.has(lower)) {
      result.grapes.push(tag);
      continue;
    }

    // Color from flat tags
    if (lower === 'rose' || lower === 'rosé') { result.color = 'rose'; continue; }
    if (lower === 'sparkling') { result.isSparkling = true; continue; }
    if (lower === 'extra dry' || lower === 'brut') { result.isSparkling = true; continue; }
    if (lower === 'organic') { result.isOrganic = true; continue; }
  }

  return result;
}

// ── Known grape patterns for title matching ──────────────────
const GRAPE_PATTERNS = [
  'Cabernet Sauvignon', 'Sauvignon Blanc', 'Pinot Noir', 'Pinot Grigio',
  'Pinot Gris', 'Chenin Blanc', 'Grüner Veltliner', 'Gruner Veltliner',
  'Petite Sirah', 'Petit Verdot', 'Cabernet Franc', 'Ribolla Gialla',
  'Touriga Nacional', 'Nero d\'Avola', 'Pineau d\'Aunis',
  'Chardonnay', 'Zinfandel', 'Merlot', 'Syrah', 'Shiraz', 'Malbec',
  'Grenache', 'Nebbiolo', 'Sangiovese', 'Tempranillo', 'Primitivo',
  'Carignan', 'Mourvèdre', 'Mourvedre', 'Viognier', 'Riesling',
  'Gewurztraminer', 'Gewürztraminer', 'Muscat', 'Moscato', 'Barbera',
  'Aglianico', 'Gamay', 'Verdejo', 'Verdicchio', 'Trebbiano',
  'Montepulciano', 'Lambrusco', 'Prosecco', 'Loureiro', 'Elbling',
  'Albarino', 'Albariño',
  'Red Blend', 'Proprietary Red', 'Bordeaux Blend',
];

// ── Score extraction from body_html ───────────────────────────
function extractScores(bodyHtml) {
  if (!bodyHtml) return [];
  const text = bodyHtml.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ');
  const scores = [];
  const pointsPattern = /(\d{2,3})\s*(?:POINTS?|points?)/g;
  let match;
  while ((match = pointsPattern.exec(text)) !== null) {
    const score = parseInt(match[1]);
    if (score >= 80 && score <= 100) {
      const context = text.slice(Math.max(0, match.index - 200), match.index + 200);
      let publication = null;
      if (/Robert Parker|Wine Advocate/i.test(context)) publication = 'Wine Advocate';
      else if (/James Suckling/i.test(context)) publication = 'James Suckling';
      else if (/Wine Spectator/i.test(context)) publication = 'Wine Spectator';
      else if (/Wine Enthusiast/i.test(context)) publication = 'Wine Enthusiast';
      else if (/Decanter/i.test(context)) publication = 'Decanter';
      else if (/Vinous|Antonio Galloni/i.test(context)) publication = 'Vinous';
      else if (/Jeb Dunnuck/i.test(context)) publication = 'Jeb Dunnuck';
      if (!scores.find(s => s.score === score && s.publication === publication)) {
        scores.push({ score, publication });
      }
    }
  }
  return scores;
}

// ── Bottle format from title ──────────────────────────────────
function extractBottleFormat(title) {
  const formats = [
    { pattern: /\(6\s*Liter\)/i, name: '6L', ml: 6000 },
    { pattern: /\(3\s*Liter\)/i, name: 'Jeroboam', ml: 3000 },
    { pattern: /\(Magnum\s*1?\.?5?L?\)/i, name: 'Magnum', ml: 1500 },
    { pattern: /Magnum\s*1\.5L/i, name: 'Magnum', ml: 1500 },
    { pattern: /\(Half\s*Bottle\s*375\s*m[Ll]\)/i, name: 'Half Bottle', ml: 375 },
    { pattern: /375\s*m[Ll]/i, name: 'Half Bottle', ml: 375 },
    { pattern: /500\s*m[Ll]/i, name: '500ml', ml: 500 },
    { pattern: /250\s*m[Ll]/i, name: '250ml', ml: 250 },
    { pattern: /187\s*m[Ll]/i, name: '187ml', ml: 187 },
    { pattern: /1\s*L\b/i, name: 'Liter', ml: 1000 },
    { pattern: /1500\s*m[Ll]/i, name: 'Magnum', ml: 1500 },
  ];
  for (const f of formats) {
    if (f.pattern.test(title)) {
      return { name: f.name, ml: f.ml, cleanTitle: title.replace(f.pattern, '').trim() };
    }
  }
  return { name: 'Standard', ml: 750, cleanTitle: title };
}

// ── Title parser ──────────────────────────────────────────────
function parseTitle(rawTitle, appellationMap, regionMap) {
  const { cleanTitle } = extractBottleFormat(rawTitle);

  // Extract vintage or NV
  let vintage = null;
  let title = cleanTitle;
  const vintageMatch = title.match(/\b((?:19|20)\d{2})\s*$/);
  if (vintageMatch) {
    vintage = parseInt(vintageMatch[1]);
    title = title.slice(0, vintageMatch.index).trim();
  } else if (/\bNV\s*$/i.test(title)) {
    title = title.replace(/\bNV\s*$/i, '').trim();
  }

  // Strip format suffixes that weren't caught
  title = title.replace(/\s*\d+\s*(Pack|pk)\s*$/i, '').trim();

  // Match grape variety
  let grape = null;
  let grapeStart = -1;
  let grapeEnd = -1;
  for (const gp of GRAPE_PATTERNS) {
    const regex = new RegExp(`\\b${gp.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
    const gm = title.match(regex);
    if (gm) {
      grape = gp;
      grapeStart = gm.index;
      grapeEnd = gm.index + gm[0].length;
      break;
    }
  }

  // Match appellation/region from remaining title
  let appellation = null;
  let appellationName = null;
  let region = null;
  let regionName = null;

  const geoSection = grape ? title.slice(grapeEnd).trim() : title;
  const geoWords = geoSection.split(/\s+/).filter(Boolean);

  for (let start = 0; start < geoWords.length; start++) {
    const candidate = geoWords.slice(start).join(' ');
    const candidateNorm = normalize(candidate);

    const app = appellationMap.get(candidateNorm);
    if (app) {
      appellation = app;
      appellationName = candidate;
      break;
    }

    const reg = regionMap.get(candidateNorm);
    if (reg && !reg.is_catch_all) {
      region = reg;
      regionName = candidate;
      break;
    }
  }

  // Determine producer name
  let producerName = null;
  if (grape && grapeStart > 0) {
    producerName = title.slice(0, grapeStart).trim();
  } else if (appellationName) {
    const appIdx = title.indexOf(appellationName);
    if (appIdx > 0) producerName = title.slice(0, appIdx).trim();
  } else {
    producerName = title;
  }

  // Build display name
  let displayName = rawTitle;
  if (vintage) displayName = displayName.replace(/\s*\b\d{4}\s*$/, '').trim();
  displayName = displayName
    .replace(/\(Magnum\s*1?\.?5?L?\)/i, '')
    .replace(/\(Half\s*Bottle\s*375\s*m[Ll]\)/i, '')
    .replace(/\(500\s*m[Ll]\)/i, '')
    .replace(/\s*\d+\s*(Pack|pk)\s*$/i, '')
    .trim();

  return { producerName: producerName || 'Unknown', displayName, grape, vintage, appellation, appellationName, region, regionName };
}

// ── Country inference ─────────────────────────────────────────
const COUNTRY_PATTERNS = [
  { pattern: /napa|sonoma|california|oregon|washington|willamette|paso robles|russian river|carneros|columbia valley|walla walla|amador|lodi|mendocino|santa barbara|santa maria|santa cruz|central coast|stags? leap|moon mountain|dry creek|alexander valley|knights valley|oak knoll|oakville|rutherford|calistoga|spring mountain|st\.?\s*helena|suisun|red hills|lake county|north coast|anderson valley|adelaida|cienega|arroyo seco|diamond mountain|mokelumne|san luis obispo|sta\.?\s*rita|temecula|los carneros|contra costa|el dorado/i, country: 'United States' },
  { pattern: /toscana|tuscany|barolo|brunello|montalcino|chianti|piemonte|piedmont|langhe|barbaresco|delle venezie|valpolicella|salento|carmignano|basilicata|molise|montello|sant.antimo|morellino|scansano|abruzzo|marche|puglia|sicily|sicilia|calabria|veneto|friuli|emilia|romagna|campania|sardinia|umbria/i, country: 'Italy' },
  { pattern: /bordeaux|bourgogne|burgundy|champagne|rhône|rhone|chablis|sancerre|loire|languedoc|roussillon|limoux|luberon|corbières|corbieres|gigondas|châteauneuf|chateauneuf|hermitage|crozes|tavel|juliénas|julienas|chiroubles|fronsac|saint[- ]émilion|saint[- ]emilion|pessac|graves|menetou|vouvray|muscadet|côtes du rhône|cotes du rhone|costieres|nimes|sauternes|alsace|provence|beaujolais|jura|savoie|cremant|giennois/i, country: 'France' },
  { pattern: /rioja|toro|jumilla|txakolina|getariako|terra alta|catalonia/i, country: 'Spain' },
  { pattern: /marlborough|canterbury|hawkes bay/i, country: 'New Zealand' },
  { pattern: /south australia|south eastern australia|clare valley|coonawarra|yarra valley|grampians|barossa|mclaren vale|hunter valley|victoria/i, country: 'Australia' },
  { pattern: /stellenbosch|noordhoek|cape point|constantia/i, country: 'South Africa' },
  { pattern: /mendoza|uco valley/i, country: 'Argentina' },
  { pattern: /kremstal|wagram|kamptal/i, country: 'Austria' },
  { pattern: /tokaji/i, country: 'Hungary' },
  { pattern: /vinho verde|douro|alentejo/i, country: 'Portugal' },
  { pattern: /mosel|pfalz|rheingau|rheinhessen/i, country: 'Germany' },
  { pattern: /crete/i, country: 'Greece' },
];

// ── PUB aliases ─────────────────────────────────────────────
const PUB_ALIASES = {
  'wine advocate': 'Wine Advocate',
  'james suckling': 'James Suckling',
  'wine spectator': 'Wine Spectator',
  'wine enthusiast': 'Wine Enthusiast',
  'decanter': 'Decanter',
  'vinous': 'Vinous',
  'jeb dunnuck': 'Jeb Dunnuck',
};

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`  SHOPIFY WINE IMPORT: ${sourceName}`);
  console.log(`  ${DRY_RUN ? '(DRY RUN)' : '(INSERT MODE)'}${MAX_PRICE < Infinity ? ` | Max price: $${MAX_PRICE}` : ''}`);
  console.log(`${'='.repeat(60)}\n`);

  // Load catalog
  const catalog = JSON.parse(readFileSync(jsonPath, 'utf-8'));
  let wineProducts = catalog.filter(p => !shouldSkip(p));
  if (MAX_PRICE < Infinity) {
    wineProducts = wineProducts.filter(p => p.price <= MAX_PRICE);
  }
  console.log(`Catalog: ${catalog.length} total, ${wineProducts.length} wines to import\n`);

  // ── Load reference data ──────────────────────────────────
  console.log('Loading reference data...');

  const countries = await fetchAll('countries', 'id,name,iso_code');
  const countryMap = new Map();
  for (const c of countries) {
    countryMap.set(c.name.toLowerCase(), c.id);
    if (c.iso_code) countryMap.set(c.iso_code.toLowerCase(), c.id);
  }

  const regions = await fetchAll('regions', 'id,name,country_id,parent_id,is_catch_all');
  const regionMap = new Map();
  for (const r of regions) {
    regionMap.set(normalize(r.name), r);
    regionMap.set(`${normalize(r.name)}|${r.country_id}`, r);
  }

  const appellations = await fetchAll('appellations', 'id,name,designation_type,country_id,region_id');
  const appellationMap = new Map();
  for (const a of appellations) appellationMap.set(normalize(a.name), a);

  const aliases = await fetchAll('appellation_aliases', 'appellation_id,alias_normalized');
  for (const al of aliases) {
    const app = appellations.find(a => a.id === al.appellation_id);
    if (app && !appellationMap.has(al.alias_normalized)) {
      appellationMap.set(al.alias_normalized, app);
    }
  }

  const grapes = await fetchAll('grapes', 'id,name,display_name,color');
  const grapeMap = new Map();
  for (const g of grapes) {
    if (g.display_name) grapeMap.set(g.display_name.toLowerCase(), g);
    grapeMap.set(g.name.toLowerCase(), g);
  }

  const synonyms = await fetchAll('grape_synonyms', 'grape_id,synonym');
  const synMap = new Map();
  for (const s of synonyms) synMap.set(s.synonym.toLowerCase(), s.grape_id);

  const publications = await fetchAll('publications', 'id,name,slug');
  const pubMap = new Map();
  for (const p of publications) {
    pubMap.set(p.name.toLowerCase(), p.id);
    pubMap.set(p.slug, p.id);
  }
  for (const [alias, canonical] of Object.entries(PUB_ALIASES)) {
    const id = pubMap.get(canonical.toLowerCase());
    if (id) pubMap.set(alias.toLowerCase(), id);
  }

  const sourceTypes = await fetchAll('source_types', 'id,slug');
  const sourceTypeMap = new Map(sourceTypes.map(s => [s.slug, s.id]));
  const retailerSourceId = sourceTypeMap.get('retailer-website') || sourceTypeMap.get('importer-website') || sourceTypeMap.get('producer-website');

  console.log(`  Countries: ${countries.length}, Regions: ${regions.length}`);
  console.log(`  Appellations: ${appellations.length}, Aliases: ${aliases.length}`);
  console.log(`  Grapes: ${grapes.length}, Synonyms: ${synonyms.length}\n`);

  // ── Grape resolver ────────────────────────────────────────
  function resolveGrape(name) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    if (['red blend', 'proprietary red', 'bordeaux blend', 'white blend', 'bordeaux blend red', 'fruit flavored'].includes(lower)) return null;
    if (lower === 'pinot grigio') return grapeMap.get('pinot gris')?.id || null;
    if (lower === 'gruner veltliner') return grapeMap.get('grüner veltliner')?.id || null;
    if (lower === 'petite sirah') return grapeMap.get('durif')?.id || null;
    if (lower === 'prosecco') return grapeMap.get('glera')?.id || null;
    if (lower === 'moscato') return grapeMap.get('muscat blanc à petits grains')?.id || synMap.get('moscato') || null;
    const g = grapeMap.get(lower);
    if (g) return g.id;
    const synId = synMap.get(lower);
    if (synId) return synId;
    const stripped = normalize(name);
    const g2 = grapeMap.get(stripped);
    if (g2) return g2.id;
    const syn2 = synMap.get(stripped);
    if (syn2) return syn2;
    return null;
  }

  // ── Stats ──────────────────────────────────────────────────
  const stats = {
    producers: 0, wines: 0, vintages: 0, scores: 0,
    wineGrapes: 0, prices: 0,
    appellationHits: 0, appellationMisses: 0,
    grapeHits: 0, grapeMisses: 0,
    producerReuses: 0, skipped: 0,
    warnings: [],
    grapeMissNames: new Set(),
  };

  const producerIdMap = new Map();
  let processed = 0;

  for (const product of wineProducts) {
    processed++;
    const tagData = parseTags(product.tags);
    const parsed = parseTitle(product.title, appellationMap, regionMap);
    const scores = extractScores(product.body_html);

    // Color: from tags, product_type, or title
    let color = tagData.color;
    if (!color) {
      const pt = (product.product_type || '').toLowerCase();
      if (pt.includes('red')) color = 'red';
      else if (pt.includes('white')) color = 'white';
      else if (pt.includes('ros')) color = 'rose';
      else if (/Rosé|Rose\b/i.test(product.title)) color = 'rose';
    }

    // Wine type
    let wineType = 'table';
    if (tagData.isSparkling || product.product_type === 'Champagne' || /Brut|Cremant|Champagne|Sparkling|Pét-Nat|Prosecco/i.test(product.title)) {
      wineType = 'sparkling';
    }
    if (/Sauternes|Late Harvest|Tokaji|Dessert/i.test(product.title)) wineType = 'dessert';
    if (/Port|Sherry|Madeira|Marsala|Vermouth/i.test(product.title)) wineType = 'fortified';

    // Vintage: from tags or title
    let vintage = tagData.vintage || parsed.vintage;

    // Grapes: from tags or title
    const grapeNames = tagData.grapes.length > 0 ? tagData.grapes : (parsed.grape ? [parsed.grape] : []);

    // Country: from tags, appellation, title patterns
    let countryId = null;
    if (tagData.countries.length > 0) {
      countryId = countryMap.get(tagData.countries[0].toLowerCase());
    }
    if (!countryId) countryId = parsed.appellation?.country_id || parsed.region?.country_id || null;
    if (!countryId) {
      for (const cp of COUNTRY_PATTERNS) {
        if (cp.pattern.test(product.title)) {
          countryId = countryMap.get(cp.country.toLowerCase());
          break;
        }
      }
    }
    if (!countryId) {
      countryId = countryMap.get('united states'); // default
    }

    // Region
    let regionId = parsed.region?.id || null;
    if (!regionId && parsed.appellation?.region_id) regionId = parsed.appellation.region_id;
    if (!regionId && tagData.regions.length > 0) {
      for (const rn of tagData.regions) {
        const r = regionMap.get(normalize(rn));
        if (r && !r.is_catch_all) { regionId = r.id; break; }
      }
    }

    // Producer
    const producerName = product.vendor && product.vendor !== sourceName
      ? product.vendor
      : parsed.producerName;
    const producerSlug = slugify(producerName);

    let producerId;
    if (producerIdMap.has(producerSlug)) {
      producerId = producerIdMap.get(producerSlug);
      stats.producerReuses++;
    } else {
      const { data: existing } = await sb.from('producers')
        .select('id').eq('slug', producerSlug).single();

      if (existing) {
        producerId = existing.id;
        producerIdMap.set(producerSlug, producerId);
        stats.producerReuses++;
      } else if (!DRY_RUN) {
        producerId = randomUUID();
        const { error } = await sb.from('producers').insert({
          id: producerId,
          name: producerName,
          slug: producerSlug,
          name_normalized: normalize(producerName),
          country_id: countryId,
          region_id: regionId,
          metadata: { source: sourceSlug },
        });
        if (error) {
          stats.warnings.push(`Producer error "${producerName}": ${error.message}`);
          continue;
        }
        producerIdMap.set(producerSlug, producerId);
        stats.producers++;
      } else {
        producerId = `dry-${producerSlug}`;
        producerIdMap.set(producerSlug, producerId);
        stats.producers++;
      }
    }

    // Wine
    const wineSlug = slugify(parsed.displayName);
    const wineId = randomUUID();

    const wineRow = {
      id: wineId,
      producer_id: producerId,
      slug: wineSlug,
      name: parsed.displayName,
      name_normalized: normalize(parsed.displayName),
      color,
      wine_type: wineType,
      effervescence: wineType === 'sparkling' ? 'sparkling' : null,
      appellation_id: parsed.appellation?.id || null,
      country_id: countryId,
      region_id: regionId,
      metadata: { source: sourceSlug, shopify_id: product.shopify_id },
    };

    if (!DRY_RUN) {
      const { data: existingWine } = await sb.from('wines')
        .select('id').eq('slug', wineSlug).single();
      if (existingWine) { stats.skipped++; continue; }

      const { error } = await sb.from('wines').insert(wineRow);
      if (error) {
        stats.warnings.push(`Wine error "${parsed.displayName}": ${error.message}`);
        continue;
      }
    }
    stats.wines++;

    if (parsed.appellation) stats.appellationHits++;
    else stats.appellationMisses++;

    // Grapes
    for (const gName of grapeNames) {
      const grapeId = resolveGrape(gName);
      if (grapeId) {
        if (!DRY_RUN) {
          await sb.from('wine_grapes').insert({ wine_id: wineId, grape_id: grapeId, percentage: grapeNames.length === 1 ? 100 : null });
        }
        stats.wineGrapes++;
        stats.grapeHits++;
      } else {
        stats.grapeMisses++;
        stats.grapeMissNames.add(gName);
      }
    }

    // Vintage
    const vintageYear = vintage || 0;
    if (!DRY_RUN) {
      await sb.from('wine_vintages').insert({ id: randomUUID(), wine_id: wineId, vintage_year: vintageYear, metadata: { source: sourceSlug } });
    }
    stats.vintages++;

    // Scores
    for (const sc of scores) {
      const pubId = sc.publication ? pubMap.get(sc.publication.toLowerCase()) : null;
      if (!DRY_RUN) {
        const { error } = await sb.from('wine_vintage_scores').insert({
          wine_id: wineId, vintage_year: vintageYear, publication_id: pubId,
          score: sc.score, score_scale: '100-point', source_id: retailerSourceId,
        });
        if (!error) stats.scores++;
      } else stats.scores++;
    }

    // Price
    if (product.price) {
      if (!DRY_RUN) {
        await sb.from('wine_vintage_prices').insert({
          wine_id: wineId, vintage_year: vintageYear, price_usd: product.price,
          price_type: 'retail', merchant_name: sourceName,
          price_date: new Date().toISOString().slice(0, 10),
        });
      }
      stats.prices++;
    }

    if (processed % 50 === 0) console.log(`  Processed ${processed}/${wineProducts.length}...`);
  }

  // ── Summary ────────────────────────────────────────────────
  console.log(`\n${'='.repeat(60)}`);
  console.log(`  IMPORT SUMMARY${DRY_RUN ? ' (DRY RUN)' : ''}: ${sourceName}`);
  console.log(`${'='.repeat(60)}`);
  console.log(`  Producers created:  ${stats.producers} (${stats.producerReuses} reused)`);
  console.log(`  Wines created:      ${stats.wines} (${stats.skipped} skipped)`);
  console.log(`  Vintages created:   ${stats.vintages}`);
  console.log(`  Scores inserted:    ${stats.scores}`);
  console.log(`  Wine grapes linked: ${stats.wineGrapes}`);
  console.log(`  Prices recorded:    ${stats.prices}`);
  const appTotal = stats.appellationHits + stats.appellationMisses;
  console.log(`  Appellation hits:   ${stats.appellationHits}/${appTotal} (${appTotal > 0 ? Math.round(stats.appellationHits / appTotal * 100) : 0}%)`);
  const grapeTotal = stats.grapeHits + stats.grapeMisses;
  console.log(`  Grape hits:         ${stats.grapeHits}/${grapeTotal} (${grapeTotal > 0 ? Math.round(stats.grapeHits / grapeTotal * 100) : 0}%)`);

  if (stats.grapeMissNames.size > 0) {
    console.log(`\n  Unresolved grapes: ${[...stats.grapeMissNames].join(', ')}`);
  }
  if (stats.warnings.length > 0) {
    console.log(`\n  Warnings (${stats.warnings.length}):`);
    for (const w of stats.warnings.slice(0, 15)) console.log(`    ⚠ ${w}`);
    if (stats.warnings.length > 15) console.log(`    ... and ${stats.warnings.length - 15} more`);
  }
  console.log(`\n  Done!\n`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
