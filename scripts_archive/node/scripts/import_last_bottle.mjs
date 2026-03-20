#!/usr/bin/env node
/**
 * import_last_bottle.mjs — Last Bottle Wines bulk import
 *
 * Imports wines from Last Bottle's Shopify JSON catalog.
 * Multi-producer portfolio import — tests title parsing, score extraction
 * from marketing copy, appellation/grape resolution with minimal structured data.
 *
 * Usage:
 *   node scripts/import_last_bottle.mjs [--dry-run] [--replace]
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

const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const REPLACE = args.includes('--replace');

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

// ── Skip list ─────────────────────────────────────────────────
const SKIP_TITLES = new Set([
  'Cool-Pack Shipping',
  'Last Bottle Gift Card',
  'Upsell Product 2',
  'Last Bottle Harvest 2022 T-Shirt (M)',
]);

function shouldSkip(product) {
  if (SKIP_TITLES.has(product.title)) return true;
  if (/^(TEST|DBG)/.test(product.title)) return true;
  if (/DO NOT SELL/i.test(product.title)) return true;
  if (/T-Shirt|Gift Card|Cool-Pack|Shipping/i.test(product.title)) return true;
  if (product.product_type === '' && !product.title.match(/\d{4}|NV/)) return true;
  return false;
}

// ── Color from product_type ───────────────────────────────────
function inferColor(productType) {
  if (!productType) return null;
  const t = productType.toLowerCase().trim();
  if (t.includes('red')) return 'red';
  if (t.includes('white') || t === 'white wine') return 'white';
  if (t.includes('ros')) return 'rose';
  if (t.includes('sparkling')) return null; // could be any color
  return null;
}

// ── Bottle format from title ──────────────────────────────────
function extractBottleFormat(title) {
  const formats = [
    { pattern: /\(6\s*Liter\)/i, name: '6L', ml: 6000 },
    { pattern: /\(3\s*Liter\)/i, name: 'Jeroboam', ml: 3000 },
    { pattern: /\(Magnum\s*1\.5L\)/i, name: 'Magnum', ml: 1500 },
    { pattern: /\(Magnum\)/i, name: 'Magnum', ml: 1500 },
    { pattern: /Magnum/i, name: 'Magnum', ml: 1500 },
    { pattern: /\(Half\s*Bottle\s*375\s*m[Ll]\)/i, name: 'Half Bottle', ml: 375 },
    { pattern: /\(500\s*m[Ll]\)/i, name: '500ml', ml: 500 },
    { pattern: /375\s*m[Ll]/i, name: 'Half Bottle', ml: 375 },
  ];
  for (const f of formats) {
    if (f.pattern.test(title)) {
      return { name: f.name, ml: f.ml, cleanTitle: title.replace(f.pattern, '').trim() };
    }
  }
  return { name: 'Standard', ml: 750, cleanTitle: title };
}

// ── Score extraction from body_html ───────────────────────────
function extractScores(bodyHtml) {
  if (!bodyHtml) return [];
  // Strip HTML tags
  const text = bodyHtml.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ');
  const scores = [];

  // Pattern: "93 POINTS" or "94 points" or "96, 95, 94 POINTS"
  // Also: "DOUBLE 93 POINTS"
  const pointsPattern = /(\d{2,3})\s*(?:POINTS?|points?)/g;
  let match;
  while ((match = pointsPattern.exec(text)) !== null) {
    const score = parseInt(match[1]);
    if (score >= 80 && score <= 100) {
      // Try to find the publication nearby (within ~100 chars after the score)
      const contextAfter = text.slice(match.index, match.index + 200);
      const contextBefore = text.slice(Math.max(0, match.index - 200), match.index + match[0].length);
      const context = contextBefore + ' ' + contextAfter;

      let publication = null;
      if (/Robert Parker|Wine Advocate/i.test(context)) publication = 'Wine Advocate';
      else if (/James Suckling/i.test(context)) publication = 'James Suckling';
      else if (/Wine Spectator/i.test(context)) publication = 'Wine Spectator';
      else if (/Wine Enthusiast/i.test(context)) publication = 'Wine Enthusiast';
      else if (/Decanter/i.test(context)) publication = 'Decanter';
      else if (/Vinous|Antonio Galloni/i.test(context)) publication = 'Vinous';
      else if (/Jancis Robinson/i.test(context)) publication = 'Jancis Robinson';
      else if (/Jeb Dunnuck/i.test(context)) publication = 'Jeb Dunnuck';
      else if (/Wine & Spirits/i.test(context)) publication = 'Wine & Spirits';

      // Don't add duplicate scores
      if (!scores.find(s => s.score === score && s.publication === publication)) {
        scores.push({ score, publication });
      }
    }
  }

  return scores;
}

// ── Known grape varieties for title matching ──────────────────
// Ordered longest-first for greedy matching
const GRAPE_PATTERNS = [
  'Cabernet Sauvignon', 'Sauvignon Blanc', 'Pinot Noir', 'Pinot Grigio',
  'Pinot Gris', 'Chenin Blanc', 'Grüner Veltliner', 'Gruner Veltliner',
  'Petite Sirah', 'Petit Verdot', 'Cabernet Franc', 'Ribolla Gialla',
  'Tinta de Toro', 'Sangiovese Grosso',
  'Chardonnay', 'Zinfandel', 'Merlot', 'Syrah', 'Shiraz', 'Malbec',
  'Grenache', 'Nebbiolo', 'Sangiovese', 'Tempranillo', 'Primitivo',
  'Carignan', 'Mourvèdre', 'Mourvedre', 'Viognier', 'Riesling',
  'Gewürztraminer', 'Muscat', 'Aglianico', 'Barbera', 'Loureiro',
  'Susumaniello', 'Niellucciu',
  // Two-word varieties that might appear
  'Red Blend', 'Proprietary Red', 'Bordeaux Blend',
];

// ── Title parser ──────────────────────────────────────────────
function parseTitle(rawTitle, appellationMap, regionMap, grapeMap) {
  // Step 1: Extract bottle format
  const { cleanTitle } = extractBottleFormat(rawTitle);

  // Step 2: Extract vintage or NV
  let vintage = null;
  let title = cleanTitle;
  const vintageMatch = title.match(/\b((?:19|20)\d{2})\s*$/);
  if (vintageMatch) {
    vintage = parseInt(vintageMatch[1]);
    title = title.slice(0, vintageMatch.index).trim();
  } else if (/\bNV\s*$/i.test(title)) {
    vintage = null; // Non-vintage
    title = title.replace(/\bNV\s*$/i, '').trim();
  }

  // Step 3: Try to match grape variety (case-insensitive)
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

  // Step 4: Try to match appellation/region from the right side
  // Build candidates from remaining title words (after grape removal)
  let appellation = null;
  let appellationName = null;
  let region = null;
  let regionName = null;

  // Get the portion of title after grape (or full title if no grape)
  const geoSection = grape
    ? title.slice(grapeEnd).trim()
    : title;

  // Try progressively shorter substrings from the right of geoSection
  const geoWords = geoSection.split(/\s+/);
  for (let start = 0; start < geoWords.length; start++) {
    const candidate = geoWords.slice(start).join(' ');
    const candidateNorm = normalize(candidate);

    // Check appellations first
    const app = appellationMap.get(candidateNorm);
    if (app) {
      appellation = app;
      appellationName = candidate;
      break;
    }

    // Check regions
    const reg = regionMap.get(candidateNorm);
    if (reg && !reg.is_catch_all) {
      region = reg;
      regionName = candidate;
      break;
    }
  }

  // Step 5: Determine producer name and wine name
  let producerName = null;
  let wineName = null;

  if (grape && grapeStart > 0) {
    // Producer is everything before the grape
    producerName = title.slice(0, grapeStart).trim();

    // Wine name is everything between grape and appellation
    const afterGrape = title.slice(grapeEnd).trim();
    if (appellationName) {
      const appIdx = afterGrape.indexOf(appellationName);
      if (appIdx > 0) {
        wineName = afterGrape.slice(0, appIdx).trim();
      }
    } else if (regionName) {
      const regIdx = afterGrape.indexOf(regionName);
      if (regIdx > 0) {
        wineName = afterGrape.slice(0, regIdx).trim();
      }
    } else {
      // No appellation found — everything after grape is wine name
      wineName = afterGrape || null;
    }
  } else if (appellationName) {
    // No grape found — try to split producer from the beginning
    const appIdx = title.indexOf(appellationName);
    if (appIdx > 0) {
      const beforeApp = title.slice(0, appIdx).trim();
      // Heuristic: producer is the first "logical" portion
      producerName = beforeApp;
    }
  } else {
    // No grape, no appellation — just use the whole title as producer + wine
    producerName = title;
  }

  // Clean up wine name
  if (wineName) {
    wineName = wineName.replace(/^\s*[-–—]\s*/, '').trim();
    if (!wineName) wineName = null;
  }

  // Build the full wine display name
  let displayName = rawTitle;
  if (vintage) displayName = displayName.replace(/\s*\b\d{4}\s*$/, '').trim();
  displayName = displayName
    .replace(/\(Magnum\s*1?\.?5?L?\)/i, '')
    .replace(/\(Half\s*Bottle\s*375\s*m[Ll]\)/i, '')
    .replace(/\(500\s*m[Ll]\)/i, '')
    .replace(/\(6\s*Liter\)/i, '')
    .replace(/\(3\s*Liter\)/i, '')
    .trim();

  return {
    producerName: producerName || 'Unknown Producer',
    wineName: wineName || null,
    displayName,
    grape,
    vintage,
    appellation,
    appellationName,
    region,
    regionName,
  };
}

// ── Publication aliases ─────────────────────────────────────
const PUB_ALIASES = {
  'robert parker': 'Wine Advocate',
  "robert parker's wine advocate": 'Wine Advocate',
  'wine advocate': 'Wine Advocate',
  'james suckling': 'James Suckling',
  'wine spectator': 'Wine Spectator',
  'wine enthusiast': 'Wine Enthusiast',
  'decanter': 'Decanter',
  'vinous': 'Vinous',
  'jancis robinson': 'Jancis Robinson',
  'jeb dunnuck': 'Jeb Dunnuck',
  'wine & spirits': 'Wine & Spirits',
};

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`  LAST BOTTLE WINES IMPORT`);
  console.log(`  ${DRY_RUN ? '(DRY RUN)' : REPLACE ? '(REPLACE MODE)' : '(INSERT MODE)'}`);
  console.log(`${'='.repeat(60)}\n`);

  // Load raw catalog
  const catalog = JSON.parse(readFileSync('data/imports/last_bottle_raw.json', 'utf-8'));
  const wineProducts = catalog.filter(p => !shouldSkip(p));
  console.log(`Catalog: ${catalog.length} products, ${wineProducts.length} wines (${catalog.length - wineProducts.length} skipped)\n`);

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
  for (const a of appellations) {
    appellationMap.set(normalize(a.name), a);
  }

  // Load appellation aliases
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

  console.log(`  Countries: ${countries.length}, Regions: ${regions.length}`);
  console.log(`  Appellations: ${appellations.length}, Aliases: ${aliases.length}`);
  console.log(`  Grapes: ${grapes.length}, Synonyms: ${synonyms.length}`);
  console.log(`  Publications: ${publications.length}\n`);

  // ── Resolve grape helper ──────────────────────────────────
  function resolveGrape(name) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    // Handle multi-word names like "Red Blend", "Proprietary Red"
    if (['red blend', 'proprietary red', 'bordeaux blend'].includes(lower)) return null;
    // Handle accent-stripped variants and synonyms not in synonym table
    if (lower === 'gruner veltliner') return grapeMap.get('grüner veltliner')?.id || null;
    if (lower === 'pinot grigio') return grapeMap.get('pinot gris')?.id || null;
    if (lower === 'petite sirah') return grapeMap.get('durif')?.id || null;
    const g = grapeMap.get(lower);
    if (g) return g.id;
    const synId = synMap.get(lower);
    if (synId) return synId;
    // Try without accents
    const stripped = normalize(name);
    const g2 = grapeMap.get(stripped);
    if (g2) return g2.id;
    return null;
  }

  // ── Create/find Last Bottle as retailer ────────────────────
  console.log('Setting up Last Bottle retailer...');
  const retailerSourceId = sourceTypeMap.get('retailer-website') || sourceTypeMap.get('importer-website') || sourceTypeMap.get('producer-website');
  console.log(`  Source type: ${retailerSourceId ? 'found' : 'using producer-website fallback'}\n`);

  // ── Stats ──────────────────────────────────────────────────
  const stats = {
    producers: 0, wines: 0, vintages: 0, scores: 0,
    wineGrapes: 0, prices: 0,
    appellationHits: 0, appellationMisses: 0,
    grapeHits: 0, grapeMisses: 0,
    producerReuses: 0,
    warnings: [],
    regionMisses: new Set(),
    grapeMissNames: new Set(),
    appellationMissNames: new Set(),
  };

  function warn(msg) {
    stats.warnings.push(msg);
  }

  // ── Process wines ─────────────────────────────────────────
  const producerIdMap = new Map(); // slug → id
  let processed = 0;

  for (const product of wineProducts) {
    processed++;
    const parsed = parseTitle(product.title, appellationMap, regionMap, grapeMap);
    const color = inferColor(product.product_type) || (parsed.grape === 'Red Blend' || parsed.grape === 'Proprietary Red' ? 'red' : null);
    const scores = extractScores(product.body_html);
    const { name: formatName, ml: formatMl } = extractBottleFormat(product.title);

    // Determine wine type (CHECK constraint: table, sparkling, dessert, fortified)
    let wineType = 'table';
    if (product.product_type === 'Sparkling' || /Brut|Cremant|Champagne|Sparkling|Pét-Nat/i.test(product.title)) {
      wineType = 'sparkling';
    }
    if (/Sauternes|Late Harvest|Tokaji/i.test(product.title)) {
      wineType = 'dessert';
    }
    if (/Port|Sherry|Madeira|Marsala|Vermouth/i.test(product.title)) {
      wineType = 'fortified';
    }

    // Infer country from appellation/region or title keywords
    let countryId = parsed.appellation?.country_id || parsed.region?.country_id || null;
    if (!countryId) {
      // Try common country/region keywords in the title
      const titleLower = product.title.toLowerCase();
      if (/napa|sonoma|california|oregon|washington|willamette|paso robles|russian river|carneros|columbia valley|walla walla|amador|lodi|mendocino|santa barbara|santa maria|santa cruz|central coast|stags? leap|moon mountain|dry creek|alexander valley|knights valley|oak knoll|oakville|rutherford|calistoga|spring mountain|st\.?\s*helena|suisun|red hills|lake county|north coast|anderson valley|rouge valley|adelaida|cienega|arroyo seco|fountaingrove|diamond mountain|mokelumne|san luis obispo|sta\.?\s*rita/i.test(titleLower)) {
        countryId = countryMap.get('united states');
      } else if (/toscana|tuscany|barolo|brunello|montalcino|chianti|piemonte|piedmont|langhe|barbaresco|delle venezie|valpolicella|salento|carmignano|basilicata|molise|montello|sant.antimo|morellino|scansano/i.test(titleLower)) {
        countryId = countryMap.get('italy');
      } else if (/bordeaux|bourgogne|burgundy|champagne|rhône|rhone|chablis|sancerre|loire|languedoc|roussillon|limoux|luberon|corbières|corbieres|gigondas|châteauneuf|chateauneuf|hermitage|crozes|tavel|juliénas|julienas|chiroubles|fronsac|saint[- ]émilion|saint[- ]emilion|pessac|graves|menetou|vouvray|muscadet|côtes du rhône|cotes du rhone|costieres|nimes|sauternes/i.test(titleLower)) {
        countryId = countryMap.get('france');
      } else if (/rioja|toro|jumilla|txakolina|getariako/i.test(titleLower)) {
        countryId = countryMap.get('spain');
      } else if (/marlborough|canterbury/i.test(titleLower)) {
        countryId = countryMap.get('new zealand');
      } else if (/south australia|clare valley|coonawarra|yarra valley|grampians|barossa|victoria/i.test(titleLower)) {
        countryId = countryMap.get('australia');
      } else if (/stellenbosch|noordhoek|cape point/i.test(titleLower)) {
        countryId = countryMap.get('south africa');
      } else if (/mendoza|uco valley/i.test(titleLower)) {
        countryId = countryMap.get('argentina');
      } else if (/kremstal|wagram/i.test(titleLower)) {
        countryId = countryMap.get('austria');
      } else if (/tokaji/i.test(titleLower)) {
        countryId = countryMap.get('hungary');
      } else if (/vinho verde/i.test(titleLower)) {
        countryId = countryMap.get('portugal');
      }
    }

    // Last resort: default to US (most Last Bottle wines are US)
    if (!countryId) {
      countryId = countryMap.get('united states');
      warn(`Country defaulted to US for: "${product.title}"`);
    }

    // Resolve region if we have appellation but no region
    let regionId = parsed.region?.id || null;
    if (!regionId && parsed.appellation?.region_id) {
      regionId = parsed.appellation.region_id;
    }

    // ── Create/find producer ────────────────────────────────
    const producerName = parsed.producerName;
    const producerSlug = slugify(producerName);

    let producerId;
    if (producerIdMap.has(producerSlug)) {
      producerId = producerIdMap.get(producerSlug);
      stats.producerReuses++;
    } else {
      // Check DB
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
          metadata: { source: 'last-bottle-wines', shopify_vendor: product.vendor },
        });
        if (error) {
          warn(`Producer insert error for "${producerName}": ${error.message}`);
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

    // ── Create wine ─────────────────────────────────────────
    const wineSlug = slugify(parsed.displayName);
    const wineId = randomUUID();

    // Determine wine_type and effervescence
    let effervescence = null;
    if (wineType === 'sparkling') effervescence = 'sparkling';

    const wineRow = {
      id: wineId,
      producer_id: producerId,
      slug: wineSlug,
      name: parsed.displayName,
      name_normalized: normalize(parsed.displayName),
      color: color,
      wine_type: wineType,
      effervescence: effervescence,
      appellation_id: parsed.appellation?.id || null,
      country_id: countryId,
      region_id: regionId,
      metadata: {
        source: 'last-bottle-wines',
        shopify_id: product.shopify_id,
        shopify_handle: product.handle,
        retail_price: product.compare_at_price,
        last_bottle_price: product.price,
      },
    };

    if (!DRY_RUN) {
      // Check for existing wine with same slug
      const { data: existingWine } = await sb.from('wines')
        .select('id').eq('slug', wineSlug).single();

      if (existingWine) {
        // Skip — already imported
        continue;
      }

      const { error } = await sb.from('wines').insert(wineRow);
      if (error) {
        warn(`Wine insert error for "${parsed.displayName}": ${error.message}`);
        continue;
      }
    }
    stats.wines++;

    // Track appellation resolution
    if (parsed.appellation) {
      stats.appellationHits++;
    } else {
      stats.appellationMisses++;
      // Try to figure out what appellation might be in the title
      const titleWithoutProducer = product.title.replace(producerName, '').trim();
      stats.appellationMissNames.add(titleWithoutProducer);
    }

    // ── Link grape variety ──────────────────────────────────
    if (parsed.grape) {
      const grapeId = resolveGrape(parsed.grape);
      if (grapeId) {
        if (!DRY_RUN) {
          const { error } = await sb.from('wine_grapes').insert({
            wine_id: wineId,
            grape_id: grapeId,
            percentage: 100,
          });
          if (error && !error.message.includes('duplicate')) {
            warn(`Wine grape error: ${error.message}`);
          }
        }
        stats.wineGrapes++;
        stats.grapeHits++;
      } else {
        stats.grapeMisses++;
        stats.grapeMissNames.add(parsed.grape);
      }
    }

    // ── Create vintage ──────────────────────────────────────
    const vintageYear = parsed.vintage || 0; // 0 = NV
    const vintageId = randomUUID();

    if (!DRY_RUN) {
      const { error } = await sb.from('wine_vintages').insert({
        id: vintageId,
        wine_id: wineId,
        vintage_year: vintageYear,
        metadata: {
          source: 'last-bottle-wines',
          available_on_site: product.available,
          created_on_site: product.created_at,
        },
      });
      if (error) {
        warn(`Vintage insert error: ${error.message}`);
        continue;
      }
    }
    stats.vintages++;

    // ── Insert scores ───────────────────────────────────────
    for (const sc of scores) {
      const pubId = sc.publication ? pubMap.get(sc.publication.toLowerCase()) : null;

      if (!DRY_RUN) {
        const { error } = await sb.from('wine_vintage_scores').insert({
          wine_id: wineId,
          vintage_year: vintageYear,
          publication_id: pubId,
          score: sc.score,
          score_scale: '100-point',
          source_id: retailerSourceId,
        });
        if (error && !error.message.includes('duplicate')) {
          warn(`Score insert error: ${error.message}`);
        } else {
          stats.scores++;
        }
      } else {
        stats.scores++;
      }
    }

    // ── Insert price ────────────────────────────────────────
    if (product.price && !DRY_RUN) {
      const { error } = await sb.from('wine_vintage_prices').insert({
        wine_id: wineId,
        vintage_year: vintageYear,
        price_usd: product.price,
        price_type: 'retail',
        merchant_name: 'Last Bottle Wines',
        price_date: new Date().toISOString().slice(0, 10),
      });
      if (error && !error.message.includes('duplicate')) {
        warn(`Price insert error: ${error.message}`);
      } else {
        stats.prices++;
      }
    } else if (DRY_RUN) {
      stats.prices++;
    }

    // Log progress every 25 wines
    if (processed % 25 === 0) {
      console.log(`  Processed ${processed}/${wineProducts.length}...`);
    }
  }

  // ── Summary ────────────────────────────────────────────────
  console.log(`\n${'='.repeat(60)}`);
  console.log(`  IMPORT SUMMARY${DRY_RUN ? ' (DRY RUN)' : ''}`);
  console.log(`${'='.repeat(60)}`);
  console.log(`  Producers created:  ${stats.producers} (${stats.producerReuses} reused)`);
  console.log(`  Wines created:      ${stats.wines}`);
  console.log(`  Vintages created:   ${stats.vintages}`);
  console.log(`  Scores inserted:    ${stats.scores}`);
  console.log(`  Wine grapes linked: ${stats.wineGrapes}`);
  console.log(`  Prices recorded:    ${stats.prices}`);
  console.log(`  Appellation hits:   ${stats.appellationHits}/${stats.appellationHits + stats.appellationMisses} (${Math.round(stats.appellationHits / (stats.appellationHits + stats.appellationMisses) * 100)}%)`);
  console.log(`  Grape hits:         ${stats.grapeHits}/${stats.grapeHits + stats.grapeMisses} (${stats.grapeHits + stats.grapeMisses > 0 ? Math.round(stats.grapeHits / (stats.grapeHits + stats.grapeMisses) * 100) : 0}%)`);

  if (stats.grapeMissNames.size > 0) {
    console.log(`\n  Unresolved grapes: ${[...stats.grapeMissNames].join(', ')}`);
  }

  if (stats.warnings.length > 0) {
    console.log(`\n  Warnings (${stats.warnings.length}):`);
    for (const w of stats.warnings.slice(0, 20)) {
      console.log(`    ⚠ ${w}`);
    }
    if (stats.warnings.length > 20) {
      console.log(`    ... and ${stats.warnings.length - 20} more`);
    }
  }

  console.log(`\n  Done!\n`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
