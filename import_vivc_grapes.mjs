#!/usr/bin/env node
/**
 * import_vivc_grapes.mjs
 *
 * Rebuilds the grapes table from VIVC (Vitis International Variety Catalogue).
 * Scrapes passport pages for all wine grapes, including synonyms and planting area data.
 *
 * Data source: VIVC (vivc.de) — JKI Federal Research Centre for Cultivated Plants
 *
 * Phases:
 *   Phase 1: Crawl passport pages (IDs 1–25000), filter wine grapes, extract all
 *            fields + synonyms in a single pass, cache to JSON
 *   Phase 2: Fetch area/planting sub-pages + EU catalog for each wine grape
 *   Phase 3: Import into Supabase (grapes, grape_synonyms, grape_plantings)
 *   Phase 4: Resolve parentage (second pass after all grapes inserted)
 *   Phase 5: Reconnect varietal_categories.grape_id from saved mappings
 *
 * Usage:
 *   node import_vivc_grapes.mjs                    # full run (resume-safe)
 *   node import_vivc_grapes.mjs --phase=1          # crawl only
 *   node import_vivc_grapes.mjs --phase=2          # enrich only (areas + EU catalog)
 *   node import_vivc_grapes.mjs --phase=3          # import only
 *   node import_vivc_grapes.mjs --phase=4          # resolve parentage
 *   node import_vivc_grapes.mjs --phase=5          # reconnect varietal categories
 *   node import_vivc_grapes.mjs --start=5000       # resume crawl from ID 5000
 *   node import_vivc_grapes.mjs --dry-run          # preview without DB writes
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, writeFileSync, existsSync } from 'fs';

// ---------------------------------------------------------------------------
// .env loading
// ---------------------------------------------------------------------------
const envPath = new URL('.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envLines = readFileSync(envPath, 'utf8').split('\n');
const env = {};
for (const l of envLines) {
  const m = l.replace(/\r/g, '').match(/^(\w+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE);

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------
const DRY_RUN = process.argv.includes('--dry-run');
const PHASE_ARG = process.argv.find(a => a.startsWith('--phase='));
const PHASE_ONLY = PHASE_ARG ? parseInt(PHASE_ARG.split('=')[1]) : null;
const START_ARG = process.argv.find(a => a.startsWith('--start='));
const START_ID = START_ARG ? parseInt(START_ARG.split('=')[1]) : 1;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const VIVC_BASE = 'https://www.vivc.de/index.php';
const MAX_ID = 25000;
const CRAWL_DELAY_MS = 300;       // polite delay between requests
const ENRICH_DELAY_MS = 500;      // delay for sub-page fetches
const CACHE_FILE = 'data/vivc_grapes_cache.json';
const SAVE_INTERVAL = 50;         // save cache every N grapes found

// Wine grape utilization values to include (lowercased for matching)
const WINE_UTILIZATIONS = new Set([
  'wine grape',
  'wine and table grape',
  'table and wine grape',
  'wine/table grape',
  'table/wine grape',
]);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function slugify(s) {
  return s.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[''`]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function loadCache() {
  if (existsSync(CACHE_FILE)) {
    return JSON.parse(readFileSync(CACHE_FILE, 'utf8'));
  }
  return { lastScannedId: 0, grapes: {}, stats: { scanned: 0, wineGrapes: 0, skipped: 0, errors: 0 } };
}

function saveCache(cache) {
  writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2));
}

/** Extract value from a hidden input field by ID */
function inputValue(html, fieldId) {
  const re = new RegExp(`id="${fieldId}"[^>]*value="([^"]*)"`, 'i');
  const m = html.match(re);
  return m ? m[1].trim() : null;
}

/** Extract text from kv-attribute div after a th label */
function kvAttribute(html, label) {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`>${escaped}</th>[\\s\\S]*?kv-attribute">\\s*(?:<a[^>]*>)?([^<]*)`, 'i');
  const m = html.match(re);
  return m && m[1].trim() ? m[1].trim() : null;
}

// ---------------------------------------------------------------------------
// Phase 1: Crawl passport pages
// ---------------------------------------------------------------------------
async function crawlPassports(cache) {
  console.log(`\n=== PHASE 1: Crawling VIVC passport pages ===`);
  const startId = Math.max(START_ID, (cache.lastScannedId || 0) + 1);
  console.log(`Starting from ID ${startId}, scanning to ${MAX_ID}`);
  console.log(`Current cache: ${Object.keys(cache.grapes).length} wine grapes found\n`);

  let newFound = 0;
  let consecutiveNotFound = 0;

  for (let id = startId; id <= MAX_ID; id++) {
    try {
      const url = `${VIVC_BASE}?r=passport%2Fview&id=${id}`;
      const resp = await fetch(url);

      if (!resp.ok) {
        cache.stats.errors++;
        if (id % 500 === 0) console.log(`  [${id}] HTTP ${resp.status}`);
        cache.lastScannedId = id;
        await sleep(CRAWL_DELAY_MS);
        continue;
      }

      const html = await resp.text();

      // Check for non-existent ID — VIVC shows "return to the initial search"
      if (html.includes('return to the initial search') || html.includes('Please return to')) {
        consecutiveNotFound++;
        cache.stats.skipped++;
        cache.lastScannedId = id;
        if (consecutiveNotFound >= 500) {
          console.log(`\n  500 consecutive not-found IDs — stopping at ${id}`);
          break;
        }
        await sleep(50);
        continue;
      }

      consecutiveNotFound = 0;
      cache.stats.scanned++;

      // --- Extract prime name from input field ---
      const primeName = inputValue(html, 'passport-leitname');
      if (!primeName) {
        cache.lastScannedId = id;
        await sleep(CRAWL_DELAY_MS);
        continue;
      }

      // --- Extract utilization from link URL parameter ---
      // HTML: <a href="...SpeciesSearch[utilization22]=wine%20grape&...">WINE GRAPE</a>
      const utilMatch = html.match(/utilization22\]=([^&"]+)/i);
      const utilization = utilMatch ? decodeURIComponent(utilMatch[1]).trim().toLowerCase() : null;

      // Filter: only wine grapes
      if (!utilization || !WINE_UTILIZATIONS.has(utilization)) {
        cache.stats.skipped++;
        cache.lastScannedId = id;
        if (id % 1000 === 0) {
          console.log(`  [${id}] scanned=${cache.stats.scanned} wine=${Object.keys(cache.grapes).length} skip=${cache.stats.skipped}`);
        }
        await sleep(50);
        continue;
      }

      // ===== This is a wine grape — extract everything =====
      const grape = {
        vivc_number: String(id),
        name: primeName,
        utilization,
      };

      // Berry skin color — from hidden input passport-b_farbe
      grape.berry_skin_color = inputValue(html, 'passport-b_farbe') || null;

      // Country of origin — ISO3 from hidden input passport-landescode
      const originCode = inputValue(html, 'passport-landescode');
      // Display name from kv-attribute (full label includes "of the variety")
      const originDisplay = kvAttribute(html, 'Country or region of origin of the variety');
      grape.origin_country_code = originCode || null;
      grape.origin_country = originDisplay || null;

      // Species — from kv-attribute link text
      const speciesMatch = html.match(/>Species<\/th>[\s\S]*?kv-attribute">\s*<a[^>]*>([^<]*)/i);
      grape.species = speciesMatch ? speciesMatch[1].trim() : null;

      // Pedigree text — from kv-attribute
      grape.pedigree_text = kvAttribute(html, 'Pedigree as given by breeder/bibliography') || null;
      grape.pedigree_confirmed_text = kvAttribute(html, 'Pedigree confirmed by markers') || null;

      // Parent IDs — from hidden input fields
      const p1Id = inputValue(html, 'passport-kenn_nr_e1');
      const p2Id = inputValue(html, 'passport-kenn_nr_e2');
      grape.parent1_vivc_id = p1Id ? parseInt(p1Id) : null;
      grape.parent2_vivc_id = p2Id ? parseInt(p2Id) : null;

      // Parent names — find the passport link matching the parent VIVC ID
      // More reliable than position-based matching
      if (grape.parent1_vivc_id) {
        const p1Re = new RegExp(`passport%2Fview&amp;id=${grape.parent1_vivc_id}">([^<]+)`, 'i');
        const p1m = html.match(p1Re);
        grape.parent1_name = p1m ? p1m[1].trim() : null;
      } else {
        grape.parent1_name = null;
      }
      if (grape.parent2_vivc_id) {
        const p2Re = new RegExp(`passport%2Fview&amp;id=${grape.parent2_vivc_id}">([^<]+)`, 'i');
        const p2m = html.match(p2Re);
        grape.parent2_name = p2m ? p2m[1].trim() : null;
      } else {
        grape.parent2_name = null;
      }

      // Full pedigree (YES/NO) — from kv-attribute
      const fullPed = kvAttribute(html, 'Full pedigree');
      grape.parentage_confirmed = fullPed === 'YES';

      // Breeder — from kv-attribute
      grape.breeder = kvAttribute(html, 'Breeder') || null;

      // Breeding institute code — from kv-attribute
      grape.breeding_institute = kvAttribute(html, 'Breeder institute code') || null;

      // Year of crossing — from kv-attribute
      const yearStr = kvAttribute(html, 'Year of crossing');
      grape.crossing_year = yearStr && /^\d{4}$/.test(yearStr) ? parseInt(yearStr) : null;

      // --- Synonyms (extracted from same passport page) ---
      // Synonyms are embedded as links: ...%5Bsname%5D=BIDURE&...
      const synRegex = /%5Bsname%5D=([^&"]+)/gi;
      const synonyms = new Set();
      let synMatch;
      while ((synMatch = synRegex.exec(html)) !== null) {
        const syn = decodeURIComponent(synMatch[1]).trim();
        if (syn && syn !== primeName) {
          synonyms.add(syn);
        }
      }
      grape.synonyms = [...synonyms];

      // Synonym count from header
      const synCountMatch = html.match(/Synonyms:\s*(\d+)/i);
      grape.synonym_count = synCountMatch ? parseInt(synCountMatch[1]) : 0;

      // Area data available?
      grape.has_area_data = html.includes('arealisting') || html.includes('Area tabular listing');

      // EU catalog available?
      grape.has_eu_catalog = html.includes('europ-catalogue') || html.includes('European Catalogue');

      cache.grapes[id] = grape;
      cache.stats.wineGrapes++;
      newFound++;

      console.log(`  [${id}] ✓ ${primeName} (${grape.berry_skin_color || '?'}) — ${grape.origin_country || '?'} — ${grape.synonyms.length} syn`);

      cache.lastScannedId = id;

      // Save periodically
      if (newFound % SAVE_INTERVAL === 0) {
        saveCache(cache);
        console.log(`    >> Cache saved (${Object.keys(cache.grapes).length} wine grapes)`);
      }

      await sleep(CRAWL_DELAY_MS);

    } catch (err) {
      console.error(`  [${id}] ERROR: ${err.message}`);
      cache.stats.errors++;
      cache.lastScannedId = id;
      await sleep(1000);
    }
  }

  saveCache(cache);
  console.log(`\nPhase 1 complete:`);
  console.log(`  Scanned: ${cache.stats.scanned}`);
  console.log(`  Wine grapes found: ${Object.keys(cache.grapes).length}`);
  console.log(`  Skipped (non-wine/not found): ${cache.stats.skipped}`);
  console.log(`  Errors: ${cache.stats.errors}`);
}

// ---------------------------------------------------------------------------
// Phase 2: Enrich with area data + EU catalog
// ---------------------------------------------------------------------------
async function enrichGrapes(cache) {
  console.log(`\n=== PHASE 2: Enriching wine grapes with area data + EU catalog ===`);

  const grapeIds = Object.keys(cache.grapes).filter(id => {
    const g = cache.grapes[id];
    return !g.areas_fetched || !g.eu_catalog_fetched;
  });

  console.log(`${grapeIds.length} grapes need enrichment\n`);

  for (let i = 0; i < grapeIds.length; i++) {
    const id = grapeIds[i];
    const grape = cache.grapes[id];

    // Fetch area/planting data
    if (!grape.areas_fetched && grape.has_area_data) {
      try {
        const url = `${VIVC_BASE}?r=flaechen%2Farealisting&FlaechenSearch%5Bleitname2%5D=${encodeURIComponent(grape.name)}&FlaechenSearch%5Bkenn_nr2%5D=${id}`;
        const resp = await fetch(url);
        const html = await resp.text();

        // Parse area data — table rows with country, hectares, year
        const areas = [];
        const rowRegex = /<td[^>]*>([^<]+)<\/td>\s*<td[^>]*>([\d,.]+)\s*<\/td>\s*<td[^>]*>(\d{4})\s*<\/td>/gi;
        let m;
        while ((m = rowRegex.exec(html)) !== null) {
          const country = m[1].trim();
          const area = parseFloat(m[2].replace(/,/g, ''));
          const year = parseInt(m[3]);
          if (country && !isNaN(area) && year) {
            areas.push({ country, area_ha: area, year });
          }
        }

        // Keep only the most recent entry per country
        const latestByCountry = {};
        for (const a of areas) {
          if (!latestByCountry[a.country] || a.year > latestByCountry[a.country].year) {
            latestByCountry[a.country] = a;
          }
        }
        grape.areas = Object.values(latestByCountry);
        grape.areas_fetched = true;

        await sleep(ENRICH_DELAY_MS);
      } catch (err) {
        console.error(`  [${id}] Area fetch error: ${err.message}`);
      }
    } else if (!grape.areas_fetched) {
      grape.areas = [];
      grape.areas_fetched = true;
    }

    // Fetch EU catalog countries
    if (!grape.eu_catalog_fetched && grape.has_eu_catalog) {
      try {
        const url = `${VIVC_BASE}?r=www-europ-catalogue%2Fpassportresult&WwwEuropCatalogueSearch%5Bvivc_var_id%5D=${id}`;
        const resp = await fetch(url);
        const html = await resp.text();

        // Extract country names from EU catalog table
        const countries = new Set();
        const countryRegex = /<td[^>]*>([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*<\/td>/g;
        let m;
        while ((m = countryRegex.exec(html)) !== null) {
          const c = m[1].trim();
          if (c.length > 2 && !['YES', 'NO', 'Showing'].includes(c)) {
            countries.add(c);
          }
        }
        grape.eu_catalog_countries = [...countries];
        grape.eu_catalog_fetched = true;

        await sleep(ENRICH_DELAY_MS);
      } catch (err) {
        console.error(`  [${id}] EU catalog fetch error: ${err.message}`);
      }
    } else if (!grape.eu_catalog_fetched) {
      grape.eu_catalog_countries = [];
      grape.eu_catalog_fetched = true;
    }

    if ((i + 1) % 25 === 0 || i + 1 === grapeIds.length) {
      saveCache(cache);
      console.log(`  Enriched ${i + 1}/${grapeIds.length}: ${grape.name} (${grape.areas?.length || 0} area entries, ${grape.eu_catalog_countries?.length || 0} EU countries)`);
    }
  }

  saveCache(cache);
  console.log(`\nPhase 2 complete: ${grapeIds.length} grapes enriched`);
}

// ---------------------------------------------------------------------------
// Display name logic — three-tier strategy
// ---------------------------------------------------------------------------

// Tier 1: Explicit overrides — VIVC prime name → industry-standard display name
const DISPLAY_NAME_OVERRIDES = {
  'MERLOT NOIR': 'Merlot',
  'CHARDONNAY BLANC': 'Chardonnay',
  'RIESLING WEISS': 'Riesling',
  'TEMPRANILLO TINTO': 'Tempranillo',
  'GAMAY NOIR': 'Gamay',
  'BARBERA NERA': 'Barbera',
  'GARNACHA TINTA': 'Grenache',
  'COT': 'Malbec',
  'CALABRESE': 'Nero d\'Avola',
  'MONASTRELL': 'Mourvèdre',
  'VELTLINER GRUEN': 'Grüner Veltliner',
  'VERDOT PETIT': 'Petit Verdot',
  'ZWEIGELTREBE BLAU': 'Zweigelt',
  'NEGRO AMARO': 'Negramaro',
  'UVA DI TROIA': 'Nero di Troia',
  'GOUVEIO': 'Godello',
  'XYNOMAVRO': 'Xinomavro',
  'BLAUFRAENKISCH': 'Blaufränkisch',
  'GEWUERZTRAMINER': 'Gewürztraminer',
  'SILVANER GRUEN': 'Silvaner',
  'VERDEJO BLANCO': 'Verdejo',
  'GARNACHA BLANCA': 'Grenache Blanc',
  'MUELLER THURGAU WEISS': 'Müller-Thurgau',
  'HARSLEVELUE': 'Hárslevelű',
  'MUSCAT A PETITS GRAINS BLANCS': 'Muscat Blanc à Petits Grains',
  'ALVARINHO': 'Albariño',
};

// Tier 2: Grape families where suffix MUST be kept (multiple commercial variants)
const KEEP_SUFFIX_FAMILIES = new Set([
  'PINOT', 'SAUVIGNON', 'CABERNET', 'MUSCAT', 'CHENIN', 'MOSCATO',
  'MALVASIA', 'TREBBIANO', 'TOCAI', 'ARAMON', 'GRIGNOLINO',
]);

// Color suffixes that can be stripped for Tier 3
const COLOR_SUFFIXES = /\s+(NOIR|BLANC|BLANCHE|BLANCO|BLANCA|BIANCO|BIANCA|WEISS|ROUGE|ROSE|ROSSO|ROSSA|TINTO|TINTA|NERO|NERA|GRIS|GRIGIO|GRIGIA|GRUEN|BLAU|ROT)$/i;

/**
 * Derive display_name from VIVC prime name using three-tier strategy:
 *   Tier 1: Explicit override (26 major grapes)
 *   Tier 2: Keep suffix for multi-variant families (Pinot Noir, Cabernet Sauvignon, etc.)
 *   Tier 3: Title-case, strip color suffix if single-variant
 */
function deriveDisplayName(vivcName, allNames) {
  // Tier 1: Explicit override
  if (DISPLAY_NAME_OVERRIDES[vivcName]) {
    return DISPLAY_NAME_OVERRIDES[vivcName];
  }

  // Tier 2: Check if this is a multi-variant family
  const firstWord = vivcName.split(/\s+/)[0];
  if (KEEP_SUFFIX_FAMILIES.has(firstWord)) {
    return titleCase(vivcName);
  }

  // Tier 3: Strip color suffix if this grape has no sibling variants
  const suffixMatch = vivcName.match(COLOR_SUFFIXES);
  if (suffixMatch) {
    const baseName = vivcName.replace(COLOR_SUFFIXES, '').trim();
    // Check if other grapes share this base name (multi-variant)
    const siblings = allNames.filter(n => n !== vivcName && n.startsWith(baseName + ' '));
    if (siblings.length > 0) {
      // Multi-variant — keep suffix
      return titleCase(vivcName);
    }
    // Single variant — strip suffix
    return titleCase(baseName);
  }

  // No suffix to strip — just title-case
  return titleCase(vivcName);
}

function titleCase(s) {
  return s.toLowerCase()
    .split(/(\s+|-)/g)
    .map(word => {
      if (word.match(/^\s+$/) || word === '-') return word;
      // Preserve common lowercase particles
      if (['de', 'di', 'du', 'da', 'do', 'des', 'del', 'della', 'delle', 'à'].includes(word)) return word;
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join('');
}

// ---------------------------------------------------------------------------
// VIVC ISO3 → our ISO2 country code mapping
// ---------------------------------------------------------------------------
const ISO3_TO_ISO2 = {
  'FRA': 'FR', 'ITA': 'IT', 'ESP': 'ES', 'PRT': 'PT', 'DEU': 'DE',
  'AUT': 'AT', 'GRC': 'GR', 'HUN': 'HU', 'HRV': 'HR', 'SVN': 'SI',
  'GEO': 'GE', 'USA': 'US', 'AUS': 'AU', 'NZL': 'NZ', 'ZAF': 'ZA',
  'ARG': 'AR', 'CHL': 'CL', 'BRA': 'BR', 'URY': 'UY', 'CHE': 'CH',
  'BGR': 'BG', 'ROU': 'RO', 'SRB': 'RS', 'MKD': 'MK', 'MDA': 'MD',
  'CAN': 'CA', 'JPN': 'JP', 'CHN': 'CN', 'ISR': 'IL', 'TUR': 'TR',
  'MAR': 'MA', 'CZE': 'CZ', 'SVK': 'SK', 'RUS': 'RU', 'UKR': 'UA',
  'IND': 'IN', 'MEX': 'MX', 'LBN': 'LB', 'CYP': 'CY', 'TUN': 'TN',
  'DZA': 'DZ', 'ARM': 'AM', 'AZE': 'AZ', 'GBR': 'GB', 'PER': 'PE',
  'POL': 'PL', 'BEL': 'BE', 'LUX': 'LU', 'NLD': 'NL', 'DNK': 'DK',
  'SWE': 'SE', 'MNE': 'ME', 'ALB': 'AL', 'MLT': 'MT', 'BOL': 'BO',
  'COL': 'CO', 'JOR': 'JO', 'LIE': 'LI', 'SMR': 'SM', 'SYR': 'SY',
  'THA': 'TH', 'MMR': 'MM', 'BLR': 'BY',
};

// ---------------------------------------------------------------------------
// Species cleanup
// ---------------------------------------------------------------------------
function cleanSpecies(raw) {
  if (!raw) return null;
  const upper = raw.toUpperCase().trim();
  if (upper.includes('VINIFERA') && !upper.includes('X ') && !upper.includes(' X')) return 'vinifera';
  if (upper.includes('LABRUSCA') && !upper.includes('X ') && !upper.includes(' X')) return 'labrusca';
  if (upper.includes('RIPARIA') && !upper.includes('X ') && !upper.includes(' X')) return 'riparia';
  if (upper.includes('RUPESTRIS') && !upper.includes('X ') && !upper.includes(' X')) return 'rupestris';
  if (upper.includes('INTERSPECIFIC CROSSING') || upper.includes(' X ')) return 'hybrid';
  if (upper.includes('COMPLEX CROSSING') || upper.includes('COMPLEX HYBRID')) return 'complex_hybrid';
  // Corrupted or unrecognized — store null
  if (upper.length < 5 || !upper.includes('VITIS')) return null;
  return 'vinifera'; // default for clean VITIS entries
}

// ---------------------------------------------------------------------------
// Phase 3: Import into Supabase
// ---------------------------------------------------------------------------
async function importToSupabase(cache) {
  console.log(`\n=== PHASE 3: Importing to Supabase ===`);
  if (DRY_RUN) console.log('  [DRY RUN — no DB writes]\n');

  const grapes = Object.values(cache.grapes);
  const allVivcNames = grapes.map(g => g.name);
  console.log(`${grapes.length} wine grapes to import\n`);

  // Load countries for origin matching — use iso_code (2-letter)
  let allCountries = [];
  const { data: countriesData } = await sb.from('countries').select('id, name, iso_code');
  if (countriesData) allCountries = countriesData;

  const countryByName = {};
  const countryByIso2 = {};
  for (const c of allCountries) {
    countryByName[c.name.toUpperCase()] = c.id;
    if (c.iso_code) countryByIso2[c.iso_code.toUpperCase()] = c.id;
  }

  // VIVC country name → our country name mapping
  const COUNTRY_NAME_MAP = {
    'UNITED STATES OF AMERICA': 'UNITED STATES', 'USA': 'UNITED STATES',
    'RUSSIAN FEDERATION': 'RUSSIA', 'MACEDONIA': 'NORTH MACEDONIA',
    'ENGLAND': 'UNITED KINGDOM', 'GREAT BRITAIN': 'UNITED KINGDOM',
    'REPUBLIC OF KOREA': 'SOUTH KOREA', 'KOREA': 'SOUTH KOREA',
  };

  function resolveCountryId(vivcCountry, vivcIso3) {
    // Try ISO3 → ISO2 first (most reliable)
    if (vivcIso3) {
      const iso2 = ISO3_TO_ISO2[vivcIso3.toUpperCase()];
      if (iso2 && countryByIso2[iso2]) return countryByIso2[iso2];
    }
    if (!vivcCountry) return null;
    const upper = vivcCountry.toUpperCase().trim();
    if (countryByName[upper]) return countryByName[upper];
    const mapped = COUNTRY_NAME_MAP[upper];
    if (mapped && countryByName[mapped]) return countryByName[mapped];
    return null;
  }

  // Map VIVC berry color to our wine color
  function mapColor(berrySkin) {
    if (!berrySkin) return null;
    const c = berrySkin.toLowerCase();
    if (c === 'black' || c === 'dark') return 'red';
    if (c === 'white' || c === 'green-yellow' || c === 'green') return 'white';
    if (c === 'grey' || c === 'gray') return 'white';
    if (c === 'rose' || c === 'pink' || c === 'red') return 'red';
    return null;
  }

  function mapGrapeType(utilization) {
    if (!utilization) return 'wine';
    if (utilization.includes('table')) return 'dual';
    return 'wine';
  }

  // Pre-compute display names (two-pass to resolve collisions)
  console.log('  Computing display names...');
  const displayNames = {};
  let tier1 = 0, tier2 = 0, tier3 = 0;
  for (const g of grapes) {
    const dn = deriveDisplayName(g.name, allVivcNames);
    displayNames[g.vivc_number] = dn;
    if (DISPLAY_NAME_OVERRIDES[g.name]) tier1++;
    else if (KEEP_SUFFIX_FAMILIES.has(g.name.split(/\s+/)[0])) tier2++;
    else tier3++;
  }

  // Second pass: fix collisions by reverting to title-cased full VIVC name
  const dnCounts = {};
  for (const [vivc, dn] of Object.entries(displayNames)) {
    if (dnCounts[dn] === undefined) dnCounts[dn] = [];
    dnCounts[dn].push(vivc);
  }
  let collisionsFixed = 0;
  for (const [dn, ids] of Object.entries(dnCounts)) {
    if (ids.length <= 1) continue;
    for (const vivc of ids) {
      const grape = cache.grapes[vivc];
      if (grape && displayNames[vivc] !== titleCase(grape.name)) {
        displayNames[vivc] = titleCase(grape.name);
        collisionsFixed++;
      }
    }
  }

  console.log(`  Display names: ${tier1} Tier 1 overrides, ${tier2} Tier 2 family-kept, ${tier3} Tier 3 auto`);
  if (collisionsFixed > 0) console.log(`  Fixed ${collisionsFixed} display name collisions (reverted to full name)`);

  // Report remaining collisions (true VIVC duplicates — same prime name)
  const dnCounts2 = {};
  for (const [vivc, dn] of Object.entries(displayNames)) {
    if (dnCounts2[dn] === undefined) dnCounts2[dn] = [];
    dnCounts2[dn].push(vivc);
  }
  const remaining = Object.entries(dnCounts2).filter(([, ids]) => ids.length > 1);
  if (remaining.length > 0) {
    console.log(`  ${remaining.length} remaining collisions (true VIVC duplicates — same prime name, different grapes)`);
  }

  // Detect duplicate VIVC prime names to make slugs unique
  const nameCounts = {};
  for (const g of grapes) {
    nameCounts[g.name] = (nameCounts[g.name] || 0) + 1;
  }
  const dupeNames = new Set(Object.keys(nameCounts).filter(n => nameCounts[n] > 1));
  if (dupeNames.size > 0) {
    console.log(`  ${dupeNames.size} duplicate prime names — slugs will include VIVC number`);
  }

  // Insert grapes in batches
  const BATCH_SIZE = 100;
  let inserted = 0;
  let errors = 0;
  let unmappedCountries = {};
  const vivcToUuid = {};

  for (let i = 0; i < grapes.length; i += BATCH_SIZE) {
    const batch = grapes.slice(i, i + BATCH_SIZE);
    const rows = batch.map(g => {
      const countryId = resolveCountryId(g.origin_country, g.origin_country_code);
      if (!countryId && (g.origin_country || g.origin_country_code)) {
        const key = g.origin_country || g.origin_country_code;
        unmappedCountries[key] = (unmappedCountries[key] || 0) + 1;
      }
      const slug = dupeNames.has(g.name)
        ? slugify(g.name) + '-vivc-' + g.vivc_number
        : slugify(g.name);
      return {
        slug,
        name: g.name,
        display_name: displayNames[g.vivc_number],
        color: mapColor(g.berry_skin_color),
        berry_skin_color: g.berry_skin_color || null,
        origin_country_id: countryId,
        origin_region: g.origin_country || null,
        vivc_number: g.vivc_number,
        species: cleanSpecies(g.species),
        grape_type: mapGrapeType(g.utilization),
        crossing_year: g.crossing_year || null,
        breeder: g.breeder || null,
        breeding_institute: g.breeding_institute || null,
        origin_type: g.pedigree_text || g.pedigree_confirmed_text ? 'cross' : null,
        eu_catalog_countries: g.eu_catalog_countries?.length > 0 ? g.eu_catalog_countries : null,
        parentage_confirmed: g.parentage_confirmed || false,
      };
    });

    if (!DRY_RUN) {
      const { data, error } = await sb.from('grapes').insert(rows).select('id, vivc_number');
      if (error) {
        console.error(`  Batch ${Math.floor(i / BATCH_SIZE) + 1} error: ${error.message}`);
        for (const row of rows) {
          const { data: d, error: e } = await sb.from('grapes').insert(row).select('id, vivc_number');
          if (e) {
            console.error(`    ✗ ${row.name}: ${e.message}`);
            errors++;
          } else if (d && d[0]) {
            vivcToUuid[d[0].vivc_number] = d[0].id;
            inserted++;
          }
        }
      } else if (data) {
        for (const d of data) vivcToUuid[d.vivc_number] = d.id;
        inserted += data.length;
      }
    } else {
      inserted += rows.length;
    }

    if ((i + BATCH_SIZE) % 500 === 0 || i + BATCH_SIZE >= grapes.length) {
      console.log(`  Inserted ${inserted}/${grapes.length} grapes (${errors} errors)`);
    }
  }

  console.log(`\nGrapes inserted: ${inserted} (${errors} errors)`);

  if (Object.keys(unmappedCountries).length > 0) {
    console.log(`\n  Unmapped origin countries:`);
    for (const [c, n] of Object.entries(unmappedCountries).sort((a, b) => b[1] - a[1])) {
      console.log(`    ${c}: ${n} grapes`);
    }
  }

  if (DRY_RUN) {
    console.log('  [DRY RUN — skipping synonyms and areas]');
    return;
  }

  // Load UUID mapping if needed
  if (Object.keys(vivcToUuid).length === 0) {
    console.log('  Loading grape UUID mapping from DB...');
    let allGrapes = [];
    let from = 0;
    while (true) {
      const { data } = await sb.from('grapes').select('id, vivc_number').range(from, from + 999);
      if (!data || data.length === 0) break;
      allGrapes = allGrapes.concat(data);
      from += 1000;
    }
    for (const g of allGrapes) {
      if (g.vivc_number) vivcToUuid[g.vivc_number] = g.id;
    }
    console.log(`  Loaded ${Object.keys(vivcToUuid).length} grape UUIDs`);
  }

  // Insert synonyms
  console.log('\n  Inserting synonyms...');
  let synInserted = 0;
  let synErrors = 0;
  const synBatch = [];

  for (const g of grapes) {
    const grapeId = vivcToUuid[g.vivc_number];
    if (!grapeId || !g.synonyms || g.synonyms.length === 0) continue;
    for (const syn of g.synonyms) {
      synBatch.push({
        grape_id: grapeId,
        synonym: syn,
        source: 'vivc',
        synonym_type: 'synonym',
      });
    }
  }

  for (let i = 0; i < synBatch.length; i += BATCH_SIZE) {
    const batch = synBatch.slice(i, i + BATCH_SIZE);
    const { error } = await sb.from('grape_synonyms').insert(batch);
    if (error) {
      // Try one-by-one for unique constraint violations
      for (const row of batch) {
        const { error: e } = await sb.from('grape_synonyms').insert(row);
        if (e) {
          synErrors++;
        } else {
          synInserted++;
        }
      }
    } else {
      synInserted += batch.length;
    }

    if ((i + BATCH_SIZE) % 2000 === 0 || i + BATCH_SIZE >= synBatch.length) {
      console.log(`    Synonyms: ${synInserted}/${synBatch.length} (${synErrors} dupes/errors)`);
    }
  }
  console.log(`  Synonyms inserted: ${synInserted} (${synErrors} dupes/errors)`);

  // Insert planting area data
  console.log('\n  Inserting planting area data...');
  let areaInserted = 0;
  const areaBatch = [];

  for (const g of grapes) {
    const grapeId = vivcToUuid[g.vivc_number];
    if (!grapeId || !g.areas || g.areas.length === 0) continue;
    for (const a of g.areas) {
      const countryId = resolveCountryId(a.country, null);
      if (!countryId) continue;
      areaBatch.push({
        grape_id: grapeId,
        country_id: countryId,
        area_ha: a.area_ha,
        survey_year: a.year,
        source: 'VIVC',
      });
    }
  }

  for (let i = 0; i < areaBatch.length; i += BATCH_SIZE) {
    const batch = areaBatch.slice(i, i + BATCH_SIZE);
    const { error } = await sb.from('grape_plantings').insert(batch);
    if (error) {
      console.error(`    Area batch error: ${error.message}`);
    } else {
      areaInserted += batch.length;
    }
  }
  console.log(`  Area entries inserted: ${areaInserted}`);
}

// ---------------------------------------------------------------------------
// Phase 4: Resolve parentage
// ---------------------------------------------------------------------------
async function resolveParentage(cache) {
  console.log(`\n=== PHASE 4: Resolving parentage ===`);
  if (DRY_RUN) { console.log('  [DRY RUN]'); return; }

  let allGrapes = [];
  let from = 0;
  while (true) {
    const { data } = await sb.from('grapes').select('id, vivc_number, name').range(from, from + 999);
    if (!data || data.length === 0) break;
    allGrapes = allGrapes.concat(data);
    from += 1000;
  }

  const byVivc = {};
  for (const g of allGrapes) {
    if (g.vivc_number) byVivc[g.vivc_number] = g.id;
  }

  let resolved = 0;
  let unresolved = 0;

  for (const [id, grape] of Object.entries(cache.grapes)) {
    const grapeId = byVivc[grape.vivc_number];
    if (!grapeId) continue;

    const updates = {};
    if (grape.parent1_vivc_id && byVivc[String(grape.parent1_vivc_id)]) {
      updates.parent1_grape_id = byVivc[String(grape.parent1_vivc_id)];
    }
    if (grape.parent2_vivc_id && byVivc[String(grape.parent2_vivc_id)]) {
      updates.parent2_grape_id = byVivc[String(grape.parent2_vivc_id)];
    }

    if (Object.keys(updates).length > 0) {
      const { error } = await sb.from('grapes').update(updates).eq('id', grapeId);
      if (error) {
        console.error(`  ✗ ${grape.name}: ${error.message}`);
      } else {
        resolved++;
      }
    } else if (grape.parent1_vivc_id || grape.parent2_vivc_id) {
      unresolved++;
    }
  }

  console.log(`\nParentage resolved: ${resolved}`);
  console.log(`Unresolved (parent not a wine grape): ${unresolved}`);
}

// ---------------------------------------------------------------------------
// Phase 5: Reconnect varietal categories
// ---------------------------------------------------------------------------
async function reconnectVarietalCategories() {
  console.log(`\n=== PHASE 5: Reconnecting varietal categories ===`);
  if (DRY_RUN) { console.log('  [DRY RUN]'); return; }

  const MAPPING_FILE = 'data/varietal_category_grape_mappings.json';
  if (!existsSync(MAPPING_FILE)) {
    console.error('  ✗ Mapping file not found: ' + MAPPING_FILE);
    return;
  }
  const mappings = JSON.parse(readFileSync(MAPPING_FILE, 'utf8'));

  // Load all grapes by name (VIVC names are UPPERCASE)
  let allGrapes = [];
  let from = 0;
  while (true) {
    const { data } = await sb.from('grapes').select('id, name').range(from, from + 999);
    if (!data || data.length === 0) break;
    allGrapes = allGrapes.concat(data);
    from += 1000;
  }

  const grapeByName = {};
  for (const g of allGrapes) {
    grapeByName[g.name.toUpperCase()] = g.id;
  }

  const { data: categories } = await sb.from('varietal_categories').select('id, name');

  let matched = 0;
  let unmatched = 0;

  for (const mapping of mappings) {
    const category = categories.find(c => c.name === mapping.category);
    if (!category) {
      console.log(`  ✗ Category not found: ${mapping.category}`);
      unmatched++;
      continue;
    }

    // Try VIVC UPPERCASE name match
    let grapeId = grapeByName[mapping.grape.toUpperCase()];

    // Try exact case match
    if (!grapeId) {
      const g = allGrapes.find(g => g.name === mapping.grape);
      if (g) grapeId = g.id;
    }

    // Try synonym lookup
    if (!grapeId) {
      const { data: synData } = await sb.from('grape_synonyms')
        .select('grape_id')
        .ilike('synonym', mapping.grape)
        .limit(1);
      if (synData && synData.length > 0) {
        grapeId = synData[0].grape_id;
      }
    }

    if (grapeId) {
      const { error } = await sb.from('varietal_categories')
        .update({ grape_id: grapeId })
        .eq('id', category.id);
      if (error) {
        console.error(`  ✗ ${mapping.category}: ${error.message}`);
        unmatched++;
      } else {
        matched++;
      }
    } else {
      console.log(`  ✗ No grape match for: ${mapping.grape} (category: ${mapping.category})`);
      unmatched++;
    }
  }

  console.log(`\nVarietal categories reconnected: ${matched}/${mappings.length}`);
  if (unmatched > 0) console.log(`Unmatched: ${unmatched}`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('╔══════════════════════════════════════════════╗');
  console.log('║  VIVC Grape Rebuild                         ║');
  console.log('║  Source: vivc.de (JKI)                      ║');
  console.log('╚══════════════════════════════════════════════╝');
  if (DRY_RUN) console.log('\n*** DRY RUN MODE ***\n');

  const cache = loadCache();

  const shouldRun = (phase) => !PHASE_ONLY || PHASE_ONLY === phase;

  if (shouldRun(1)) await crawlPassports(cache);
  if (shouldRun(2)) await enrichGrapes(cache);
  if (shouldRun(3)) await importToSupabase(cache);
  if (shouldRun(4)) await resolveParentage(cache);
  if (shouldRun(5)) await reconnectVarietalCategories();

  console.log('\n✓ Done');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
