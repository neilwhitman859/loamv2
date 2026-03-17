#!/usr/bin/env node
/**
 * import_lwin.mjs — Import LWIN database as identity backbone
 *
 * Parses the LWIN CSV (186K wine records) and imports producers + wines
 * with LWIN-7 codes. No vintages, scores, or prices — LWIN is pure identity.
 *
 * Modes:
 *   --analyze     Show match rates without writing anything
 *   --dry-run     Show what would be imported without writing
 *   --import      Actually import to DB
 *   --limit N     Process only first N wine rows (for testing)
 *   --country XX  Only process wines from country XX (e.g., "France")
 *
 * Usage:
 *   node scripts/import_lwin.mjs --analyze
 *   node scripts/import_lwin.mjs --analyze --country France
 *   node scripts/import_lwin.mjs --import --limit 500
 *   node scripts/import_lwin.mjs --import
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

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY);

// ── CLI ─────────────────────────────────────────────────────
const args = process.argv.slice(2);
const MODE = args.includes('--import') ? 'import' : args.includes('--dry-run') ? 'dry-run' : 'analyze';
const LIMIT = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : Infinity;
const COUNTRY_FILTER = args.includes('--country') ? args[args.indexOf('--country') + 1] : null;

console.log(`Mode: ${MODE}${LIMIT < Infinity ? `, limit: ${LIMIT}` : ''}${COUNTRY_FILTER ? `, country: ${COUNTRY_FILTER}` : ''}`);

// ── Helpers ─────────────────────────────────────────────────
function normalize(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

function slugify(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

function parseCSVLine(line) {
  const cols = [];
  let inQuote = false, field = '';
  for (const ch of line) {
    if (ch === '"') { inQuote = !inQuote; continue; }
    if (ch === ',' && !inQuote) { cols.push(field); field = ''; continue; }
    field += ch;
  }
  cols.push(field);
  return cols;
}

async function fetchAll(table, columns = '*', batchSize = 1000) {
  const all = [];
  let offset = 0;
  while (true) {
    const { data, error } = await sb.from(table).select(columns).range(offset, offset + batchSize - 1);
    if (error) throw new Error(`fetchAll ${table}: ${error.message}`);
    all.push(...data);
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return all;
}

// ── LWIN → Loam Field Mapping ───────────────────────────────

// LWIN COLOUR → wines.color
const COLOR_MAP = {
  'Red': 'red',
  'White': 'white',
  'Rose': 'rose',
  'Mixed': null, // multi-pack or mixed — skip color
};

// LWIN TYPE/SUB_TYPE → wines.wine_type + wines.effervescence
function mapWineType(type, subType) {
  if (type === 'Wine' && subType === 'Still') return { wine_type: 'table', effervescence: 'still' };
  if (type === 'Wine' && subType === 'Sparkling') return { wine_type: 'sparkling', effervescence: 'sparkling' };
  if (type === 'Champagne') return { wine_type: 'sparkling', effervescence: 'sparkling' };
  if (type === 'Fortified Wine') {
    if (subType === 'Port') return { wine_type: 'fortified', effervescence: 'still' };
    if (subType === 'Madeira') return { wine_type: 'fortified', effervescence: 'still' };
    if (subType === 'Sherry') return { wine_type: 'fortified', effervescence: 'still' };
    if (subType === 'Vin Doux Naturel') return { wine_type: 'dessert', effervescence: 'still' };
    if (subType === 'Marsala') return { wine_type: 'fortified', effervescence: 'still' };
    if (subType === 'Moscatel de Setubal') return { wine_type: 'dessert', effervescence: 'still' };
    if (subType === 'Rutherglen') return { wine_type: 'fortified', effervescence: 'still' };
    if (subType === 'Montilla-Moriles') return { wine_type: 'fortified', effervescence: 'still' };
    return { wine_type: 'fortified', effervescence: 'still' };
  }
  // Fallback
  return { wine_type: 'table', effervescence: 'still' };
}

// LWIN VINTAGE_CONFIG → wines.is_nv
function mapVintageConfig(vc) {
  if (vc === 'nonSequential') return true; // NV wines
  return false; // sequential or singleVintageOnly
}

// LWIN REGION → Loam region name mapping
// LWIN uses English region names; our DB has a mix of English and local names
const REGION_NAME_MAP = {
  // France
  'burgundy': 'bourgogne',
  'rhone': 'rhône valley',
  'loire': 'loire valley',
  'champagne': 'champagne',
  'bordeaux': 'bordeaux',
  'alsace': 'alsace',
  'languedoc': 'languedoc-roussillon',
  'beaujolais': 'beaujolais',
  'provence': 'provence',
  'corsica': 'corse',
  'jura': 'jura',
  'savoie': 'savoie',
  'roussillon': 'languedoc-roussillon',
  'south west france': 'southwest france',
  // Italy (LWIN uses English/Italian mix; our DB uses English L1 names)
  'piedmont': 'piemonte',
  'tuscany': 'tuscany',
  'sicily': 'sicily',
  'sardinia': 'sardinia',
  'trentino alto adige': 'trentino-alto adige',
  'friuli venezia giulia': 'friuli-venezia giulia',
  'emilia romagna': 'emilia-romagna',
  'lombardia': 'lombardy',
  'puglia': 'puglia',
  'campania': 'campania',
  'veneto': 'veneto',
  'abruzzo': 'abruzzo',
  'umbria': 'umbria',
  'lazio': 'lazio',
  'liguria': 'liguria',
  'calabria': 'calabria',
  'marche': 'marche',
  'basilicata': 'basilicata',
  'molise': 'molise',
  'prosecco': 'veneto', // Prosecco is in Veneto/FVG
  // Germany
  'mosel': 'mosel',
  'pfalz': 'pfalz',
  'rheingau': 'rheingau',
  'rheinhessen': 'rheinhessen',
  'nahe': 'nahe',
  'franken': 'franken',
  'wurttemberg': 'württemberg',
  'mittelrhein': 'mittelrhein',
  'sachsen': 'sachsen',
  'saale unstrut': 'saale-unstrut',
  'ahr': 'ahr',
  // Spain
  'castilla y leon': 'castilla y león',
  'castilla la mancha': 'castilla-la mancha',
  'catalunya': 'catalunya',
  'andalucia': 'andalucía',
  'aragon': 'aragón',
  'pais vasco': 'país vasco',
  'extremadura': 'extremadura',
  'galicia': 'the north west', // Galicia is in NW Spain region grouping
  'murcia': 'the levante', // Murcia is in Levante grouping
  'navarra': 'navarra',
  'cava': 'catalunya', // Cava is based in Catalunya
  // Portugal
  'douro': 'douro',
  'dao': 'dão',
  'alentejano': 'alentejo',
  'porto': 'douro', // Port is from Douro
  // US
  'california': 'california',
  'washington': 'washington',
  'oregon': 'oregon',
  'new york': 'new york',
  'virginia': 'virginia',
  'walla walla valley': 'washington', // This is an appellation, not a region
  'arizona': 'arizona',
  'texas': 'texas',
  'michigan': 'michigan',
  'colorado': 'colorado',
  'pennsylvania': 'pennsylvania',
  'idaho': 'idaho',
  'north carolina': 'north carolina',
  // Australia
  'south australia': 'south australia',
  'victoria': 'victoria',
  'western australia': 'western australia',
  'new south wales': 'new south wales',
  'tasmania': 'tasmania',
  'south eastern australia': 'south eastern australia',
  'queensland': 'queensland',
  // New Zealand
  'marlborough': 'marlborough',
  "hawke's bay": "hawke's bay",
  'central otago': 'central otago',
  'wairarapa': 'martinborough', // Wairarapa's main sub-region; try both
  'canterbury': 'canterbury',
  'auckland': 'north island', // Auckland falls under North Island L1
  'nelson': 'nelson',
  'gisborne': 'gisborne',
  // South Africa
  'coastal region': 'coastal region',
  'cape south coast': 'cape south coast',
  'breede river valley': 'breede river valley',
  'olifants river': 'olifants river',
  'klein karoo': 'klein karoo',
  // Austria
  'niederosterreich': 'niederösterreich',
  'burgenland': 'burgenland',
  'steiermark': 'steiermark',
  'wien': 'wien',
  // Argentina
  'mendoza': 'mendoza',
  'patagonia': 'patagonia',
  'salta': 'salta',
  // Chile (LWIN uses short names; our DB uses "X Region" format)
  'central valley': 'central valley region',
  'aconcagua': 'aconcagua region',
  'sur': 'southern region',
  'coquimbo': 'coquimbo region',
};

// LWIN DESIGNATION → our designation system
const DESIGNATION_MAP = {
  'AOP': 'AOP/AOC',
  'AOC': 'AOP/AOC',
  'DOC': 'DOC',
  'DOCG': 'DOCG',
  'AVA': 'AVA',
  'GI': 'GI',
  'DO': 'DO',
  'DOCa': 'DOCa',
  'WO': 'WO',
  'VQA': 'VQA',
  'DAC': 'DAC',
  'IGT': 'IGT',
  'IGP': 'IGP',
  'VdF': 'VdF',
  'Qualitatswein': 'Qualitätswein',
  'Pradikatswein': 'Prädikatswein',
  'VR': 'VR',
  'VdT': 'VdT',
  'PDO': 'PDO',
};

// LWIN CLASSIFICATION → our classification system + level
const CLASSIFICATION_MAP = {
  'Grand Cru': { system_slug: 'burgundy-vineyard', level_name: 'Grand Cru' },
  'Premier Cru': { system_slug: 'burgundy-vineyard', level_name: 'Premier Cru' },
  'Grand Cru Classe': { system_slug: 'saint-emilion', level_name: 'Grand Cru Classé' },
  'Premier Cru Classe': { system_slug: 'bordeaux-1855-medoc', level_name: 'Premier Cru' },
  '2eme Cru Classe': { system_slug: 'bordeaux-1855-medoc', level_name: 'Deuxième Cru' },
  '3eme Cru Classe': { system_slug: 'bordeaux-1855-medoc', level_name: 'Troisième Cru' },
  '4eme Cru Classe': { system_slug: 'bordeaux-1855-medoc', level_name: 'Quatrième Cru' },
  '5eme Cru Classe': { system_slug: 'bordeaux-1855-medoc', level_name: 'Cinquième Cru' },
  'Premier Grand Cru Classe A': { system_slug: 'saint-emilion', level_name: 'Premier Grand Cru Classé A' },
  'Premier Grand Cru Classe B': { system_slug: 'saint-emilion', level_name: 'Premier Grand Cru Classé B' },
  'Premier Cru Superieur': { system_slug: 'bordeaux-1855-sauternes', level_name: 'Premier Cru Supérieur' },
  'Erste Lage': { system_slug: 'vdp-classification', level_name: 'Erste Lage' },
  'Cru Classe': { system_slug: 'graves-pessac-leognan', level_name: 'Cru Classé' },
};

// ── Parse LWIN CSV ──────────────────────────────────────────
console.log('\nParsing LWIN CSV...');
const csvData = readFileSync('data/lwin_database.csv', 'utf8');
const lines = csvData.split('\n');
const header = parseCSVLine(lines[0]);

const wineRows = [];
let skippedNonWine = 0;
let skippedStatus = 0;

for (let i = 1; i < lines.length; i++) {
  if (!lines[i].trim()) continue;
  const cols = parseCSVLine(lines[i]);

  const type = cols[12];
  if (type !== 'Wine' && type !== 'Fortified Wine' && type !== 'Champagne') {
    skippedNonWine++;
    continue;
  }

  const status = cols[1];
  if (status === 'Deleted') {
    skippedStatus++;
    continue;
  }

  const row = {
    lwin: cols[0],
    status: cols[1],
    display_name: cols[2],
    producer_title: cols[3] === 'NA' ? null : cols[3],
    producer_name: cols[4] === 'NA' ? null : cols[4],
    wine_name: cols[5] === 'NA' ? null : cols[5],
    country: cols[6] === 'NA' ? null : cols[6],
    region: cols[7] === 'NA' ? null : cols[7],
    sub_region: cols[8] === 'NA' ? null : cols[8],
    site: cols[9] === 'NA' ? null : cols[9],
    parcel: cols[10] === 'NA' ? null : cols[10],
    color: cols[11] === 'NA' ? null : cols[11],
    type: cols[12],
    sub_type: cols[13] === 'NA' ? null : cols[13],
    designation: cols[14] === 'NA' ? null : cols[14],
    classification: cols[15] === 'NA' ? null : cols[15],
    vintage_config: cols[16] === 'NA' ? null : cols[16],
    first_vintage: cols[17] === 'NA' ? null : parseInt(cols[17]) || null,
    final_vintage: cols[18] === 'NA' ? null : parseInt(cols[18]) || null,
  };

  if (COUNTRY_FILTER && row.country !== COUNTRY_FILTER) continue;

  wineRows.push(row);
  if (wineRows.length >= LIMIT) break;
}

console.log(`Parsed ${wineRows.length} wine rows (skipped ${skippedNonWine} non-wine, ${skippedStatus} deleted)`);

// ── Load Reference Data ─────────────────────────────────────
console.log('\nLoading reference data...');
const [countries, regions, appellations, appAliases, regionAliases, classifications, classificationLevels] = await Promise.all([
  fetchAll('countries', 'id,name'),
  fetchAll('regions', 'id,name,country_id,parent_id,is_catch_all'),
  fetchAll('appellations', 'id,name,country_id,region_id'),
  fetchAll('appellation_aliases', 'id,alias,appellation_id'),
  fetchAll('region_aliases', 'id,name,region_id'),
  fetchAll('classifications', 'id,slug,name'),
  fetchAll('classification_levels', 'id,classification_id,level_name,level_rank'),
]);

// Build lookup maps
const countryMap = new Map();
for (const c of countries) {
  countryMap.set(c.name.toLowerCase(), c.id);
}
// Add common aliases
countryMap.set('united states', countryMap.get('united states') || countryMap.get('usa'));
countryMap.set('usa', countryMap.get('united states') || countryMap.get('usa'));

const regionMap = new Map(); // key: normalized name | country_id → { id, name }
for (const r of regions) {
  const lower = r.name.toLowerCase();
  const norm = normalize(r.name);
  regionMap.set(`${lower}|${r.country_id}`, r);
  regionMap.set(`${norm}|${r.country_id}`, r);
  regionMap.set(lower, r); // fallback without country
}
for (const ra of regionAliases) {
  const region = regions.find(r => r.id === ra.region_id);
  if (region) {
    const norm = normalize(ra.name);
    regionMap.set(`${norm}|${region.country_id}`, region);
    regionMap.set(norm, region);
  }
}

const appellationMap = new Map(); // key: normalized name → id
for (const a of appellations) {
  const lower = a.name.toLowerCase();
  const norm = normalize(a.name);
  appellationMap.set(lower, a);
  appellationMap.set(norm, a);
}
for (const aa of appAliases) {
  const app = appellations.find(a => a.id === aa.appellation_id);
  if (app) {
    const norm = normalize(aa.alias);
    appellationMap.set(norm, app);
    appellationMap.set(aa.alias.toLowerCase(), app);
  }
}

const classificationMap = new Map();
for (const c of classifications) {
  classificationMap.set(c.slug, c);
}
const classLevelMap = new Map(); // key: classification_id|level_name → level
for (const cl of classificationLevels) {
  classLevelMap.set(`${cl.classification_id}|${cl.level_name.toLowerCase()}`, cl);
}

console.log(`  ${countries.length} countries, ${regions.length} regions, ${appellations.length} appellations`);
console.log(`  ${appAliases.length} appellation aliases, ${regionAliases.length} region aliases`);
console.log(`  ${classifications.length} classifications, ${classificationLevels.length} classification levels`);

// ── Resolution Functions ────────────────────────────────────

function resolveCountry(name) {
  if (!name) return null;
  return countryMap.get(name.toLowerCase()) || null;
}

function resolveRegion(lwinRegion, countryId) {
  if (!lwinRegion) return null;
  const lower = lwinRegion.toLowerCase();

  // Try mapped name first
  const mapped = REGION_NAME_MAP[lower];
  if (mapped) {
    const norm = normalize(mapped);
    if (countryId) {
      const r = regionMap.get(`${norm}|${countryId}`);
      if (r) return r;
    }
    const r2 = regionMap.get(norm);
    if (r2) return r2;
  }

  // Try direct match
  const norm = normalize(lower);
  if (countryId) {
    const r = regionMap.get(`${norm}|${countryId}`);
    if (r) return r;
  }
  const r2 = regionMap.get(norm);
  if (r2) return r2;

  return null;
}

function resolveAppellation(subRegion, site, countryId) {
  // LWIN sub_region often IS an appellation (e.g., "Margaux", "Barolo", "Napa Valley")
  // LWIN site is even more specific (vineyard names like "Les Charmes", "Bussia")
  // Try sub_region first as appellation
  if (subRegion) {
    const norm = normalize(subRegion);
    const a = appellationMap.get(norm) || appellationMap.get(subRegion.toLowerCase());
    if (a) return a;
  }
  // Try site as appellation (for Burgundy Premier/Grand Cru vineyards)
  if (site) {
    const norm = normalize(site);
    const a = appellationMap.get(norm) || appellationMap.get(site.toLowerCase());
    if (a) return a;
  }
  return null;
}

function resolveClassification(lwinClass) {
  if (!lwinClass) return null;
  const mapping = CLASSIFICATION_MAP[lwinClass];
  if (!mapping) return null;

  const system = classificationMap.get(mapping.system_slug);
  if (!system) return null;

  let level = null;
  if (mapping.level_name) {
    const norm = mapping.level_name.toLowerCase();
    level = classLevelMap.get(`${system.id}|${norm}`);
  }

  return { system, level };
}

// ── Analysis ────────────────────────────────────────────────
console.log('\nResolving references...');

const stats = {
  total: wineRows.length,
  countryResolved: 0,
  countryMissing: new Map(),
  regionResolved: 0,
  regionMissing: new Map(),
  appellationResolved: 0,
  appellationMissing: new Map(),
  classificationResolved: 0,
  classificationMissing: new Map(),
  uniqueProducers: new Set(),
  producerNamesBlank: 0,
  colorMapped: 0,
  typeMapped: 0,
  hasFirstVintage: 0,
  isNV: 0,
};

const resolvedRows = [];

for (const row of wineRows) {
  const countryId = resolveCountry(row.country);
  if (countryId) stats.countryResolved++;
  else if (row.country) stats.countryMissing.set(row.country, (stats.countryMissing.get(row.country) || 0) + 1);

  const region = resolveRegion(row.region, countryId);
  if (region) stats.regionResolved++;
  else if (row.region) stats.regionMissing.set(`${row.country}|${row.region}`, (stats.regionMissing.get(`${row.country}|${row.region}`) || 0) + 1);

  const appellation = resolveAppellation(row.sub_region, row.site, countryId);
  if (appellation) stats.appellationResolved++;
  else if (row.sub_region) stats.appellationMissing.set(`${row.country}|${row.sub_region}`, (stats.appellationMissing.get(`${row.country}|${row.sub_region}`) || 0) + 1);

  const classification = resolveClassification(row.classification);
  if (classification) stats.classificationResolved++;
  else if (row.classification) stats.classificationMissing.set(row.classification, (stats.classificationMissing.get(row.classification) || 0) + 1);

  if (row.color && COLOR_MAP[row.color] !== undefined) stats.colorMapped++;
  stats.typeMapped++;
  if (row.first_vintage) stats.hasFirstVintage++;
  if (mapVintageConfig(row.vintage_config)) stats.isNV++;

  const producerKey = row.producer_name || row.display_name?.split(',')[0]?.trim();
  if (producerKey) stats.uniqueProducers.add(producerKey);
  else stats.producerNamesBlank++;

  resolvedRows.push({
    ...row,
    _countryId: countryId,
    _region: region,
    _appellation: appellation,
    _classification: classification,
    _producerKey: producerKey,
  });
}

// ── Print Analysis Report ───────────────────────────────────
console.log('\n═══════════════════════════════════════════════════');
console.log('  LWIN IMPORT ANALYSIS REPORT');
console.log('═══════════════════════════════════════════════════\n');

console.log(`Total wine rows: ${stats.total}`);
console.log(`Unique producers: ${stats.uniqueProducers.size}`);
console.log(`Producer names blank: ${stats.producerNamesBlank}`);
console.log(`NV wines: ${stats.isNV}`);
console.log(`Has first_vintage: ${stats.hasFirstVintage}\n`);

const pct = (n) => `${((n / stats.total) * 100).toFixed(1)}%`;

console.log('RESOLUTION RATES:');
console.log(`  Country:        ${stats.countryResolved}/${stats.total} (${pct(stats.countryResolved)})`);
console.log(`  Region:         ${stats.regionResolved}/${stats.total} (${pct(stats.regionResolved)})`);
console.log(`  Appellation:    ${stats.appellationResolved}/${stats.total} (${pct(stats.appellationResolved)})`);
console.log(`  Classification: ${stats.classificationResolved}/${stats.total} (${pct(stats.classificationResolved)})`);

if (stats.countryMissing.size > 0) {
  console.log('\nUNRESOLVED COUNTRIES:');
  [...stats.countryMissing.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20)
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));
}

if (stats.regionMissing.size > 0) {
  console.log('\nUNRESOLVED REGIONS (top 30):');
  [...stats.regionMissing.entries()].sort((a, b) => b[1] - a[1]).slice(0, 30)
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));
}

if (stats.appellationMissing.size > 0) {
  console.log('\nUNRESOLVED APPELLATIONS (top 30):');
  [...stats.appellationMissing.entries()].sort((a, b) => b[1] - a[1]).slice(0, 30)
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));
}

if (stats.classificationMissing.size > 0) {
  console.log('\nUNRESOLVED CLASSIFICATIONS:');
  [...stats.classificationMissing.entries()].sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));
}

if (MODE === 'analyze') {
  console.log('\n✅ Analysis complete. Run with --import to write to DB.');
  process.exit(0);
}

// ── Import Mode ─────────────────────────────────────────────
console.log('\n\nStarting import...');

// Group by producer
const producerGroups = new Map();
for (const row of resolvedRows) {
  const key = row._producerKey;
  if (!key) continue;
  if (!producerGroups.has(key)) producerGroups.set(key, []);
  producerGroups.get(key).push(row);
}

console.log(`${producerGroups.size} producers to process`);

// Check for existing LWIN wines to avoid duplicates
const existingLwins = new Set();
let lwOffset = 0;
while (true) {
  const { data, error } = await sb.from('wines').select('lwin').not('lwin', 'is', null).range(lwOffset, lwOffset + 999);
  if (error) throw error;
  data.forEach(w => existingLwins.add(w.lwin));
  if (data.length < 1000) break;
  lwOffset += 1000;
}
console.log(`${existingLwins.size} wines with LWIN already in DB`);

// Check for existing producers by name_normalized
const existingProducers = new Map(); // normalized name → id
let prOffset = 0;
while (true) {
  const { data, error } = await sb.from('producers').select('id,name,name_normalized').range(prOffset, prOffset + 999);
  if (error) throw error;
  data.forEach(p => {
    existingProducers.set(p.name_normalized, p.id);
    existingProducers.set(p.name.toLowerCase(), p.id);
  });
  if (data.length < 1000) break;
  prOffset += 1000;
}
console.log(`${existingProducers.size / 2} existing producers in DB`);

let producersCreated = 0;
let producersSkipped = 0;
let winesCreated = 0;
let winesSkipped = 0;
let classificationsLinked = 0;
let errors = 0;
let batchNum = 0;

// Process in batches of 50 producers
const producerEntries = [...producerGroups.entries()];
const BATCH_SIZE = 50;

for (let b = 0; b < producerEntries.length; b += BATCH_SIZE) {
  batchNum++;
  const batch = producerEntries.slice(b, b + BATCH_SIZE);

  const producerInserts = [];
  const wineInserts = [];
  const classInserts = [];

  for (const [producerName, rows] of batch) {
    // Check if producer exists
    const normName = normalize(producerName);
    let producerId = existingProducers.get(normName) || existingProducers.get(producerName.toLowerCase());

    if (!producerId) {
      // Determine producer country from most common wine country
      const countryCounts = {};
      for (const r of rows) {
        if (r._countryId) countryCounts[r._countryId] = (countryCounts[r._countryId] || 0) + 1;
      }
      const topCountry = Object.entries(countryCounts).sort((a, b) => b[1] - a[1])[0];

      // Determine producer region from most common wine region
      const regionCounts = {};
      for (const r of rows) {
        if (r._region) regionCounts[r._region.id] = (regionCounts[r._region.id] || 0) + 1;
      }
      const topRegion = Object.entries(regionCounts).sort((a, b) => b[1] - a[1])[0];

      producerId = randomUUID();
      const producerTitle = rows[0].producer_title;
      const fullName = producerTitle ? `${producerTitle} ${producerName}` : producerName;

      producerInserts.push({
        id: producerId,
        slug: slugify(fullName),
        name: fullName,
        name_normalized: normName,
        country_id: topCountry ? topCountry[0] : null,
        region_id: topRegion ? topRegion[0] : null,
        producer_type: 'estate', // default; LWIN doesn't distinguish
        source_id: null, // TODO: create LWIN source_type
      });

      existingProducers.set(normName, producerId);
      producersCreated++;
    } else {
      producersSkipped++;
    }

    // Create wines
    for (const row of rows) {
      if (existingLwins.has(row.lwin)) {
        winesSkipped++;
        continue;
      }

      const { wine_type, effervescence } = mapWineType(row.type, row.sub_type);
      const color = row.color ? COLOR_MAP[row.color] : null;
      const isNV = mapVintageConfig(row.vintage_config);

      const wineName = row.wine_name || row.display_name || 'Unknown';
      const wineSlug = slugify(`${row._producerKey}-${wineName}-${row.lwin}`);

      const wine = {
        id: randomUUID(),
        slug: wineSlug,
        name: wineName,
        name_normalized: normalize(wineName),
        producer_id: producerId,
        country_id: row._countryId,
        region_id: row._region?.id || null,
        appellation_id: row._appellation?.id || null,
        color: color,
        wine_type: wine_type,
        effervescence: effervescence,
        is_nv: isNV,
        lwin: row.lwin,
        first_vintage_year: row.first_vintage,
      };

      wineInserts.push(wine);
      existingLwins.add(row.lwin);
      winesCreated++;

      // Link classification if resolved
      if (row._classification && row._classification.level) {
        classInserts.push({
          id: randomUUID(),
          entity_type: 'wine',
          entity_id: wine.id,
          classification_id: row._classification.system.id,
          classification_level_id: row._classification.level.id,
        });
        classificationsLinked++;
      }
    }
  }

  // Insert batch
  if (MODE === 'import' && producerInserts.length > 0) {
    const { error } = await sb.from('producers').upsert(producerInserts, { onConflict: 'slug', ignoreDuplicates: true });
    if (error) {
      console.error(`  Batch ${batchNum} producer error: ${error.message}`);
      errors++;
    }
  }

  if (MODE === 'import' && wineInserts.length > 0) {
    // Insert wines in sub-batches of 200 to avoid statement timeout
    for (let w = 0; w < wineInserts.length; w += 200) {
      const chunk = wineInserts.slice(w, w + 200);
      const { error } = await sb.from('wines').upsert(chunk, { onConflict: 'slug', ignoreDuplicates: true });
      if (error) {
        console.error(`  Batch ${batchNum} wine chunk error: ${error.message}`);
        errors++;
      }
    }
  }

  if (MODE === 'import' && classInserts.length > 0) {
    const { error } = await sb.from('entity_classifications').insert(classInserts);
    if (error && !error.message.includes('duplicate')) {
      console.error(`  Batch ${batchNum} classification error: ${error.message}`);
      errors++;
    }
  }

  if (batchNum % 20 === 0 || b + BATCH_SIZE >= producerEntries.length) {
    console.log(`  Batch ${batchNum}/${Math.ceil(producerEntries.length / BATCH_SIZE)} — producers: +${producerInserts.length}, wines: +${wineInserts.length}`);
  }
}

console.log('\n═══════════════════════════════════════════════════');
console.log('  IMPORT COMPLETE');
console.log('═══════════════════════════════════════════════════\n');
console.log(`Producers created: ${producersCreated} (skipped ${producersSkipped} existing)`);
console.log(`Wines created:     ${winesCreated} (skipped ${winesSkipped} existing LWIN)`);
console.log(`Classifications:   ${classificationsLinked}`);
console.log(`Errors:            ${errors}`);
