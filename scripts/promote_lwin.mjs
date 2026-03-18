#!/usr/bin/env node
/**
 * promote_lwin.mjs — Promote LWIN staging records to canonical tables.
 *
 * Reads from source_lwin staging table, cross-matches against existing
 * canonical producers/wines, then creates new canonical records for unmatched.
 * Updates staging rows with canonical_wine_id/canonical_producer_id links.
 * Stores LWIN-7 codes in external_ids.
 *
 * Usage:
 *   node scripts/promote_lwin.mjs --analyze           # Match stats only
 *   node scripts/promote_lwin.mjs --dry-run            # Show what would happen
 *   node scripts/promote_lwin.mjs --import             # Actually promote
 *   node scripts/promote_lwin.mjs --import --limit 500 # Test with small batch
 *   node scripts/promote_lwin.mjs --import --country France
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
const VERBOSE = args.includes('--verbose');

console.log(`Mode: ${MODE}${LIMIT < Infinity ? `, limit: ${LIMIT}` : ''}${COUNTRY_FILTER ? `, country: ${COUNTRY_FILTER}` : ''}`);

// ── Helpers ─────────────────────────────────────────────────
function normalize(s) {
  if (!s) return '';
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

function slugify(s) {
  if (!s) return '';
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

/** Strip corporate suffixes from producer names */
function stripCorporateSuffix(name) {
  if (!name) return name;
  return name
    .replace(/\s*,?\s*\b(Inc\.?|LLC|Ltd\.?|S\.?A\.?S\.?|S\.?r\.?l\.?|GmbH|S\.?A\.?|S\.?L\.?|AG|Co\.?|Corp\.?|Pty\.?|Ltda\.?)\s*$/i, '')
    .trim();
}

async function fetchAll(table, columns = '*', filter = null, batchSize = 1000) {
  const all = [];
  let offset = 0;
  while (true) {
    let query = sb.from(table).select(columns).range(offset, offset + batchSize - 1);
    if (filter) query = filter(query);
    const { data, error } = await query;
    if (error) throw new Error(`fetchAll ${table}: ${error.message}`);
    all.push(...data);
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return all;
}

// ── LWIN Field Maps ─────────────────────────────────────────

const COLOR_MAP = { 'Red': 'red', 'White': 'white', 'Rose': 'rose', 'Mixed': null };

function mapWineType(wineType) {
  if (!wineType) return { wine_type: 'table', effervescence: 'still' };
  const t = wineType.toLowerCase();
  if (t === 'sparkling' || t === 'champagne') return { wine_type: 'sparkling', effervescence: 'sparkling' };
  if (t === 'fortified') return { wine_type: 'fortified', effervescence: 'still' };
  return { wine_type: 'table', effervescence: 'still' };
}

// LWIN REGION → Loam region name mapping
const REGION_NAME_MAP = {
  'burgundy': 'bourgogne', 'rhone': 'rhône valley', 'loire': 'loire valley',
  'languedoc': 'languedoc-roussillon', 'corsica': 'corse', 'roussillon': 'languedoc-roussillon',
  'south west france': 'southwest france',
  'piedmont': 'piemonte', 'trentino alto adige': 'trentino-alto adige',
  'friuli venezia giulia': 'friuli-venezia giulia', 'emilia romagna': 'emilia-romagna',
  'lombardia': 'lombardy', 'prosecco': 'veneto',
  'wurttemberg': 'württemberg', 'saale unstrut': 'saale-unstrut',
  'castilla y leon': 'castilla y león', 'castilla la mancha': 'castilla-la mancha',
  'andalucia': 'andalucía', 'aragon': 'aragón', 'pais vasco': 'país vasco',
  'galicia': 'the north west', 'murcia': 'the levante', 'cava': 'catalunya',
  'dao': 'dão', 'alentejano': 'alentejo', 'porto': 'douro',
  'walla walla valley': 'washington',
  'south eastern australia': 'south eastern australia',
  'wairarapa': 'martinborough', 'auckland': 'north island',
  'niederosterreich': 'niederösterreich',
  'central valley': 'central valley region', 'aconcagua': 'aconcagua region',
  'sur': 'southern region', 'coquimbo': 'coquimbo region',
};

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

// ── Load Reference Data ─────────────────────────────────────
console.log('\nLoading reference data...');

const [countries, regions, appellations, appAliases, regionAliases,
  classifications, classificationLevels, existingProducers, existingWines] = await Promise.all([
  fetchAll('countries', 'id,name'),
  fetchAll('regions', 'id,name,country_id,parent_id,is_catch_all'),
  fetchAll('appellations', 'id,name,country_id,region_id'),
  fetchAll('appellation_aliases', 'id,alias,appellation_id'),
  fetchAll('region_aliases', 'id,name,region_id'),
  fetchAll('classifications', 'id,slug,name'),
  fetchAll('classification_levels', 'id,classification_id,level_name,level_rank'),
  fetchAll('producers', 'id,name,name_normalized,country_id,region_id', q => q.is('deleted_at', null)),
  fetchAll('wines', 'id,name,name_normalized,producer_id,country_id,lwin,identity_confidence', q => q.is('deleted_at', null)),
]);

console.log(`  ${countries.length} countries, ${regions.length} regions, ${appellations.length} appellations`);
console.log(`  ${appAliases.length} app aliases, ${regionAliases.length} region aliases`);
console.log(`  ${existingProducers.length} existing producers, ${existingWines.length} existing wines`);

// ── Build Lookup Maps ───────────────────────────────────────

// Country
const countryMap = new Map();
for (const c of countries) countryMap.set(c.name.toLowerCase(), c.id);
countryMap.set('usa', countryMap.get('united states'));

// Region
const regionMap = new Map();
for (const r of regions) {
  regionMap.set(`${normalize(r.name)}|${r.country_id}`, r);
  regionMap.set(normalize(r.name), r);
}
for (const ra of regionAliases) {
  const region = regions.find(r => r.id === ra.region_id);
  if (region) {
    regionMap.set(`${normalize(ra.name)}|${region.country_id}`, region);
    regionMap.set(normalize(ra.name), region);
  }
}

// Appellation
const appellationMap = new Map();
for (const a of appellations) {
  appellationMap.set(normalize(a.name), a);
  appellationMap.set(a.name.toLowerCase(), a);
}
for (const aa of appAliases) {
  const app = appellations.find(a => a.id === aa.appellation_id);
  if (app) {
    appellationMap.set(normalize(aa.alias), app);
    appellationMap.set(aa.alias.toLowerCase(), app);
  }
}

// Classification
const classificationSlugMap = new Map();
for (const c of classifications) classificationSlugMap.set(c.slug, c);
const classLevelMap = new Map();
for (const cl of classificationLevels) {
  classLevelMap.set(`${cl.classification_id}|${cl.level_name.toLowerCase()}`, cl);
}

// Existing producers: normalized name → producer
const producerByNorm = new Map();
const producerById = new Map();
for (const p of existingProducers) {
  producerByNorm.set(p.name_normalized, p);
  producerByNorm.set(normalize(p.name), p);
  // Also try without corporate suffix
  const stripped = normalize(stripCorporateSuffix(p.name));
  if (stripped !== p.name_normalized) producerByNorm.set(stripped, p);
  producerById.set(p.id, p);
}

// Existing wines: producer_id + normalized name → wine
const wineByKey = new Map(); // producer_id|name_normalized → wine
const wineByLwin = new Map(); // lwin → wine
for (const w of existingWines) {
  wineByKey.set(`${w.producer_id}|${w.name_normalized}`, w);
  if (w.lwin) wineByLwin.set(w.lwin, w);
}

// ── Resolution Functions ────────────────────────────────────

function resolveCountry(name) {
  if (!name) return null;
  return countryMap.get(name.toLowerCase()) || null;
}

function resolveRegion(lwinRegion, countryId) {
  if (!lwinRegion) return null;
  const lower = lwinRegion.toLowerCase();
  const mapped = REGION_NAME_MAP[lower];
  if (mapped) {
    const norm = normalize(mapped);
    if (countryId) { const r = regionMap.get(`${norm}|${countryId}`); if (r) return r; }
    const r2 = regionMap.get(norm); if (r2) return r2;
  }
  const norm = normalize(lower);
  if (countryId) { const r = regionMap.get(`${norm}|${countryId}`); if (r) return r; }
  return regionMap.get(norm) || null;
}

function resolveAppellation(subRegion, countryId) {
  if (!subRegion) return null;
  const norm = normalize(subRegion);
  return appellationMap.get(norm) || appellationMap.get(subRegion.toLowerCase()) || null;
}

function resolveClassification(lwinClass) {
  if (!lwinClass) return null;
  const mapping = CLASSIFICATION_MAP[lwinClass];
  if (!mapping) return null;
  const system = classificationSlugMap.get(mapping.system_slug);
  if (!system) return null;
  const level = classLevelMap.get(`${system.id}|${mapping.level_name.toLowerCase()}`);
  return level ? { system, level } : null;
}

// ── Match Functions (in-memory first, RPC fallback) ─────────

function matchProducerInMemory(name, countryId) {
  const norm = normalize(name);
  const stripped = normalize(stripCorporateSuffix(name));

  // Exact normalized match
  let match = producerByNorm.get(norm);
  if (match) {
    if (!countryId || !match.country_id || match.country_id === countryId) {
      return { ...match, confidence: 1.0, method: 'exact_normalized' };
    }
  }

  // Stripped corporate suffix match
  if (stripped !== norm) {
    match = producerByNorm.get(stripped);
    if (match) {
      if (!countryId || !match.country_id || match.country_id === countryId) {
        return { ...match, confidence: 0.95, method: 'stripped_suffix' };
      }
    }
  }

  return null;
}

async function matchProducerFuzzy(name, countryId) {
  const norm = normalize(name);
  const { data, error } = await sb.rpc('match_producer_fuzzy', {
    p_name_normalized: norm,
    p_country_id: countryId || null,
    p_threshold: 0.6, // Higher threshold for auto-accept
    p_limit: 3,
  });
  if (error || !data || data.length === 0) return null;
  const best = data[0];
  return { id: best.id, name: best.name, country_id: best.country_id,
    confidence: parseFloat(best.sim), method: 'fuzzy_rpc' };
}

function matchWineInMemory(producerId, wineName) {
  const norm = normalize(wineName);
  const key = `${producerId}|${norm}`;
  const match = wineByKey.get(key);
  if (match) return { ...match, confidence: 0.95, method: 'exact_normalized' };
  return null;
}

// ── Fetch LWIN Staging Rows ─────────────────────────────────
console.log('\nFetching LWIN staging rows...');

let lwinQuery = sb.from('source_lwin').select('*').is('canonical_wine_id', null);
if (COUNTRY_FILTER) lwinQuery = lwinQuery.eq('country', COUNTRY_FILTER);

const lwinRows = [];
let offset = 0;
const FETCH_BATCH = 1000;
while (true) {
  let q = sb.from('source_lwin').select('*').is('canonical_wine_id', null);
  if (COUNTRY_FILTER) q = q.eq('country', COUNTRY_FILTER);
  q = q.range(offset, offset + FETCH_BATCH - 1);
  const { data, error } = await q;
  if (error) throw new Error(`Fetch source_lwin: ${error.message}`);
  lwinRows.push(...data);
  if (data.length < FETCH_BATCH) break;
  offset += FETCH_BATCH;
  if (lwinRows.length >= LIMIT) break;
}
if (lwinRows.length > LIMIT) lwinRows.length = LIMIT;

console.log(`  ${lwinRows.length} unprocessed LWIN rows to promote`);

// ── Process: Resolve, Match, Promote ────────────────────────
console.log('\nProcessing...');

const stats = {
  total: lwinRows.length,
  // Resolution
  countryResolved: 0, countryMissing: new Map(),
  regionResolved: 0, regionMissing: new Map(),
  appellationResolved: 0,
  classificationResolved: 0,
  // Producer matching
  producerExactMatch: 0,
  producerFuzzyMatch: 0,
  producerCreated: 0,
  producerNoName: 0,
  // Wine matching
  wineExactMatch: 0,
  wineLwinMatch: 0,
  wineCreated: 0,
  // Errors
  errors: 0,
};

// Batch accumulators
let producerInsertBatch = [];
let wineInsertBatch = [];
let classificationInsertBatch = [];
let externalIdBatch = [];
let stagingUpdateBatch = []; // { lwin, canonical_wine_id, canonical_producer_id }
let aliasInsertBatch = [];

const WRITE_BATCH = 200;

async function flushProducers() {
  if (producerInsertBatch.length === 0) return;
  if (MODE === 'import') {
    for (const p of producerInsertBatch) {
      // First try insert
      const { error } = await sb.from('producers').insert(p);
      if (error) {
        if (error.message.includes('duplicate') || error.message.includes('unique')) {
          // Slug already exists — look up existing producer and remap
          const { data: existing } = await sb.from('producers')
            .select('id,name')
            .eq('slug', p.slug)
            .is('deleted_at', null)
            .limit(1);
          if (existing && existing.length > 0) {
            const oldId = p.id;
            const realId = existing[0].id;
            // Update our in-memory caches to point to the real ID
            producerByNorm.set(normalize(p.name), { id: realId, name: existing[0].name, country_id: p.country_id });
            producerById.set(realId, { id: realId, name: existing[0].name });
            // Fix any wines in the current batch that reference the old ID
            for (const w of wineInsertBatch) {
              if (w.producer_id === oldId) w.producer_id = realId;
            }
            // Fix staging updates
            for (const s of stagingUpdateBatch) {
              if (s.canonical_producer_id === oldId) s.canonical_producer_id = realId;
            }
            if (VERBOSE) console.log(`  Slug conflict: "${p.name}" → existing "${existing[0].name}" (${realId})`);
          }
        } else {
          console.error(`  Producer insert error (${p.name}): ${error.message}`);
          stats.errors++;
        }
      }
    }
  }
  producerInsertBatch = [];
}

async function flushWines() {
  if (wineInsertBatch.length === 0) return;
  if (MODE === 'import') {
    for (let i = 0; i < wineInsertBatch.length; i += WRITE_BATCH) {
      const chunk = wineInsertBatch.slice(i, i + WRITE_BATCH);
      const { error } = await sb.from('wines').insert(chunk);
      if (error) {
        if (error.message.includes('duplicate') || error.message.includes('unique')) {
          // Fall back to one-by-one insert for this chunk
          for (const w of chunk) {
            const { error: e2 } = await sb.from('wines').insert(w);
            if (e2) {
              if (e2.message.includes('duplicate') || e2.message.includes('unique')) {
                // Wine slug exists — look up and remap
                const { data: existing } = await sb.from('wines')
                  .select('id').eq('slug', w.slug).is('deleted_at', null).limit(1);
                if (existing && existing.length > 0) {
                  const oldId = w.id;
                  const realId = existing[0].id;
                  // Update staging batch
                  for (const s of stagingUpdateBatch) {
                    if (s.canonical_wine_id === oldId) s.canonical_wine_id = realId;
                  }
                  // Update external ID batch
                  for (const ext of externalIdBatch) {
                    if (ext.entity_id === oldId) ext.entity_id = realId;
                  }
                  // Update classification batch
                  for (const cl of classificationInsertBatch) {
                    if (cl.entity_id === oldId) cl.entity_id = realId;
                  }
                }
              } else if (!e2.message.includes('foreign key')) {
                console.error(`  Wine insert error: ${e2.message}`);
                stats.errors++;
              } else {
                // FK error — producer doesn't exist
                console.error(`  Wine FK error (producer_id=${w.producer_id}): ${w.name}`);
                stats.errors++;
              }
            }
          }
        } else {
          console.error(`  Wine batch error: ${error.message}`);
          stats.errors++;
        }
      }
    }
  }
  wineInsertBatch = [];
}

async function flushClassifications() {
  if (classificationInsertBatch.length === 0) return;
  if (MODE === 'import') {
    // Insert individually — entity_classifications has no simple upsert key
    for (const c of classificationInsertBatch) {
      const { error } = await sb.from('entity_classifications').insert(c);
      if (error && !error.message.includes('duplicate')) {
        console.error(`  Classification error: ${error.message}`);
        stats.errors++;
      }
    }
  }
  classificationInsertBatch = [];
}

async function flushExternalIds() {
  if (externalIdBatch.length === 0) return;
  if (MODE === 'import') {
    for (let i = 0; i < externalIdBatch.length; i += WRITE_BATCH) {
      const chunk = externalIdBatch.slice(i, i + WRITE_BATCH);
      const { error } = await sb.from('external_ids').insert(chunk);
      if (error && !error.message.includes('duplicate')) {
        console.error(`  External ID batch error: ${error.message}`);
        stats.errors++;
      }
    }
  }
  externalIdBatch = [];
}

async function flushStagingUpdates() {
  // DEFERRED: staging updates done via batch SQL after import completes
  // See: UPDATE source_lwin SET canonical_wine_id = w.id ... FROM wines w WHERE w.lwin = sl.lwin_7
  stagingUpdateBatch = [];
}

async function flushAll() {
  // Order matters: producers first, then wines, then everything else
  await flushProducers();
  await flushWines();
  await flushClassifications();
  await flushExternalIds();
  // Staging updates deferred — just clear the batch
  stagingUpdateBatch = [];
}

// ── Main Loop ───────────────────────────────────────────────
// Group by producer for efficient batching
const producerGroups = new Map();
for (const row of lwinRows) {
  const producerName = row.producer_name || row.display_name?.split(',')[0]?.trim();
  if (!producerName) { stats.producerNoName++; continue; }
  if (!producerGroups.has(producerName)) producerGroups.set(producerName, []);
  producerGroups.get(producerName).push(row);
}

console.log(`  ${producerGroups.size} unique producers to process`);

let processed = 0;

for (const [producerName, rows] of producerGroups) {
  // Determine most common country for this producer
  const countryCounts = {};
  for (const r of rows) {
    const cid = resolveCountry(r.country);
    if (cid) countryCounts[cid] = (countryCounts[cid] || 0) + 1;
  }
  const topCountryEntry = Object.entries(countryCounts).sort((a, b) => b[1] - a[1])[0];
  const producerCountryId = topCountryEntry ? topCountryEntry[0] : null;

  // ── Trust LWIN producer groupings ──
  // Each unique producer_name in LWIN = one canonical producer.
  // No fuzzy matching — LWIN is the authority for producer identity.
  // Dedup across LWIN producer names is a separate future pass.
  const normName = normalize(producerName);
  let producerId;

  // Check if we already created this producer (exact name match only)
  const existing = producerByNorm.get(normName);
  if (existing) {
    producerId = existing.id;
    stats.producerExactMatch++;
  } else {
    // Create new producer — one per unique LWIN producer_name
    producerId = randomUUID();
    const regionCounts = {};
    for (const r of rows) {
      const reg = resolveRegion(r.region, producerCountryId);
      if (reg) regionCounts[reg.id] = (regionCounts[reg.id] || 0) + 1;
    }
    const topRegion = Object.entries(regionCounts).sort((a, b) => b[1] - a[1])[0];

    const cleanName = stripCorporateSuffix(producerName);
    const slug = slugify(cleanName);

    producerInsertBatch.push({
      id: producerId,
      slug: slug,
      name: cleanName,
      name_normalized: normName,
      country_id: producerCountryId,
      region_id: topRegion ? topRegion[0] : null,
      producer_type: 'estate',
    });

    // Add to in-memory cache
    producerByNorm.set(normName, { id: producerId, name: cleanName, country_id: producerCountryId });
    producerById.set(producerId, { id: producerId, name: cleanName });
    stats.producerCreated++;

    // If the original name differs from cleaned, store as alias
    if (cleanName !== producerName) {
      aliasInsertBatch.push({
        id: randomUUID(),
        producer_id: producerId,
        name: producerName,
        name_normalized: normalize(producerName),
        alias_type: 'source_variant',
      });
    }

    // Flush producer immediately so wines can reference it
    await flushProducers();

    // Re-read producerId from cache — it may have been remapped by slug conflict
    const cachedProducer = producerByNorm.get(normName);
    if (cachedProducer && cachedProducer.id !== producerId) {
      producerId = cachedProducer.id;
    }
  }

  // ── Process wines for this producer ──
  for (const row of rows) {
    const countryId = resolveCountry(row.country);
    if (countryId) stats.countryResolved++;
    else if (row.country) stats.countryMissing.set(row.country, (stats.countryMissing.get(row.country) || 0) + 1);

    const region = resolveRegion(row.region, countryId);
    if (region) stats.regionResolved++;
    else if (row.region) stats.regionMissing.set(`${row.country}|${row.region}`, (stats.regionMissing.get(`${row.country}|${row.region}`) || 0) + 1);

    const appellation = resolveAppellation(row.sub_region, countryId);
    if (appellation) stats.appellationResolved++;

    const classification = resolveClassification(row.classification);
    if (classification) stats.classificationResolved++;

    // Wine name = display_name minus producer prefix.
    // LWIN wine_name is often just "Rouge"/"Blanc"/"Riesling" — the real identity
    // includes the appellation from display_name: "Domaine X, Bourgogne, Rouge" → "Bourgogne, Rouge"
    const lwin7 = row.lwin_7 || row.lwin;
    let wineName;
    if (row.display_name) {
      // Strip producer name/title from the beginning of display_name
      let dn = row.display_name;
      // LWIN display_name format: "ProducerTitle ProducerName, WinePart1, WinePart2"
      // The first comma-separated segment is the producer
      const firstComma = dn.indexOf(',');
      if (firstComma > 0) {
        wineName = dn.slice(firstComma + 1).trim();
        // Remove trailing comma if present
        if (wineName.endsWith(',')) wineName = wineName.slice(0, -1).trim();
      } else {
        wineName = row.wine_name || dn;
      }
    } else {
      wineName = row.wine_name || 'Unknown';
    }
    // Fallback: if wine name is empty after stripping, use wine_name field
    if (!wineName || !wineName.trim()) wineName = row.wine_name || 'Unknown';

    // Check if this LWIN already exists in canonical (only check — no fuzzy wine matching)
    // Every LWIN-7 is a unique wine identity. We only skip if already imported.
    let wineMatch = wineByLwin.get(lwin7);
    if (wineMatch) {
      stats.wineLwinMatch++;
      stagingUpdateBatch.push({ lwin: row.lwin, canonical_wine_id: wineMatch.id, canonical_producer_id: producerId });
      processed++;
      continue;
    }

    // No match — create new canonical wine
    const { wine_type, effervescence } = mapWineType(row.wine_type);
    const color = row.colour ? COLOR_MAP[row.colour] : null;

    const wineId = randomUUID();
    const wineSlug = slugify(`${producerName}-${wineName}-${lwin7}`);
    const wineNorm = normalize(wineName);

    wineInsertBatch.push({
      id: wineId,
      slug: wineSlug,
      name: wineName,
      name_normalized: wineNorm,
      producer_id: producerId,
      country_id: countryId,
      region_id: region?.id || null,
      appellation_id: appellation?.id || null,
      color: color,
      wine_type: wine_type,
      effervescence: effervescence,
      lwin: lwin7,
      identity_confidence: 'unverified',
    });

    // Add to LWIN cache only (not name cache — we don't dedup within LWIN)
    wineByLwin.set(lwin7, { id: wineId, name: wineName, lwin: lwin7 });

    // External ID
    externalIdBatch.push({
      entity_type: 'wine', entity_id: wineId,
      system: 'lwin', external_id: lwin7,
    });

    // Classification
    if (classification) {
      classificationInsertBatch.push({
        id: randomUUID(),
        entity_type: 'wine',
        entity_id: wineId,
        classification_level_id: classification.level.id,
      });
    }

    stagingUpdateBatch.push({ lwin: row.lwin, canonical_wine_id: wineId, canonical_producer_id: producerId });
    stats.wineCreated++;
    processed++;

    // Periodic flush
    if (processed % 1000 === 0) {
      await flushAll();
      console.log(`  ${processed}/${lwinRows.length} processed (${stats.producerCreated} new producers, ${stats.wineCreated} new wines, ${stats.wineExactMatch + stats.wineLwinMatch} matched)`);
    }
  }
}

// Final flush
await flushAll();

// Flush aliases
if (MODE === 'import' && aliasInsertBatch.length > 0) {
  for (let i = 0; i < aliasInsertBatch.length; i += WRITE_BATCH) {
    const chunk = aliasInsertBatch.slice(i, i + WRITE_BATCH);
    const { error } = await sb.from('producer_aliases').upsert(chunk,
      { onConflict: 'producer_id,name_normalized', ignoreDuplicates: true });
    if (error && !error.message.includes('duplicate')) {
      console.error(`  Alias batch error: ${error.message}`);
    }
  }
}

// ── Report ──────────────────────────────────────────────────
console.log('\n═══════════════════════════════════════════════════');
console.log('  LWIN PROMOTION REPORT');
console.log('═══════════════════════════════════════════════════\n');

console.log(`Total LWIN rows: ${stats.total}`);
console.log(`Producers with no name: ${stats.producerNoName}\n`);

const pct = (n, total) => total > 0 ? `${((n / total) * 100).toFixed(1)}%` : '0%';

console.log('RESOLUTION RATES:');
console.log(`  Country:        ${stats.countryResolved}/${stats.total} (${pct(stats.countryResolved, stats.total)})`);
console.log(`  Region:         ${stats.regionResolved}/${stats.total} (${pct(stats.regionResolved, stats.total)})`);
console.log(`  Appellation:    ${stats.appellationResolved}/${stats.total} (${pct(stats.appellationResolved, stats.total)})`);
console.log(`  Classification: ${stats.classificationResolved}/${stats.total} (${pct(stats.classificationResolved, stats.total)})`);

console.log('\nPRODUCER RESULTS:');
console.log(`  Existing match: ${stats.producerExactMatch}`);
console.log(`  Created new:    ${stats.producerCreated}`);

console.log('\nWINE RESULTS:');
console.log(`  Already existed: ${stats.wineLwinMatch}`);
console.log(`  Created new:     ${stats.wineCreated}`);

console.log(`\nErrors: ${stats.errors}`);

if (stats.countryMissing.size > 0) {
  console.log('\nUNRESOLVED COUNTRIES (top 20):');
  [...stats.countryMissing.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20)
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));
}

if (stats.regionMissing.size > 0) {
  console.log('\nUNRESOLVED REGIONS (top 20):');
  [...stats.regionMissing.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20)
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));
}

if (MODE !== 'import') {
  console.log(`\n${MODE === 'analyze' ? '✅ Analysis' : '✅ Dry run'} complete. Run with --import to write to DB.`);
}

console.log(`\nDone.`);
