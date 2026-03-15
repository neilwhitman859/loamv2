#!/usr/bin/env node
/**
 * seed_appellation_aliases.mjs — Populate appellation_aliases from primary sources + mechanical generation
 *
 * Sources:
 *   1. INAO OpenDataSoft API — French AOC/IGP product variants (color, cru, style)
 *   2. Eurac PDO_EU_cat.csv — EU PDO wine product categories
 *   3. Tier 1 mechanical generation — color suffixes, accent-stripped, designation types
 *   4. Tier 2 known translations — English ↔ local name pairs
 *
 * Usage: node scripts/seed_appellation_aliases.mjs [--dry-run]
 */

import { readFileSync, writeFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

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
const DRY_RUN = process.argv.includes('--dry-run');

// ── Helpers ─────────────────────────────────────────────────
function normalize(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
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

// ── Color suffix maps per country ───────────────────────────
const COLOR_SUFFIXES = {
  // French
  FR: ['rouge', 'blanc', 'rosé', 'clairet'],
  // Italian
  IT: ['rosso', 'bianco', 'rosato'],
  // Spanish
  ES: ['tinto', 'blanco', 'rosado'],
  // Portuguese
  PT: ['tinto', 'branco', 'rosado', 'rosé'],
  // German — less common but exists
  DE: ['rot', 'weiß', 'weiss', 'rosé'],
  // Austrian
  AT: ['rot', 'weiß', 'weiss', 'rosé'],
  // Generic English (for New World)
  US: ['red', 'white', 'rosé', 'rose'],
  AU: ['red', 'white', 'rosé', 'rose'],
  NZ: ['red', 'white', 'rosé', 'rose'],
  ZA: ['red', 'white', 'rosé', 'rose'],
  CL: ['tinto', 'blanco', 'rosado'],
  AR: ['tinto', 'blanco', 'rosado'],
};

// ── Known English ↔ local translations ──────────────────────
// Format: [english_variant, local_name, iso_code]
// These are appellation-level names that appear differently in English vs local language
const TRANSLATIONS = [
  // French
  ['Burgundy', 'Bourgogne', 'FR'],
  ['Rhone Valley', 'Vallée du Rhône', 'FR'],
  ['Rhone', 'Rhône', 'FR'],
  ['Cotes du Rhone', 'Côtes du Rhône', 'FR'],
  ['Cotes du Rhone Villages', 'Côtes du Rhône Villages', 'FR'],
  ['Beaujolais Villages', 'Beaujolais-Villages', 'FR'],
  ['Cote de Beaune', 'Côte de Beaune', 'FR'],
  ['Cote de Nuits', 'Côte de Nuits', 'FR'],
  ['Cotes de Provence', 'Côtes de Provence', 'FR'],
  ['Cotes du Roussillon', 'Côtes du Roussillon', 'FR'],
  ['Coteaux du Layon', 'Coteaux du Layon', 'FR'],
  ['Saint-Emilion', 'Saint-Émilion', 'FR'],
  ['Pouilly-Fume', 'Pouilly-Fumé', 'FR'],
  ['Pouilly-Fuisse', 'Pouilly-Fuissé', 'FR'],
  ['Chateauneuf-du-Pape', 'Châteauneuf-du-Pape', 'FR'],
  ['Cote Rotie', 'Côte Rôtie', 'FR'],
  ['Cote-Rotie', 'Côte Rôtie', 'FR'],
  ['Gevrey-Chambertin', 'Gevrey-Chambertin', 'FR'], // same but accent variants exist
  ['Cremant d\'Alsace', 'Crémant d\'Alsace', 'FR'],
  ['Cremant de Bourgogne', 'Crémant de Bourgogne', 'FR'],
  ['Cremant de Loire', 'Crémant de Loire', 'FR'],
  ['Cremant de Limoux', 'Crémant de Limoux', 'FR'],
  ['Corbieres', 'Corbières', 'FR'],
  ['Fitou', 'Fitou', 'FR'],
  ['Medoc', 'Médoc', 'FR'],
  ['Haut-Medoc', 'Haut-Médoc', 'FR'],
  ['Premieres Cotes de Bordeaux', 'Premières Côtes de Bordeaux', 'FR'],
  ['Entre-Deux-Mers', 'Entre-deux-Mers', 'FR'],
  ['Cotes de Bourg', 'Côtes de Bourg', 'FR'],
  ['Cotes de Blaye', 'Côtes de Blaye', 'FR'],
  // Italian
  ['Piedmont', 'Piemonte', 'IT'],
  ['Tuscany', 'Toscana', 'IT'],
  ['Brunello di Montalcino', 'Brunello di Montalcino', 'IT'],
  ['Vino Nobile di Montepulciano', 'Vino Nobile di Montepulciano', 'IT'],
  // Spanish
  ['Rioja', 'Rioja', 'ES'],
  ['Sherry', 'Jerez-Xérès-Sherry', 'ES'],
  ['Jerez', 'Jerez-Xérès-Sherry', 'ES'],
  ['Cava', 'Cava', 'ES'],
  ['Priorat', 'Priorat', 'ES'],
  ['Priorato', 'Priorat', 'ES'],
  // Portuguese
  ['Port', 'Porto', 'PT'],
  ['Oporto', 'Porto', 'PT'],
  ['Douro', 'Douro', 'PT'],
  ['Madeira', 'Madeira', 'PT'],
  ['Vinho Verde', 'Vinho Verde', 'PT'],
  ['Dao', 'Dão', 'PT'],
  // German
  ['Mosel', 'Mosel', 'DE'],
  ['Moselle', 'Mosel', 'DE'],
  ['Rhine', 'Rheingau', 'DE'],
  ['Pfalz', 'Pfalz', 'DE'],
  ['Palatinate', 'Pfalz', 'DE'],
  // Austrian
  ['Wachau', 'Wachau', 'AT'],
  // Hungarian
  ['Tokaj', 'Tokaji', 'HU'],
  ['Tokay', 'Tokaji', 'HU'],
  // Greek
  ['Santorini', 'Σαντορίνη', 'GR'],
  ['Naoussa', 'Νάουσα', 'GR'],
  ['Nemea', 'Νεμέα', 'GR'],
];

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log(`\n${'='.repeat(60)}`);
  console.log('  APPELLATION ALIASES — SEED FROM PRIMARY SOURCES');
  console.log(`  ${DRY_RUN ? '(DRY RUN)' : '(LIVE)'}`);
  console.log(`${'='.repeat(60)}\n`);

  // Load all appellations with country info
  const appellations = await fetchAll('appellations', 'id,name,designation_type,country_id');
  const countries = await fetchAll('countries', 'id,name,iso_code');
  const countryMap = new Map(countries.map(c => [c.id, c]));
  const isoToCountryId = new Map(countries.map(c => [c.iso_code, c.id]));

  // Build appellation lookup by normalized name
  const appellationByName = new Map();
  const appellationByNorm = new Map();
  for (const a of appellations) {
    appellationByName.set(a.name.toLowerCase(), a);
    appellationByNorm.set(normalize(a.name), a);
  }

  console.log(`Loaded ${appellations.length} appellations from ${countries.length} countries\n`);

  // Collect all aliases to insert
  const aliases = []; // { appellation_id, alias, alias_normalized, alias_type, source }
  const seen = new Set(); // dedupe by alias_normalized

  function addAlias(appellationId, alias, aliasType, source) {
    const norm = normalize(alias);
    // Skip if same as the canonical name
    const appellation = appellations.find(a => a.id === appellationId);
    if (appellation && normalize(appellation.name) === norm) return;
    // Skip duplicates
    if (seen.has(norm)) return;
    seen.add(norm);
    aliases.push({
      appellation_id: appellationId,
      alias,
      alias_normalized: norm,
      alias_type: aliasType,
      source,
    });
  }

  // ════════════════════════════════════════════════════════════
  // SOURCE 1: INAO OpenDataSoft API — French product variants
  // ════════════════════════════════════════════════════════════
  console.log('1. Fetching INAO product variants...');

  const INAO_API = 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/aires-et-produits-aocaop-et-igp/records';
  let inaoProducts = [];
  let inaoOffset = 0;
  const INAO_LIMIT = 100;

  // Filter to wine products only (AOC/IGP designation)
  const INAO_WHERE = encodeURIComponent("signe_fr LIKE 'AOC%' OR signe_fr LIKE 'IGP%'");

  while (true) {
    const url = `${INAO_API}?select=aire_geographique,produit,signe_fr,idproduit&group_by=aire_geographique,produit,signe_fr,idproduit&where=${INAO_WHERE}&limit=${INAO_LIMIT}&offset=${inaoOffset}`;
    const resp = await fetch(url);
    const data = await resp.json();
    if (!data.results || data.results.length === 0) break;
    inaoProducts.push(...data.results);
    inaoOffset += INAO_LIMIT;
    if (data.results.length < INAO_LIMIT) break;
    // Rate limit
    await new Promise(r => setTimeout(r, 200));
  }

  console.log(`  Fetched ${inaoProducts.length} INAO wine product entries`);

  // Match INAO products to our appellations
  let inaoMatched = 0;
  let inaoUnmatched = 0;
  const inaoMissedSet = new Set();

  for (const p of inaoProducts) {
    const aireNorm = normalize(p.aire_geographique);
    const produit = p.produit;
    const produitNorm = normalize(produit);

    // Skip if produit is same as aire_geographique (no variant)
    if (aireNorm === produitNorm) continue;

    // Find matching appellation by aire_geographique
    let appellation = appellationByNorm.get(aireNorm);

    // If no direct match, try matching just the base appellation name
    // (INAO has "Aloxe-Corton premier cru Clos des Maréchaudes" as aire_geographique
    //  but we might only have "Aloxe-Corton")
    if (!appellation) {
      // Try progressively shorter prefixes
      const words = p.aire_geographique.split(' ');
      for (let len = words.length - 1; len >= 1; len--) {
        const prefix = words.slice(0, len).join(' ');
        appellation = appellationByNorm.get(normalize(prefix));
        if (appellation) break;
      }
    }

    if (appellation) {
      // Determine alias type based on what's different
      let aliasType = 'synonym';
      const diff = produit.replace(p.aire_geographique, '').trim().toLowerCase();
      if (['rouge', 'blanc', 'rosé', 'clairet'].includes(diff)) aliasType = 'with_color';
      else if (diff.includes('vendanges tardives') || diff.includes('sélection de grains nobles')
        || diff.includes('vin jaune') || diff.includes('vin de paille')
        || diff.includes('mousseux') || diff.includes('primeur')
        || diff.includes('supérieur') || diff.includes('grand cru')
        || diff.includes('premier cru')) aliasType = 'with_designation';

      addAlias(appellation.id, produit, aliasType, 'inao-opendatasoft');
      inaoMatched++;
    } else {
      inaoUnmatched++;
      inaoMissedSet.add(p.aire_geographique);
    }
  }

  console.log(`  Matched: ${inaoMatched}, Unmatched: ${inaoUnmatched} (${inaoMissedSet.size} unique aires)`);
  if (inaoMissedSet.size > 0) {
    console.log(`  Sample unmatched: ${[...inaoMissedSet].slice(0, 10).join(', ')}`);
  }

  // ════════════════════════════════════════════════════════════
  // SOURCE 2: Tier 1 — Mechanical color suffix generation
  // ════════════════════════════════════════════════════════════
  console.log('\n2. Generating Tier 1 color suffix aliases...');

  let colorCount = 0;
  for (const a of appellations) {
    const country = countryMap.get(a.country_id);
    if (!country || !country.iso_code) continue;
    const suffixes = COLOR_SUFFIXES[country.iso_code];
    if (!suffixes) continue;

    for (const suffix of suffixes) {
      addAlias(a.id, `${a.name} ${suffix}`, 'with_color', 'mechanical-color-suffix');
      colorCount++;
    }
  }
  console.log(`  Generated ${colorCount} color suffix candidates (${aliases.length - colorCount} already existed from INAO)`);

  // ════════════════════════════════════════════════════════════
  // SOURCE 3: Tier 1 — Accent-stripped variants
  // ════════════════════════════════════════════════════════════
  console.log('\n3. Generating accent-stripped aliases...');

  let accentCount = 0;
  for (const a of appellations) {
    const stripped = a.name.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    if (stripped !== a.name) {
      addAlias(a.id, stripped, 'synonym', 'mechanical-accent-strip');
      accentCount++;
    }
  }
  console.log(`  Generated ${accentCount} accent-stripped aliases`);

  // ════════════════════════════════════════════════════════════
  // SOURCE 4: Tier 1 — Designation type suffixes
  // ════════════════════════════════════════════════════════════
  console.log('\n4. Generating designation type suffix aliases...');

  let designationCount = 0;
  for (const a of appellations) {
    if (!a.designation_type) continue;
    // Skip if the name already contains the designation
    if (a.name.toLowerCase().includes(a.designation_type.toLowerCase())) continue;
    addAlias(a.id, `${a.name} ${a.designation_type}`, 'with_designation', 'mechanical-designation-suffix');
    designationCount++;
  }
  console.log(`  Generated ${designationCount} designation suffix aliases`);

  // ════════════════════════════════════════════════════════════
  // SOURCE 5: Tier 2 — Known translations
  // ════════════════════════════════════════════════════════════
  console.log('\n5. Adding known translation aliases...');

  let translationCount = 0;
  for (const [english, local, iso] of TRANSLATIONS) {
    // Find the canonical appellation — could be either the english or local name
    let appellation = appellationByName.get(local.toLowerCase())
      || appellationByName.get(english.toLowerCase());
    if (!appellation) {
      // Try normalized
      appellation = appellationByNorm.get(normalize(local))
        || appellationByNorm.get(normalize(english));
    }
    if (appellation) {
      // Add the variant that ISN'T the canonical name
      if (normalize(english) !== normalize(appellation.name)) {
        addAlias(appellation.id, english, 'synonym', 'known-translation');
        translationCount++;
      }
      if (normalize(local) !== normalize(appellation.name)) {
        addAlias(appellation.id, local, 'local_name', 'known-translation');
        translationCount++;
      }
    }
  }
  console.log(`  Added ${translationCount} translation aliases`);

  // ════════════════════════════════════════════════════════════
  // INSERT INTO DATABASE
  // ════════════════════════════════════════════════════════════
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Total aliases to insert: ${aliases.length}`);

  if (DRY_RUN) {
    console.log('\n(DRY RUN — not inserting)\n');
    // Show sample
    console.log('Sample aliases:');
    const samples = aliases.filter(a => a.source === 'inao-opendatasoft').slice(0, 10);
    for (const s of samples) {
      const app = appellations.find(a => a.id === s.appellation_id);
      console.log(`  "${s.alias}" → ${app?.name} [${s.alias_type}] (${s.source})`);
    }
    console.log('...');
    const colorSamples = aliases.filter(a => a.source === 'mechanical-color-suffix').slice(0, 5);
    for (const s of colorSamples) {
      const app = appellations.find(a => a.id === s.appellation_id);
      console.log(`  "${s.alias}" → ${app?.name} [${s.alias_type}] (${s.source})`);
    }

    // Stats by source
    const bySrc = {};
    for (const a of aliases) bySrc[a.source] = (bySrc[a.source] || 0) + 1;
    console.log('\nBy source:');
    for (const [src, count] of Object.entries(bySrc).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${src}: ${count}`);
    }
    return;
  }

  // Batch insert
  const BATCH = 500;
  let inserted = 0;
  for (let i = 0; i < aliases.length; i += BATCH) {
    const batch = aliases.slice(i, i + BATCH);
    const { error } = await sb.from('appellation_aliases').upsert(batch, {
      onConflict: 'alias_normalized',
      ignoreDuplicates: true,
    });
    if (error) {
      console.error(`  Batch error at ${i}: ${error.message}`);
      // Try one by one for this batch
      for (const a of batch) {
        const { error: e2 } = await sb.from('appellation_aliases').upsert(a, {
          onConflict: 'alias_normalized',
          ignoreDuplicates: true,
        });
        if (!e2) inserted++;
      }
    } else {
      inserted += batch.length;
    }
  }

  console.log(`\nInserted ${inserted} aliases`);

  // Final stats
  const { data: finalCount } = await sb.from('appellation_aliases').select('source', { count: 'exact', head: false });
  const bySrc = {};
  for (const row of finalCount || []) bySrc[row.source] = (bySrc[row.source] || 0) + 1;
  console.log('\nFinal counts by source:');
  for (const [src, count] of Object.entries(bySrc).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${src}: ${count}`);
  }
}

main().catch(console.error);
