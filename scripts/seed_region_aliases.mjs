#!/usr/bin/env node
/**
 * Seed region_aliases table with common alternative names.
 *
 * Sources:
 * - WSET Level 3 naming conventions (English/local pairs)
 * - Wine trade standard names (Wine-Searcher, Decanter conventions)
 * - Import friction encountered during 13+ producer imports
 *
 * Usage: node scripts/seed_region_aliases.mjs [--dry-run]
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
const vars = {};
for (const line of envContent.split('\n')) {
  const trimmed = line.replace(/\r/g, '').trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx === -1) continue;
  vars[trimmed.slice(0, eqIdx)] = trimmed.slice(eqIdx + 1);
}

const sb = createClient(vars.SUPABASE_URL, vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY);
const DRY_RUN = process.argv.includes('--dry-run');

function normalize(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

// Format: [alias, canonical_region_name, alias_type, language_code]
const ALIASES = [
  // Italian regions — English ↔ Italian
  ['Piedmont', 'Piemonte', 'translation', 'en'],
  ['Tuscany', 'Toscana', 'translation', 'en'],
  ['Lombardy', 'Lombardia', 'translation', 'en'],
  ['Sicily', 'Sicilia', 'translation', 'en'],
  ['Sardinia', 'Sardegna', 'translation', 'en'],
  ['Apulia', 'Puglia', 'translation', 'en'],
  ['Friuli', 'Friuli-Venezia Giulia', 'abbreviation', null],
  ['FVG', 'Friuli-Venezia Giulia', 'abbreviation', null],
  ['Friuli Venezia Giulia', 'Friuli-Venezia Giulia', 'alternate_name', null],
  ['Trentino', 'Trentino-Alto Adige', 'abbreviation', null],
  ['Alto Adige', 'Trentino-Alto Adige', 'abbreviation', null],
  ['Südtirol', 'Trentino-Alto Adige', 'translation', 'de'],
  ['South Tyrol', 'Trentino-Alto Adige', 'translation', 'en'],
  ['Trentino Alto Adige', 'Trentino-Alto Adige', 'alternate_name', null],

  // Italian L2 sub-regions
  ['Langhe', 'Langhe', 'alternate_name', null],
  ['Monferrato', 'Monferrato', 'alternate_name', null],
  ['Chianti region', 'Chianti', 'alternate_name', 'en'],
  ['Etna region', 'Etna', 'alternate_name', 'en'],

  // French regions — English ↔ French
  ['Bourgogne', 'Burgundy', 'translation', 'fr'],
  ['Rhône', 'Rhône Valley', 'abbreviation', null],
  ['Rhone', 'Rhône Valley', 'abbreviation', null],
  ['Rhone Valley', 'Rhône Valley', 'alternate_name', null],
  ['Northern Rhone', 'Northern Rhône', 'alternate_name', null],
  ['Southern Rhone', 'Southern Rhône', 'alternate_name', null],
  ['Loire', 'Loire Valley', 'abbreviation', null],
  ['Val de Loire', 'Loire Valley', 'translation', 'fr'],
  ['Languedoc-Roussillon', 'Languedoc', 'historical_name', null],
  ['South West France', 'Southwest France', 'alternate_name', 'en'],
  ['The Dordogne and South West France', 'Southwest France', 'alternate_name', 'en'],
  ['Southern France', 'Southern France', 'alternate_name', null],
  ['Midi', 'Southern France', 'alternate_name', 'fr'],

  // Spanish regions
  ['Catalonia', 'Catalunya', 'translation', 'en'],
  ['Castile and León', 'Castilla y León', 'translation', 'en'],
  ['Castile and Leon', 'Castilla y León', 'translation', 'en'],
  ['Castilla y Leon', 'Castilla y León', 'alternate_name', null],
  ['Andalusia', 'Andalucía', 'translation', 'en'],
  ['Andalucia', 'Andalucía', 'alternate_name', null],
  ['Upper Ebro', 'Upper Ebro', 'alternate_name', null],

  // German regions
  ['Moselle', 'Mosel', 'translation', 'en'],
  ['Palatinate', 'Pfalz', 'translation', 'en'],
  ['Rhine', 'Rheingau', 'abbreviation', 'en'],
  ['Franconia', 'Franken', 'translation', 'en'],
  ['Baden', 'Baden', 'alternate_name', null],
  ['Württemberg', 'Württemberg', 'alternate_name', null],
  ['Wuerttemberg', 'Württemberg', 'alternate_name', null],

  // Portuguese regions
  ['Dão', 'Dão', 'alternate_name', null],
  ['Dao', 'Dão', 'alternate_name', null],
  ['Minho', 'Vinho Verde', 'alternate_name', 'pt'],
  ['Porto', 'Douro', 'alternate_name', null],

  // Austrian regions
  ['Lower Austria', 'Niederösterreich', 'translation', 'en'],
  ['Niederosterreich', 'Niederösterreich', 'alternate_name', null],
  ['Burgenland', 'Burgenland', 'alternate_name', null],
  ['Styria', 'Steiermark', 'translation', 'en'],
  ['Steiermark', 'Steiermark', 'alternate_name', null],
  ['Vienna', 'Wien', 'translation', 'en'],

  // New World
  ['Hawkes Bay', "Hawke's Bay", 'alternate_name', null],
  ['Hawke\'s Bay', "Hawke's Bay", 'alternate_name', null],
  ['Barossa', 'Barossa', 'alternate_name', null],
  ['McLaren Vale', 'McLaren Vale', 'alternate_name', null],
  ['Margaret River', 'Margaret River', 'alternate_name', null],
  ['Napa', 'Napa Valley', 'abbreviation', null],
  ['Sonoma', 'Sonoma County', 'abbreviation', null],
  ['Willamette', 'Willamette Valley', 'abbreviation', null],
  ['Maipo', 'Maipo Valley', 'abbreviation', null],
  ['Colchagua', 'Colchagua Valley', 'abbreviation', null],
  ['Casablanca', 'Casablanca Valley', 'abbreviation', null],

  // South Africa
  ['Stellenbosch', 'Stellenbosch', 'alternate_name', null],
  ['Constantia', 'Constantia', 'alternate_name', null],
  ['Swartland', 'Swartland', 'alternate_name', null],
];

async function main() {
  // Load regions
  const allRegions = [];
  let from = 0;
  while (true) {
    const { data, error } = await sb.from('regions').select('id,name,slug').range(from, from + 999);
    if (error) throw error;
    allRegions.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }

  const regionByName = new Map();
  const regionByNorm = new Map();
  for (const r of allRegions) {
    regionByName.set(r.name.toLowerCase(), r);
    regionByNorm.set(normalize(r.name), r);
  }

  let inserted = 0, skipped = 0, notFound = 0;

  for (const [alias, canonical, aliasType, lang] of ALIASES) {
    const region = regionByName.get(canonical.toLowerCase()) || regionByNorm.get(normalize(canonical));
    if (!region) {
      console.log(`  ⚠ Region not found: "${canonical}" (alias: "${alias}")`);
      notFound++;
      continue;
    }

    const aliasNorm = normalize(alias);
    // Skip if alias is same as canonical (normalized)
    if (aliasNorm === normalize(region.name)) {
      skipped++;
      continue;
    }

    if (DRY_RUN) {
      console.log(`  [DRY RUN] "${alias}" → ${region.name} (${aliasType})`);
      inserted++;
      continue;
    }

    const { error } = await sb.from('region_aliases').upsert({
      region_id: region.id,
      alias: alias,
      alias_normalized: aliasNorm,
      alias_type: aliasType,
      language_code: lang,
      source: 'wset-l3-conventions',
    }, { onConflict: 'alias_normalized' });

    if (error) {
      console.log(`  ⚠ Error for "${alias}": ${error.message}`);
    } else {
      inserted++;
    }
  }

  console.log(`\nDone. Inserted: ${inserted}, Skipped (same as canonical): ${skipped}, Not found: ${notFound}`);
}

main().catch(console.error);
