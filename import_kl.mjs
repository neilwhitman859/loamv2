#!/usr/bin/env node
/**
 * import_kl.mjs — Kermit Lynch bulk catalog import
 *
 * Imports 1,468 wines from 193 growers as extracted from kermitlynch.com API.
 * This is a multi-producer portfolio import — tests importers, bulk producer
 * creation, grape/region/appellation resolution, farming certifications.
 *
 * Usage:
 *   node import_kl.mjs [--dry-run] [--replace]
 *
 * --replace: deletes all KL-imported data before re-importing
 * --dry-run: logs what would happen without writing to DB
 */

import { readFileSync } from 'fs';
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

// ── Wine color inference ────────────────────────────────────
function inferColor(wineType) {
  if (!wineType) return null;
  const t = wineType.toLowerCase();
  if (t === 'red') return 'red';
  if (t === 'white') return 'white';
  if (t === 'rosé') return 'rose';
  if (t === 'sparkling') return null; // could be any
  if (t === 'dessert') return null;
  return null;
}

// ── Grape parsing ───────────────────────────────────────────
// KL blend strings: "Grenache", "100% Petite Arvine", "Gamay, Chardonnay",
// "50% Syrah, 50% Grenache", "Mourvèdre/Grenache/Cinsault"
function parseBlend(blendStr) {
  if (!blendStr || blendStr === 'N/A') return [];
  // Skip "Varies" entries
  if (/^varies/i.test(blendStr.trim())) return [];
  if (/^see below/i.test(blendStr.trim())) return [];
  if (/^approximately/i.test(blendStr.trim())) {
    // "Approximately 60% Pinot Meunier..." — try to parse
    blendStr = blendStr.replace(/^approximately\s+/i, '');
  }
  // Handle "X and Y" pattern: "Corvina and Corvinone" → split
  let normalized = blendStr.replace(/\s+and\s+/gi, ', ').replace(/\s*&\s*/g, ', ');
  // Handle "X (Y% Z Rouge)" patterns — strip parentheticals
  normalized = normalized.replace(/\([^)]*\)/g, '');
  // Split by comma or /
  const parts = normalized.split(/[,\/]/).map(s => s.trim()).filter(Boolean);
  const grapes = [];
  for (const part of parts) {
    // Skip pure numbers, "see below", etc.
    if (/^\d+$/.test(part)) continue;
    if (/^see\s/i.test(part)) continue;
    // "50% Syrah" or "100% Petite Arvine" or just "Grenache"
    const pctMatch = part.match(/^(\d+)%?\s+(.+)/);
    if (pctMatch) {
      const name = pctMatch[2].trim();
      if (name.length > 1) grapes.push({ name, percentage: parseInt(pctMatch[1]) });
    } else {
      const name = part.trim();
      // Skip very short strings or numbers
      if (name.length > 2 && !/^\d/.test(name)) {
        grapes.push({ name, percentage: null });
      }
    }
  }
  return grapes;
}

// ── KL farming → Loam farming certification mapping ─────────
const FARMING_MAP = {
  'Biodynamic (certified)': 'Biodynamic',
  'Biodynamic (practicing)': 'Biodynamic',
  'Organic (certified)': 'Organic',
  'Organic (practicing)': 'Organic',
  'Sustainable': null, // no exact match
  'Lutte Raisonnée': null,
  'Traditional': null,
  'N/A': null,
  'Haute Valeur Environnementale (certified)': 'HVE',
};

// ── KL region → Loam region mapping ─────────────────────────
const REGION_MAP = {
  'Alsace': 'Alsace',
  'Alto Adige': 'Alto Adige',
  'Beaujolais': 'Beaujolais',
  'Bordeaux': 'Bordeaux',
  'Burgundy': 'Burgundy',
  'Campania': 'Campania',
  'Champagne': 'Champagne',
  'Corsica': 'Corsica',
  'Emilia-Romagna': 'Emilia-Romagna',
  'Friuli': 'Friuli-Venezia Giulia',
  'Jura': 'Jura',
  'Languedoc-Roussillon': 'Languedoc',
  'Liguria': 'Liguria',
  'Loire': 'Loire Valley',
  'Marche': 'Marche',
  'Molise': 'Molise',
  'Northern Rhône': 'Northern Rhône',
  'Piedmont': 'Piemonte',
  'Provence': 'Provence',
  'Puglia': 'Puglia',
  'Sardinia': 'Sardinia',
  'Savoie, Bugey, Hautes-Alpes': 'Savoie',
  'Sicily': 'Sicily',
  'Southern Rhône': 'Southern Rhône',
  'Southwest': 'Southwest France',
  'Tuscany': 'Tuscany',
  "Valle d'Aosta": "Valle d'Aosta",
  'Veneto': 'Veneto',
};

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`  KERMIT LYNCH BULK IMPORT`);
  console.log(`  ${DRY_RUN ? '(DRY RUN)' : REPLACE ? '(REPLACE MODE)' : '(INSERT MODE)'}`);
  console.log(`${'='.repeat(60)}\n`);

  // Load catalog
  const catalog = JSON.parse(readFileSync('data/imports/kermit_lynch_catalog.json', 'utf-8'));
  console.log(`Catalog: ${catalog.wines.length} wines, ${catalog.growers.length} growers\n`);

  // ── Load reference data ────────────────────────────────────
  console.log('Loading reference data...');

  const countries = await fetchAll('countries', 'id,name,iso_code');
  const countryMap = new Map();
  for (const c of countries) {
    countryMap.set(c.name.toLowerCase(), c.id);
    if (c.iso_code) countryMap.set(c.iso_code.toLowerCase(), c.id);
  }

  const regions = await fetchAll('regions', 'id,name,country_id,is_catch_all');
  const regionMap = new Map();
  for (const r of regions) {
    regionMap.set(r.name.toLowerCase(), r);
    regionMap.set(`${r.name.toLowerCase()}|${r.country_id}`, r);
  }

  const appellations = await fetchAll('appellations', 'id,name,country_id,region_id');
  const appellationMap = new Map();
  for (const a of appellations) {
    appellationMap.set(a.name.toLowerCase(), a);
    appellationMap.set(normalize(a.name), a);
  }

  // Load appellation aliases for fuzzy matching
  const aliases = await fetchAll('appellation_aliases', 'appellation_id,alias_normalized');
  for (const al of aliases) {
    const app = appellations.find(a => a.id === al.appellation_id);
    if (app && !appellationMap.has(al.alias_normalized)) {
      appellationMap.set(al.alias_normalized, app);
    }
  }
  console.log(`  Appellation aliases loaded: ${aliases.length}`);

  const grapes = await fetchAll('grapes', 'id,name,display_name,color');
  const grapeMap = new Map();
  for (const g of grapes) {
    if (g.display_name) grapeMap.set(g.display_name.toLowerCase(), g);
    grapeMap.set(g.name.toLowerCase(), g);
  }

  const synonyms = await fetchAll('grape_synonyms', 'grape_id,synonym');
  const synMap = new Map();
  for (const s of synonyms) synMap.set(s.synonym.toLowerCase(), s.grape_id);

  const farmingCerts = await fetchAll('farming_certifications', 'id,name');
  const farmingCertMap = new Map();
  for (const f of farmingCerts) farmingCertMap.set(f.name.toLowerCase(), f.id);

  const sourceTypes = await fetchAll('source_types', 'id,slug');
  const sourceTypeMap = new Map(sourceTypes.map(s => [s.slug, s.id]));
  const importerSourceId = sourceTypeMap.get('importer-website') || sourceTypeMap.get('producer-website');

  const varietalCategories = await fetchAll('varietal_categories', 'id,name,slug');
  const vcMap = new Map();
  for (const vc of varietalCategories) {
    vcMap.set(vc.name.toLowerCase(), vc.id);
    vcMap.set(vc.slug, vc.id);
  }

  console.log(`  Countries: ${countries.length}, Regions: ${regions.length}`);
  console.log(`  Appellations: ${appellations.length}, Grapes: ${grapes.length}`);
  console.log(`  Farming certs: ${farmingCerts.length}, Synonyms: ${synonyms.length}\n`);

  // ── Resolve grape helper ────────────────────────────────────
  function resolveGrape(name) {
    const lower = name.toLowerCase().trim();
    // Direct match by display_name or VIVC name
    const g = grapeMap.get(lower);
    if (g) return g.id;
    // Synonym
    const synId = synMap.get(lower);
    if (synId) return synId;
    // Try accent-stripped
    const stripped = lower.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    if (stripped !== lower) {
      const g2 = grapeMap.get(stripped);
      if (g2) return g2.id;
      const synId2 = synMap.get(stripped);
      if (synId2) return synId2;
    }
    return null;
  }

  // ── Stats ─────────────────────────────────────────────────
  const stats = {
    producers: 0, wines: 0, wineGrapes: 0,
    farmingCerts: 0, importerLinks: 0,
    warnings: [], regionMisses: new Set(), grapeMisses: new Set(),
    appellationHits: 0, appellationMisses: 0,
  };

  function warn(msg) {
    stats.warnings.push(msg);
  }

  // ── Create/find Kermit Lynch as importer ────────────────────
  console.log('Setting up Kermit Lynch importer...');
  let importerId;
  const { data: existingImporter } = await sb.from('importers')
    .select('id').eq('slug', 'kermit-lynch').single();

  if (existingImporter) {
    importerId = existingImporter.id;
    console.log(`  Importer exists: ${importerId}`);
  } else if (!DRY_RUN) {
    importerId = randomUUID();
    const { error } = await sb.from('importers').insert({
      id: importerId,
      name: 'Kermit Lynch Wine Merchant',
      slug: 'kermit-lynch',
      country_id: countryMap.get('united states'),
      website_url: 'https://kermitlynch.com',
      metadata: { founded: 1972, location: 'Berkeley, CA', type: 'importer-retailer' },
    });
    if (error) throw new Error(`Importer create error: ${error.message}`);
    console.log(`  Created importer: Kermit Lynch (${importerId})`);
  } else {
    console.log('  [DRY RUN] Would create importer: Kermit Lynch');
    importerId = 'dry-run-id';
  }

  // ── Import growers as producers ─────────────────────────────
  console.log('\nImporting growers as producers...\n');
  const producerIdMap = new Map(); // kl_id → our producer UUID

  for (const grower of catalog.growers) {
    const slug = slugify(grower.name);
    const countryId = countryMap.get((grower.country || '').toLowerCase());
    if (!countryId) {
      warn(`Country not found for grower: ${grower.name} (${grower.country})`);
      continue;
    }

    // Resolve region
    const loamRegion = REGION_MAP[grower.region] || grower.region;
    const regionData = loamRegion
      ? (regionMap.get(`${loamRegion.toLowerCase()}|${countryId}`) || regionMap.get(loamRegion.toLowerCase()))
      : null;
    const regionId = regionData?.id || null;
    if (!regionId && grower.region) {
      stats.regionMisses.add(grower.region);
    }

    // Check existing
    const { data: existing } = await sb.from('producers')
      .select('id').eq('slug', slug).single();

    if (existing) {
      producerIdMap.set(grower.kl_id, existing.id);
      continue;
    }

    const producerId = randomUUID();
    producerIdMap.set(grower.kl_id, producerId);

    // Parse founded year
    let foundedYear = null;
    if (grower.founded_year && typeof grower.founded_year === 'number') {
      foundedYear = grower.founded_year;
    } else if (typeof grower.founded_year === 'string') {
      const yearMatch = String(grower.founded_year).match(/\d{4}/);
      if (yearMatch) foundedYear = parseInt(yearMatch[0]);
    }

    const row = {
      id: producerId,
      slug,
      name: grower.name,
      name_normalized: normalize(grower.name),
      country_id: countryId,
      region_id: regionId,
      website_url: grower.website || null,
      year_established: foundedYear,
      producer_type: 'estate',
      philosophy: grower.viticulture_notes ? grower.viticulture_notes.substring(0, 1000) : null,
      metadata: {
        kl_id: grower.kl_id,
        kl_slug: grower.slug,
        winemaker: grower.winemaker,
        annual_production: grower.annual_production,
        location: grower.location,
        source: 'kermitlynch.com',
      },
    };

    if (!DRY_RUN) {
      const { error } = await sb.from('producers').insert(row);
      if (error) {
        warn(`Producer insert error for "${grower.name}": ${error.message}`);
        continue;
      }
    }
    stats.producers++;

    // Link to KL as importer
    if (!DRY_RUN && importerId !== 'dry-run-id') {
      const { error } = await sb.from('producer_importers').insert({
        producer_id: producerId,
        importer_id: importerId,
      });
      if (error && !error.message.includes('duplicate')) {
        warn(`Importer link error for "${grower.name}": ${error.message}`);
      } else {
        stats.importerLinks++;
      }
    }

    // Farming certifications
    if (grower.farming && grower.farming.length > 0) {
      for (const farmName of grower.farming) {
        const loamName = FARMING_MAP[farmName];
        if (!loamName) continue;
        const certId = farmingCertMap.get(loamName.toLowerCase());
        if (!certId) { warn(`Farming cert not found: ${loamName}`); continue; }
        if (!DRY_RUN) {
          const { error } = await sb.from('producer_farming_certifications').insert({
            producer_id: producerId,
            farming_certification_id: certId,
            source_id: importerSourceId,
          });
          if (error && !error.message.includes('duplicate')) {
            warn(`Farming cert error: ${error.message}`);
          } else {
            stats.farmingCerts++;
          }
        }
      }
    }

    if (stats.producers % 20 === 0) {
      console.log(`  Created ${stats.producers} producers...`);
    }
  }

  console.log(`\nCreated ${stats.producers} producers, ${stats.importerLinks} importer links, ${stats.farmingCerts} farming certs`);

  // ── Import wines ──────────────────────────────────────────
  console.log('\nImporting wines...\n');

  // Build wine dedup set
  const existingSlugs = new Set();

  for (const wine of catalog.wines) {
    const producerId = producerIdMap.get(wine.grower_kl_id);
    if (!producerId) {
      warn(`No producer for wine: ${wine.wine_name} (grower KL ID: ${wine.grower_kl_id})`);
      continue;
    }

    const countryId = countryMap.get((wine.country || '').toLowerCase());
    if (!countryId) continue;

    // Resolve region
    const loamRegion = REGION_MAP[wine.region] || wine.region;
    const regionData = loamRegion
      ? (regionMap.get(`${loamRegion.toLowerCase()}|${countryId}`) || regionMap.get(loamRegion.toLowerCase()))
      : null;
    const regionId = regionData?.id || null;

    // Infer wine properties
    const color = inferColor(wine.wine_type);
    const wineType = (wine.wine_type === 'Dessert') ? 'dessert' :
                     (wine.wine_type === 'Sparkling') ? 'sparkling' : 'table';
    const effervescence = (wine.wine_type === 'Sparkling') ? 'sparkling' : 'still';

    // Generate slug (deduplicate)
    let slug = slugify(`${wine.grower_name} ${wine.wine_name}`);
    if (existingSlugs.has(slug)) {
      slug = `${slug}-${wine.sku.toLowerCase()}`;
    }
    existingSlugs.add(slug);

    // Check existing
    const { data: existing } = await sb.from('wines')
      .select('id').eq('slug', slug).single();

    let wineId;
    if (existing) {
      wineId = existing.id;
    } else {
      wineId = randomUUID();

      // Try to resolve appellation from wine name
      let appellationId = null;
      const wineName = wine.wine_name;

      // Strategy 1: Full name (normalized) — catches exact matches and accent variants
      let app = appellationMap.get(normalize(wineName));

      // Strategy 2: Name before double quotes — "Bandol Rouge "La Migoua"" → "Bandol Rouge"
      // Only split on double quotes (straight or curly), NOT apostrophes (which appear in d'Alba etc.)
      if (!app) {
        const beforeQuote = wineName.split(/["\u201c\u201d\u201e]/)[0].trim();
        app = appellationMap.get(normalize(beforeQuote));
      }

      // Strategy 3: Strip color suffix — "Bandol Rouge" → "Bandol"
      if (!app) {
        const noColor = wineName.replace(/["\u201c\u201d\u201e].*$/, '').trim()
          .replace(/\s+(Rouge|Blanc|Rosé|Rosato|Rosso|Bianco|Clairet)\s*$/i, '').trim();
        app = appellationMap.get(normalize(noColor));
      }

      // Strategy 4: Strip cru/vineyard info — "Auxey-Duresses Rouge 1er Cru Les Bretterins" → "Auxey-Duresses"
      if (!app) {
        const noCru = wineName.replace(/["\u201c\u201d\u201e].*$/, '').trim()
          .replace(/\s+(Rouge|Blanc|Rosé|Rosato|Rosso|Bianco|Clairet)\s*$/i, '')
          .replace(/\s+(1er\s+Cru|Premier\s+Cru|Grand\s+Cru|Cru)\b.*$/i, '').trim();
        app = appellationMap.get(normalize(noCru));
      }

      // Strategy 5: Progressive word trimming from the right
      if (!app) {
        const words = wineName.replace(/["\u201c\u201d\u201e].*$/, '').trim().split(/\s+/);
        for (let len = words.length; len >= 1; len--) {
          const candidate = words.slice(0, len).join(' ');
          app = appellationMap.get(normalize(candidate));
          if (app) break;
        }
      }

      if (app) {
        appellationId = app.id;
        stats.appellationHits++;
      } else {
        stats.appellationMisses++;
      }

      // Resolve varietal category from blend
      let varietalCategoryId = null;
      const grapeEntries = parseBlend(wine.blend);
      if (grapeEntries.length === 1) {
        // Single grape — try to match a varietal category by name
        const gName = grapeEntries[0].name;
        varietalCategoryId = vcMap.get(gName.toLowerCase()) || vcMap.get(slugify(gName));
      }
      if (!varietalCategoryId && grapeEntries.length > 0) {
        // Multi-grape blend — try "X Blend" categories or default to color blend
        if (color === 'red') varietalCategoryId = vcMap.get('red-blend') || vcMap.get('red blend');
        else if (color === 'white') varietalCategoryId = vcMap.get('white-blend') || vcMap.get('white blend');
        else if (color === 'rose') varietalCategoryId = vcMap.get('rosé') || vcMap.get('rose');
        else varietalCategoryId = vcMap.get('red-blend'); // default
      }
      if (!varietalCategoryId) {
        // Last resort default
        if (wineType === 'sparkling') varietalCategoryId = vcMap.get('sparkling-blend') || vcMap.get('champagne-blend');
        else if (color === 'rose') varietalCategoryId = vcMap.get('rosé blend') || vcMap.get('rose-blend');
        else if (color === 'white') varietalCategoryId = vcMap.get('white-blend') || vcMap.get('white blend');
        else varietalCategoryId = vcMap.get('red-blend') || vcMap.get('red blend');
      }

      // Parse vinification metadata
      const metadata = {
        kl_id: wine.kl_id,
        kl_sku: wine.sku,
        soil: wine.soil || null,
        vine_age: wine.vine_age || null,
        vineyard_area: wine.vineyard_area || null,
        vinification: wine.vinification || null,
        source: 'kermitlynch.com',
      };

      const row = {
        id: wineId,
        slug,
        name: wine.wine_name,
        name_normalized: normalize(wine.wine_name),
        producer_id: producerId,
        country_id: countryId,
        region_id: regionId,
        appellation_id: appellationId,
        color,
        wine_type: wineType,
        effervescence,
        varietal_category_id: varietalCategoryId,
        metadata,
      };

      if (!DRY_RUN) {
        const { error } = await sb.from('wines').insert(row);
        if (error) {
          warn(`Wine insert error for "${wine.wine_name}": ${error.message}`);
          continue;
        }
      }
      stats.wines++;
    }

    // Insert grape composition
    const grapeEntriesToInsert = parseBlend(wine.blend);
    for (const entry of grapeEntriesToInsert) {
      const grapeId = resolveGrape(entry.name);
      if (!grapeId) {
        stats.grapeMisses.add(entry.name);
        continue;
      }
      if (!DRY_RUN) {
        const { error } = await sb.from('wine_grapes').insert({
          wine_id: wineId,
          grape_id: grapeId,
          percentage: entry.percentage,
        });
        if (error && !error.message.includes('duplicate')) {
          warn(`Wine grape error: ${error.message}`);
        } else {
          stats.wineGrapes++;
        }
      } else {
        stats.wineGrapes++;
      }
    }

    if (stats.wines % 100 === 0 && stats.wines > 0) {
      console.log(`  Imported ${stats.wines} wines, ${stats.wineGrapes} grape links...`);
    }
  }

  // ── Summary ───────────────────────────────────────────────
  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT SUMMARY');
  console.log(`${'='.repeat(60)}`);
  console.log(`  Producers created: ${stats.producers}`);
  console.log(`  Wines created: ${stats.wines}`);
  console.log(`  Wine grape links: ${stats.wineGrapes}`);
  console.log(`  Importer links: ${stats.importerLinks}`);
  console.log(`  Farming certs: ${stats.farmingCerts}`);
  console.log(`  Appellation resolved: ${stats.appellationHits}/${stats.appellationHits + stats.appellationMisses} (${(100*stats.appellationHits/(stats.appellationHits+stats.appellationMisses)).toFixed(0)}%)`);
  if (stats.regionMisses.size > 0) {
    console.log(`  Region misses: ${[...stats.regionMisses].join(', ')}`);
  }
  if (stats.grapeMisses.size > 0) {
    console.log(`  Grape misses (${stats.grapeMisses.size}): ${[...stats.grapeMisses].slice(0, 20).join(', ')}`);
  }
  if (stats.warnings.length > 0) {
    console.log(`\n  Warnings (${stats.warnings.length}):`);
    // Show unique warnings
    const unique = [...new Set(stats.warnings)];
    unique.slice(0, 20).forEach(w => console.log(`    - ${w}`));
    if (unique.length > 20) console.log(`    ... and ${unique.length - 20} more`);
  }
  console.log(`\n${'='.repeat(60)}\n`);
}

main().catch(console.error);
