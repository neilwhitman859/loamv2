#!/usr/bin/env node
/**
 * promote_staging.mjs — Match staging table rows against canonical, promote or link.
 *
 * Usage:
 *   node scripts/promote_staging.mjs --source polaner [--dry-run]
 *   node scripts/promote_staging.mjs --source skurnik [--dry-run] [--limit 100]
 *
 * For each staging row:
 *   1. Parse producer name from the source data
 *   2. Match producer against canonical (3-tier: exact, alias, fuzzy RPC)
 *   3. Match wine against canonical (within matched producer)
 *   4. If matched: link staging row, merge any new fields
 *   5. If no match: create new canonical producer/wine, link staging row
 *   6. Log match decisions for audit
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { MergeEngine, normalize, slugify } from '../lib/merge.mjs';

// ── Load .env ───────────────────────────────────────────────
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

// ── CLI Args ────────────────────────────────────────────────
const args = process.argv.slice(2);
const sourceArg = args.find(a => a.startsWith('--source='))?.split('=')[1]
  || args[args.indexOf('--source') + 1];
const dryRun = args.includes('--dry-run');
const limitArg = args.find(a => a.startsWith('--limit='))?.split('=')[1]
  || args[args.indexOf('--limit') + 1];
const limit = limitArg ? parseInt(limitArg) : null;

if (!sourceArg) {
  console.log('Usage: node scripts/promote_staging.mjs --source <name> [--dry-run] [--limit N]');
  console.log('Sources: polaner, kl, skurnik, winebow, empson, ec');
  process.exit(1);
}

// ── Source Adapters ─────────────────────────────────────────
// Each adapter extracts producer name, wine name, and resolved fields from the staging row.

function parsePolaner(row) {
  // Title format: "Producer Name Wine Region" — heuristic split
  const title = row.title || '';
  // For Polaner, there's no separate producer field. We'll use the title as wine_name
  // and try to split it later. For now, use the first 2-3 words as producer guess.
  // Better: use the slug structure or just set the full title as wine name.
  return {
    producerName: null, // Can't reliably parse from title alone
    wineName: title,
    country: row.country,
    region: row.region,
    appellation: row.appellation,
    color: null,
    wineType: null,
    grape: null,
    certifications: row.certifications,
    sourceTable: 'source_polaner',
    sourceId: row.id,
  };
}

function normalizeWineType(rawType) {
  if (!rawType) return 'table';
  const t = rawType.toLowerCase().trim();
  if (t === 'sparkling') return 'sparkling';
  if (t === 'dessert') return 'dessert';
  if (t === 'fortified') return 'fortified';
  if (t === 'aromatized') return 'aromatized';
  // Red, White, Rosé, etc. are colors, not types — default to table
  return 'table';
}

function extractColorFromType(rawType) {
  if (!rawType) return null;
  const t = rawType.toLowerCase().trim();
  if (t === 'red') return 'red';
  if (t === 'white') return 'white';
  if (t === 'rosé' || t === 'rose') return 'rose';
  return null;
}

function parseKL(row) {
  return {
    producerName: row.grower_name,
    wineName: row.wine_name,
    country: row.country,
    region: row.region,
    appellation: null, // KL doesn't have appellation field
    color: extractColorFromType(row.wine_type),
    wineType: normalizeWineType(row.wine_type),
    grape: row.blend,
    soil: row.soil,
    vineAge: row.vine_age,
    vineyardArea: row.vineyard_area,
    vinification: row.vinification,
    farming: row.farming,
    sourceTable: 'source_kermit_lynch',
    sourceId: row.id,
    externalIds: row.kl_id ? [{ system: 'kermit_lynch', id: row.kl_id }] : [],
  };
}

function parseSkurnik(row) {
  return {
    producerName: row.producer,
    wineName: row.name,
    country: row.country,
    region: row.region,
    appellation: row.appellation,
    color: row.color?.toLowerCase(),
    wineType: null,
    grape: row.grape,
    vintage: row.vintage,
    sourceTable: 'source_skurnik',
    sourceId: row.id,
    externalIds: row.sku ? [{ system: 'skurnik_sku', id: row.sku }] : [],
  };
}

function parseWinebow(row) {
  return {
    producerName: row.producer,
    wineName: row.name,
    country: null, // Winebow doesn't have country
    region: null,
    appellation: row.appellation,
    color: null,
    wineType: null,
    grape: row.grape,
    vintage: row.vintage,
    soil: row.soil,
    abv: row.abv,
    ph: row.ph,
    acidity: row.acidity,
    residualSugar: row.residual_sugar,
    scores: row.scores,
    sourceTable: 'source_winebow',
    sourceId: row.id,
  };
}

function parseEmpson(row) {
  return {
    producerName: row.producer,
    wineName: row.name,
    country: 'Italy', // Empson is Italy-only
    region: null,
    appellation: null,
    color: null,
    wineType: null,
    grape: row.grape,
    soil: row.soil,
    vinification: row.vinification,
    winemaker: row.winemaker,
    altitude: row.altitude,
    sourceTable: 'source_empson',
    sourceId: row.id,
  };
}

function parseEC(row) {
  return {
    producerName: row.producer,
    wineName: row.name,
    country: null, // EC doesn't have country
    region: null,
    appellation: row.appellation,
    color: row.color?.toLowerCase(),
    wineType: null,
    grape: row.grape,
    soil: row.soil,
    certifications: row.certifications,
    scores: row.scores,
    sourceTable: 'source_european_cellars',
    sourceId: row.id,
  };
}

const ADAPTERS = {
  polaner: { table: 'source_polaner', parse: parsePolaner },
  kl: { table: 'source_kermit_lynch', parse: parseKL },
  skurnik: { table: 'source_skurnik', parse: parseSkurnik },
  winebow: { table: 'source_winebow', parse: parseWinebow },
  empson: { table: 'source_empson', parse: parseEmpson },
  ec: { table: 'source_european_cellars', parse: parseEC },
};

// ── Polaner Title Parser ────────────────────────────────────
// Polaner titles follow "Producer Wine Region" pattern. We need to split them.
// Strategy: match the region/appellation from the end, then the producer is
// typically the first 2-3 words.

function splitPolanerTitle(title, resolvedAppellation, resolvedRegion) {
  if (!title) return { producer: null, wine: title };

  let remaining = title;

  // Strip appellation/region from end if it appears
  if (resolvedAppellation) {
    const appName = resolvedAppellation.name;
    if (remaining.toLowerCase().endsWith(appName.toLowerCase())) {
      remaining = remaining.slice(0, -appName.length).trim();
    }
  }
  if (resolvedRegion && remaining.length === title.length) {
    const regName = resolvedRegion.name;
    if (remaining.toLowerCase().endsWith(regName.toLowerCase())) {
      remaining = remaining.slice(0, -regName.length).trim();
    }
  }

  // Now remaining is "Producer WineName". Split heuristically.
  // Most Polaner titles have the pattern "ProducerWord1 ProducerWord2 WineDescription"
  // We'll try to match the beginning against known producers first (in canonical),
  // then fall back to taking the first N words.
  return { producer: null, wine: remaining };
}

// ── Fetch Staging Rows ──────────────────────────────────────

async function fetchUnprocessed(table, batchLimit = 1000) {
  let query = sb.from(table).select('*').is('processed_at', null);
  if (batchLimit) query = query.limit(batchLimit);
  const { data, error } = await query;
  if (error) throw new Error(`fetchUnprocessed ${table}: ${error.message}`);
  return data || [];
}

// ── Match & Promote ─────────────────────────────────────────

async function promoteSource(sourceName) {
  const adapter = ADAPTERS[sourceName];
  if (!adapter) throw new Error(`Unknown source: ${sourceName}`);

  console.log(`\n=== Promoting ${sourceName} ===`);

  const engine = new MergeEngine({ verbose: true });
  await engine.init();

  const rows = await fetchUnprocessed(adapter.table, limit || 10000);
  console.log(`  ${rows.length} unprocessed rows`);

  if (rows.length === 0) return;

  const stats = {
    producerMatched: 0,
    producerCreated: 0,
    wineMatched: 0,
    wineCreated: 0,
    errors: 0,
    skipped: 0,
  };

  // Cache resolved producers to avoid repeated lookups
  const producerCache = new Map(); // normalized name -> { id, name, confidence }
  let producerNamesCache = null; // For Polaner prefix matching

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    let parsed;
    try {
      parsed = adapter.parse(row);
    } catch (err) {
      console.error(`  Parse error row ${i}:`, err.message);
      stats.errors++;
      continue;
    }

    // ── Special handling for Polaner (no producer field) ──
    if (sourceName === 'polaner') {
      const countryId = engine.resolveCountry(parsed.country);
      const resolvedApp = engine.resolveAppellation(parsed.appellation);

      // Load all producer names once for prefix matching
      if (!producerNamesCache) {
        const { data: allProducers } = await sb.from('producers')
          .select('id,name,name_normalized,country_id')
          .is('deleted_at', null)
          .order('name');
        producerNamesCache = (allProducers || [])
          .sort((a, b) => b.name.length - a.name.length); // longest first for greedy match
      }

      // Strategy: check if any known producer name is a prefix of the title
      const titleLower = parsed.wineName.toLowerCase();
      const titleNorm = normalize(parsed.wineName);
      let producerMatch = null;
      let remainingWineName = null;

      for (const p of producerNamesCache) {
        const pNorm = p.name_normalized;
        if (titleNorm.startsWith(pNorm + ' ')) {
          // Country check if available
          if (countryId && p.country_id && p.country_id !== countryId) continue;
          producerMatch = { id: p.id, name: p.name, confidence: 0.9, match_tier: 1 };
          remainingWineName = parsed.wineName.slice(p.name.length).trim();
          // Strip appellation/region suffix
          if (resolvedApp && remainingWineName.toLowerCase().endsWith(resolvedApp.name.toLowerCase())) {
            remainingWineName = remainingWineName.slice(0, -resolvedApp.name.length).trim();
          }
          if (parsed.region && remainingWineName.toLowerCase().endsWith(parsed.region.toLowerCase())) {
            remainingWineName = remainingWineName.slice(0, -parsed.region.length).trim();
          }
          break;
        }
      }

      if (!producerMatch) {
        stats.skipped++;
        if (stats.skipped <= 5) console.log(`  Skipped (no producer): "${parsed.wineName.slice(0, 60)}"`);
        continue;
      }

      parsed.producerName = producerMatch.name;
      parsed.wineName = remainingWineName || parsed.wineName;
      parsed._producerMatch = producerMatch;
    }

    if (!parsed.producerName) {
      stats.skipped++;
      continue;
    }

    try {
      // ── Resolve References ──
      const countryId = engine.resolveCountry(parsed.country);
      const region = engine.resolveRegion(parsed.region, countryId);
      const appellation = engine.resolveAppellation(parsed.appellation, countryId);

      // ── Match Producer ──
      const producerKey = normalize(parsed.producerName) + '|' + (countryId || '');
      let producer = producerCache.get(producerKey);

      if (!producer) {
        // Try using RPC fuzzy match
        producer = parsed._producerMatch || await matchProducerWithRPC(engine, parsed.producerName, countryId);

        if (producer) {
          stats.producerMatched++;
          producerCache.set(producerKey, producer);
        } else if (!dryRun) {
          // Create new producer
          const newId = await engine.createProducer({
            name: parsed.producerName,
            countryId,
            regionId: region?.id || appellation?.region_id || null,
            metadata: { first_source: adapter.table },
          }, adapter.table);
          producer = { id: newId, name: parsed.producerName, confidence: 1.0, isNew: true };
          stats.producerCreated++;
          producerCache.set(producerKey, producer);
        } else {
          producer = { id: 'dry-run-new', name: parsed.producerName, confidence: 0, isNew: true };
          stats.producerCreated++;
          producerCache.set(producerKey, producer);
        }
      }

      // ── Match Wine ──
      if (!parsed.wineName) {
        stats.skipped++;
        continue;
      }

      let wine = await matchWineWithRPC(engine, producer.id, parsed.wineName);

      if (wine) {
        stats.wineMatched++;
        // Merge any new fields
        if (!dryRun) {
          const mergeFields = {};
          if (parsed.soil) mergeFields.soil_description = parsed.soil;
          if (parsed.vinification) mergeFields.vinification_notes = parsed.vinification;
          if (parsed.color) mergeFields.color = parsed.color;
          if (appellation) mergeFields.appellation_id = appellation.id;
          if (region) mergeFields.region_id = region.id;
          if (countryId) mergeFields.country_id = countryId;

          if (Object.keys(mergeFields).length > 0) {
            await engine.mergeWineFields(wine.id, mergeFields, adapter.table);
          }

          // Store external IDs
          if (parsed.externalIds) {
            for (const ext of parsed.externalIds) {
              await engine.storeExternalId('wine', wine.id, ext.system, ext.id);
            }
          }
        }
      } else if (!dryRun) {
        // Create new wine
        const wineId = await engine.createWine({
          name: parsed.wineName,
          producerId: producer.id,
          producerName: producer.name,
          countryId: countryId || appellation?.country_id || null,
          regionId: region?.id || appellation?.region_id || null,
          appellationId: appellation?.id || null,
          color: parsed.color || null,
          wineType: parsed.wineType || 'table',
          soilDescription: parsed.soil || null,
          vinificationNotes: parsed.vinification || null,
          metadata: { first_source: adapter.table },
        }, adapter.table);
        wine = { id: wineId, name: parsed.wineName, isNew: true };
        stats.wineCreated++;

        // Resolve and link grapes
        if (parsed.grape) {
          await linkGrapes(engine, wineId, parsed.grape);
        }

        // Store external IDs
        if (parsed.externalIds) {
          for (const ext of parsed.externalIds) {
            await engine.storeExternalId('wine', wineId, ext.system, ext.id);
          }
        }
      } else {
        stats.wineCreated++;
        wine = { id: 'dry-run-new', name: parsed.wineName, isNew: true };
      }

      // ── Update staging row ──
      if (!dryRun && wine && wine.id !== 'dry-run-new') {
        const updateData = {
          canonical_wine_id: wine.id,
          canonical_producer_id: producer.id,
          processed_at: new Date().toISOString(),
          match_confidence: wine.confidence || (wine.isNew ? null : 0.95),
        };
        await sb.from(adapter.table).update(updateData).eq('id', parsed.sourceId);
      }

    } catch (err) {
      console.error(`  Error row ${i} ("${parsed.wineName?.slice(0, 40)}"):`, err.message);
      stats.errors++;
    }

    // Progress
    if ((i + 1) % 200 === 0) {
      console.log(`  Processed ${i + 1}/${rows.length}...`);
    }
  }

  console.log(`\n  Results for ${sourceName}:`);
  console.log(`    Producers: ${stats.producerMatched} matched, ${stats.producerCreated} created`);
  console.log(`    Wines: ${stats.wineMatched} matched, ${stats.wineCreated} created`);
  console.log(`    Skipped: ${stats.skipped}, Errors: ${stats.errors}`);
  if (dryRun) console.log('    (DRY RUN — no changes written)');

  return stats;
}

// ── Fuzzy Match Helpers (using RPC) ─────────────────────────

async function matchProducerWithRPC(engine, name, countryId) {
  if (!name) return null;
  const norm = normalize(name);

  // Tier 1: Exact normalized match
  const { data: exactMatches } = await sb.from('producers')
    .select('id,name,country_id')
    .eq('name_normalized', norm)
    .is('deleted_at', null)
    .limit(5);

  if (exactMatches && exactMatches.length > 0) {
    const sameCountry = countryId ? exactMatches.find(p => p.country_id === countryId) : null;
    const best = sameCountry || exactMatches[0];
    return { id: best.id, name: best.name, confidence: 1.0, match_tier: 1 };
  }

  // Tier 2: Alias match
  const { data: aliasMatches } = await sb.from('producer_aliases')
    .select('producer_id,name')
    .eq('name_normalized', norm)
    .limit(5);

  if (aliasMatches && aliasMatches.length > 0) {
    const { data: producer } = await sb.from('producers')
      .select('id,name,country_id')
      .eq('id', aliasMatches[0].producer_id)
      .is('deleted_at', null)
      .single();
    if (producer) {
      return { id: producer.id, name: producer.name, confidence: 0.9, match_tier: 2 };
    }
  }

  // Tier 3: Fuzzy match via RPC
  const { data: fuzzyMatches, error } = await sb.rpc('match_producer_fuzzy', {
    p_name_normalized: norm,
    p_country_id: countryId || null,
    p_threshold: 0.4,
    p_limit: 3,
  });

  if (!error && fuzzyMatches && fuzzyMatches.length > 0) {
    const best = fuzzyMatches[0];
    return { id: best.id, name: best.name, confidence: parseFloat(best.sim), match_tier: 3 };
  }

  return null;
}

async function matchWineWithRPC(engine, producerId, wineName) {
  if (!producerId || !wineName || producerId === 'dry-run-new') return null;
  const norm = normalize(wineName);

  // Tier 1: Exact normalized name within producer
  const { data: exactMatches } = await sb.from('wines')
    .select('id,name,lwin')
    .eq('producer_id', producerId)
    .eq('name_normalized', norm)
    .is('deleted_at', null)
    .limit(5);

  if (exactMatches && exactMatches.length > 0) {
    return { id: exactMatches[0].id, name: exactMatches[0].name, confidence: 0.95, match_tier: 2 };
  }

  // Tier 2: Fuzzy match via RPC
  const { data: fuzzyMatches, error } = await sb.rpc('match_wine_fuzzy', {
    p_producer_id: producerId,
    p_name_normalized: norm,
    p_threshold: 0.4,
    p_limit: 3,
  });

  if (!error && fuzzyMatches && fuzzyMatches.length > 0) {
    const best = fuzzyMatches[0];
    return { id: best.id, name: best.name, confidence: parseFloat(best.sim), match_tier: 3 };
  }

  return null;
}

// ── Grape Linking ───────────────────────────────────────────

async function linkGrapes(engine, wineId, grapeString) {
  if (!grapeString) return;

  // Parse grape string: "100% Pinot Noir", "Syrah, Grenache", "50% Merlot / 50% Cabernet"
  const parts = grapeString.split(/[,/;&]+/).map(s => s.trim()).filter(Boolean);

  for (const part of parts) {
    // Extract percentage if present
    const pctMatch = part.match(/(\d+(?:\.\d+)?)\s*%/);
    const pct = pctMatch ? parseFloat(pctMatch[1]) : null;
    const name = part.replace(/\d+(?:\.\d+)?\s*%\s*/, '').trim();

    if (!name) continue;

    const grape = engine.resolveGrape(name);
    if (grape) {
      const { error } = await sb.from('wine_grapes')
        .upsert({ wine_id: wineId, grape_id: grape.id, percentage: pct },
          { onConflict: 'wine_id,grape_id' });
      if (error && !error.message.includes('duplicate')) {
        console.warn(`    Grape link error (${name}):`, error.message);
      }
    }
  }
}

// ── Main ────────────────────────────────────────────────────

async function main() {
  const t0 = Date.now();
  const stats = await promoteSource(sourceArg);
  console.log(`\nDone in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
