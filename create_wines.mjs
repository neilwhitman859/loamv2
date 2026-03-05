#!/usr/bin/env node
/**
 * Create wine, wine_vintage, and wine_grapes records from wine_candidates.
 *
 * Phase 1: Load reference data (producers, grapes, varietal_categories, countries, regions, region_name_mappings)
 * Phase 2: Process wine_candidates → deduplicated wine records with FKs
 * Phase 3: Batch insert wines, wine_vintages, wine_grapes
 *
 * Usage:
 *   node create_wines.mjs --dry-run     # Preview without inserting
 *   node create_wines.mjs               # Insert everything
 */

import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "fs";
import { randomUUID } from "crypto";

// ── Load .env ───────────────────────────────────────────────
const envPath = new URL(".env", import.meta.url).pathname.replace(/^\/([A-Z]:)/, "$1");
const envContent = readFileSync(envPath, "utf-8");
for (const line of envContent.split("\n")) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) continue;
  const eqIdx = trimmed.indexOf("=");
  if (eqIdx === -1) continue;
  const key = trimmed.slice(0, eqIdx);
  const val = trimmed.slice(eqIdx + 1);
  if (!process.env[key]) process.env[key] = val;
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

const DRY_RUN = process.argv.includes("--dry-run");

// ── Helpers ─────────────────────────────────────────────────
function normalize(s) {
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function slugify(s) {
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/** Fetch all rows from a table with pagination */
async function fetchAll(table, columns = "*", batchSize = 1000) {
  const rows = [];
  let offset = 0;
  while (true) {
    const { data, error } = await sb
      .from(table)
      .select(columns)
      .range(offset, offset + batchSize - 1);
    if (error) throw new Error(`Fetch ${table}: ${error.message}`);
    rows.push(...data);
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return rows;
}

/** Batch insert with progress */
async function batchInsert(table, rows, batchSize = 500) {
  let inserted = 0;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const { error } = await sb.from(table).insert(batch);
    if (error) {
      console.error(`  ERROR inserting ${table} batch at offset ${i}: ${error.message}`);
      // Try smaller batches to isolate the issue
      for (let j = 0; j < batch.length; j += 50) {
        const micro = batch.slice(j, j + 50);
        const { error: e2 } = await sb.from(table).insert(micro);
        if (e2) {
          console.error(`    Sub-batch at ${i + j}: ${e2.message}`);
          // Try individual inserts to find the offender
          for (const row of micro) {
            const { error: e3 } = await sb.from(table).insert([row]);
            if (e3) console.error(`    Row error: ${e3.message} — slug=${row.slug || 'n/a'}, name=${row.name || 'n/a'}`);
            else inserted++;
          }
        } else {
          inserted += micro.length;
        }
      }
    } else {
      inserted += batch.length;
    }
    if ((i + batchSize) % 5000 < batchSize || i + batchSize >= rows.length) {
      process.stdout.write(`  ${Math.min(i + batchSize, rows.length)}/${rows.length}\r`);
    }
  }
  console.log(`  Inserted ${inserted}/${rows.length} rows into ${table}`);
  return inserted;
}

// ── Elaborate → Varietal Category name mapping ──────────────
const ELABORATE_BLEND_MAP = {
  "Assemblage/Bordeaux Red Blend": "Bordeaux Blend",
  "Assemblage/Rhône Red Blend": "Rhône Blend",
  "Assemblage/Valpolicella Red Blend": "Valpolicella Blend",
  "Assemblage/Champagne Blend": "Champagne Blend",
  "Assemblage/Portuguese Red Blend": "Douro Blend",
  "Assemblage/Port Blend": "Port",
  "Assemblage/Provence Rosé Blend": "Provence Blend",
  "Assemblage/Meritage Red Blend": "Meritage",
  "Assemblage/Portuguese White Blend": "White Douro Blend",
  "Assemblage/Rioja Red Blend": "Rioja Blend",
  "Assemblage/Cava Blend": "Cava Blend",
  "Assemblage/Tuscan Red Blend": "Super Tuscan",
  "Assemblage/Priorat Red Blend": "Priorat Blend",
  "Assemblage/Chianti Red Blend": "Chianti Blend",
  "Assemblage/Meritage White Blend": "White Meritage",
  // No specific categories for these — use generic
  "Assemblage/Bourgogne Red Blend": null,   // → Red Blend
  "Assemblage/Bourgogne White Blend": null,  // → White Blend
  "Assemblage/Soave White Blend": null,      // → White Blend
  "Assemblage/Rioja White Blend": null,      // → White Blend
};

// wine_type → generic blend category name
const WINE_TYPE_GENERIC_BLEND = {
  Red: "Red Blend",
  White: "White Blend",
  "Rosé": "Rosé Blend",
  Sparkling: "Sparkling Blend",
  Dessert: "Dessert Blend",
  "Dessert/Port": "Port",
};

// wine_type → target color for single varietal matching
function targetColorForWineType(wineType, grapeColor) {
  switch (wineType) {
    case "Red": return "red";
    case "White": return "white";
    case "Rosé": return "rose";
    case "Sparkling": return grapeColor || "white"; // match grape's natural color
    case "Dessert": return grapeColor || "white";
    case "Dessert/Port": return "red";
    default: return grapeColor || "red";
  }
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log("Phase 1: Loading reference data...");

  // Countries: name → id
  const countries = await fetchAll("countries", "id,name");
  const countryMap = new Map(); // name → id
  for (const c of countries) countryMap.set(c.name, c.id);
  console.log(`  ${countries.length} countries`);

  // Producers: name → {id, country_id, slug}
  const producers = await fetchAll("producers", "id,name,name_normalized,country_id,slug");
  const producerMap = new Map(); // "name|country_id" → {id, slug}
  for (const p of producers) {
    producerMap.set(`${p.name}|${p.country_id}`, { id: p.id, slug: p.slug });
  }
  console.log(`  ${producers.length} producers`);

  // Producer aliases: alias_name → producer_id
  const aliases = await fetchAll("producer_aliases", "name,producer_id");
  // Also load the producer's country_id and slug for each alias
  const aliasProducerIds = new Set(aliases.map((a) => a.producer_id));
  const producerById = new Map();
  for (const p of producers) producerById.set(p.id, p);

  const aliasMap = new Map(); // "alias_name|country_id" → {id, slug}
  for (const a of aliases) {
    const prod = producerById.get(a.producer_id);
    if (prod) {
      aliasMap.set(`${a.name}|${prod.country_id}`, { id: prod.id, slug: prod.slug });
    }
  }
  console.log(`  ${aliases.length} producer aliases`);

  // Grapes: name → {id, color}, plus aliases
  const grapes = await fetchAll("grapes", "id,name,aliases,color");
  const grapeMap = new Map(); // normalized_name → {id, color}
  for (const g of grapes) {
    grapeMap.set(g.name.toLowerCase(), { id: g.id, color: g.color });
    if (g.aliases) {
      for (const alias of g.aliases) {
        grapeMap.set(alias.toLowerCase(), { id: g.id, color: g.color });
      }
    }
  }
  console.log(`  ${grapes.length} grapes (${grapeMap.size} names including aliases)`);

  // Varietal categories: name → {id, color, grape_id, type}
  const vcats = await fetchAll("varietal_categories", "id,name,color,type,grape_id");
  const vcatByName = new Map(); // name → {id, ...}
  const vcatByGrapeColor = new Map(); // "grape_id|color" → id
  const vcatByGrape = new Map(); // grape_id → first match id
  for (const v of vcats) {
    vcatByName.set(v.name, v.id);
    if (v.grape_id) {
      vcatByGrapeColor.set(`${v.grape_id}|${v.color}`, v.id);
      if (!vcatByGrape.has(v.grape_id)) vcatByGrape.set(v.grape_id, v.id);
    }
  }
  console.log(`  ${vcats.length} varietal categories`);

  // Regions: catch-all by country_id
  const regions = await fetchAll("regions", "id,country_id,is_catch_all");
  const catchAllRegion = new Map(); // country_id → region_id
  for (const r of regions) {
    if (r.is_catch_all) catchAllRegion.set(r.country_id, r.id);
  }
  console.log(`  ${regions.length} regions (${catchAllRegion.size} catch-alls)`);

  // Region name mappings
  const rnm = await fetchAll("region_name_mappings", "region_name,country,region_id,appellation_id");
  const regionMapping = new Map(); // "region_name|country" → {region_id, appellation_id}
  for (const r of rnm) {
    regionMapping.set(`${r.region_name}|${r.country}`, {
      region_id: r.region_id,
      appellation_id: r.appellation_id,
    });
  }
  console.log(`  ${rnm.length} region name mappings`);

  // ── Phase 2: Process wine_candidates ──────────────────────
  console.log("\nPhase 2: Processing wine_candidates...");

  const allCandidates = await fetchAll(
    "wine_candidates",
    "id,producer_name,wine_name,wine_type,grapes,primary_grape,elaborate,abv,country,region_name,vintage_years"
  );
  console.log(`  ${allCandidates.length} wine_candidates fetched`);

  // Resolve producer_id for each candidate's producer_name + country
  // Track unresolved producers
  const unresolvedProducers = new Map(); // name|country → count
  let resolvedCount = 0;

  function resolveProducer(producerName, countryName) {
    const countryId = countryMap.get(countryName);
    if (!countryId) return null;

    // Try canonical name
    const key = `${producerName}|${countryId}`;
    if (producerMap.has(key)) return producerMap.get(key);

    // Try alias
    if (aliasMap.has(key)) return aliasMap.get(key);

    return null;
  }

  function resolveGrape(grapeName) {
    if (!grapeName) return null;
    // Strip surrounding double-quotes (data artifact from PostgreSQL arrays)
    const cleaned = grapeName.replace(/^"+|"+$/g, "");
    if (!cleaned) return null;
    const lower = cleaned.toLowerCase();
    return grapeMap.get(lower) || null;
  }

  function resolveVarietalCategory(elaborate, primaryGrape, wineType) {
    // 1. Named blend from elaborate field
    if (elaborate && elaborate.startsWith("Assemblage/")) {
      const mapped = ELABORATE_BLEND_MAP[elaborate];
      if (mapped !== undefined) {
        if (mapped === null) {
          // No specific category — use generic
          return vcatByName.get(WINE_TYPE_GENERIC_BLEND[wineType]) || null;
        }
        return vcatByName.get(mapped) || null;
      }
      // Unknown assemblage type — if it's "Assemblage/Blend", use generic
      if (elaborate === "Assemblage/Blend") {
        return vcatByName.get(WINE_TYPE_GENERIC_BLEND[wineType]) || null;
      }
      // Fallback for any other Assemblage/ type
      return vcatByName.get(WINE_TYPE_GENERIC_BLEND[wineType]) || null;
    }

    // 2. Single varietal: match grape → varietal_category
    const grape = resolveGrape(primaryGrape);
    if (grape) {
      const targetColor = targetColorForWineType(wineType, grape.color);
      // Try exact color match
      const vcId = vcatByGrapeColor.get(`${grape.id}|${targetColor}`);
      if (vcId) return vcId;

      // For rosé, we already tried "rose"; try the grape's own color as fallback
      if (targetColor === "rose") {
        const fallback = vcatByGrapeColor.get(`${grape.id}|${grape.color}`);
        if (fallback) return fallback;
      }

      // Any color match for this grape
      const anyColor = vcatByGrape.get(grape.id);
      if (anyColor) return anyColor;
    }

    // 3. Fallback to generic blend
    return vcatByName.get(WINE_TYPE_GENERIC_BLEND[wineType]) || vcatByName.get("Red Blend");
  }

  // Deduplicate wine_candidates on (producer_id, wine_name_normalized)
  // Merge vintage_years and grapes from duplicates
  const wineDedup = new Map(); // "producer_id|wine_name_norm" → merged data

  for (const wc of allCandidates) {
    const countryId = countryMap.get(wc.country);
    if (!countryId) {
      unresolvedProducers.set(`COUNTRY:${wc.country}`, (unresolvedProducers.get(`COUNTRY:${wc.country}`) || 0) + 1);
      continue;
    }

    const prod = resolveProducer(wc.producer_name, wc.country);
    if (!prod) {
      const uKey = `${wc.producer_name}|${wc.country}`;
      unresolvedProducers.set(uKey, (unresolvedProducers.get(uKey) || 0) + 1);
      continue;
    }
    resolvedCount++;

    const wineNameNorm = normalize(wc.wine_name);
    const dedupKey = `${prod.id}|${wineNameNorm}`;

    if (wineDedup.has(dedupKey)) {
      // Merge: union vintage_years, pick more complete grapes
      const existing = wineDedup.get(dedupKey);
      if (wc.vintage_years) {
        const vintSet = new Set(existing.vintage_years || []);
        for (const v of wc.vintage_years) vintSet.add(v);
        existing.vintage_years = [...vintSet].sort((a, b) => a - b);
      }
      if (wc.grapes && (!existing.grapes || wc.grapes.length > existing.grapes.length)) {
        existing.grapes = wc.grapes;
        existing.primary_grape = wc.primary_grape;
        existing.elaborate = wc.elaborate;
      }
      existing.candidate_ids.push(wc.id);
    } else {
      wineDedup.set(dedupKey, {
        producer_id: prod.id,
        producer_slug: prod.slug,
        wine_name: wc.wine_name,
        wine_name_norm: wineNameNorm,
        wine_type: wc.wine_type,
        grapes: wc.grapes || [],
        primary_grape: wc.primary_grape,
        elaborate: wc.elaborate,
        abv: wc.abv,
        country: wc.country,
        country_id: countryId,
        region_name: wc.region_name,
        vintage_years: wc.vintage_years || [],
        candidate_ids: [wc.id],
      });
    }
  }

  console.log(`  ${resolvedCount}/${allCandidates.length} candidates resolved to producers`);
  console.log(`  ${wineDedup.size} unique wines after dedup (${allCandidates.length - resolvedCount} unresolved, ${resolvedCount - wineDedup.size} merged dups)`);

  if (unresolvedProducers.size > 0) {
    console.log(`\n  Top unresolved producers:`);
    const sorted = [...unresolvedProducers.entries()].sort((a, b) => b[1] - a[1]);
    for (const [name, count] of sorted.slice(0, 15)) {
      console.log(`    ${name} (${count} wines)`);
    }
  }

  // Build wine records, vintage records, wine_grapes records
  const wines = [];
  const vintages = [];
  const wineGrapes = [];
  const slugCounts = new Map(); // track slug collisions
  const unresolvedGrapes = new Map(); // grape_name → count

  // Pre-compute all base slugs for collision detection
  const baseSlugs = [];
  for (const [, wd] of wineDedup) {
    const baseSlug = `${wd.producer_slug}-${slugify(wd.wine_name)}`.slice(0, 120) || wd.producer_slug;
    baseSlugs.push(baseSlug);
  }

  // Count slug occurrences
  for (const s of baseSlugs) {
    slugCounts.set(s, (slugCounts.get(s) || 0) + 1);
  }

  // Now build records
  let idx = 0;
  const slugUsed = new Map(); // baseSlug → next suffix number

  for (const [, wd] of wineDedup) {
    const wineId = randomUUID();
    let baseSlug = baseSlugs[idx++];

    // Disambiguate collisions
    let slug;
    if (slugCounts.get(baseSlug) > 1) {
      const num = (slugUsed.get(baseSlug) || 1);
      slugUsed.set(baseSlug, num + 1);
      slug = num === 1 ? baseSlug : `${baseSlug}-${num}`;
    } else {
      slug = baseSlug;
    }

    // Resolve region
    let regionId = null;
    let appellationId = null;
    if (wd.region_name) {
      const rm = regionMapping.get(`${wd.region_name}|${wd.country}`);
      if (rm) {
        regionId = rm.region_id;
        appellationId = rm.appellation_id;
      }
    }
    if (!regionId) {
      regionId = catchAllRegion.get(wd.country_id) || null;
    }

    // Resolve varietal category
    const vcatId = resolveVarietalCategory(wd.elaborate, wd.primary_grape, wd.wine_type);

    // Determine effervescence
    let effervescence = null;
    if (wd.wine_type === "Sparkling") effervescence = "sparkling";

    wines.push({
      id: wineId,
      slug,
      name: wd.wine_name,
      name_normalized: wd.wine_name_norm,
      producer_id: wd.producer_id,
      country_id: wd.country_id,
      region_id: regionId,
      appellation_id: appellationId,
      varietal_category_id: vcatId,
      effervescence,
      is_nv: false,
    });

    // Wine vintages
    for (const year of wd.vintage_years) {
      vintages.push({
        wine_id: wineId,
        vintage_year: year,
        alcohol_pct: wd.abv ? parseFloat(wd.abv) : null,
      });
    }

    // Wine grapes
    const seenGrapeIds = new Set();
    for (const grapeName of wd.grapes) {
      const grape = resolveGrape(grapeName);
      if (grape && !seenGrapeIds.has(grape.id)) {
        seenGrapeIds.add(grape.id);
        wineGrapes.push({
          wine_id: wineId,
          grape_id: grape.id,
        });
      } else if (!grape) {
        unresolvedGrapes.set(grapeName, (unresolvedGrapes.get(grapeName) || 0) + 1);
      }
    }
  }

  // Report slug collisions
  const collisions = [...slugCounts.entries()].filter(([, c]) => c > 1);
  console.log(`\n  ${collisions.length} slug collisions disambiguated`);
  if (collisions.length > 0) {
    const topCollisions = collisions.sort((a, b) => b[1] - a[1]).slice(0, 10);
    for (const [slug, count] of topCollisions) {
      console.log(`    "${slug}" x ${count}`);
    }
  }

  // Report unresolved grapes
  if (unresolvedGrapes.size > 0) {
    console.log(`\n  ${unresolvedGrapes.size} unresolved grape names (${[...unresolvedGrapes.values()].reduce((a, b) => a + b, 0)} wine_grapes rows skipped):`);
    const sorted = [...unresolvedGrapes.entries()].sort((a, b) => b[1] - a[1]);
    for (const [name, count] of sorted.slice(0, 20)) {
      console.log(`    "${name}" (${count})`);
    }
  }

  console.log(`\n── Summary ──`);
  console.log(`  Wines: ${wines.length}`);
  console.log(`  Wine vintages: ${vintages.length}`);
  console.log(`  Wine grapes: ${wineGrapes.length}`);

  if (DRY_RUN) {
    console.log(`\nDRY RUN — no database changes made.`);
    console.log(`\nSample wines:`);
    for (const w of wines.slice(0, 10)) {
      console.log(`  ${w.name} (${w.slug}) — producer_id=${w.producer_id.slice(0, 8)}...`);
    }
    return;
  }

  // ── Phase 3: Batch insert ─────────────────────────────────
  console.log("\nPhase 3: Inserting wines...");
  await batchInsert("wines", wines, 500);

  console.log("\nPhase 3b: Inserting wine_vintages...");
  await batchInsert("wine_vintages", vintages, 2000);

  console.log("\nPhase 3c: Inserting wine_grapes...");
  await batchInsert("wine_grapes", wineGrapes, 2000);

  console.log("\n✅ Done!");
}

main().catch((err) => {
  console.error("FATAL:", err);
  process.exit(1);
});
