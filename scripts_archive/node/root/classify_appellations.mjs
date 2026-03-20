#!/usr/bin/env node
/**
 * classify_appellations.mjs
 *
 * Uses Claude Haiku to classify region_name_mappings with NULL appellation_id:
 *   - formal_appellation → insert into appellations table + link mapping
 *   - not_appellation → leave mapping as-is (broad region / IGP / sub-zone)
 *
 * Then updates wines by tracing back through wine_candidates to get the
 * original region_name and applying the new appellation_id.
 *
 * Usage:
 *   node classify_appellations.mjs --dry-run    # Preview without DB changes
 *   node classify_appellations.mjs              # Full run
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
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);
const DRY_RUN = process.argv.includes("--dry-run");

// ── Helpers ─────────────────────────────────────────────────
function slugify(s) {
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function normalize(s) {
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchAll(table, columns = "*", batchSize = 1000) {
  const rows = [];
  let offset = 0;
  while (true) {
    const { data, error } = await sb.from(table).select(columns).range(offset, offset + batchSize - 1);
    if (error) throw new Error(`Fetch ${table}: ${error.message}`);
    rows.push(...data);
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return rows;
}

async function callHaiku(messages, maxTokens = 4096) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-haiku-4-5-20251001",
      max_tokens: maxTokens,
      messages,
    }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Haiku API error ${res.status}: ${err}`);
  }
  const data = await res.json();
  return data.content[0].text;
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log(`\n${"=".repeat(70)}`);
  console.log(`  APPELLATION CLASSIFICATION & INSERTION`);
  console.log(`  Mode: ${DRY_RUN ? "DRY RUN" : "LIVE"}`);
  console.log(`${"=".repeat(70)}\n`);

  // ── Phase 1: Load reference data ──────────────────────────
  console.log("Phase 1: Loading reference data...");

  const regions = await fetchAll("regions", "id,name,country_id,is_catch_all");
  const regionMap = new Map(); // name|country_id → region
  const regionById = new Map();
  for (const r of regions) {
    regionMap.set(`${r.name}|${r.country_id}`, r);
    regionById.set(r.id, r);
  }
  console.log(`  ${regions.length} regions`);

  const countries = await fetchAll("countries", "id,name");
  const countryByName = new Map();
  const countryById = new Map();
  for (const c of countries) {
    countryByName.set(c.name, c);
    countryById.set(c.id, c);
  }
  console.log(`  ${countries.length} countries`);

  const appellations = await fetchAll("appellations", "id,name,slug,designation_type,region_id,country_id");
  const appellationLookup = new Map(); // normalized_name|country_id → appellation
  for (const a of appellations) {
    appellationLookup.set(`${normalize(a.name)}|${a.country_id}`, a);
  }
  console.log(`  ${appellations.length} existing appellations`);

  const mappings = await fetchAll("region_name_mappings", "region_name,country,region_id,appellation_id,match_type");
  const nullAppMappings = mappings.filter((m) => !m.appellation_id);
  console.log(`  ${mappings.length} total mappings, ${nullAppMappings.length} with NULL appellation_id`);

  // Get wine_candidate counts per region_name|country
  console.log("  Fetching wine_candidate counts per region_name...");
  const allCandidatesForCounts = await fetchAll("wine_candidates", "region_name,country");
  const wcCountMap = new Map(); // region_name|country → count
  for (const wc of allCandidatesForCounts) {
    if (!wc.region_name) continue;
    const key = `${wc.region_name}|${wc.country}`;
    wcCountMap.set(key, (wcCountMap.get(key) || 0) + 1);
  }
  console.log(`  Wine candidate counts computed for ${wcCountMap.size} region_name combos`);

  // ── Phase 2: Quick backfill ───────────────────────────────
  console.log("\nPhase 2: Quick backfill — matching existing appellations...");
  const quickBackfills = [];
  const needsClassification = [];

  for (const m of nullAppMappings) {
    const country = countryByName.get(m.country);
    if (!country) {
      // Unknown country, skip
      continue;
    }
    const normName = normalize(m.region_name);
    const existing = appellationLookup.get(`${normName}|${country.id}`);
    if (existing) {
      quickBackfills.push({
        region_name: m.region_name,
        country: m.country,
        appellation_id: existing.id,
        appellation_name: existing.name,
      });
    } else {
      needsClassification.push({
        region_name: m.region_name,
        country: m.country,
        country_id: country.id,
        region_id: m.region_id,
        resolved_region: regionById.get(m.region_id)?.name || "UNKNOWN",
        is_catch_all: regionById.get(m.region_id)?.is_catch_all || false,
        match_type: m.match_type,
        candidate_count: wcCountMap.get(`${m.region_name}|${m.country}`) || 0,
      });
    }
  }

  console.log(`  ${quickBackfills.length} mappings matched existing appellations (quick backfill)`);
  console.log(`  ${needsClassification.length} mappings need Haiku classification`);

  // Apply quick backfills
  if (quickBackfills.length > 0 && !DRY_RUN) {
    console.log("  Applying quick backfills...");
    let backfilled = 0;
    for (const bf of quickBackfills) {
      const { error } = await sb
        .from("region_name_mappings")
        .update({ appellation_id: bf.appellation_id })
        .eq("region_name", bf.region_name)
        .eq("country", bf.country);
      if (error) {
        console.log(`    ERROR backfilling "${bf.region_name}|${bf.country}": ${error.message}`);
      } else {
        backfilled++;
      }
    }
    console.log(`  Backfilled ${backfilled}/${quickBackfills.length} mappings`);
  } else if (quickBackfills.length > 0) {
    console.log("  [DRY RUN] Would backfill:");
    for (const bf of quickBackfills) {
      console.log(`    "${bf.region_name}" (${bf.country}) → appellation "${bf.appellation_name}"`);
    }
  }

  // ── Phase 3: Haiku classification ─────────────────────────
  console.log("\nPhase 3: Haiku classification...");

  // Sort by candidate count descending
  needsClassification.sort((a, b) => b.candidate_count - a.candidate_count);

  // Build context about existing designation types and regions
  const existingDesignationTypes = [...new Set(appellations.map((a) => a.designation_type).filter(Boolean))].sort();

  // Send to Haiku in batches of 40
  const BATCH_SIZE = 40;
  const allClassifications = [];

  for (let i = 0; i < needsClassification.length; i += BATCH_SIZE) {
    const batch = needsClassification.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(needsClassification.length / BATCH_SIZE);
    console.log(`  Batch ${batchNum}/${totalBatches} (${batch.length} entries)...`);

    const itemsList = batch
      .map(
        (m, idx) =>
          `${idx + 1}. region_name="${m.region_name}" | country="${m.country}" | resolved_region="${m.resolved_region}" | is_catch_all=${m.is_catch_all} | wines=${m.candidate_count}`
      )
      .join("\n");

    const prompt = `You are a wine geography expert. Classify each region_name below as either a FORMAL APPELLATION or NOT an appellation.

A FORMAL APPELLATION is a legally defined wine production area with an official designation (AOC, DOC, DOCG, DO, AVA, GI, WO, DAC, VQA, PDO, IGP, IGT, etc.). Examples: "Côtes du Rhône" (AOC), "Barossa Valley" (GI), "Chianti Classico" (DOCG).

NOT an appellation includes:
- Broad administrative regions (California, Veneto, Piedmont, Burgundy, Oregon)
- Sub-zones without formal designation (Verona, Lombardia, Guyenne)
- Country-level catch-alls
- US states that are regions not AVAs
- Generic geographic terms

Existing designation types in our DB: ${existingDesignationTypes.join(", ")}

For each item, respond with a JSON object. Return a JSON array with one object per item:
{
  "index": <1-based index>,
  "region_name": "<the region_name>",
  "is_appellation": true/false,
  "designation_type": "<designation type if appellation, null otherwise>",
  "canonical_name": "<proper appellation name if different from region_name, otherwise same as region_name>",
  "confidence": "high" | "medium" | "low",
  "reason": "<brief reason>"
}

IMPORTANT RULES:
- Italian regions like "Veneto", "Piedmont/Piemonte", "Tuscany/Toscana", "Puglia", "Abruzzo", "Campania", "Umbria", "Marche", "Lombardy/Lombardia", "Emilia-Romagna", "Friuli-Venezia Giulia", "Trentino-Alto Adige", "Liguria", "Sardinia/Sardegna", "Sicily/Sicilia", "Basilicata", "Calabria", "Molise" are REGIONS, not appellations. BUT "Sicilia DOC" or "Trentino DOC" IS an appellation — only classify as appellation if the designation is explicit or well-known.
- French broad regions (Bordeaux, Burgundy, Loire Valley, Rhône Valley, Southern Rhône, Languedoc-Roussillon, Provence, Southwest France, Alsace, Jura, Corsica) are REGIONS. But many sub-zones ARE appellations (Côtes de Provence AOC, Minervois AOC, etc.).
- US states (California, Oregon, Washington) are REGIONS. Sub-regions like Central Coast, Sierra Foothills, Mendocino are AVAs.
- Australian states (South Australia, Victoria, New South Wales, Western Australia) are REGIONS. But some sub-zones ARE GIs (Barossa Valley GI, etc.).
- Spanish administrative regions (Castilla y León, Catalonia, Galicia) are REGIONS. But DO/DOCa zones (La Mancha DO, Valencia DO) ARE appellations.
- When in doubt about a small/obscure region, mark as is_appellation=true with confidence="low" — it's better to include than miss.
- For wines from countries with less formal systems (Czech Republic, Romania, Moldova, Brazil, etc.), use your best judgment.
- "Canelones" (Uruguay) IS a formal wine region/appellation.

Items to classify:
${itemsList}

Respond with ONLY the JSON array, no other text.`;

    try {
      const response = await callHaiku([{ role: "user", content: prompt }], 8192);
      // Parse JSON from response
      const jsonMatch = response.match(/\[[\s\S]*\]/);
      if (!jsonMatch) {
        console.log(`    WARNING: Could not parse JSON from Haiku response`);
        console.log(`    Response preview: ${response.substring(0, 200)}`);
        continue;
      }
      const classifications = JSON.parse(jsonMatch[0]);
      console.log(
        `    Got ${classifications.length} classifications: ${classifications.filter((c) => c.is_appellation).length} appellations, ${classifications.filter((c) => !c.is_appellation).length} not`
      );

      // Merge with batch metadata
      for (const cls of classifications) {
        const orig = batch[cls.index - 1];
        if (!orig) continue;
        allClassifications.push({
          ...orig,
          ...cls,
          country_id: orig.country_id,
          region_id: orig.region_id,
        });
      }
    } catch (err) {
      console.log(`    ERROR in batch ${batchNum}: ${err.message}`);
    }

    // Rate limit: small delay between batches
    if (i + BATCH_SIZE < needsClassification.length) {
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  // ── Phase 4: Process classifications ──────────────────────
  console.log("\nPhase 4: Processing Haiku classifications...");

  // Post-process: strip designation types from canonical names
  // (Haiku often appends "AOC", "DOC", "GI", etc. to canonical names)
  const DESIGNATION_SUFFIXES = [
    "DOCG", "DOCa", "DOC", "AOC", "AOP", "AVA", "VQA", "DAC",
    "GI", "WO", "DO", "PGI", "PDO", "IGP", "IGT", "IG",
    "Anbaugebiet", "Weinbaugebiet", "OEM", "AOG", "DOK", "VdT", "VdF", "Landwein",
  ];
  const designationPattern = new RegExp(
    `\\s+(?:${DESIGNATION_SUFFIXES.join("|")})\\s*$`, "i"
  );

  for (const c of allClassifications) {
    if (c.canonical_name) {
      // Strip trailing designation type from canonical name
      c.canonical_name = c.canonical_name.replace(designationPattern, "").trim();
    }
    // Fix known misclassifications
    if (c.region_name === "Oloroso" || c.region_name === "Manzanilla") {
      // These are sherry styles, not geographic appellations
      // However Manzanilla DO de Sanlúcar does exist - keep it
      if (c.region_name === "Oloroso") {
        c.is_appellation = false;
        c.reason = "Oloroso is a sherry style, not a geographic appellation";
      }
    }
  }

  const newAppellations = allClassifications.filter((c) => c.is_appellation);
  const notAppellations = allClassifications.filter((c) => !c.is_appellation);

  console.log(`  ${newAppellations.length} classified as formal appellations`);
  console.log(`  ${notAppellations.length} classified as not appellations`);

  // Print classifications summary
  console.log("\n  APPELLATIONS TO ADD:");
  let totalAppWines = 0;
  for (const a of newAppellations) {
    const name = a.canonical_name || a.region_name;
    console.log(
      `    ${name.padEnd(45)} | ${(a.designation_type || "?").padEnd(12)} | ${a.country.padEnd(18)} | ${String(a.candidate_count).padStart(5)} wines | ${a.confidence} | ${a.reason}`
    );
    totalAppWines += a.candidate_count;
  }
  console.log(`  Total wines affected: ${totalAppWines}`);

  console.log("\n  NOT APPELLATIONS (top 30):");
  notAppellations.sort((a, b) => b.candidate_count - a.candidate_count);
  for (const na of notAppellations.slice(0, 30)) {
    console.log(
      `    ${na.region_name.padEnd(45)} | ${na.country.padEnd(18)} | ${String(na.candidate_count).padStart(5)} wines | ${na.reason}`
    );
  }

  if (DRY_RUN) {
    console.log("\n[DRY RUN] Would insert appellations and update mappings. Exiting.");
    return;
  }

  // ── Phase 5: Insert new appellations ──────────────────────
  console.log("\nPhase 5: Inserting new appellations...");

  const appellationInserts = [];
  const slugsSeen = new Set(appellations.map((a) => a.slug));

  for (const a of newAppellations) {
    const name = a.canonical_name || a.region_name;

    // Check if already exists (might have been classified as appellation but already in DB)
    const normKey = `${normalize(name)}|${a.country_id}`;
    if (appellationLookup.has(normKey)) {
      console.log(`  SKIP (already exists): "${name}" in ${a.country}`);
      // Still update the mapping
      const existing = appellationLookup.get(normKey);
      await sb
        .from("region_name_mappings")
        .update({ appellation_id: existing.id })
        .eq("region_name", a.region_name)
        .eq("country", a.country);
      continue;
    }

    // Generate unique slug
    let slug = slugify(name);
    if (slugsSeen.has(slug)) {
      const countrySlug = slugify(countryById.get(a.country_id)?.name || "unknown");
      slug = `${slug}-${countrySlug}`;
    }
    if (slugsSeen.has(slug)) {
      slug = `${slug}-${randomUUID().slice(0, 6)}`;
    }
    slugsSeen.add(slug);

    const id = randomUUID();
    appellationInserts.push({
      id,
      slug,
      name,
      designation_type: a.designation_type || "Appellation",
      country_id: a.country_id,
      region_id: a.region_id,
      _original_region_name: a.region_name,
      _original_country: a.country,
    });
  }

  console.log(`  Inserting ${appellationInserts.length} new appellations...`);

  let insertedCount = 0;
  let insertErrors = 0;
  const IBATCH = 50;
  for (let i = 0; i < appellationInserts.length; i += IBATCH) {
    const batch = appellationInserts.slice(i, i + IBATCH);
    // Remove internal fields before insert
    const cleanBatch = batch.map(({ _original_region_name, _original_country, ...rest }) => rest);

    const { error } = await sb.from("appellations").insert(cleanBatch);
    if (error) {
      console.log(`    ERROR inserting batch at ${i}: ${error.message}`);
      // Try one by one
      for (const item of cleanBatch) {
        const { error: e2 } = await sb.from("appellations").insert([item]);
        if (e2) {
          console.log(`      SKIP "${item.name}": ${e2.message}`);
          insertErrors++;
        } else {
          insertedCount++;
        }
      }
    } else {
      insertedCount += batch.length;
    }
  }

  console.log(`  Inserted ${insertedCount} new appellations (${insertErrors} errors)`);

  // ── Phase 6: Update region_name_mappings ──────────────────
  console.log("\nPhase 6: Updating region_name_mappings with new appellation_ids...");

  let mappingUpdates = 0;
  for (const ai of appellationInserts) {
    const { error } = await sb
      .from("region_name_mappings")
      .update({ appellation_id: ai.id })
      .eq("region_name", ai._original_region_name)
      .eq("country", ai._original_country);
    if (error) {
      console.log(`    ERROR updating mapping "${ai._original_region_name}|${ai._original_country}": ${error.message}`);
    } else {
      mappingUpdates++;
    }
  }
  console.log(`  Updated ${mappingUpdates} mappings`);

  // ── Phase 7: Update wines via wine_candidates ─────────────
  console.log("\nPhase 7: Updating wines with new appellations via wine_candidates...");
  console.log("  Loading all producers and candidates for matching...");

  // Load producers (name → id mapping)
  const producers = await fetchAll("producers", "id,name");
  const producerByName = new Map();
  for (const p of producers) {
    producerByName.set(p.name.toLowerCase().trim(), p.id);
  }

  // Load aliases
  const aliases = await fetchAll("producer_aliases", "producer_id,name");
  for (const a of aliases) {
    producerByName.set(a.name.toLowerCase().trim(), a.producer_id);
  }
  console.log(`  ${producers.length} producers + ${aliases.length} aliases loaded`);

  // Build list of all region_name|country combos that now have appellation_ids
  const updatedMappingKeys = new Set();
  const mappingAppellation = new Map(); // region_name|country → appellation_id

  for (const bf of quickBackfills) {
    const key = `${bf.region_name}|${bf.country}`;
    updatedMappingKeys.add(key);
    mappingAppellation.set(key, bf.appellation_id);
  }
  for (const ai of appellationInserts) {
    const key = `${ai._original_region_name}|${ai._original_country}`;
    updatedMappingKeys.add(key);
    mappingAppellation.set(key, ai.id);
  }

  console.log(`  ${updatedMappingKeys.size} region_name|country combos to process`);

  // Load ALL wine_candidates (we need region_name, country, producer_name, wine_name)
  console.log("  Loading wine_candidates...");
  const allWC = await fetchAll("wine_candidates", "producer_name,wine_name,region_name,country");
  console.log(`  ${allWC.length} wine_candidates loaded`);

  // Filter to candidates whose region_name|country has a newly-assigned appellation
  const relevantCandidates = allWC.filter((wc) => {
    if (!wc.region_name) return false;
    return updatedMappingKeys.has(`${wc.region_name}|${wc.country}`);
  });
  console.log(`  ${relevantCandidates.length} candidates match updated mappings`);

  // Group by appellation_id → list of (producer_id, wine_name_normalized)
  const updatesByAppellation = new Map(); // appellation_id → [{producer_id, name_normalized}]

  let skippedNoProducer = 0;
  for (const wc of relevantCandidates) {
    const prodId = producerByName.get(wc.producer_name.toLowerCase().trim());
    if (!prodId) {
      skippedNoProducer++;
      continue;
    }
    const appId = mappingAppellation.get(`${wc.region_name}|${wc.country}`);
    if (!appId) continue;

    const normWineName = normalize(wc.wine_name);
    if (!updatesByAppellation.has(appId)) updatesByAppellation.set(appId, new Set());
    updatesByAppellation.get(appId).add(`${prodId}|||${normWineName}`);
  }
  if (skippedNoProducer > 0) {
    console.log(`  ${skippedNoProducer} candidates skipped (producer not found)`);
  }

  // Now batch-update wines
  let wineUpdates = 0;
  let wineErrors = 0;
  let appIdx = 0;
  const totalApps = updatesByAppellation.size;

  for (const [appId, wineKeySet] of updatesByAppellation) {
    appIdx++;
    const wineKeys = [...wineKeySet];
    if (appIdx % 10 === 0 || appIdx === 1) {
      console.log(`  Appellation ${appIdx}/${totalApps}: ${wineKeys.length} wine keys to update...`);
    }

    // Batch update: group by producer_id
    const byProducer = new Map();
    for (const key of wineKeys) {
      const [pid, normName] = key.split("|||");
      if (!byProducer.has(pid)) byProducer.set(pid, []);
      byProducer.get(pid).push(normName);
    }

    for (const [pid, normNames] of byProducer) {
      // Update wines in batches of normalized names
      for (let ni = 0; ni < normNames.length; ni += 50) {
        const batch = normNames.slice(ni, ni + 50);
        const { error: uErr, count } = await sb
          .from("wines")
          .update({ appellation_id: appId })
          .eq("producer_id", pid)
          .in("name_normalized", batch)
          .is("appellation_id", null);
        if (uErr) {
          wineErrors++;
        } else if (count > 0) {
          wineUpdates += count;
        }
      }
    }
  }

  console.log(`  Updated ${wineUpdates} wines with new appellation_ids (${wineErrors} errors)`);

  // ── Summary ───────────────────────────────────────────────
  console.log(`\n${"=".repeat(70)}`);
  console.log("  SUMMARY");
  console.log(`${"=".repeat(70)}`);
  console.log(`  Quick backfills (existing appellation matched): ${quickBackfills.length}`);
  console.log(`  Haiku classifications: ${allClassifications.length}`);
  console.log(`    - Formal appellations: ${newAppellations.length}`);
  console.log(`    - Not appellations: ${notAppellations.length}`);
  console.log(`  New appellations inserted: ${insertedCount}`);
  console.log(`  Region_name_mappings updated: ${mappingUpdates + quickBackfills.length}`);
  console.log(`  Wines updated: ${wineUpdates}`);
  console.log(`${"=".repeat(70)}\n`);
}

main().catch((err) => {
  console.error("FATAL:", err);
  process.exit(1);
});
