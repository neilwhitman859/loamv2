#!/usr/bin/env node
/**
 * create_wines_from_vivino.mjs
 *
 * Creates new Loam wines from unmatched Vivino listings.
 *
 * Phase 0: Second-chance matching — recover false negatives (suffix stripping + Haiku)
 * Phase 1: Create new producers for truly unmatched wineries
 * Phase 2: Classify varietal categories (grape parsing + regional + Haiku + generic)
 * Phase 3: Resolve regions / appellations
 * Phase 4: Create wine records
 * Phase 5: Create vintages, scores, prices, grape links
 *
 * Usage:
 *   node create_wines_from_vivino.mjs --dry-run         # Preview only
 *   node create_wines_from_vivino.mjs --skip-rematch     # Skip Phase 0
 *   node create_wines_from_vivino.mjs --input file.json  # Custom input
 *   node create_wines_from_vivino.mjs                    # Full run
 */

import { readFileSync, writeFileSync } from "fs";
import { createClient } from "@supabase/supabase-js";
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

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── CLI args ────────────────────────────────────────────────
const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const SKIP_REMATCH = args.includes("--skip-rematch");

function getArg(name, fallback) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : fallback;
}
const INPUT_FILE = getArg("input", "vivino_unmatched.json");

const VIVINO_PUBLICATION_ID = "ed228eae-c3bf-41e6-9a90-d78c8efaf97e";
const TODAY = new Date().toISOString().split("T")[0];

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

async function batchInsert(table, rows, batchSize = 500) {
  let inserted = 0;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const { error } = await sb.from(table).insert(batch);
    if (error) {
      for (const row of batch) {
        const { error: e2 } = await sb.from(table).insert([row]);
        if (e2) console.error(`    Row error: ${e2.message} — name=${row.name || row.slug || "n/a"}`);
        else inserted++;
      }
    } else {
      inserted += batch.length;
    }
    process.stdout.write(`  ${Math.min(i + batchSize, rows.length)}/${rows.length}\r`);
  }
  console.log(`  Inserted ${inserted}/${rows.length} rows into ${table}`);
  return inserted;
}

function levenshtein(a, b) {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const dp = Array.from({ length: m + 1 }, () => new Array(n + 1));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i - 1] === b[j - 1]
        ? dp[i - 1][j - 1]
        : 1 + Math.min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]);
    }
  }
  return dp[m][n];
}

// Vivino wine_type_id → Loam wine type string
const VIVINO_TYPE_MAP = {
  1: "Red",
  2: "White",
  3: "Sparkling",
  4: "Rosé",
  7: "Dessert",
  24: "Dessert/Port",
};

// wine_type → generic blend varietal category
const WINE_TYPE_GENERIC_BLEND = {
  Red: "Red Blend",
  White: "White Blend",
  Rosé: "Rosé Blend",
  Sparkling: "Sparkling Blend",
  Dessert: "Dessert Blend",
  "Dessert/Port": "Port",
};

function targetColorForWineType(wineType, grapeColor) {
  switch (wineType) {
    case "Red": return "red";
    case "White": return "white";
    case "Rosé": return "rose";
    case "Sparkling": return grapeColor || "white";
    case "Dessert": return grapeColor || "white";
    case "Dessert/Port": return "red";
    default: return grapeColor || "red";
  }
}

// Country name normalization
const COUNTRY_ALIASES = {
  "united states": "United States",
  usa: "United States",
  us: "United States",
  uk: "United Kingdom",
  "great britain": "United Kingdom",
};

function normalizeCountryName(name) {
  if (!name) return name;
  return COUNTRY_ALIASES[name.toLowerCase()] || name;
}

// Suffixes to strip for producer matching
const PRODUCER_SUFFIXES = [
  "vineyards", "vineyard", "winery", "estate", "wines", "wine",
  "family", "cellars", "cellar", "estates", "bodegas", "bodega",
  "domaine", "champagne", "chateau", "château", "casa", "cantina",
  "tenuta", "fattoria", "azienda", "weingut",
];

function stripProducerSuffixes(name) {
  let n = normalize(name);
  for (const suffix of PRODUCER_SUFFIXES) {
    n = n.replace(new RegExp(`\\b${suffix}\\b`, "g"), " ").replace(/\s+/g, " ").trim();
  }
  return n;
}

// Regional designations → varietal category names
const REGIONAL_DESIGNATION_MAP = {
  champagne: "Champagne Blend",
  port: "Port",
  porto: "Port",
  prosecco: "Prosecco",
  cava: "Cava Blend",
  chianti: "Chianti Blend",
  barolo: "Nebbiolo",
  barbaresco: "Nebbiolo",
  beaujolais: "Beaujolais",
  "cotes du rhone": "Rhône Blend",
  "chateauneuf du pape": "Rhône Blend",
  bordeaux: "Bordeaux Blend",
  rioja: "Rioja Blend",
  sauternes: "Sauternes",
  valpolicella: "Valpolicella Blend",
  amarone: "Valpolicella Blend",
  cremant: "Sparkling Blend",
  asti: "Moscato",
  brunello: "Sangiovese",
  priorat: "Priorat Blend",
};

/** Call Claude Haiku */
async function callHaiku(messages, maxTokens = 2000) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "claude-haiku-4-5-20251001",
      max_tokens: maxTokens,
      messages,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Haiku API ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json();
  return {
    text: data.content[0]?.text || "",
    inputTokens: data.usage?.input_tokens || 0,
    outputTokens: data.usage?.output_tokens || 0,
  };
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log("=== Create Wines from Vivino Unmatched ===\n");

  // 1. Load & dedup input
  let rawListings;
  if (INPUT_FILE.endsWith(".jsonl")) {
    rawListings = readFileSync(INPUT_FILE, "utf-8")
      .split("\n")
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line));
  } else {
    rawListings = JSON.parse(readFileSync(INPUT_FILE, "utf-8"));
  }
  console.log(`Loaded ${rawListings.length} listings from ${INPUT_FILE}`);

  // Dedup by vivino_wine_id — keep best listing per wine (highest rating_count)
  const byWineId = new Map();
  for (const l of rawListings) {
    const key = l.vivino_wine_id;
    if (!byWineId.has(key) || (l.rating_count || 0) > (byWineId.get(key).listing.rating_count || 0)) {
      if (!byWineId.has(key)) {
        byWineId.set(key, { listing: l, vintages: new Set(), prices: [] });
      } else {
        byWineId.get(key).listing = l;
      }
    }
    const entry = byWineId.get(key);
    if (l.vintage_year) entry.vintages.add(l.vintage_year);
    if (l.price_usd != null) {
      entry.prices.push({
        price_usd: l.price_usd,
        price_raw: l.price_raw,
        merchant_name: l.merchant_name,
        source_url: l.source_url,
        vintage_year: l.vintage_year,
      });
    }
  }

  const uniqueWines = [...byWineId.values()];
  console.log(`Deduped to ${uniqueWines.length} unique wines\n`);

  // 2. Load reference data
  console.log("Loading reference data...");

  const countries = await fetchAll("countries", "id,name");
  const countryMap = new Map();
  for (const c of countries) countryMap.set(c.name, c.id);
  console.log(`  ${countries.length} countries`);

  const producers = await fetchAll("producers", "id,name,name_normalized,country_id,slug");
  const producerByNorm = new Map(); // normalized_name → [producer, ...]
  const producerById = new Map();
  for (const p of producers) {
    producerById.set(p.id, p);
    const norm = normalize(p.name);
    if (!producerByNorm.has(norm)) producerByNorm.set(norm, []);
    producerByNorm.get(norm).push(p);
  }
  console.log(`  ${producers.length} producers`);

  const aliases = await fetchAll("producer_aliases", "name,producer_id");
  const aliasNormMap = new Map(); // normalized alias → producer
  for (const a of aliases) {
    const prod = producerById.get(a.producer_id);
    if (prod) {
      aliasNormMap.set(normalize(a.name), prod);
    }
  }
  console.log(`  ${aliases.length} producer aliases`);

  const grapes = await fetchAll("grapes", "id,name,aliases,color");
  const grapeMap = new Map(); // lowercase name → {id, color}
  const grapeNames = []; // sorted longest-first for matching
  for (const g of grapes) {
    grapeMap.set(g.name.toLowerCase(), { id: g.id, color: g.color, name: g.name });
    grapeNames.push(g.name.toLowerCase());
    if (g.aliases) {
      for (const alias of g.aliases) {
        grapeMap.set(alias.toLowerCase(), { id: g.id, color: g.color, name: g.name });
        grapeNames.push(alias.toLowerCase());
      }
    }
  }
  grapeNames.sort((a, b) => b.length - a.length); // longest first
  console.log(`  ${grapes.length} grapes (${grapeMap.size} names)`);

  const vcats = await fetchAll("varietal_categories", "id,name,color,type,grape_id");
  const vcatByName = new Map();
  const vcatByGrapeColor = new Map();
  const vcatByGrape = new Map();
  for (const v of vcats) {
    vcatByName.set(v.name, v.id);
    if (v.grape_id) {
      vcatByGrapeColor.set(`${v.grape_id}|${v.color}`, v.id);
      if (!vcatByGrape.has(v.grape_id)) vcatByGrape.set(v.grape_id, v.id);
    }
  }
  console.log(`  ${vcats.length} varietal categories`);

  const regions = await fetchAll("regions", "id,country_id,is_catch_all");
  const catchAllRegion = new Map();
  for (const r of regions) {
    if (r.is_catch_all) catchAllRegion.set(r.country_id, r.id);
  }

  const rnm = await fetchAll("region_name_mappings", "region_name,country,region_id,appellation_id");
  const regionMapping = new Map();
  for (const r of rnm) {
    regionMapping.set(`${r.region_name}|${r.country}`, {
      region_id: r.region_id,
      appellation_id: r.appellation_id,
    });
  }
  console.log(`  ${rnm.length} region name mappings`);

  // Load existing wines for dedup checking
  const existingWines = await fetchAll("wines", "id,name,name_normalized,producer_id");
  const existingWineMap = new Map(); // "producer_id||name_normalized" → wine
  const winesByProducer = new Map();
  for (const w of existingWines) {
    existingWineMap.set(`${w.producer_id}||${w.name_normalized}`, w);
    if (!winesByProducer.has(w.producer_id)) winesByProducer.set(w.producer_id, []);
    winesByProducer.get(w.producer_id).push(w);
  }
  console.log(`  ${existingWines.length} existing wines`);

  // Load existing wine slugs for collision check
  const existingSlugs = new Set();
  for (const w of existingWines) {
    // We don't have slugs loaded — query separately
  }
  const slugRows = await fetchAll("wines", "slug");
  for (const r of slugRows) existingSlugs.add(r.slug);
  console.log(`  ${existingSlugs.size} existing wine slugs\n`);

  // All producer normalized names for Levenshtein matching
  const allProducerNorms = [...producerByNorm.keys()];

  // Pre-compute suffix-stripped producer names for fast lookup
  const strippedProducerMap = new Map(); // stripped_name → [producer, ...]
  for (const [norm, prods] of producerByNorm) {
    const stripped = stripProducerSuffixes(norm);
    if (stripped !== norm && stripped.length > 2) {
      if (!strippedProducerMap.has(stripped)) strippedProducerMap.set(stripped, []);
      strippedProducerMap.get(stripped).push(...prods);
    }
  }
  console.log(`  Pre-computed ${strippedProducerMap.size} stripped producer names\n`);

  let totalHaikuInput = 0;
  let totalHaikuOutput = 0;

  // ────────────────────────────────────────────────────────────
  // PHASE 0: Second-chance matching
  // ────────────────────────────────────────────────────────────
  const rematched = [];
  const trulyNew = [];

  if (SKIP_REMATCH) {
    console.log("--- Phase 0: SKIPPED (--skip-rematch) ---\n");
    trulyNew.push(...uniqueWines);
  } else {
    console.log("--- Phase 0: Second-chance matching ---");

    let phase0idx = 0;
    for (const entry of uniqueWines) {
      phase0idx++;
      if (phase0idx % 1000 === 0) process.stdout.write(`  Phase 0: ${phase0idx}/${uniqueWines.length}\r`);
      const l = entry.listing;
      if (!l.winery_name) { trulyNew.push(entry); continue; }
      const normWinery = normalize(l.winery_name);
      const normCountry = normalizeCountryName(l.country_name);
      const countryId = countryMap.get(normCountry);

      let matchedProducer = null;

      // 0a. Exact normalized match
      const candidates = producerByNorm.get(normWinery);
      if (candidates) {
        matchedProducer = countryId
          ? candidates.find((p) => p.country_id === countryId) || candidates[0]
          : candidates[0];
      }

      // 0b. Alias match
      if (!matchedProducer) {
        const aliased = aliasNormMap.get(normWinery);
        if (aliased) matchedProducer = aliased;
      }

      // 0c. Suffix-stripped match
      if (!matchedProducer) {
        const stripped = stripProducerSuffixes(l.winery_name);
        if (stripped !== normWinery && stripped.length > 2) {
          const cands = producerByNorm.get(stripped);
          if (cands) {
            matchedProducer = countryId
              ? cands.find((p) => p.country_id === countryId) || cands[0]
              : cands[0];
          }
          // Also try alias
          if (!matchedProducer) {
            const aliased = aliasNormMap.get(stripped);
            if (aliased) matchedProducer = aliased;
          }
        }
      }

      // 0c2. Try stripped lookup (pre-computed, O(1) instead of O(N))
      if (!matchedProducer) {
        const cands = strippedProducerMap.get(normWinery) || strippedProducerMap.get(stripProducerSuffixes(l.winery_name));
        if (cands && cands.length > 0) {
          matchedProducer = countryId
            ? cands.find((p) => p.country_id === countryId) || cands[0]
            : cands[0];
        }
      }

      if (matchedProducer) {
        // Try matching the wine
        const normWine = normalize(l.wine_name);
        const wineKey = `${matchedProducer.id}||${normWine}`;
        let matchedWine = existingWineMap.get(wineKey);

        // Substring match
        if (!matchedWine) {
          const prodWines = winesByProducer.get(matchedProducer.id) || [];
          matchedWine = prodWines.find(
            (w) => w.name_normalized.includes(normWine) || normWine.includes(w.name_normalized)
          );
        }

        if (matchedWine) {
          rematched.push({ entry, producerId: matchedProducer.id, wineId: matchedWine.id, winery: l.winery_name });
        } else {
          // Producer found but wine is new — still a "new wine" but with existing producer
          entry._resolvedProducer = matchedProducer;
          trulyNew.push(entry);
        }
      } else {
        trulyNew.push(entry);
      }
    }

    // 0d. Haiku fuzzy match for remaining wines without a producer
    const needsHaiku = trulyNew.filter((e) => !e._resolvedProducer);
    if (needsHaiku.length > 0) {
      console.log(`  Running Haiku fuzzy match for ${needsHaiku.length} unresolved wineries...`);

      // Build trigram index for fast Levenshtein candidate narrowing
      function trigrams(s) {
        const tris = new Set();
        const padded = `  ${s} `;
        for (let i = 0; i < padded.length - 2; i++) {
          tris.add(padded.slice(i, i + 3));
        }
        return tris;
      }
      const trigramIndex = new Map(); // trigram → [producer_norm_name, ...]
      for (const name of allProducerNorms) {
        for (const tri of trigrams(name)) {
          if (!trigramIndex.has(tri)) trigramIndex.set(tri, []);
          trigramIndex.get(tri).push(name);
        }
      }
      console.log(`  Built trigram index with ${trigramIndex.size} trigrams`);

      function findCandidatesByTrigram(normName, maxResults = 3) {
        const queryTris = trigrams(normName);
        // Count trigram overlap per producer
        const counts = new Map();
        for (const tri of queryTris) {
          const matches = trigramIndex.get(tri) || [];
          for (const name of matches) {
            counts.set(name, (counts.get(name) || 0) + 1);
          }
        }
        // Keep only those with >=2 shared trigrams
        return [...counts.entries()]
          .filter(([_, count]) => count >= 2)
          .map(([name, count]) => ({ name, dist: levenshtein(normName, name) }))
          .filter((s) => s.dist <= Math.max(normName.length * 0.4, 5))
          .sort((a, b) => a.dist - b.dist)
          .slice(0, maxResults);
      }

      const BATCH_SIZE = 10;
      for (let i = 0; i < needsHaiku.length; i += BATCH_SIZE) {
        const batch = needsHaiku.slice(i, i + BATCH_SIZE);

        const prompt = batch
          .map((entry, idx) => {
            const l = entry.listing;
            if (!l.winery_name) return null;
            const normWinery = normalize(l.winery_name);

            // Find Levenshtein candidates using trigram index
            const scored = findCandidatesByTrigram(normWinery, 3);

            if (scored.length === 0) return null;

            entry._haikuCandidates = scored.map((s) => {
              const prods = producerByNorm.get(s.name);
              return prods[0];
            });

            return (
              `[${idx}] Vivino: "${l.winery_name}" (${l.country_name})\n` +
              `    Candidates:\n` +
              entry._haikuCandidates
                .map((p, j) => `      ${String.fromCharCode(65 + j)}) "${p.name}"`)
                .join("\n")
            );
          })
          .filter(Boolean)
          .join("\n\n");

        if (!prompt) continue;

        try {
          const { text, inputTokens, outputTokens } = await callHaiku([
            {
              role: "user",
              content:
                `You are a wine producer matcher. For each Vivino winery, determine if any candidate is the same producer. Account for:\n` +
                `- Different transliterations (Château vs Chateau)\n` +
                `- Abbreviated names (Dr. vs Doktor, Dom. vs Domaine)\n` +
                `- Suffix variations (adding/dropping Estate, Vineyards, Winery, Bodegas)\n\n` +
                `Reply with JSON array. For each index, return the candidate letter (A/B/C) or "none".\n` +
                `Example: [{"index":0,"match":"A"},{"index":1,"match":"none"}]\n\n` +
                prompt,
            },
            { role: "assistant", content: "[" },
          ]);

          totalHaikuInput += inputTokens;
          totalHaikuOutput += outputTokens;

          const cleaned = ("[" + text).replace(/```json\s*/g, "").replace(/```/g, "").trim();
          const results = JSON.parse(cleaned);

          for (const r of results) {
            if (r.match === "none" || !r.match) continue;
            const idx = r.index;
            if (idx == null || idx >= batch.length) continue;

            const entry = batch[idx];
            if (!entry._haikuCandidates) continue;

            const candIdx = r.match.charCodeAt(0) - 65;
            if (candIdx < 0 || candIdx >= entry._haikuCandidates.length) continue;

            const matchedProducer = entry._haikuCandidates[candIdx];
            entry._resolvedProducer = matchedProducer;

            // Check if wine also matches
            const normWine = normalize(entry.listing.wine_name);
            const wineKey = `${matchedProducer.id}||${normWine}`;
            let matchedWine = existingWineMap.get(wineKey);

            if (!matchedWine) {
              const prodWines = winesByProducer.get(matchedProducer.id) || [];
              matchedWine = prodWines.find(
                (w) => w.name_normalized.includes(normWine) || normWine.includes(w.name_normalized)
              );
            }

            if (matchedWine) {
              // Move from trulyNew to rematched
              const tidx = trulyNew.indexOf(entry);
              if (tidx !== -1) trulyNew.splice(tidx, 1);
              rematched.push({ entry, producerId: matchedProducer.id, wineId: matchedWine.id, winery: entry.listing.winery_name });
            }
          }
        } catch (err) {
          console.error(`  Haiku batch error: ${err.message}`);
        }

        if (i + BATCH_SIZE < needsHaiku.length) {
          await new Promise((r) => setTimeout(r, 300));
        }
      }
    }

    console.log(`  Rematched: ${rematched.length} wines (false negatives recovered)`);
    console.log(`  Truly new: ${trulyNew.length} wines`);
    if (rematched.length > 0) {
      console.log(`\n  Recovered matches:`);
      for (const m of rematched) {
        console.log(`    ${m.winery} — ${m.entry.listing.wine_name}`);
      }
    }
    console.log();
  }

  // ── Insert data for rematched wines ───────────────────────
  if (rematched.length > 0 && !DRY_RUN) {
    console.log(`--- Inserting data for ${rematched.length} rematched wines ---`);

    // Vintages
    const vintageRows = [];
    const vintageKeys = new Set();
    for (const m of rematched) {
      for (const year of m.entry.vintages) {
        const key = `${m.wineId}||${year}`;
        if (!vintageKeys.has(key)) {
          vintageKeys.add(key);
          vintageRows.push({
            wine_id: m.wineId,
            vintage_year: year,
          });
        }
      }
    }
    if (vintageRows.length > 0) {
      const { error } = await sb
        .from("wine_vintages")
        .upsert(vintageRows, { onConflict: "wine_id,vintage_year", ignoreDuplicates: false });
      if (error) console.error(`  Vintage upsert error: ${error.message}`);
      else console.log(`  Upserted ${vintageRows.length} vintages`);
    }

    // Scores
    const scoreRows = [];
    const scoreKeys = new Set();
    for (const m of rematched) {
      const l = m.entry.listing;
      if (!l.rating_average || !l.rating_count) continue;
      const key = `${m.wineId}||${l.vintage_year || "nv"}`;
      if (scoreKeys.has(key)) continue;
      scoreKeys.add(key);
      scoreRows.push({
        wine_id: m.wineId,
        vintage_year: l.vintage_year || null,
        score: l.rating_average,
        score_scale: "5",
        publication_id: VIVINO_PUBLICATION_ID,
        critic: "Vivino Community",
        is_community: true,
        rating_count: l.rating_count,
        review_date: TODAY,
        url: `https://www.vivino.com/w/${l.vivino_wine_id}`,
      });
    }
    if (scoreRows.length > 0) {
      let inserted = 0;
      for (const row of scoreRows) {
        const { error } = await sb.from("wine_vintage_scores").insert([row]);
        if (!error) inserted++;
      }
      console.log(`  Inserted ${inserted}/${scoreRows.length} scores`);
    }

    // Prices
    const priceRows = [];
    const priceKeys = new Set();
    for (const m of rematched) {
      for (const p of m.entry.prices) {
        const key = `${m.wineId}||${p.vintage_year}||${p.price_usd}||${p.merchant_name}`;
        if (priceKeys.has(key)) continue;
        priceKeys.add(key);
        priceRows.push({
          wine_id: m.wineId,
          vintage_year: p.vintage_year || null,
          price_usd: p.price_usd,
          price_original: p.price_raw,
          currency: "USD",
          price_type: "retail",
          source_url: p.source_url,
          merchant_name: p.merchant_name || "Vivino Marketplace",
          price_date: TODAY,
        });
      }
    }
    if (priceRows.length > 0) {
      let inserted = 0;
      for (const row of priceRows) {
        const { error } = await sb.from("wine_vintage_prices").insert([row]);
        if (!error) inserted++;
      }
      console.log(`  Inserted ${inserted}/${priceRows.length} prices`);
    }

    // Create producer aliases for suffix-match recoveries
    const aliasRows = [];
    for (const m of rematched) {
      const normWinery = normalize(m.winery);
      const prod = producerById.get(m.producerId);
      if (prod && normalize(prod.name) !== normWinery) {
        aliasRows.push({
          producer_id: m.producerId,
          name: m.winery,
        });
      }
    }
    if (aliasRows.length > 0) {
      let inserted = 0;
      for (const row of aliasRows) {
        const { error } = await sb.from("producer_aliases").insert([row]);
        if (!error) inserted++;
      }
      console.log(`  Created ${inserted} new producer aliases`);
    }
    console.log();
  }

  if (trulyNew.length === 0) {
    console.log("No truly new wines to create. Done!");
    return;
  }

  // ────────────────────────────────────────────────────────────
  // PHASE 1: Create new producers
  // ────────────────────────────────────────────────────────────
  console.log(`--- Phase 1: Resolve/create producers for ${trulyNew.length} wines ---`);

  const newProducers = [];
  const producerSlugsUsed = new Set();
  for (const p of producers) producerSlugsUsed.add(p.slug || slugify(p.name));

  // Track producers created in this run to reuse for multiple wines from same winery
  const createdProducerMap = new Map(); // "norm_name|country_id" → producer obj

  for (const entry of trulyNew) {
    if (entry._resolvedProducer) continue; // already resolved in Phase 0

    const l = entry.listing;
    if (!l.winery_name || !l.wine_name) continue; // skip incomplete listings

    const normCountry = normalizeCountryName(l.country_name);
    const countryId = countryMap.get(normCountry);
    if (!countryId) {
      console.error(`  Unknown country: ${l.country_name}`);
      continue;
    }

    // Check if we already created a producer for this winery in this run
    const dedupKey = `${normalize(l.winery_name)}|${countryId}`;
    if (createdProducerMap.has(dedupKey)) {
      entry._resolvedProducer = createdProducerMap.get(dedupKey);
      continue;
    }

    // Create new producer
    const prodId = randomUUID();
    let prodSlug = slugify(l.winery_name);
    if (producerSlugsUsed.has(prodSlug)) {
      const countrySuffix = slugify(normCountry);
      prodSlug = `${prodSlug}-${countrySuffix}`;
    }
    if (producerSlugsUsed.has(prodSlug)) {
      prodSlug = `${prodSlug}-${prodId.slice(0, 6)}`;
    }
    producerSlugsUsed.add(prodSlug);

    const newProd = {
      id: prodId,
      slug: prodSlug,
      name: l.winery_name,
      name_normalized: normalize(l.winery_name),
      country_id: countryId,
    };

    newProducers.push(newProd);
    entry._resolvedProducer = { id: prodId, slug: prodSlug, name: l.winery_name, country_id: countryId };
    createdProducerMap.set(dedupKey, entry._resolvedProducer);
  }

  console.log(`  New producers to create: ${newProducers.length}`);
  if (newProducers.length > 0) {
    for (const p of newProducers) {
      console.log(`    ${p.name} (${p.slug})`);
    }
  }

  if (!DRY_RUN && newProducers.length > 0) {
    await batchInsert("producers", newProducers);
  }

  // ────────────────────────────────────────────────────────────
  // PHASE 2: Classify varietal categories
  // ────────────────────────────────────────────────────────────
  console.log(`\n--- Phase 2: Classify varietal categories ---`);

  const needsHaikuVcat = [];

  for (const entry of trulyNew) {
    const l = entry.listing;
    const wineType = VIVINO_TYPE_MAP[l.wine_type_id] || "Red";
    const normWineName = normalize(l.wine_name).toLowerCase();

    let vcatId = null;
    let grapeId = null;
    let method = null;

    // 2a. Parse grape from wine name (longest-first matching)
    for (const grapeName of grapeNames) {
      if (normWineName.includes(grapeName)) {
        const grape = grapeMap.get(grapeName);
        if (grape) {
          const targetColor = targetColorForWineType(wineType, grape.color);
          vcatId = vcatByGrapeColor.get(`${grape.id}|${targetColor}`)
            || vcatByGrape.get(grape.id);
          if (vcatId) {
            grapeId = grape.id;
            method = `grape:${grape.name}`;
            break;
          }
        }
      }
    }

    // 2b. Regional designation matching (word-boundary to avoid "Castilla" matching "asti")
    if (!vcatId) {
      const normRegion = normalize(l.region_name || "").toLowerCase();
      for (const [regionKey, vcName] of Object.entries(REGIONAL_DESIGNATION_MAP)) {
        const re = new RegExp(`\\b${regionKey}\\b`);
        if (re.test(normWineName) || re.test(normRegion)) {
          vcatId = vcatByName.get(vcName);
          if (vcatId) {
            method = `regional:${regionKey}→${vcName}`;
            break;
          }
        }
      }
    }

    // 2c. Still unresolved → queue for Haiku
    if (!vcatId) {
      needsHaikuVcat.push(entry);
    }

    entry._vcatId = vcatId;
    entry._grapeId = grapeId;
    entry._wineType = wineType;
    entry._vcatMethod = method;
  }

  const resolvedCount = trulyNew.filter((e) => e._vcatId).length;
  console.log(`  Resolved by grape parsing: ${trulyNew.filter((e) => e._vcatMethod?.startsWith("grape:")).length}`);
  console.log(`  Resolved by regional: ${trulyNew.filter((e) => e._vcatMethod?.startsWith("regional:")).length}`);
  console.log(`  Needs Haiku: ${needsHaikuVcat.length}`);

  // 2c. Haiku classification
  if (needsHaikuVcat.length > 0) {
    const vcatNames = vcats.map((v) => v.name).join(", ");

    const prompt = needsHaikuVcat
      .map((entry, idx) => {
        const l = entry.listing;
        return `[${idx}] "${l.wine_name}" — Type: ${entry._wineType}, Region: ${l.region_name || "?"}(${l.country_name})`;
      })
      .join("\n");

    try {
      const { text, inputTokens, outputTokens } = await callHaiku([
        {
          role: "user",
          content:
            `You are a wine classification expert. For each wine, determine the most likely varietal category.\n\n` +
            `Available categories: ${vcatNames}\n\n` +
            `For each wine, return JSON array with: {"index": N, "category": "exact name from list"}\n\n` +
            prompt,
        },
        { role: "assistant", content: "[" },
      ]);

      totalHaikuInput += inputTokens;
      totalHaikuOutput += outputTokens;

      const cleaned = ("[" + text).replace(/```json\s*/g, "").replace(/```/g, "").trim();
      const results = JSON.parse(cleaned);

      for (const r of results) {
        if (r.index == null || r.index >= needsHaikuVcat.length) continue;
        const entry = needsHaikuVcat[r.index];
        const vcId = vcatByName.get(r.category);
        if (vcId) {
          entry._vcatId = vcId;
          entry._vcatMethod = `haiku:${r.category}`;
        }
      }

      console.log(`  Haiku resolved: ${needsHaikuVcat.filter((e) => e._vcatId).length}/${needsHaikuVcat.length}`);
    } catch (err) {
      console.error(`  Haiku classification error: ${err.message}`);
    }
  }

  // 2d. Generic fallback for any remaining
  for (const entry of trulyNew) {
    if (!entry._vcatId) {
      const genericName = WINE_TYPE_GENERIC_BLEND[entry._wineType] || "Red Blend";
      entry._vcatId = vcatByName.get(genericName) || vcatByName.get("Red Blend");
      entry._vcatMethod = `fallback:${genericName}`;
    }
  }

  console.log(`  Fallback (generic blend): ${trulyNew.filter((e) => e._vcatMethod?.startsWith("fallback:")).length}`);

  // ────────────────────────────────────────────────────────────
  // PHASE 3: Resolve regions
  // ────────────────────────────────────────────────────────────
  console.log(`\n--- Phase 3: Resolve regions ---`);

  let regionHits = 0;
  let catchAllHits = 0;

  for (const entry of trulyNew) {
    const l = entry.listing;
    const normCountry = normalizeCountryName(l.country_name);
    const countryId = countryMap.get(normCountry);

    let regionId = null;
    let appellationId = null;

    if (l.region_name) {
      const rm = regionMapping.get(`${l.region_name}|${normCountry}`);
      if (rm) {
        regionId = rm.region_id;
        appellationId = rm.appellation_id;
        regionHits++;
      }
    }
    if (!regionId && countryId) {
      regionId = catchAllRegion.get(countryId) || null;
      if (regionId) catchAllHits++;
    }

    entry._regionId = regionId;
    entry._appellationId = appellationId;
    entry._countryId = countryId;
  }

  console.log(`  Region mapped: ${regionHits}`);
  console.log(`  Catch-all fallback: ${catchAllHits}`);

  // ────────────────────────────────────────────────────────────
  // PHASE 4: Create wine records
  // ────────────────────────────────────────────────────────────
  console.log(`\n--- Phase 4: Create wine records ---`);

  const newWines = [];
  const skippedDupes = [];

  for (const entry of trulyNew) {
    const l = entry.listing;
    const prod = entry._resolvedProducer;
    if (!prod || !entry._countryId || !entry._vcatId) {
      console.log(`  SKIP (missing data): ${l.winery_name} — ${l.wine_name}`);
      continue;
    }

    // Dedup safety check
    const normWine = normalize(l.wine_name);
    const dedupKey = `${prod.id}||${normWine}`;
    if (existingWineMap.has(dedupKey)) {
      skippedDupes.push(l);
      continue;
    }

    // Generate slug with collision check
    let slug = `${prod.slug || slugify(prod.name)}-${slugify(l.wine_name)}`.slice(0, 120);
    if (!slug) slug = prod.slug || slugify(prod.name);
    if (existingSlugs.has(slug)) {
      slug = `${slug}-vivino`;
    }
    if (existingSlugs.has(slug)) {
      slug = `${slug}-${randomUUID().slice(0, 6)}`;
    }
    existingSlugs.add(slug);

    const wineId = randomUUID();
    const wineType = entry._wineType;

    newWines.push({
      id: wineId,
      slug,
      name: l.wine_name,
      name_normalized: normWine,
      producer_id: prod.id,
      country_id: entry._countryId,
      region_id: entry._regionId,
      appellation_id: entry._appellationId,
      varietal_category_id: entry._vcatId,
      effervescence: wineType === "Sparkling" ? "sparkling" : null,
      is_nv: !l.vintage_year && entry.vintages.size === 0,
    });

    // Store wineId for Phase 5
    entry._wineId = wineId;
    // Mark as existing for future dedup checks within this run
    existingWineMap.set(dedupKey, { id: wineId });
  }

  console.log(`  New wines to create: ${newWines.length}`);
  console.log(`  Skipped (dedup): ${skippedDupes.length}`);

  if (DRY_RUN) {
    console.log(`\n[DRY RUN] Would create:`);
    console.log(`  ${newProducers.length} producers`);
    console.log(`  ${newWines.length} wines`);
    console.log(`\n  Wines detail:`);
    for (const entry of trulyNew) {
      if (!entry._wineId) continue;
      const l = entry.listing;
      console.log(`    [${entry._wineType}] ${l.winery_name} — ${l.wine_name} | vcat: ${entry._vcatMethod}`);
    }
    console.log(`\n  Rematched: ${rematched.length}`);
    for (const m of rematched) {
      console.log(`    ${m.winery} — ${m.entry.listing.wine_name}`);
    }

    const haikuCost = (totalHaikuInput * 0.8 + totalHaikuOutput * 4) / 1_000_000;
    console.log(`\n  Haiku cost: $${haikuCost.toFixed(4)} (${totalHaikuInput} in / ${totalHaikuOutput} out)`);
    return;
  }

  if (newWines.length > 0) {
    await batchInsert("wines", newWines, 500);
  }

  // ────────────────────────────────────────────────────────────
  // PHASE 5: Vintages, scores, prices, grape links
  // ────────────────────────────────────────────────────────────
  console.log(`\n--- Phase 5: Vintages, scores, prices, grape links ---`);

  const vintageRows = [];
  const scoreRows = [];
  const priceRows = [];
  const grapeRows = [];

  const vintageKeys = new Set();
  const scoreKeys = new Set();
  const priceKeySet = new Set();

  for (const entry of trulyNew) {
    if (!entry._wineId) continue;
    const l = entry.listing;

    // Vintages
    for (const year of entry.vintages) {
      const key = `${entry._wineId}||${year}`;
      if (!vintageKeys.has(key)) {
        vintageKeys.add(key);
        vintageRows.push({
          wine_id: entry._wineId,
          vintage_year: year,
        });
      }
    }

    // Score (one per wine, using the main listing)
    if (l.rating_average && l.rating_count > 0) {
      const key = `${entry._wineId}||${l.vintage_year || "nv"}`;
      if (!scoreKeys.has(key)) {
        scoreKeys.add(key);
        scoreRows.push({
          wine_id: entry._wineId,
          vintage_year: l.vintage_year || null,
          score: l.rating_average,
          score_scale: "5",
          publication_id: VIVINO_PUBLICATION_ID,
          critic: "Vivino Community",
          is_community: true,
          rating_count: l.rating_count,
          review_date: TODAY,
          url: `https://www.vivino.com/w/${l.vivino_wine_id}`,
        });
      }
    }

    // Prices (deduped)
    for (const p of entry.prices) {
      const key = `${entry._wineId}||${p.vintage_year}||${p.price_usd}||${p.merchant_name}`;
      if (!priceKeySet.has(key)) {
        priceKeySet.add(key);
        priceRows.push({
          wine_id: entry._wineId,
          vintage_year: p.vintage_year || null,
          price_usd: p.price_usd,
          price_original: p.price_raw,
          currency: "USD",
          price_type: "retail",
          source_url: p.source_url,
          merchant_name: p.merchant_name || "Vivino Marketplace",
          price_date: TODAY,
        });
      }
    }

    // Grape link
    if (entry._grapeId) {
      grapeRows.push({
        wine_id: entry._wineId,
        grape_id: entry._grapeId,
      });
    }
  }

  if (vintageRows.length > 0) {
    const { error } = await sb
      .from("wine_vintages")
      .upsert(vintageRows, { onConflict: "wine_id,vintage_year", ignoreDuplicates: false });
    if (error) console.error(`  Vintage upsert error: ${error.message}`);
    else console.log(`  Upserted ${vintageRows.length} vintages`);
  }

  if (scoreRows.length > 0) {
    let inserted = 0;
    for (const row of scoreRows) {
      const { error } = await sb.from("wine_vintage_scores").insert([row]);
      if (!error) inserted++;
    }
    console.log(`  Inserted ${inserted}/${scoreRows.length} scores`);
  }

  if (priceRows.length > 0) {
    let inserted = 0;
    for (const row of priceRows) {
      const { error } = await sb.from("wine_vintage_prices").insert([row]);
      if (!error) inserted++;
    }
    console.log(`  Inserted ${inserted}/${priceRows.length} prices`);
  }

  if (grapeRows.length > 0) {
    await batchInsert("wine_grapes", grapeRows);
  }

  // ── Final Summary ─────────────────────────────────────────
  const haikuCost = (totalHaikuInput * 0.8 + totalHaikuOutput * 4) / 1_000_000;

  console.log("\n=== SUMMARY ===");
  console.log(`  Rematched (false negatives recovered): ${rematched.length}`);
  console.log(`  New producers created: ${newProducers.length}`);
  console.log(`  New wines created: ${newWines.length}`);
  console.log(`  Vintages upserted: ${vintageRows.length}`);
  console.log(`  Scores inserted: ${scoreRows.length}`);
  console.log(`  Prices inserted: ${priceRows.length}`);
  console.log(`  Grape links: ${grapeRows.length}`);
  console.log(`  Skipped (dedup): ${skippedDupes.length}`);
  console.log(`  Haiku cost: $${haikuCost.toFixed(4)}`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
