#!/usr/bin/env node
/**
 * fetch_producer_wines.mjs
 *
 * Vivino Producer Depth Crawl — fetches complete wine catalogs for all producers.
 *
 * Phase 1:  Resolve Vivino winery IDs from producer names (web page scrape + slug matching)
 * Phase 2:  Fetch all wines per winery via /api/wineries/{id}/wines
 * Phase 2b: Fetch prices & per-vintage scores via /api/explore/explore (batched by winery)
 * Phase 3:  Match & create new wines in the Loam DB (dedup, varietal classification, region mapping)
 *           Also inserts prices, per-vintage scores, and vintage records from explore data
 *
 * Usage:
 *   node fetch_producer_wines.mjs --phase 1              # Resolve winery IDs only
 *   node fetch_producer_wines.mjs --phase 2              # Fetch wine catalogs only
 *   node fetch_producer_wines.mjs --phase 2b             # Fetch prices & per-vintage scores
 *   node fetch_producer_wines.mjs --phase 3              # Match & create only
 *   node fetch_producer_wines.mjs --phase 3 --dry-run    # Preview Phase 3
 *   node fetch_producer_wines.mjs --resume                # Resume interrupted run
 *   node fetch_producer_wines.mjs --limit 100             # Only process first N producers
 *   node fetch_producer_wines.mjs --delay-ms 2000         # Custom delay between API calls
 *   node fetch_producer_wines.mjs --stats                 # Show progress stats
 */

import { readFileSync, writeFileSync, appendFileSync, existsSync, createReadStream } from "fs";
import { createInterface } from "readline";
import { createClient } from "@supabase/supabase-js";
import { randomUUID } from "crypto";

// ── Load .env ────────────────────────────────────────────────
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

// ── CLI args ─────────────────────────────────────────────────
const args = process.argv.slice(2);
function getArg(name, fallback) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : fallback;
}
const PHASE_RAW = getArg("phase", "0");
const PHASE = PHASE_RAW === "2b" ? "2b" : (parseInt(PHASE_RAW, 10) || 0); // 0 = all phases
const DRY_RUN = args.includes("--dry-run");
const RESUME = args.includes("--resume");
const LIMIT = parseInt(getArg("limit", "0"), 10) || Infinity;
const BASE_DELAY = parseInt(getArg("delay-ms", "1500"), 10);
const SHOW_STATS = args.includes("--stats");

// ── Constants ────────────────────────────────────────────────
const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36";

const WINERY_MAP_FILE = "producer_winery_map.jsonl";
const WINES_DATA_FILE = "producer_wines_data.jsonl";
const PRICES_DATA_FILE = "producer_wines_prices.jsonl";
const VIVINO_PUBLICATION_ID = "ed228eae-c3bf-41e6-9a90-d78c8efaf97e";
const TODAY = new Date().toISOString().split("T")[0];

// Vivino wine_type_id → Loam wine type string
const VIVINO_TYPE_MAP = {
  1: "Red", 2: "White", 3: "Sparkling", 4: "Rosé", 7: "Dessert", 24: "Dessert/Port",
};

// wine_type → generic blend varietal category
const WINE_TYPE_GENERIC_BLEND = {
  Red: "Red Blend", White: "White Blend", "Rosé": "Rosé Blend",
  Sparkling: "Sparkling Blend", Dessert: "Dessert Blend", "Dessert/Port": "Port",
};

// Regional designations → varietal category names
const REGIONAL_DESIGNATION_MAP = {
  champagne: "Champagne Blend", port: "Port", porto: "Port", prosecco: "Prosecco",
  cava: "Cava Blend", chianti: "Chianti Blend", barolo: "Nebbiolo",
  barbaresco: "Nebbiolo", beaujolais: "Beaujolais", "cotes du rhone": "Rhône Blend",
  "chateauneuf du pape": "Rhône Blend", bordeaux: "Bordeaux Blend",
  rioja: "Rioja Blend", sauternes: "Sauternes", valpolicella: "Valpolicella Blend",
  amarone: "Valpolicella Blend", cremant: "Sparkling Blend", asti: "Moscato",
  brunello: "Sangiovese", priorat: "Priorat Blend",
};

// Producer name suffixes to try (most likely first, keep short for speed)
const SLUG_SUFFIXES = ["", "-winery", "-wines", "-vineyards"];

const PRODUCER_STRIP_WORDS = [
  "winery", "wines", "wine", "vineyards", "vineyard", "estate", "estates",
  "cellars", "cellar", "family", "bodegas", "bodega", "domaine", "dom",
  "chateau", "château", "casa", "cantina", "tenuta", "fattoria",
  "azienda", "weingut", "cave", "caves", "maison", "champagne",
];

// ── Helpers ──────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function normalize(s) {
  return s.normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .toLowerCase().replace(/[^a-z0-9 ]/g, " ").replace(/\s+/g, " ").trim();
}

function slugify(s) {
  return s.normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function stripProducerSuffixes(name) {
  let n = normalize(name);
  for (const suffix of PRODUCER_STRIP_WORDS) {
    n = n.replace(new RegExp(`\\b${suffix}\\b`, "g"), " ").replace(/\s+/g, " ").trim();
  }
  return n;
}

async function fetchAll(table, columns = "*", batchSize = 1000) {
  const rows = [];
  let offset = 0;
  while (true) {
    const { data, error } = await sb
      .from(table).select(columns).range(offset, offset + batchSize - 1);
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
        if (e2) console.error(`    Row error: ${e2.message}`);
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

/** Load JSONL file into a Map keyed by a field */
function loadJsonlMap(file, keyField) {
  const map = new Map();
  if (!existsSync(file)) return map;
  const lines = readFileSync(file, "utf-8").split("\n");
  for (const line of lines) {
    if (!line.trim()) continue;
    try {
      const obj = JSON.parse(line);
      if (obj[keyField]) map.set(obj[keyField], obj);
    } catch {}
  }
  return map;
}

/** Load JSONL file into array */
function loadJsonlArray(file) {
  if (!existsSync(file)) return [];
  const lines = readFileSync(file, "utf-8").split("\n");
  const arr = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    try { arr.push(JSON.parse(line)); } catch {}
  }
  return arr;
}

// ── PHASE 1: Resolve Winery IDs ─────────────────────────────
async function phase1() {
  console.log("=== PHASE 1: Resolve Vivino Winery IDs ===\n");

  // Load producers from DB
  console.log("Loading producers...");
  const producers = await fetchAll("producers", "id,name,name_normalized,country_id,slug");
  console.log(`  ${producers.length} producers loaded`);

  // Load countries for ISO code mapping
  const countries = await fetchAll("countries", "id,name,iso_code");
  const countryById = new Map();
  for (const c of countries) countryById.set(c.id, c);

  // Load existing mappings for resume
  const existingMap = loadJsonlMap(WINERY_MAP_FILE, "producer_id");
  console.log(`  ${existingMap.size} existing mappings loaded (resume)\n`);

  // Sort producers: fewest wines first (most to gain from depth crawl)
  // Actually, just process in order — resume handles already-done ones
  let toProcess = producers;
  if (LIMIT < Infinity) toProcess = toProcess.slice(0, LIMIT);

  let resolved = 0, notFound = 0, skipped = 0, errors = 0;
  let currentDelay = BASE_DELAY;
  const startTime = Date.now();

  for (let i = 0; i < toProcess.length; i++) {
    const producer = toProcess[i];

    // Skip if already resolved
    if (existingMap.has(producer.id)) {
      skipped++;
      continue;
    }

    const country = countryById.get(producer.country_id);
    const countryCode = country?.iso_code?.toLowerCase() || "";

    // Generate slug variations to try
    const baseSlug = slugify(producer.name);
    const strippedSlug = slugify(stripProducerSuffixes(producer.name));
    const slugsToTry = new Set();

    // Add variations with suffixes
    for (const suffix of SLUG_SUFFIXES) {
      slugsToTry.add(baseSlug + suffix);
      if (strippedSlug !== baseSlug && strippedSlug.length > 2) {
        slugsToTry.add(strippedSlug + suffix);
      }
    }

    let wineryId = null;
    let wineryName = null;
    let winerySeoName = null;
    let matchConfidence = "none";
    let winesCount = 0;

    // Try each slug variation
    for (const slug of slugsToTry) {
      if (!slug || slug.length < 2) continue;
      try {
        const url = `https://www.vivino.com/wineries/${slug}`;
        const res = await fetch(url, {
          headers: { "User-Agent": USER_AGENT },
          redirect: "follow",
        });

        if (res.status === 200) {
          const html = await res.text();

          // Extract winery ID from page
          const idMatch = html.match(/"winery":\{"id":(\d+)/) ||
                          html.match(/data-winery-id="(\d+)"/) ||
                          html.match(/"id":(\d+),"name":"[^"]+","seo_name":"[^"]+"/);

          if (idMatch) {
            wineryId = parseInt(idMatch[1], 10);

            // Extract winery name
            const nameMatch = html.match(/"winery":\{"id":\d+,"name":"([^"]+)"/);
            wineryName = nameMatch ? nameMatch[1] : slug;

            // Extract seo_name
            const seoMatch = html.match(/"seo_name":"([^"]+)"/);
            winerySeoName = seoMatch ? seoMatch[1] : slug;

            // Extract wines count
            const countMatch = html.match(/"wines_count":(\d+)/);
            winesCount = countMatch ? parseInt(countMatch[1], 10) : 0;

            // Determine confidence
            const normVivino = normalize(wineryName);
            const normProducer = normalize(producer.name);
            if (normVivino === normProducer) {
              matchConfidence = "exact";
            } else if (normVivino.includes(normProducer) || normProducer.includes(normVivino)) {
              matchConfidence = "substring";
            } else if (stripProducerSuffixes(wineryName) === stripProducerSuffixes(producer.name)) {
              matchConfidence = "suffix_stripped";
            } else {
              matchConfidence = "slug_match";
            }

            break; // Found it
          }
        }

        await sleep(150); // Short delay between slug variations
      } catch (err) {
        if (err.message?.includes("429")) {
          currentDelay = Math.min(currentDelay * 2, 30000);
          console.log(`\n  Rate limited. Backing off to ${currentDelay}ms`);
          await sleep(currentDelay);
        }
      }
    }

    // Write result
    const record = {
      producer_id: producer.id,
      producer_name: producer.name,
      country_code: countryCode,
      vivino_winery_id: wineryId,
      vivino_winery_name: wineryName,
      vivino_seo_name: winerySeoName,
      match_confidence: matchConfidence,
      wines_count: winesCount,
    };
    appendFileSync(WINERY_MAP_FILE, JSON.stringify(record) + "\n");

    if (wineryId) {
      resolved++;
    } else {
      notFound++;
    }

    // Progress
    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    const processed = resolved + notFound;
    const total = toProcess.length - skipped;
    process.stdout.write(
      `  ${processed}/${total} (${resolved} resolved, ${notFound} not found) [${elapsed}m]\r`
    );

    // Rate limit between producers (shorter for web pages than API)
    await sleep(Math.max(500, currentDelay));
  }

  console.log(`\n\n=== PHASE 1 COMPLETE ===`);
  console.log(`  Resolved: ${resolved}`);
  console.log(`  Not found: ${notFound}`);
  console.log(`  Skipped (already resolved): ${skipped}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Output: ${WINERY_MAP_FILE}\n`);

  // Quick stats on confidence distribution
  const allMappings = loadJsonlMap(WINERY_MAP_FILE, "producer_id");
  const confDist = {};
  for (const [, m] of allMappings) {
    confDist[m.match_confidence] = (confDist[m.match_confidence] || 0) + 1;
  }
  console.log("  Confidence distribution:", confDist);
}

// ── PHASE 2: Fetch Wine Catalogs ─────────────────────────────
async function phase2() {
  console.log("=== PHASE 2: Fetch Wine Catalogs ===\n");

  // Load winery mappings
  const wineryMap = loadJsonlMap(WINERY_MAP_FILE, "producer_id");
  console.log(`  ${wineryMap.size} winery mappings loaded`);

  // Filter to resolved wineries only
  const resolvedEntries = [...wineryMap.values()].filter(m => m.vivino_winery_id);
  console.log(`  ${resolvedEntries.length} with Vivino winery IDs`);

  // Load existing wine data for resume — track which winery IDs are done
  const existingWines = loadJsonlArray(WINES_DATA_FILE);
  const doneWineryIds = new Set();
  for (const w of existingWines) {
    if (w.vivino_winery_id) doneWineryIds.add(w.vivino_winery_id);
  }
  console.log(`  ${doneWineryIds.size} wineries already fetched (resume)\n`);

  let toProcess = resolvedEntries.filter(m => !doneWineryIds.has(m.vivino_winery_id));
  if (LIMIT < Infinity) toProcess = toProcess.slice(0, LIMIT);

  let fetched = 0, totalWines = 0, errors = 0;
  let currentDelay = BASE_DELAY;
  const startTime = Date.now();

  for (let i = 0; i < toProcess.length; i++) {
    const mapping = toProcess[i];
    const wineryId = mapping.vivino_winery_id;

    try {
      const url = `https://www.vivino.com/api/wineries/${wineryId}/wines?start_from=0&limit=500`;
      const res = await fetch(url, {
        headers: { "User-Agent": USER_AGENT, Accept: "application/json" },
      });

      if (res.status === 429) {
        currentDelay = Math.min(currentDelay * 2, 30000);
        console.log(`\n  Rate limited. Backing off to ${currentDelay}ms`);
        await sleep(currentDelay);
        i--; // Retry
        continue;
      }

      if (!res.ok) {
        errors++;
        continue;
      }

      const data = await res.json();
      const wines = data.wines || [];
      const vintages = data.vintages || [];

      // Build vintage lookup: vivino_wine_id → best vintage info
      const vintageByWine = new Map();
      for (const v of vintages) {
        const wid = v.wine?.id;
        if (!wid) continue;
        const existing = vintageByWine.get(wid);
        // Keep the one with the most ratings
        if (!existing || (v.statistics?.ratings_count || 0) > (existing.statistics?.ratings_count || 0)) {
          vintageByWine.set(wid, v);
        }
      }

      // Process each wine
      for (const wine of wines) {
        const vintage = vintageByWine.get(wine.id);
        const stats = wine.statistics || {};
        const vStats = vintage?.statistics || {};
        const region = wine.region || {};
        const country = region.country || {};
        const grapes = vintage?.grapes || [];

        const record = {
          vivino_winery_id: wineryId,
          producer_id: mapping.producer_id,
          producer_name: mapping.producer_name,
          vivino_wine_id: wine.id,
          wine_name: wine.name,
          wine_seo_name: wine.seo_name,
          type_id: wine.type_id,
          is_natural: wine.is_natural || false,
          region_name: region.name || null,
          country_code: country.code || mapping.country_code || null,
          country_name: country.name || null,
          rating_average: stats.ratings_average || vStats.ratings_average || null,
          rating_count: stats.ratings_count || vStats.ratings_count || 0,
          grapes: grapes.map(g => g.name || g.grape?.name).filter(Boolean),
          vintage_year: vintage?.year > 1900 ? vintage.year : null,
          wines_count_on_winery: wines.length,
        };

        appendFileSync(WINES_DATA_FILE, JSON.stringify(record) + "\n");
        totalWines++;
      }

      fetched++;
      currentDelay = BASE_DELAY; // Reset on success

      // Progress
      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      process.stdout.write(
        `  ${fetched}/${toProcess.length} wineries, ${totalWines} wines [${elapsed}m]\r`
      );

    } catch (err) {
      errors++;
      console.error(`\n  Error for winery ${wineryId}: ${err.message}`);
    }

    await sleep(currentDelay);
  }

  console.log(`\n\n=== PHASE 2 COMPLETE ===`);
  console.log(`  Wineries fetched: ${fetched}`);
  console.log(`  Total wines found: ${totalWines}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Output: ${WINES_DATA_FILE}\n`);
}

// ── PHASE 2b: Fetch Prices via Explore API ────────────────────
async function phase2b() {
  console.log("=== PHASE 2b: Fetch Prices & Per-Vintage Scores ===\n");

  // Load winery mappings
  const wineryMap = loadJsonlMap(WINERY_MAP_FILE, "producer_id");
  const resolvedEntries = [...wineryMap.values()].filter(m => m.vivino_winery_id);
  console.log(`  ${resolvedEntries.length} resolved wineries`);

  // Build winery_id → producer_id lookup
  const wineryToProducer = new Map();
  for (const m of resolvedEntries) {
    wineryToProducer.set(m.vivino_winery_id, m.producer_id);
  }

  // Load existing price data for resume — track which winery IDs are done
  const existingPrices = loadJsonlArray(PRICES_DATA_FILE);
  const doneWineryIds = new Set();
  for (const p of existingPrices) {
    if (p.vivino_winery_id) doneWineryIds.add(p.vivino_winery_id);
  }
  console.log(`  ${doneWineryIds.size} wineries already fetched (resume)\n`);

  // Filter to remaining wineries
  let toProcess = resolvedEntries.filter(m => !doneWineryIds.has(m.vivino_winery_id));
  if (LIMIT < Infinity) toProcess = toProcess.slice(0, LIMIT);

  // Batch wineries into groups of 5 for explore API
  const BATCH_WINERIES = 5;
  const PER_PAGE = 50;
  let totalPrices = 0, totalVintageScores = 0, batchesDone = 0, errors = 0;
  let currentDelay = BASE_DELAY;
  const startTime = Date.now();

  for (let i = 0; i < toProcess.length; i += BATCH_WINERIES) {
    const batch = toProcess.slice(i, i + BATCH_WINERIES);
    const wineryIds = batch.map(m => m.vivino_winery_id);

    try {
      // Paginate through all results for this batch of wineries
      let page = 1;
      let hasMore = true;
      let batchPrices = 0;
      const batchWineriesWritten = new Set();

      while (hasMore) {
        const params = new URLSearchParams({
          min_rating: "1",
          order_by: "ratings_count",
          order: "desc",
          per_page: String(PER_PAGE),
          page: String(page),
          currency_code: "USD",
          language: "en",
        });
        for (const wid of wineryIds) {
          params.append("winery_ids[]", String(wid));
        }

        const url = `https://www.vivino.com/api/explore/explore?${params}`;
        const res = await fetch(url, {
          headers: { "User-Agent": USER_AGENT, Accept: "application/json" },
        });

        if (res.status === 429) {
          currentDelay = Math.min(currentDelay * 2, 30000);
          console.log(`\n  Rate limited. Backing off to ${currentDelay}ms`);
          await sleep(currentDelay);
          continue; // Retry same page
        }

        if (!res.ok) {
          errors++;
          break;
        }

        const data = await res.json();
        const matches = data.explore_vintage?.matches || [];

        for (const m of matches) {
          const v = m.vintage;
          const w = v?.wine;
          if (!w?.id) continue;

          const wineryId = w.winery?.id;
          const producerId = wineryId ? wineryToProducer.get(wineryId) : null;

          const record = {
            vivino_winery_id: wineryId,
            producer_id: producerId,
            vivino_wine_id: w.id,
            wine_name: w.name,
            vintage_year: v.year > 1900 ? v.year : null,
            rating_average: v.statistics?.ratings_average || null,
            rating_count: v.statistics?.ratings_count || 0,
            wine_rating_average: v.statistics?.wine_ratings_average || null,
            wine_rating_count: v.statistics?.wine_ratings_count || 0,
            price_usd: m.price?.amount || null,
            price_currency: m.price?.currency?.code || "USD",
            price_url: m.price?.url || null,
            price_bottle_type: m.price?.bottle_type?.short_name || "bottle",
            price_bottle_ml: m.price?.bottle_type?.volume_ml || 750,
          };

          appendFileSync(PRICES_DATA_FILE, JSON.stringify(record) + "\n");
          if (wineryId) batchWineriesWritten.add(wineryId);

          if (record.price_usd) totalPrices++;
          if (record.vintage_year) totalVintageScores++;
          batchPrices++;
        }

        // Check if there are more pages
        hasMore = matches.length === PER_PAGE;
        page++;
        if (hasMore) await sleep(Math.max(500, currentDelay / 2)); // Shorter delay for pagination
      }

      batchesDone++;
      currentDelay = BASE_DELAY;

      // Write sentinel for wineries that had no explore results so resume skips them
      for (const m of batch) {
        if (!doneWineryIds.has(m.vivino_winery_id)) {
          if (!batchWineriesWritten.has(m.vivino_winery_id)) {
            const sentinel = {
              vivino_winery_id: m.vivino_winery_id,
              producer_id: m.producer_id,
              vivino_wine_id: null,
              wine_name: null,
              _no_prices: true,
            };
            appendFileSync(PRICES_DATA_FILE, JSON.stringify(sentinel) + "\n");
          }
          doneWineryIds.add(m.vivino_winery_id);
        }
      }

      // Progress
      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      const processedCount = Math.min(i + BATCH_WINERIES, toProcess.length);
      process.stdout.write(
        `  ${processedCount}/${toProcess.length} wineries, ${totalPrices} prices, ${totalVintageScores} vintage scores [${elapsed}m]\r`
      );

    } catch (err) {
      errors++;
      console.error(`\n  Error for batch ${wineryIds.join(",")}: ${err.message}`);
    }

    await sleep(currentDelay);
  }

  console.log(`\n\n=== PHASE 2b COMPLETE ===`);
  console.log(`  Batches processed: ${batchesDone}`);
  console.log(`  Prices found: ${totalPrices}`);
  console.log(`  Vintage scores found: ${totalVintageScores}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Output: ${PRICES_DATA_FILE}\n`);
}

// ── PHASE 3: Match & Create ──────────────────────────────────
async function phase3() {
  console.log("=== PHASE 3: Match & Create Wines ===\n");

  let totalHaikuInput = 0, totalHaikuOutput = 0;

  // 1. Load Vivino wine data
  const rawListings = loadJsonlArray(WINES_DATA_FILE);
  console.log(`Loaded ${rawListings.length} Vivino wine listings`);

  // Dedup by vivino_wine_id — keep best listing per wine (highest rating_count)
  const byWineId = new Map();
  for (const l of rawListings) {
    const key = l.vivino_wine_id;
    if (!key) continue;
    if (!byWineId.has(key) || (l.rating_count || 0) > (byWineId.get(key).rating_count || 0)) {
      byWineId.set(key, l);
    }
  }
  const uniqueListings = [...byWineId.values()];
  console.log(`Deduped to ${uniqueListings.length} unique wines\n`);

  // 2. Load reference data
  console.log("Loading reference data...");

  const countries = await fetchAll("countries", "id,name,iso_code");
  const countryByName = new Map();
  const countryByCode = new Map();
  for (const c of countries) {
    countryByName.set(c.name, c.id);
    if (c.iso_code) countryByCode.set(c.iso_code.toLowerCase(), c.id);
  }

  const producers = await fetchAll("producers", "id,name,name_normalized,country_id,slug");
  const producerById = new Map();
  for (const p of producers) producerById.set(p.id, p);
  console.log(`  ${producers.length} producers`);

  const grapes = await fetchAll("grapes", "id,name,aliases,color");
  const grapeMap = new Map();
  const grapeNames = [];
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
  grapeNames.sort((a, b) => b.length - a.length);
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
      region_id: r.region_id, appellation_id: r.appellation_id,
    });
  }
  console.log(`  ${rnm.length} region name mappings`);

  // Load existing wines for dedup
  const existingWines = await fetchAll("wines", "id,name,name_normalized,producer_id");
  const existingWineMap = new Map();
  // Build per-producer index for fast substring matching
  const winesByProducer = new Map();
  for (const w of existingWines) {
    existingWineMap.set(`${w.producer_id}||${w.name_normalized}`, w);
    if (!winesByProducer.has(w.producer_id)) winesByProducer.set(w.producer_id, []);
    winesByProducer.get(w.producer_id).push(w);
  }
  console.log(`  ${existingWines.length} existing wines`);

  const slugRows = await fetchAll("wines", "slug");
  const existingSlugs = new Set();
  for (const r of slugRows) existingSlugs.add(r.slug);
  console.log(`  ${existingSlugs.size} existing wine slugs\n`);

  // 3. Match against existing wines
  console.log("--- Matching against existing catalog ---");
  const matched = [];
  const newWineListings = [];

  for (const listing of uniqueListings) {
    const producer = producerById.get(listing.producer_id);
    if (!producer) {
      // Producer not found — skip
      continue;
    }

    const normWine = normalize(listing.wine_name);
    const dedupKey = `${producer.id}||${normWine}`;

    if (existingWineMap.has(dedupKey)) {
      matched.push({ listing, wineId: existingWineMap.get(dedupKey).id, producerId: producer.id });
    } else {
      // Try substring match — only check this producer's wines (fast!)
      let found = false;
      const producerWines = winesByProducer.get(producer.id) || [];
      for (const w of producerWines) {
        if (w.name_normalized.includes(normWine) || normWine.includes(w.name_normalized)) {
          matched.push({ listing, wineId: w.id, producerId: producer.id });
          found = true;
          break;
        }
      }
      if (!found) {
        newWineListings.push({ listing, producer });
      }
    }
  }

  console.log(`  Matched to existing wines: ${matched.length}`);
  console.log(`  New wines to create: ${newWineListings.length}\n`);

  // 4. Classify varietal categories for new wines
  console.log("--- Classifying varietal categories ---");
  const needsHaikuVcat = [];

  for (const entry of newWineListings) {
    const l = entry.listing;
    const wineType = VIVINO_TYPE_MAP[l.type_id] || "Red";
    const normWineName = normalize(l.wine_name).toLowerCase();
    entry.wineType = wineType;

    let vcatId = null;
    let grapeId = null;
    let vcatMethod = null;

    // 4a. Parse grape from Vivino grapes array
    if (l.grapes && l.grapes.length > 0) {
      const primaryGrape = l.grapes[0].toLowerCase();
      const grape = grapeMap.get(primaryGrape);
      if (grape) {
        const targetColor = wineType === "Red" ? "red" : wineType === "White" ? "white" :
          wineType === "Rosé" ? "rose" : grape.color;
        vcatId = vcatByGrapeColor.get(`${grape.id}|${targetColor}`) || vcatByGrape.get(grape.id);
        if (vcatId) {
          grapeId = grape.id;
          vcatMethod = `grape_array:${grape.name}`;
        }
      }
    }

    // 4b. Parse grape from wine name (longest-first matching)
    if (!vcatId) {
      for (const grapeName of grapeNames) {
        if (normWineName.includes(grapeName)) {
          const grape = grapeMap.get(grapeName);
          if (grape) {
            const targetColor = wineType === "Red" ? "red" : wineType === "White" ? "white" :
              wineType === "Rosé" ? "rose" : grape.color;
            vcatId = vcatByGrapeColor.get(`${grape.id}|${targetColor}`) || vcatByGrape.get(grape.id);
            if (vcatId) {
              grapeId = grape.id;
              vcatMethod = `grape_name:${grape.name}`;
              break;
            }
          }
        }
      }
    }

    // 4c. Regional designation matching
    if (!vcatId) {
      const normRegion = normalize(l.region_name || "").toLowerCase();
      for (const [regionKey, vcName] of Object.entries(REGIONAL_DESIGNATION_MAP)) {
        const re = new RegExp(`\\b${regionKey}\\b`);
        if (re.test(normWineName) || re.test(normRegion)) {
          vcatId = vcatByName.get(vcName);
          if (vcatId) {
            vcatMethod = `regional:${regionKey}→${vcName}`;
            break;
          }
        }
      }
    }

    // 4d. Queue for Haiku if still unresolved
    if (!vcatId) {
      needsHaikuVcat.push(entry);
    }

    entry.vcatId = vcatId;
    entry.grapeId = grapeId;
    entry.vcatMethod = vcatMethod;
  }

  const resolvedCount = newWineListings.filter(e => e.vcatId).length;
  console.log(`  Resolved by grape array: ${newWineListings.filter(e => e.vcatMethod?.startsWith("grape_array:")).length}`);
  console.log(`  Resolved by grape name: ${newWineListings.filter(e => e.vcatMethod?.startsWith("grape_name:")).length}`);
  console.log(`  Resolved by regional: ${newWineListings.filter(e => e.vcatMethod?.startsWith("regional:")).length}`);
  console.log(`  Needs Haiku: ${needsHaikuVcat.length}`);

  // Haiku classification (batches of 40)
  if (needsHaikuVcat.length > 0) {
    const vcatNames = vcats.map(v => v.name).join(", ");
    const BATCH_SIZE = 40;

    for (let i = 0; i < needsHaikuVcat.length; i += BATCH_SIZE) {
      const batch = needsHaikuVcat.slice(i, i + BATCH_SIZE);
      const prompt = batch.map((entry, idx) => {
        const l = entry.listing;
        return `[${idx}] "${l.wine_name}" — Type: ${entry.wineType}, Region: ${l.region_name || "?"}(${l.country_name || "?"})`;
      }).join("\n");

      try {
        const { text, inputTokens, outputTokens } = await callHaiku([
          {
            role: "user",
            content:
              `You are a wine classification expert. For each wine, determine the most likely varietal category.\n\n` +
              `Available categories: ${vcatNames}\n\n` +
              `Reply JSON array: [{"index":N,"category":"exact name from list"}]\n\n` + prompt,
          },
          { role: "assistant", content: "[" },
        ]);
        totalHaikuInput += inputTokens;
        totalHaikuOutput += outputTokens;

        const cleaned = ("[" + text).replace(/```json\s*/g, "").replace(/```/g, "").trim();
        try {
          const results = JSON.parse(cleaned);
          for (const r of results) {
            if (r.index == null || r.index >= batch.length) continue;
            const vcId = vcatByName.get(r.category);
            if (vcId) {
              batch[r.index].vcatId = vcId;
              batch[r.index].vcatMethod = `haiku:${r.category}`;
            }
          }
        } catch (parseErr) {
          console.error(`  Haiku parse error: ${parseErr.message}`);
        }

        process.stdout.write(`  Haiku: ${Math.min(i + BATCH_SIZE, needsHaikuVcat.length)}/${needsHaikuVcat.length}\r`);
        await sleep(200);
      } catch (err) {
        console.error(`\n  Haiku error: ${err.message}`);
      }
    }
    console.log(`  Haiku resolved: ${needsHaikuVcat.filter(e => e.vcatId).length}/${needsHaikuVcat.length}`);
  }

  // Generic fallback
  for (const entry of newWineListings) {
    if (!entry.vcatId) {
      const genericName = WINE_TYPE_GENERIC_BLEND[entry.wineType] || "Red Blend";
      entry.vcatId = vcatByName.get(genericName) || vcatByName.get("Red Blend");
      entry.vcatMethod = `fallback:${genericName}`;
    }
  }
  console.log(`  Fallback: ${newWineListings.filter(e => e.vcatMethod?.startsWith("fallback:")).length}\n`);

  // 5. Resolve regions
  console.log("--- Resolving regions ---");
  let regionHits = 0, catchAllHits = 0;

  for (const entry of newWineListings) {
    const l = entry.listing;
    const countryId = l.country_code ? countryByCode.get(l.country_code.toLowerCase()) : null;
    entry.countryId = countryId || entry.producer.country_id;

    let regionId = null, appellationId = null;

    if (l.region_name) {
      // Try with country name
      const countryName = l.country_name || countries.find(c => c.id === entry.countryId)?.name;
      const rm = regionMapping.get(`${l.region_name}|${countryName}`);
      if (rm) {
        regionId = rm.region_id;
        appellationId = rm.appellation_id;
        regionHits++;
      }
    }
    if (!regionId && entry.countryId) {
      regionId = catchAllRegion.get(entry.countryId) || null;
      if (regionId) catchAllHits++;
    }

    entry.regionId = regionId;
    entry.appellationId = appellationId;
  }

  console.log(`  Region mapped: ${regionHits}`);
  console.log(`  Catch-all fallback: ${catchAllHits}\n`);

  // 6. Create wine records
  console.log("--- Creating wine records ---");
  const newWines = [];
  const skippedDupes = 0;

  for (const entry of newWineListings) {
    const l = entry.listing;
    const producer = entry.producer;
    if (!entry.countryId || !entry.vcatId) continue;

    const normWine = normalize(l.wine_name);
    const dedupKey = `${producer.id}||${normWine}`;
    if (existingWineMap.has(dedupKey)) continue; // Safety check

    let slug = `${producer.slug || slugify(producer.name)}-${slugify(l.wine_name)}`.slice(0, 120);
    if (!slug) slug = producer.slug || slugify(producer.name);
    if (existingSlugs.has(slug)) slug = `${slug}-vivino`;
    if (existingSlugs.has(slug)) slug = `${slug}-${randomUUID().slice(0, 6)}`;
    existingSlugs.add(slug);

    const wineId = randomUUID();
    const wineType = entry.wineType;

    newWines.push({
      id: wineId,
      slug,
      name: l.wine_name,
      name_normalized: normWine,
      producer_id: producer.id,
      country_id: entry.countryId,
      region_id: entry.regionId,
      appellation_id: entry.appellationId,
      varietal_category_id: entry.vcatId,
      effervescence: wineType === "Sparkling" ? "sparkling" : null,
    });

    entry.wineId = wineId;
    existingWineMap.set(dedupKey, { id: wineId });
  }

  console.log(`  New wines to create: ${newWines.length}`);

  if (DRY_RUN) {
    console.log(`\n[DRY RUN] Would create:`);
    console.log(`  ${newWines.length} new wines`);
    console.log(`  Would update scores/vintages for ${matched.length} existing wines`);

    // Show sample
    console.log(`\n  Sample new wines:`);
    for (const entry of newWineListings.filter(e => e.wineId).slice(0, 20)) {
      const l = entry.listing;
      console.log(`    ${l.producer_name} — ${l.wine_name} [${entry.wineType}] vcat: ${entry.vcatMethod}`);
    }

    const haikuCost = (totalHaikuInput * 0.8 + totalHaikuOutput * 4) / 1_000_000;
    console.log(`\n  Haiku cost: $${haikuCost.toFixed(4)} (${totalHaikuInput} in / ${totalHaikuOutput} out)`);
    return;
  }

  // Insert wines
  if (newWines.length > 0) {
    await batchInsert("wines", newWines, 500);
  }

  // 7. Create vintages, scores, grape links for NEW wines
  console.log("\n--- Vintages, scores, grape links for new wines ---");

  const vintageRows = [];
  const scoreRows = [];
  const grapeRows = [];
  const vintageKeys = new Set();
  const scoreKeys = new Set();

  for (const entry of newWineListings) {
    if (!entry.wineId) continue;
    const l = entry.listing;

    // Vintage
    if (l.vintage_year) {
      const key = `${entry.wineId}||${l.vintage_year}`;
      if (!vintageKeys.has(key)) {
        vintageKeys.add(key);
        vintageRows.push({
          wine_id: entry.wineId,
          vintage_year: l.vintage_year,
        });
      }
    }

    // Score
    if (l.rating_average && l.rating_count > 0) {
      const key = `${entry.wineId}||${l.vintage_year || "nv"}`;
      if (!scoreKeys.has(key)) {
        scoreKeys.add(key);
        scoreRows.push({
          wine_id: entry.wineId,
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

    // Grape link
    if (entry.grapeId) {
      grapeRows.push({ wine_id: entry.wineId, grape_id: entry.grapeId });
    }
  }

  if (vintageRows.length > 0) {
    console.log(`  Upserting ${vintageRows.length} vintages...`);
    const BATCH = 2000;
    for (let i = 0; i < vintageRows.length; i += BATCH) {
      const batch = vintageRows.slice(i, i + BATCH);
      const { error } = await sb.from("wine_vintages")
        .upsert(batch, { onConflict: "wine_id,vintage_year", ignoreDuplicates: true });
      if (error) console.error(`  Vintage batch error: ${error.message}`);
    }
    console.log(`  Vintages done`);
  }

  if (scoreRows.length > 0) {
    console.log(`  Inserting ${scoreRows.length} scores...`);
    let inserted = 0;
    const BATCH = 500;
    for (let i = 0; i < scoreRows.length; i += BATCH) {
      const batch = scoreRows.slice(i, i + BATCH);
      const { error } = await sb.from("wine_vintage_scores").insert(batch);
      if (error) {
        for (const row of batch) {
          const { error: e2 } = await sb.from("wine_vintage_scores").insert([row]);
          if (!e2) inserted++;
        }
      } else {
        inserted += batch.length;
      }
    }
    console.log(`  Scores inserted: ${inserted}/${scoreRows.length}`);
  }

  if (grapeRows.length > 0) {
    await batchInsert("wine_grapes", grapeRows);
  }

  // 8. Update scores for matched existing wines
  console.log("\n--- Updating scores for matched existing wines ---");
  const matchedScoreRows = [];
  const matchedScoreKeys = new Set();

  for (const m of matched) {
    const l = m.listing;
    if (!l.rating_average || l.rating_count <= 0) continue;
    const key = `${m.wineId}||${l.vintage_year || "nv"}||${VIVINO_PUBLICATION_ID}`;
    if (matchedScoreKeys.has(key)) continue;
    matchedScoreKeys.add(key);
    matchedScoreRows.push({
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

  if (matchedScoreRows.length > 0) {
    let inserted = 0, dupes = 0;
    const BATCH = 500;
    for (let i = 0; i < matchedScoreRows.length; i += BATCH) {
      const batch = matchedScoreRows.slice(i, i + BATCH);
      const { error } = await sb.from("wine_vintage_scores").insert(batch);
      if (error) {
        for (const row of batch) {
          const { error: e2 } = await sb.from("wine_vintage_scores").insert([row]);
          if (!e2) inserted++;
          else dupes++;
        }
      } else {
        inserted += batch.length;
      }
    }
    console.log(`  Matched scores inserted: ${inserted} (${dupes} dupes)`);
  }

  // 9. Insert prices and per-vintage scores from explore data
  console.log("\n--- Prices & per-vintage scores from explore data ---");

  // Load price/vintage data from Phase 2b
  const priceListings = loadJsonlArray(PRICES_DATA_FILE).filter(p => !p._no_prices && p.vivino_wine_id);
  console.log(`  ${priceListings.length} explore price records loaded`);

  // Build vivino_wine_id → loam_wine_id mapping (both new and matched)
  const vivinoToLoam = new Map();
  for (const entry of newWineListings) {
    if (entry.wineId && entry.listing.vivino_wine_id) {
      vivinoToLoam.set(entry.listing.vivino_wine_id, entry.wineId);
    }
  }
  for (const m of matched) {
    if (m.wineId && m.listing.vivino_wine_id) {
      vivinoToLoam.set(m.listing.vivino_wine_id, m.wineId);
    }
  }
  console.log(`  ${vivinoToLoam.size} Vivino→Loam wine ID mappings`);

  // Build price rows and additional vintage/score rows from explore data
  const priceRows = [];
  const priceKeys = new Set();
  const exploreVintageRows = [];
  const exploreVintageKeys = new Set();
  const exploreScoreRows = [];
  const exploreScoreKeys = new Set();

  for (const p of priceListings) {
    const loamWineId = vivinoToLoam.get(p.vivino_wine_id);
    if (!loamWineId) continue;

    // Price
    if (p.price_usd && p.price_bottle_ml === 750) {
      const priceKey = `${loamWineId}||${p.vintage_year || "nv"}||${p.price_url || ""}`;
      if (!priceKeys.has(priceKey)) {
        priceKeys.add(priceKey);
        priceRows.push({
          wine_id: loamWineId,
          vintage_year: p.vintage_year || null,
          price_usd: p.price_usd,
          price_original: p.price_usd,
          currency: p.price_currency || "USD",
          price_type: "retail",
          source_id: VIVINO_PUBLICATION_ID,
          source_url: p.price_url || null,
          merchant_name: p.price_url ? (() => { try { return new URL(p.price_url).hostname.replace("www.", ""); } catch { return null; } })() : null,
          price_date: TODAY,
        });
      }
    }

    // Per-vintage vintage record
    if (p.vintage_year) {
      const vKey = `${loamWineId}||${p.vintage_year}`;
      if (!exploreVintageKeys.has(vKey) && !vintageKeys.has(vKey)) {
        exploreVintageKeys.add(vKey);
        exploreVintageRows.push({
          wine_id: loamWineId,
          vintage_year: p.vintage_year,
        });
      }
    }

    // Per-vintage score (more specific than wine-level aggregate)
    if (p.vintage_year && p.rating_average && p.rating_count > 0) {
      const sKey = `${loamWineId}||${p.vintage_year}`;
      if (!exploreScoreKeys.has(sKey) && !scoreKeys.has(sKey)) {
        exploreScoreKeys.add(sKey);
        exploreScoreRows.push({
          wine_id: loamWineId,
          vintage_year: p.vintage_year,
          score: p.rating_average,
          score_scale: "5",
          publication_id: VIVINO_PUBLICATION_ID,
          critic: "Vivino Community",
          is_community: true,
          rating_count: p.rating_count,
          review_date: TODAY,
          url: `https://www.vivino.com/w/${p.vivino_wine_id}`,
        });
      }
    }
  }

  console.log(`  Prices to insert: ${priceRows.length}`);
  console.log(`  Extra vintages from explore: ${exploreVintageRows.length}`);
  console.log(`  Extra per-vintage scores: ${exploreScoreRows.length}`);

  if (!DRY_RUN) {
    // Insert vintages from explore
    if (exploreVintageRows.length > 0) {
      console.log(`  Upserting ${exploreVintageRows.length} explore vintages...`);
      const BATCH = 2000;
      for (let i = 0; i < exploreVintageRows.length; i += BATCH) {
        const batch = exploreVintageRows.slice(i, i + BATCH);
        const { error } = await sb.from("wine_vintages")
          .upsert(batch, { onConflict: "wine_id,vintage_year", ignoreDuplicates: true });
        if (error) console.error(`  Explore vintage batch error: ${error.message}`);
      }
      console.log(`  Explore vintages done`);
    }

    // Insert scores from explore (per-vintage, not duplicating wine-level ones)
    if (exploreScoreRows.length > 0) {
      let inserted = 0;
      const BATCH = 500;
      for (let i = 0; i < exploreScoreRows.length; i += BATCH) {
        const batch = exploreScoreRows.slice(i, i + BATCH);
        const { error } = await sb.from("wine_vintage_scores").insert(batch);
        if (error) {
          for (const row of batch) {
            const { error: e2 } = await sb.from("wine_vintage_scores").insert([row]);
            if (!e2) inserted++;
          }
        } else {
          inserted += batch.length;
        }
      }
      console.log(`  Explore scores inserted: ${inserted}/${exploreScoreRows.length}`);
    }

    // Insert prices
    if (priceRows.length > 0) {
      let inserted = 0;
      const BATCH = 500;
      for (let i = 0; i < priceRows.length; i += BATCH) {
        const batch = priceRows.slice(i, i + BATCH);
        const { error } = await sb.from("wine_vintage_prices").insert(batch);
        if (error) {
          for (const row of batch) {
            const { error: e2 } = await sb.from("wine_vintage_prices").insert([row]);
            if (!e2) inserted++;
          }
        } else {
          inserted += batch.length;
        }
      }
      console.log(`  Prices inserted: ${inserted}/${priceRows.length}`);
    }
  }

  // Final summary
  const haikuCost = (totalHaikuInput * 0.8 + totalHaikuOutput * 4) / 1_000_000;

  console.log("\n=== PHASE 3 COMPLETE ===");
  console.log(`  Matched to existing: ${matched.length}`);
  console.log(`  New wines created: ${newWines.length}`);
  console.log(`  Vintages: ${vintageRows.length} + ${exploreVintageRows.length} (explore)`);
  console.log(`  Scores (new wine aggregate): ${scoreRows.length}`);
  console.log(`  Scores (matched aggregate): ${matchedScoreRows.length}`);
  console.log(`  Scores (per-vintage from explore): ${exploreScoreRows.length}`);
  console.log(`  Prices: ${priceRows.length}`);
  console.log(`  Grape links: ${grapeRows.length}`);
  console.log(`  Haiku cost: $${haikuCost.toFixed(4)}`);
}

// ── Stats ────────────────────────────────────────────────────
function showStats() {
  console.log("=== Pipeline Stats ===\n");

  // Phase 1 stats
  if (existsSync(WINERY_MAP_FILE)) {
    const mappings = loadJsonlArray(WINERY_MAP_FILE);
    const resolved = mappings.filter(m => m.vivino_winery_id);
    const confDist = {};
    for (const m of mappings) {
      confDist[m.match_confidence] = (confDist[m.match_confidence] || 0) + 1;
    }
    console.log(`Phase 1: ${mappings.length} producers processed`);
    console.log(`  Resolved: ${resolved.length} (${Math.round(resolved.length / mappings.length * 100)}%)`);
    console.log(`  Confidence:`, confDist);
    console.log();
  } else {
    console.log("Phase 1: No data yet\n");
  }

  // Phase 2 stats
  if (existsSync(WINES_DATA_FILE)) {
    const wines = loadJsonlArray(WINES_DATA_FILE);
    const wineryIds = new Set(wines.map(w => w.vivino_winery_id));
    const wineIds = new Set(wines.map(w => w.vivino_wine_id));
    console.log(`Phase 2: ${wines.length} wine records`);
    console.log(`  Unique wineries: ${wineryIds.size}`);
    console.log(`  Unique wines: ${wineIds.size}`);
    console.log();
  } else {
    console.log("Phase 2: No data yet\n");
  }

  // Phase 2b stats
  if (existsSync(PRICES_DATA_FILE)) {
    const prices = loadJsonlArray(PRICES_DATA_FILE).filter(p => !p._no_prices);
    const withPrice = prices.filter(p => p.price_usd);
    const withVintage = prices.filter(p => p.vintage_year);
    const wineryIds = new Set(prices.map(p => p.vivino_winery_id));
    console.log(`Phase 2b: ${prices.length} explore records`);
    console.log(`  Unique wineries: ${wineryIds.size}`);
    console.log(`  With prices: ${withPrice.length}`);
    console.log(`  With vintage year: ${withVintage.length}`);
    console.log();
  } else {
    console.log("Phase 2b: No data yet\n");
  }
}

// ── Main ─────────────────────────────────────────────────────
async function main() {
  if (SHOW_STATS) {
    showStats();
    return;
  }

  console.log(`=== Vivino Producer Depth Crawl ===`);
  console.log(`  Phase: ${PHASE || "all"}`);
  console.log(`  Limit: ${LIMIT === Infinity ? "none" : LIMIT}`);
  console.log(`  Delay: ${BASE_DELAY}ms`);
  console.log(`  Resume: ${RESUME}`);
  console.log(`  Dry run: ${DRY_RUN}\n`);

  if (PHASE === 0 || PHASE === 1) await phase1();
  if (PHASE === 0 || PHASE === 2) await phase2();
  if (PHASE === 0 || PHASE === "2b") await phase2b();
  if (PHASE === 0 || PHASE === 3) await phase3();

  console.log("\nDone!");
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
