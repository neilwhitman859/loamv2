#!/usr/bin/env node
/**
 * fetch_vivino_mega.mjs
 *
 * Smart multi-slice Vivino fetcher. Partitions by wine_type × country
 * to get non-overlapping result sets, maximizing unique wine coverage.
 *
 * Usage:
 *   node fetch_vivino_mega.mjs --probe-only          # 1 page per slice, estimate totals
 *   node fetch_vivino_mega.mjs                        # Full fetch
 *   node fetch_vivino_mega.mjs --resume               # Resume interrupted run
 *   node fetch_vivino_mega.mjs --export out.json      # JSONL → JSON array
 *   node fetch_vivino_mega.mjs --max-unique 5000      # Stop after N unique listings
 *   node fetch_vivino_mega.mjs --delay-ms 2000        # Custom delay
 */

import { readFileSync, writeFileSync, appendFileSync, existsSync, createReadStream, createWriteStream } from "fs";
import { createInterface } from "readline";

// ── CLI ─────────────────────────────────────────────────────
const args = process.argv.slice(2);
function getArg(name, fallback) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : fallback;
}
const PROBE_ONLY = args.includes("--probe-only");
const RESUME = args.includes("--resume");
const EXPORT_FILE = getArg("export", null);
const MAX_UNIQUE = parseInt(getArg("max-unique", "0"), 10) || Infinity;
const BASE_DELAY = parseInt(getArg("delay-ms", "1500"), 10);
const MAX_PAGES_PER_SLICE = parseInt(getArg("max-pages", "5000"), 10);

// ── Constants ───────────────────────────────────────────────
const BASE_URL = "https://www.vivino.com/api/explore/explore";
const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36";

const MANIFEST_FILE = "vivino_mega_manifest.json";
const DATA_FILE = "vivino_mega_data.jsonl";
const SEEN_FILE = "vivino_mega_seen.json";

const WINE_TYPES = [
  { id: "1", label: "Red" },
  { id: "2", label: "White" },
  { id: "3", label: "Sparkling" },
  { id: "4", label: "Rosé" },
  { id: "7", label: "Dessert" },
  { id: "24", label: "Fortified" },
];

// Tier 1: Big wine countries (highest catalog overlap)
const L1_COUNTRIES = ["fr", "it", "us", "es", "pt"];
// Tier 2: Medium countries
const L2_COUNTRIES = ["de", "au", "cl", "ar", "za", "at", "nz", "br"];
// Tier 3: "everything else" uses no country filter but excludes L1+L2

const COUNTRY_LABELS = {
  fr: "France", it: "Italy", us: "USA", es: "Spain", pt: "Portugal",
  de: "Germany", au: "Australia", cl: "Chile", ar: "Argentina",
  za: "South Africa", at: "Austria", nz: "New Zealand", br: "Brazil",
  _other: "Other",
};

// ── Helpers ─────────────────────────────────────────────────
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function extractListing(match) {
  const v = match.vintage || {};
  const wine = v.wine || {};
  const winery = wine.winery || {};
  const region = wine.region || {};
  const country = region.country || {};
  const stats = v.statistics || {};
  const price = match.price || null;

  let pricePerBottle = null;
  let merchantName = null;
  let sourceUrl = null;
  let bottleQty = 1;
  if (price) {
    bottleQty = price.bottle_quantity || 1;
    pricePerBottle =
      price.amount != null ? Math.round((price.amount / bottleQty) * 100) / 100 : null;
    merchantName = price.merchant_name || null;
    sourceUrl = price.url || null;
  }

  let vintageYear = null;
  if (v.year && v.year > 1900) {
    vintageYear = v.year;
  } else {
    const yearMatch = (v.seo_name || "").match(/-(\d{4})$/);
    if (yearMatch) vintageYear = parseInt(yearMatch[1], 10);
  }

  return {
    vivino_wine_id: wine.id,
    vivino_vintage_id: v.id,
    winery_name: winery.name || null,
    wine_name: wine.name || null,
    vintage_year: vintageYear,
    region_name: region.name || null,
    country_name: country.name || null,
    country_code: country.code || null,
    wine_type_id: wine.type_id,
    rating_average: stats.ratings_average || null,
    rating_count: stats.ratings_count || 0,
    price_usd: pricePerBottle,
    price_raw: price ? price.amount : null,
    bottle_quantity: bottleQty,
    merchant_name: merchantName,
    source_url: sourceUrl,
    is_natural: wine.is_natural || false,
  };
}

// ── Slice Generation ────────────────────────────────────────
function generateSlices() {
  const slices = [];

  // Execution order: quick wins first (small types), then core (big types × big countries)
  // Reorder: Sparkling, Rosé, Dessert, Fortified first, then Red, White
  const typeOrder = ["3", "4", "7", "24", "1", "2"];

  for (const typeId of typeOrder) {
    const type = WINE_TYPES.find((t) => t.id === typeId);

    // L1: Big countries
    for (const cc of L1_COUNTRIES) {
      slices.push({
        id: `${type.label.toLowerCase()}|${cc}`,
        label: `${type.label} × ${COUNTRY_LABELS[cc]}`,
        tier: "L1",
        params: { "wine_type_ids[]": typeId, "country_codes[]": cc },
      });
    }

    // L2: Medium countries
    for (const cc of L2_COUNTRIES) {
      slices.push({
        id: `${type.label.toLowerCase()}|${cc}`,
        label: `${type.label} × ${COUNTRY_LABELS[cc]}`,
        tier: "L2",
        params: { "wine_type_ids[]": typeId, "country_codes[]": cc },
      });
    }

    // L3: Everything else (no country filter, but exclude L1+L2 isn't possible via API,
    // so we just don't filter — cross-slice dedup handles overlap)
    slices.push({
      id: `${type.label.toLowerCase()}|other`,
      label: `${type.label} × Other countries`,
      tier: "L3",
      params: { "wine_type_ids[]": typeId },
    });
  }

  return slices;
}

// ── Manifest ────────────────────────────────────────────────
function loadManifest() {
  if (existsSync(MANIFEST_FILE)) {
    return JSON.parse(readFileSync(MANIFEST_FILE, "utf-8"));
  }
  return null;
}

function createManifest(slices) {
  return {
    version: 1,
    started_at: new Date().toISOString(),
    config: { delay_ms: BASE_DELAY, max_pages_per_slice: MAX_PAGES_PER_SLICE },
    global_stats: {
      total_listings_fetched: 0,
      total_unique_written: 0,
      total_duplicates_skipped: 0,
      total_pages_fetched: 0,
      total_errors: 0,
    },
    slices: slices.map((s) => ({
      ...s,
      status: "pending",
      records_matched: null,
      pages_fetched: 0,
      listings_fetched: 0,
      unique_written: 0,
      last_page: 0,
      started_at: null,
      completed_at: null,
    })),
  };
}

function saveManifest(manifest) {
  writeFileSync(MANIFEST_FILE, JSON.stringify(manifest, null, 2));
}

// ── Seen Set ────────────────────────────────────────────────
function loadSeen() {
  if (existsSync(SEEN_FILE)) {
    const arr = JSON.parse(readFileSync(SEEN_FILE, "utf-8"));
    return new Set(arr);
  }
  return new Set();
}

function saveSeen(seen) {
  writeFileSync(SEEN_FILE, JSON.stringify([...seen]));
}

// ── Page Fetcher ────────────────────────────────────────────
async function fetchPage(params, page) {
  const qs = new URLSearchParams({
    country_code: "US",
    currency_code: "USD",
    min_rating: "1",
    order_by: "ratings_count",
    order: "desc",
    price_range_min: "0",
    price_range_max: "500",
    ...params,
    page: String(page),
  });
  const url = `${BASE_URL}?${qs}`;
  const res = await fetch(url, {
    headers: { "User-Agent": USER_AGENT, Accept: "application/json" },
  });

  if (res.status === 429) {
    throw Object.assign(new Error("Rate limited"), { status: 429 });
  }
  if (!res.ok) {
    throw Object.assign(new Error(`HTTP ${res.status} ${res.statusText}`), { status: res.status });
  }

  const data = await res.json();
  return data.explore_vintage;
}

// ── Export JSONL → JSON ─────────────────────────────────────
async function exportToJson(outputPath) {
  console.log(`Exporting ${DATA_FILE} → ${outputPath}...`);
  const rl = createInterface({ input: createReadStream(DATA_FILE) });
  const ws = createWriteStream(outputPath);
  ws.write("[\n");
  let first = true;
  let count = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    if (!first) ws.write(",\n");
    ws.write("  " + line);
    first = false;
    count++;
  }
  ws.write("\n]\n");
  ws.end();
  console.log(`Exported ${count.toLocaleString()} listings to ${outputPath}`);
}

// ── Probe ───────────────────────────────────────────────────
async function runProbe(manifest) {
  console.log("=== PROBE MODE — Fetching page 1 of each slice ===\n");
  let totalRecords = 0;

  for (const slice of manifest.slices) {
    try {
      const result = await fetchPage(slice.params, 1);
      const matched = result.records_matched || 0;
      slice.records_matched = matched;
      totalRecords += matched;

      const matches = result.matches || [];
      const sample = matches.length;
      console.log(
        `  ${slice.label.padEnd(32)} ${String(matched).padStart(8)} records  (${sample} per page)  [${slice.tier}]`
      );

      await sleep(500); // Gentle probe delay
    } catch (err) {
      console.error(`  ${slice.label.padEnd(32)} ERROR: ${err.message}`);
      await sleep(1000);
    }
  }

  const uniqueEstimate = Math.round(totalRecords * 0.35); // Conservative dedup ratio
  const pagesEstimate = Math.ceil(totalRecords / 25);
  const hoursEstimate = ((pagesEstimate * BASE_DELAY) / 1000 / 3600).toFixed(1);

  console.log(`\n=== PROBE SUMMARY ===`);
  console.log(`  Total records_matched (sum):  ${totalRecords.toLocaleString()}`);
  console.log(`  Estimated unique wines:       ~${uniqueEstimate.toLocaleString()} (35% dedup ratio)`);
  console.log(`  Estimated pages:              ${pagesEstimate.toLocaleString()}`);
  console.log(`  Estimated fetch time:         ~${hoursEstimate} hours at ${BASE_DELAY}ms delay`);

  // Flag slices needing sub-slicing
  const bigSlices = manifest.slices.filter((s) => s.records_matched > 125000);
  if (bigSlices.length > 0) {
    console.log(`\n  ⚠ ${bigSlices.length} slices have >125K records (may need price sub-slicing):`);
    for (const s of bigSlices) {
      console.log(`    ${s.label}: ${s.records_matched.toLocaleString()}`);
    }
  }

  saveManifest(manifest);
  console.log(`\nManifest saved to ${MANIFEST_FILE}`);
}

// ── Full Fetch ──────────────────────────────────────────────
async function runFetch(manifest, seen) {
  const startTime = Date.now();
  let globalUniqueTarget = MAX_UNIQUE;
  let currentDelay = BASE_DELAY;

  console.log("=== VIVINO MEGA FETCH ===\n");
  console.log(`  Slices: ${manifest.slices.length}`);
  console.log(`  Delay: ${BASE_DELAY}ms`);
  console.log(`  Max pages/slice: ${MAX_PAGES_PER_SLICE}`);
  if (RESUME) console.log(`  Resuming from manifest...`);
  console.log(`  Seen IDs loaded: ${seen.size.toLocaleString()}\n`);

  for (let si = 0; si < manifest.slices.length; si++) {
    const slice = manifest.slices[si];

    // Skip completed or paused slices
    if (slice.status === "completed" || slice.status === "paused") {
      continue;
    }

    // Check global unique limit
    if (manifest.global_stats.total_unique_written >= globalUniqueTarget) {
      console.log(`\n  Reached max-unique limit (${globalUniqueTarget}). Stopping.`);
      break;
    }

    slice.status = "in_progress";
    if (!slice.started_at) slice.started_at = new Date().toISOString();
    const startPage = RESUME ? (slice.last_page || 0) + 1 : 1;

    console.log(`\n── Slice ${si + 1}/${manifest.slices.length}: ${slice.label} [${slice.tier}] ──`);
    if (startPage > 1) console.log(`  Resuming from page ${startPage}`);

    let consecutiveErrors = 0;
    let consecutiveZeroNew = 0;
    const ZERO_NEW_LIMIT = 15; // Skip slice after 15 consecutive pages with 0 new listings
    currentDelay = BASE_DELAY;

    for (let page = startPage; page <= MAX_PAGES_PER_SLICE; page++) {
      try {
        const result = await fetchPage(slice.params, page);

        if (page === startPage && !slice.records_matched) {
          slice.records_matched = result.records_matched || 0;
          const maxPages = Math.min(Math.ceil(slice.records_matched / 25), MAX_PAGES_PER_SLICE);
          console.log(`  records_matched: ${slice.records_matched.toLocaleString()} (~${maxPages} pages)`);
        }

        const matches = result.matches || [];
        if (matches.length === 0) {
          console.log(`  Page ${page}: No more results.`);
          break;
        }

        // Extract and dedup
        let sliceNew = 0;
        for (const match of matches) {
          const listing = extractListing(match);
          const vid = listing.vivino_vintage_id;

          slice.listings_fetched++;
          manifest.global_stats.total_listings_fetched++;

          if (vid && seen.has(vid)) {
            manifest.global_stats.total_duplicates_skipped++;
            continue;
          }

          if (vid) seen.add(vid);
          appendFileSync(DATA_FILE, JSON.stringify(listing) + "\n");
          sliceNew++;
          slice.unique_written++;
          manifest.global_stats.total_unique_written++;
        }

        slice.pages_fetched++;
        slice.last_page = page;
        manifest.global_stats.total_pages_fetched++;
        consecutiveErrors = 0;
        currentDelay = BASE_DELAY;

        // Early-exit: skip slice if too many consecutive pages yield nothing new
        if (sliceNew === 0) {
          consecutiveZeroNew++;
          if (consecutiveZeroNew >= ZERO_NEW_LIMIT) {
            console.log(`\n  ${ZERO_NEW_LIMIT} consecutive pages with 0 new — skipping rest of slice.`);
            break;
          }
        } else {
          consecutiveZeroNew = 0;
        }

        // Progress line
        const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
        process.stdout.write(
          `  Page ${page} — ${sliceNew} new, ${slice.unique_written} slice total, ` +
          `${manifest.global_stats.total_unique_written.toLocaleString()} global unique [${elapsed}m]\r`
        );

        // Save manifest + seen periodically
        if (page % 50 === 0) {
          saveManifest(manifest);
          saveSeen(seen);
        }

        // Check global limit
        if (manifest.global_stats.total_unique_written >= globalUniqueTarget) break;

        await sleep(currentDelay);
      } catch (err) {
        consecutiveErrors++;
        manifest.global_stats.total_errors++;

        if (err.status === 429) {
          currentDelay = Math.min(currentDelay * 2, 30000);
          console.log(`\n  Rate limited at page ${page}. Backing off to ${currentDelay}ms...`);
        } else {
          currentDelay = Math.min(currentDelay * 1.5, 15000);
          console.error(`\n  Error page ${page}: ${err.message}. Delay now ${Math.round(currentDelay)}ms`);
        }

        if (consecutiveErrors >= 5) {
          console.log(`  5 consecutive errors — pausing slice.`);
          slice.status = "paused";
          saveManifest(manifest);
          saveSeen(seen);
          break;
        }

        await sleep(currentDelay);
      }
    }

    // Mark complete if not paused
    if (slice.status !== "paused") {
      slice.status = "completed";
      slice.completed_at = new Date().toISOString();
    }

    // Save after each slice
    saveManifest(manifest);
    saveSeen(seen);
    console.log(`\n  ✓ ${slice.label}: ${slice.unique_written.toLocaleString()} unique`);
  }

  // Final summary
  const totalMin = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  const stats = manifest.global_stats;
  console.log(`\n\n=== FETCH COMPLETE ===`);
  console.log(`  Time:               ${totalMin} minutes`);
  console.log(`  Pages fetched:      ${stats.total_pages_fetched.toLocaleString()}`);
  console.log(`  Listings fetched:   ${stats.total_listings_fetched.toLocaleString()}`);
  console.log(`  Unique written:     ${stats.total_unique_written.toLocaleString()}`);
  console.log(`  Duplicates skipped: ${stats.total_duplicates_skipped.toLocaleString()}`);
  console.log(`  Errors:             ${stats.total_errors}`);

  const completed = manifest.slices.filter((s) => s.status === "completed").length;
  const paused = manifest.slices.filter((s) => s.status === "paused").length;
  console.log(`  Slices completed:   ${completed}/${manifest.slices.length}`);
  if (paused > 0) console.log(`  Slices paused:      ${paused} (run --resume to retry)`);

  console.log(`\nData saved to ${DATA_FILE}`);
  console.log(`Manifest saved to ${MANIFEST_FILE}`);

  // Quick stats on unique wines
  const wineIds = new Set();
  const rl = createInterface({ input: createReadStream(DATA_FILE) });
  for await (const line of rl) {
    if (!line.trim()) continue;
    try {
      const l = JSON.parse(line);
      if (l.vivino_wine_id) wineIds.add(l.vivino_wine_id);
    } catch {}
  }
  console.log(`  Unique wines (by vivino_wine_id): ${wineIds.size.toLocaleString()}`);
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  // Export mode
  if (EXPORT_FILE) {
    await exportToJson(EXPORT_FILE);
    return;
  }

  // Generate or load slices
  const sliceDefs = generateSlices();
  let manifest;
  let seen;

  if (RESUME && existsSync(MANIFEST_FILE)) {
    manifest = loadManifest();
    seen = loadSeen();
    console.log(`Resumed manifest with ${manifest.slices.length} slices.`);
  } else {
    manifest = createManifest(sliceDefs);
    seen = new Set();
    // Clear data file for fresh run
    if (!RESUME) writeFileSync(DATA_FILE, "");
  }

  if (PROBE_ONLY) {
    await runProbe(manifest);
  } else {
    await runFetch(manifest, seen);
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
