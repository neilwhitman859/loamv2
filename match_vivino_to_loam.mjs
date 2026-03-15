#!/usr/bin/env node
/**
 * match_vivino_to_loam.mjs
 *
 * Two-pass matching of Vivino listings against the Loam wine catalog.
 *   Pass 1: Exact normalized name matching (free)
 *   Pass 2: Haiku fuzzy matching for unmatched listings (Principle #2)
 *
 * After matching, inserts:
 *   - New vintages into wine_vintages (upsert, fills post-2022 gap)
 *   - Community scores into wine_vintage_scores (Vivino ratings)
 *   - Retail prices into wine_vintage_prices
 *
 * Usage:
 *   node match_vivino_to_loam.mjs                           # Match all
 *   node match_vivino_to_loam.mjs --input vivino_full.json
 *   node match_vivino_to_loam.mjs --dry-run                 # Don't insert anything
 *   node match_vivino_to_loam.mjs --skip-haiku              # Pass 1 only
 */

import { readFileSync, writeFileSync } from "fs";
import { createClient } from "@supabase/supabase-js";

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
const SKIP_HAIKU = args.includes("--skip-haiku");

function getArg(name, fallback) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : fallback;
}
const INPUT_FILE = getArg("input", "vivino_full.json");

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

/** Fetch all rows with pagination */
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

/** Simple Levenshtein distance */
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

/** Call Claude Haiku for fuzzy matching */
async function haikuMatch(batch) {
  const prompt = batch
    .map(
      (item, i) =>
        `[${i}] Vivino: "${item.vivino.winery_name}" — "${item.vivino.wine_name}" (${item.vivino.country_name || "?"})\n` +
        `    Candidates:\n` +
        item.candidates
          .map(
            (c, j) =>
              `      ${String.fromCharCode(65 + j)}) Producer: "${c.producerName}" — Wine: "${c.wineName}" (${c.countryName})`
          )
          .join("\n")
    )
    .join("\n\n");

  const systemMsg = `You are a wine catalog matcher. For each Vivino listing, determine if any Loam candidate is the same wine. Account for:
- Different transliterations (Château vs Chateau, ü vs u)
- Abbreviated vs full names (Dr. vs Doktor, Dom. vs Domaine)
- Minor name variations (adding/dropping "Estate", "Wines", "Winery")
- The wine name may be a subset (e.g., Vivino "Pinot Noir" matches Loam "Pinot Noir Reserve" if same producer)

Reply with JSON array. For each listing index, return the candidate letter (A/B/C) or "none".
Example: [{"index":0,"match":"A"},{"index":1,"match":"none"}]`;

  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 1000,
      messages: [
        { role: "user", content: systemMsg + "\n\n" + prompt },
        { role: "assistant", content: "[" },
      ],
    }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Haiku API ${res.status}: ${body.slice(0, 200)}`);
  }

  const msg = await res.json();
  const text = "[" + (msg.content[0]?.text || "");
  const cleaned = text.replace(/```json\s*/g, "").replace(/```/g, "").trim();

  return {
    results: JSON.parse(cleaned),
    inputTokens: msg.usage?.input_tokens || 0,
    outputTokens: msg.usage?.output_tokens || 0,
  };
}

// ── Country name mapping ────────────────────────────────────
// Vivino uses slightly different country names sometimes
const COUNTRY_ALIASES = {
  "united states": "united states",
  usa: "united states",
  us: "united states",
  uk: "united kingdom",
  "great britain": "united kingdom",
};

function normalizeCountry(name) {
  if (!name) return "";
  const n = normalize(name);
  return COUNTRY_ALIASES[n] || n;
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log("=== Vivino → Loam Catalog Matcher ===\n");

  // 1. Load Vivino listings (supports both JSON array and JSONL)
  let listings;
  if (INPUT_FILE.endsWith(".jsonl")) {
    listings = readFileSync(INPUT_FILE, "utf-8")
      .split("\n")
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line));
  } else {
    listings = JSON.parse(readFileSync(INPUT_FILE, "utf-8"));
  }
  console.log(`Loaded ${listings.length} Vivino listings from ${INPUT_FILE}`);

  // 2. Load Loam producers + wines
  console.log("Loading Loam catalog...");
  const producers = await fetchAll("producers", "id, name, country:countries(name)");
  console.log(`  ${producers.length} producers`);

  const wines = await fetchAll("wines", "id, name, producer_id, country:countries(name)");
  console.log(`  ${wines.length} wines`);

  // 3. Build lookup maps
  // Producer: normalized name → [{ id, name, countryName }]
  const producerMap = new Map();
  for (const p of producers) {
    const key = normalize(p.name);
    if (!producerMap.has(key)) producerMap.set(key, []);
    producerMap.get(key).push({
      id: p.id,
      name: p.name,
      countryName: p.country?.name || "",
    });
  }

  // Wine: (producer_id, normalized_wine_name) → wine row
  const wineMap = new Map();
  // Also: producer_id → [{ id, name, normalized }] for fuzzy wine matching
  const winesByProducer = new Map();
  for (const w of wines) {
    const normName = normalize(w.name);
    const key = `${w.producer_id}||${normName}`;
    wineMap.set(key, w);

    if (!winesByProducer.has(w.producer_id)) winesByProducer.set(w.producer_id, []);
    winesByProducer.get(w.producer_id).push({
      id: w.id,
      name: w.name,
      normalized: normName,
      countryName: w.country?.name || "",
    });
  }

  // All producer names for fuzzy candidate search
  const allProducerNames = [...producerMap.keys()];

  console.log(`  Producer lookup: ${producerMap.size} normalized names`);
  console.log(`  Wine lookup: ${wineMap.size} (producer, wine) pairs\n`);

  // ── Pass 1: Exact normalized match ──────────────────────
  console.log("--- Pass 1: Exact normalized matching ---");
  const matched = [];
  const unmatched = [];

  for (const listing of listings) {
    if (!listing.winery_name || !listing.wine_name) {
      unmatched.push(listing);
      continue;
    }

    const normWinery = normalize(listing.winery_name);
    const normWine = normalize(listing.wine_name);
    const normCountry = normalizeCountry(listing.country_name);

    // Find producer
    const producerCandidates = producerMap.get(normWinery);
    if (!producerCandidates) {
      unmatched.push(listing);
      continue;
    }

    // If multiple producers with same name, prefer country match
    let producer = producerCandidates[0];
    if (producerCandidates.length > 1 && normCountry) {
      const countryMatch = producerCandidates.find(
        (p) => normalizeCountry(p.countryName) === normCountry
      );
      if (countryMatch) producer = countryMatch;
    }

    // Find wine — exact match
    const wineKey = `${producer.id}||${normWine}`;
    let wine = wineMap.get(wineKey);

    // Try substring match if exact fails (Vivino names are often shorter)
    if (!wine) {
      const producerWines = winesByProducer.get(producer.id) || [];
      wine = producerWines.find(
        (w) => w.normalized.includes(normWine) || normWine.includes(w.normalized)
      );
    }

    if (wine) {
      matched.push({ listing, producerId: producer.id, wineId: wine.id || wine.id });
    } else {
      unmatched.push(listing);
    }
  }

  console.log(`  Exact matches: ${matched.length}/${listings.length} (${Math.round((matched.length / listings.length) * 100)}%)`);
  console.log(`  Unmatched: ${unmatched.length}\n`);

  // ── Pass 2: Haiku fuzzy match ────────────────────────────
  let haikuMatched = [];
  let haikuTokens = { input: 0, output: 0 };

  if (!SKIP_HAIKU && unmatched.length > 0) {
    console.log("--- Pass 2: Haiku fuzzy matching ---");
    const candidateBatches = [];
    const unmatchedWithCandidates = [];

    for (const listing of unmatched) {
      if (!listing.winery_name) continue;
      const normWinery = normalize(listing.winery_name);

      // Find top 3 similar producer names
      const scored = allProducerNames
        .map((name) => ({
          name,
          dist: levenshtein(normWinery, name),
          lenRatio: Math.abs(normWinery.length - name.length),
        }))
        .filter((s) => s.dist <= Math.max(normWinery.length * 0.4, 5)) // Max 40% edit distance
        .sort((a, b) => a.dist - b.dist)
        .slice(0, 3);

      if (scored.length === 0) continue;

      const candidates = scored.map((s) => {
        const prods = producerMap.get(s.name);
        const prod = prods[0];
        // Get first wine from this producer as example
        const prodWines = winesByProducer.get(prod.id) || [];
        // Find best wine name match
        const normWine = normalize(listing.wine_name || "");
        const bestWine = prodWines
          .map((w) => ({ ...w, dist: levenshtein(normWine, w.normalized) }))
          .sort((a, b) => a.dist - b.dist)[0];

        return {
          producerName: prod.name,
          producerId: prod.id,
          wineName: bestWine ? bestWine.name : "(no matching wine)",
          wineId: bestWine ? bestWine.id : null,
          countryName: prod.countryName,
        };
      });

      unmatchedWithCandidates.push({ vivino: listing, candidates });
    }

    console.log(`  Listings with candidates: ${unmatchedWithCandidates.length}`);

    // Process in batches of 10 for Haiku
    const BATCH_SIZE = 10;
    const CONCURRENCY = 3;
    let haikuProcessed = 0;

    for (let i = 0; i < unmatchedWithCandidates.length; i += BATCH_SIZE) {
      const batch = unmatchedWithCandidates.slice(i, i + BATCH_SIZE);

      try {
        const { results, inputTokens, outputTokens } = await haikuMatch(batch);
        haikuTokens.input += inputTokens;
        haikuTokens.output += outputTokens;

        for (const r of results) {
          if (r.match === "none" || !r.match) continue;
          const idx = r.index;
          if (idx == null || idx >= batch.length) continue;

          const item = batch[idx];
          const candIdx = r.match.charCodeAt(0) - 65; // A=0, B=1, C=2
          if (candIdx < 0 || candIdx >= item.candidates.length) continue;

          const cand = item.candidates[candIdx];
          if (cand.wineId) {
            haikuMatched.push({
              listing: item.vivino,
              producerId: cand.producerId,
              wineId: cand.wineId,
            });
          }
        }

        haikuProcessed += batch.length;
        process.stdout.write(
          `  Haiku: ${haikuProcessed}/${unmatchedWithCandidates.length} processed, ${haikuMatched.length} matched\r`
        );

        // Small delay between Haiku calls
        if (i + BATCH_SIZE < unmatchedWithCandidates.length) {
          await new Promise((r) => setTimeout(r, 200));
        }
      } catch (err) {
        console.error(`\n  Haiku batch error at ${i}: ${err.message}`);
      }
    }

    const haikuCost =
      (haikuTokens.input * 0.8 + haikuTokens.output * 4) / 1_000_000;
    console.log(`\n  Haiku matches: ${haikuMatched.length}`);
    console.log(
      `  Haiku tokens: ${haikuTokens.input.toLocaleString()} in / ${haikuTokens.output.toLocaleString()} out — $${haikuCost.toFixed(4)}`
    );
  }

  // ── Summary ──────────────────────────────────────────────
  const allMatched = [...matched, ...haikuMatched];
  const finalUnmatched = unmatched.filter(
    (l) => !haikuMatched.some((h) => h.listing === l)
  );

  console.log("\n=== RESULTS ===");
  console.log(`  Total listings: ${listings.length}`);
  console.log(`  Pass 1 (exact): ${matched.length}`);
  console.log(`  Pass 2 (Haiku): ${haikuMatched.length}`);
  console.log(
    `  Total matched: ${allMatched.length} (${Math.round((allMatched.length / listings.length) * 100)}%)`
  );
  console.log(`  Unmatched: ${finalUnmatched.length}`);

  // Price stats for matched
  const matchedWithPrice = allMatched.filter((m) => m.listing.price_usd != null);
  if (matchedWithPrice.length > 0) {
    const prices = matchedWithPrice.map((m) => m.listing.price_usd).sort((a, b) => a - b);
    console.log(`\n  Matched with price: ${matchedWithPrice.length}`);
    console.log(`  Price range: $${prices[0]} — $${prices[prices.length - 1]}`);
    console.log(`  Median: $${prices[Math.floor(prices.length / 2)]}`);
  }

  // Sample matches
  console.log("\n  Sample matches:");
  allMatched.slice(0, 10).forEach((m) => {
    const p = m.listing;
    console.log(`    ${p.winery_name} — ${p.wine_name} → matched (${p.price_usd ? "$" + p.price_usd : "no price"})`);
  });

  // Top unmatched
  console.log("\n  Top unmatched (by rating count):");
  finalUnmatched
    .sort((a, b) => (b.rating_count || 0) - (a.rating_count || 0))
    .slice(0, 10)
    .forEach((l) => {
      console.log(`    ${l.winery_name} — ${l.wine_name} (${l.country_name}, ${l.rating_count} ratings)`);
    });

  // ── DB Writes ──────────────────────────────────────────────
  if (DRY_RUN) {
    console.log("\n[DRY RUN] Skipping all DB inserts.");
    // Still show what would happen
    const withVintage = allMatched.filter((m) => m.listing.vintage_year);
    const withRating = allMatched.filter((m) => m.listing.rating_average);
    console.log(`  Would upsert ${withVintage.length} vintages`);
    console.log(`  Would insert ${withRating.length} scores`);
    console.log(`  Would insert ${matchedWithPrice.length} prices`);
  } else {
    // ── 1. Upsert new vintages into wine_vintages ──────────
    const vintageRows = allMatched
      .filter((m) => m.listing.vintage_year)
      .map((m) => ({
        wine_id: m.wineId,
        vintage_year: m.listing.vintage_year,
      }));

    // Deduplicate by (wine_id, vintage_year) — keep first occurrence
    const vintageDeduped = [];
    const vintageKeys = new Set();
    for (const v of vintageRows) {
      const key = `${v.wine_id}||${v.vintage_year}`;
      if (!vintageKeys.has(key)) {
        vintageKeys.add(key);
        vintageDeduped.push(v);
      }
    }

    if (vintageDeduped.length > 0) {
      console.log(`\n--- Upserting ${vintageDeduped.length} vintages ---`);
      const BATCH = 500;
      let upserted = 0;
      let skipped = 0;
      for (let i = 0; i < vintageDeduped.length; i += BATCH) {
        const batch = vintageDeduped.slice(i, i + BATCH);
        const { data, error } = await sb
          .from("wine_vintages")
          .upsert(batch, { onConflict: "wine_id,vintage_year", ignoreDuplicates: false })
          .select("id");
        if (error) {
          console.error(`  Vintage batch error at ${i}: ${error.message}`);
          // Try individual for error isolation
          for (const row of batch) {
            const { error: e2 } = await sb
              .from("wine_vintages")
              .upsert([row], { onConflict: "wine_id,vintage_year", ignoreDuplicates: false });
            if (!e2) upserted++;
            else {
              console.error(`    Vintage error: ${e2.message}`);
              skipped++;
            }
          }
        } else {
          upserted += batch.length;
        }
        process.stdout.write(`  Vintages: ${upserted + skipped}/${vintageDeduped.length}\r`);
      }
      console.log(`  Vintages upserted: ${upserted} (${skipped} errors)`);
    }

    // ── 2. Insert community scores into wine_vintage_scores ──
    const scoreRows = allMatched
      .filter((m) => m.listing.rating_average && m.listing.rating_count > 0)
      .map((m) => ({
        wine_id: m.wineId,
        vintage_year: m.listing.vintage_year || null,
        score: m.listing.rating_average,
        score_scale: "5",
        publication_id: VIVINO_PUBLICATION_ID,
        critic: "Vivino Community",
        is_community: true,
        rating_count: m.listing.rating_count,
        review_date: TODAY,
        url: `https://www.vivino.com/w/${m.listing.vivino_wine_id}`,
      }));

    // Deduplicate by (wine_id, vintage_year, publication_id)
    const scoreDeduped = [];
    const scoreKeys = new Set();
    for (const s of scoreRows) {
      const key = `${s.wine_id}||${s.vintage_year}||${s.publication_id}`;
      if (!scoreKeys.has(key)) {
        scoreKeys.add(key);
        scoreDeduped.push(s);
      }
    }

    if (scoreDeduped.length > 0) {
      console.log(`\n--- Inserting ${scoreDeduped.length} community scores ---`);
      const BATCH = 500;
      let inserted = 0;
      let errors = 0;
      for (let i = 0; i < scoreDeduped.length; i += BATCH) {
        const batch = scoreDeduped.slice(i, i + BATCH);
        const { error } = await sb.from("wine_vintage_scores").insert(batch);
        if (error) {
          // Try individual — may have some duplicates
          for (const row of batch) {
            const { error: e2 } = await sb.from("wine_vintage_scores").insert([row]);
            if (!e2) inserted++;
            else errors++;
          }
        } else {
          inserted += batch.length;
        }
        process.stdout.write(`  Scores: ${inserted + errors}/${scoreDeduped.length}\r`);
      }
      console.log(`  Scores inserted: ${inserted} (${errors} errors/dupes)`);
    }

    // ── 3. Insert prices into wine_vintage_prices ────────────
    const priceRowsRaw = matchedWithPrice.map((m) => ({
      wine_id: m.wineId,
      vintage_year: m.listing.vintage_year || null,
      price_usd: m.listing.price_usd,
      price_original: m.listing.price_raw,
      currency: "USD",
      price_type: "retail",
      source_url: m.listing.source_url,
      merchant_name: m.listing.merchant_name || "Vivino Marketplace",
      price_date: TODAY,
    }));

    // Deduplicate by (wine_id, vintage_year, price_usd, merchant_name)
    const priceRows = [];
    const priceKeys = new Set();
    for (const p of priceRowsRaw) {
      const key = `${p.wine_id}||${p.vintage_year}||${p.price_usd}||${p.merchant_name}`;
      if (!priceKeys.has(key)) {
        priceKeys.add(key);
        priceRows.push(p);
      }
    }

    if (priceRows.length > 0) {
      console.log(`\n--- Inserting ${priceRows.length} price records (deduped from ${priceRowsRaw.length}) ---`);
      const BATCH = 500;
      let inserted = 0;
      let errors = 0;
      for (let i = 0; i < priceRows.length; i += BATCH) {
        const batch = priceRows.slice(i, i + BATCH);
        const { error } = await sb.from("wine_vintage_prices").insert(batch);
        if (error) {
          for (const row of batch) {
            const { error: e2 } = await sb.from("wine_vintage_prices").insert([row]);
            if (!e2) inserted++;
            else errors++;
          }
        } else {
          inserted += batch.length;
        }
        process.stdout.write(`  Prices: ${inserted + errors}/${priceRows.length}\r`);
      }
      console.log(`  Prices inserted: ${inserted} (${errors} errors)`);
    }

    console.log("\n--- DB Summary ---");
    console.log(`  Vintages: ${vintageDeduped.length} upserted`);
    console.log(`  Scores: ${scoreDeduped.length} inserted`);
    console.log(`  Prices: ${priceRows.length} inserted`);
  }

  // Save unmatched for review / future new-wine import
  writeFileSync(
    "vivino_unmatched.json",
    JSON.stringify(finalUnmatched, null, 2)
  );
  console.log(`\nSaved ${finalUnmatched.length} unmatched listings to vivino_unmatched.json`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
