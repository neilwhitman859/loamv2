#!/usr/bin/env node
/**
 * enrich_grape_insights.mjs
 *
 * Enriches all 707 grapes with AI-generated insights using Claude Sonnet.
 * Writes results to the grape_insights table via Supabase.
 *
 * Usage:
 *   node enrich_grape_insights.mjs              # Run all unenriched
 *   node enrich_grape_insights.mjs --force      # Re-run all (overwrite)
 *   node enrich_grape_insights.mjs --dry-run    # Preview without writing
 *   node enrich_grape_insights.mjs --limit 10   # Process only N
 */

import { readFileSync } from "fs";
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
  process.env[key] = val;
}

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── CLI args ────────────────────────────────────────────────
const args = process.argv.slice(2);
const FORCE = args.includes("--force");
const DRY_RUN = args.includes("--dry-run");
const limitIdx = args.indexOf("--limit");
const LIMIT = limitIdx !== -1 ? parseInt(args[limitIdx + 1], 10) : null;

// ── Constants ───────────────────────────────────────────────
const CONCURRENCY = 3;
const MAX_TOKENS = 1500;
const TOP_APPELLATIONS = 10;
const TOP_COUNTRIES = 8;
const EXPECTED_KEYS = [
  "ai_overview", "ai_flavor_profile", "ai_growing_conditions",
  "ai_food_pairing", "ai_regions_of_note", "ai_aging_characteristics",
  "confidence"
];
const BANNED_WORDS = [
  "prestigious", "world-class", "exceptional", "unparalleled",
  "legendary", "iconic", "finest", "renowned"
];

const SYSTEM_PROMPT = `You are a wine expert writing for Loam, a wine intelligence platform. You write with genuine love for wine and deep knowledge — the way a great winemaker talks about their craft. Be specific, grounded, and real. Never use marketing fluff or generic wine-magazine language.

When you write about a grape variety, write like someone who has grown it, vinified it, and tasted it across many regions. Use specific sensory details, real place names, and honest assessments of how terroir shapes expression.

HANDLING UNCERTAINTY: If you don't know specific details about an obscure grape:
- Write shorter entries (1-2 sentences is fine).
- State the general flavor family and likely growing profile rather than guessing specifics.
- Set confidence to 0.5 or lower.
- An honest short entry is always better than a padded guess.

Return a JSON object with EXACTLY these keys (no others):

{
  "ai_overview": "...",
  "ai_flavor_profile": "...",
  "ai_growing_conditions": "...",
  "ai_food_pairing": "...",
  "ai_regions_of_note": "...",
  "ai_aging_characteristics": "...",
  "confidence": 0.9
}

Field guidelines:
- ai_overview (2-4 sentences): What this grape is — its origins, identity, and why it matters. What should someone know first?
- ai_flavor_profile (2-4 sentences): Aromas, flavors, texture, structure. How does this grape express itself in the glass? Be specific about tannin, acid, body, and aromatic character. Note how expression shifts across climates (cool-climate vs warm-climate styles) where relevant.
- ai_growing_conditions (2-3 sentences): What this grape needs in the vineyard. Climate preferences, vigor, ripening behavior, disease susceptibility. What makes a site good or bad for this variety?
- ai_food_pairing (3-5 sentences): What to eat with wines from this grape. Follow these rules strictly:
  * Start with classic/traditional pairings — they exist for a reason.
  * Name specific dishes and cuisines (Thai, Mexican, Korean, Southern US, Japanese, etc.).
  * Cover the full range — a Tuesday night meal AND a Saturday dinner where it fits.
  * Explain the flavor logic briefly (why the pairing works: acid cuts fat, tannin matches protein, etc.).
  * No sommelier theater — no "pairs beautifully with a delicate..." Just name the food.
  * No generic cop-outs like "pairs well with grilled meats and seafood."
- ai_regions_of_note (2-4 sentences): Where this grape shines and why. How does terroir shape its expression? Contrast different regional styles where relevant (e.g., Burgundy Pinot vs Oregon Pinot vs Central Otago Pinot). These should connect growing conditions to the flavors they produce.
- ai_aging_characteristics (1-3 sentences): How wines from this grape evolve over time. What develops, what fades? What's the typical drinking window range across quality levels?
- confidence: Your honest self-assessment. 0.9+ = major international grape you know deeply. 0.7-0.8 = well-known regional grape. 0.5-0.6 = you know basics. 0.3-0.4 = mostly guessing.

CRITICAL RULES:
- Do NOT reference database counts, wine counts, or producer counts from the context data.
- The context data (appellations, countries) helps ground your response — use it to inform which regions you discuss, but don't quote the numbers.
- No marketing language. No "prestigious", "world-class", "exceptional", "unparalleled", "legendary", "iconic", "finest", "renowned".
- No markdown code fences. Start your response with the opening brace.
- Every field must have a value — use shorter honest text for fields you're less sure about.`;

// ── API call with retry ─────────────────────────────────────
async function callSonnet(messages, maxTokens = MAX_TOKENS) {
  const maxRetries = 3;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": ANTHROPIC_API_KEY,
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify({
          model: "claude-sonnet-4-20250514",
          max_tokens: maxTokens,
          messages,
        }),
      });

      if (res.status === 429 || res.status === 529) {
        const wait = Math.min(2 ** attempt * 2000, 30000);
        console.log(`  ⏳ Rate limited (${res.status}), waiting ${wait / 1000}s...`);
        await new Promise(r => setTimeout(r, wait));
        continue;
      }

      if (!res.ok) {
        const err = await res.text();
        throw new Error(`Sonnet ${res.status}: ${err}`);
      }

      return await res.json();
    } catch (e) {
      if (attempt === maxRetries) throw e;
      const wait = 2 ** attempt * 1000;
      console.log(`  ⚠️ Attempt ${attempt} failed: ${e.message}, retrying in ${wait / 1000}s...`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

// ── Validation ──────────────────────────────────────────────
function validateResponse(parsed) {
  const warnings = [];
  const missing = EXPECTED_KEYS.filter(k => !(k in parsed));
  if (missing.length) warnings.push(`Missing keys: ${missing.join(", ")}`);

  const extra = Object.keys(parsed).filter(k => !EXPECTED_KEYS.includes(k));
  if (extra.length) warnings.push(`Unexpected keys: ${extra.join(", ")}`);

  if (typeof parsed.confidence !== "number" || parsed.confidence < 0 || parsed.confidence > 1) {
    warnings.push(`Bad confidence: ${parsed.confidence}`);
  }

  for (const key of EXPECTED_KEYS.filter(k => k !== "confidence")) {
    if (typeof parsed[key] === "string" && parsed[key].trim().length === 0) {
      warnings.push(`Empty field: ${key}`);
    }
  }

  const allText = EXPECTED_KEYS.filter(k => k !== "confidence").map(k => parsed[k] || "").join(" ").toLowerCase();
  const found = BANNED_WORDS.filter(w => allText.includes(w));
  if (found.length) warnings.push(`Banned words: ${found.join(", ")}`);

  return warnings;
}

// ── Fetch grapes to process ─────────────────────────────────
async function fetchGrapes() {
  const { data: grapes, error: gErr } = await supabase
    .from("grapes")
    .select("id, name, color, origin_country:countries(name)")
    .is("deleted_at", null)
    .order("name");

  if (gErr) throw new Error(`Failed to fetch grapes: ${gErr.message}`);

  // Get already enriched IDs (unless --force)
  let enrichedIds = new Set();
  if (!FORCE) {
    const { data: existing, error: exErr } = await supabase
      .from("grape_insights")
      .select("grape_id");
    if (exErr) throw new Error(`Failed to fetch existing insights: ${exErr.message}`);
    enrichedIds = new Set(existing.map(e => e.grape_id));
  }

  const toProcess = grapes.filter(g => !enrichedIds.has(g.id));
  return { toProcess, totalGrapes: grapes.length };
}

// ── Fetch top appellations & countries per grape ─────────────
async function fetchGrapeContext(grapeIds) {
  const contextMap = new Map(); // grapeId -> { appellations: [], countries: [] }

  // Build context in batches via raw SQL for efficiency
  const BATCH_SIZE = 50;

  for (let i = 0; i < grapeIds.length; i += BATCH_SIZE) {
    const batchIds = grapeIds.slice(i, i + BATCH_SIZE);
    const idList = batchIds.map(id => `'${id}'`).join(",");

    // Top appellations per grape
    const { data: appRows, error: appErr } = await supabase.rpc("exec_sql", { query: "" }).maybeSingle();
    // Use direct SQL via supabase — fall back to join queries
    const { data: appellationData, error: aErr } = await supabase
      .from("wine_grapes")
      .select("grape_id, wine:wines!inner(appellation:appellations(name), country:countries(name))")
      .in("grape_id", batchIds)
      .limit(10000);

    if (aErr) {
      console.log(`  ⚠️ Batch context fetch failed for batch ${Math.floor(i / BATCH_SIZE) + 1}: ${aErr.message}`);
      // Initialize empty context for this batch
      for (const id of batchIds) {
        if (!contextMap.has(id)) contextMap.set(id, { appellations: [], countries: [] });
      }
      continue;
    }

    if (appellationData) {
      // Tally appellations and countries per grape
      const appCounts = new Map(); // grapeId -> Map<appName, { count, country }>
      const countryCounts = new Map(); // grapeId -> Map<countryName, count>

      for (const row of appellationData) {
        const grapeId = row.grape_id;
        const appName = row.wine?.appellation?.name;
        const countryName = row.wine?.country?.name;

        if (grapeId && appName) {
          if (!appCounts.has(grapeId)) appCounts.set(grapeId, new Map());
          const ac = appCounts.get(grapeId);
          const existing = ac.get(appName) || { count: 0, country: countryName || "Unknown" };
          existing.count++;
          ac.set(appName, existing);
        }

        if (grapeId && countryName) {
          if (!countryCounts.has(grapeId)) countryCounts.set(grapeId, new Map());
          const cc = countryCounts.get(grapeId);
          cc.set(countryName, (cc.get(countryName) || 0) + 1);
        }
      }

      for (const id of batchIds) {
        const ac = appCounts.get(id) || new Map();
        const cc = countryCounts.get(id) || new Map();

        const topApps = [...ac.entries()]
          .sort((a, b) => b[1].count - a[1].count)
          .slice(0, TOP_APPELLATIONS)
          .map(([name, { country }]) => `${name} (${country})`);

        const topCountries = [...cc.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, TOP_COUNTRIES)
          .map(([name]) => name);

        contextMap.set(id, { appellations: topApps, countries: topCountries });
      }
    }
  }

  return contextMap;
}

// ── Process a single grape ──────────────────────────────────
async function processGrape(grape, context) {
  const originCountry = grape.origin_country?.name || null;

  let userMsg = `Write grape insights for:\n\nGrape: ${grape.name}\nColor: ${grape.color || "Unknown"}`;
  if (originCountry) userMsg += `\nOrigin country: ${originCountry}`;
  if (context.appellations.length > 0) {
    userMsg += `\nTop appellations in our database: ${context.appellations.join(", ")}`;
  }
  if (context.countries.length > 0) {
    userMsg += `\nTop countries: ${context.countries.join(", ")}`;
  }

  const result = await callSonnet([
    { role: "user", content: SYSTEM_PROMPT + "\n\n" + userMsg },
    { role: "assistant", content: "{" }
  ], MAX_TOKENS);

  // Check truncation
  if (result.stop_reason === "max_tokens") {
    return { error: "TRUNCATED", tokens: result.usage };
  }

  let text = "{" + result.content[0].text.trim();
  if (text.includes("```")) {
    text = text.replace(/```(?:json)?\s*/g, "").replace(/\s*```/g, "");
  }

  try {
    const parsed = JSON.parse(text);
    const warnings = validateResponse(parsed);
    return { parsed, warnings, tokens: result.usage };
  } catch (e) {
    return { error: `JSON parse failed: ${e.message}`, raw: text.slice(0, 200), tokens: result.usage };
  }
}

// ── Write to Supabase ───────────────────────────────────────
async function writeInsight(grapeId, parsed) {
  const row = {
    grape_id: grapeId,
    ai_overview: parsed.ai_overview,
    ai_flavor_profile: parsed.ai_flavor_profile,
    ai_growing_conditions: parsed.ai_growing_conditions,
    ai_food_pairing: parsed.ai_food_pairing,
    ai_regions_of_note: parsed.ai_regions_of_note,
    ai_aging_characteristics: parsed.ai_aging_characteristics,
    confidence: parsed.confidence,
    enriched_at: new Date().toISOString(),
    refresh_after: new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString(),
  };

  const { error } = await supabase
    .from("grape_insights")
    .upsert(row, { onConflict: "grape_id" });

  if (error) throw new Error(`Supabase write failed: ${error.message}`);
}

// ── Process batch with concurrency ──────────────────────────
async function processBatch(batch, contextMap) {
  return Promise.all(
    batch.map(async (grape) => {
      const context = contextMap.get(grape.id) || { appellations: [], countries: [] };
      const result = await processGrape(grape, context);
      return { grape, ...result };
    })
  );
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log("🍇 Grape Insights Enrichment Pipeline");
  console.log(`   Model: Claude Sonnet | Concurrency: ${CONCURRENCY}`);
  console.log(`   Force: ${FORCE} | Dry run: ${DRY_RUN}${LIMIT ? ` | Limit: ${LIMIT}` : ""}`);
  console.log();

  // Fetch grape list
  const { toProcess, totalGrapes } = await fetchGrapes();
  const total = LIMIT ? Math.min(toProcess.length, LIMIT) : toProcess.length;
  const grapes = toProcess.slice(0, total);

  console.log(`📋 ${total} grapes to process (${totalGrapes} total, ${toProcess.length} unenriched)\n`);

  if (total === 0) {
    console.log("Nothing to do!");
    return;
  }

  // Fetch context data (top appellations + countries per grape)
  console.log("🔍 Fetching grape context data (appellations, countries)...");
  const contextMap = await fetchGrapeContext(grapes.map(g => g.id));
  const withContext = [...contextMap.values()].filter(c => c.appellations.length > 0).length;
  console.log(`   ${withContext}/${grapes.length} grapes have appellation context\n`);

  // Process in batches
  let processed = 0;
  let succeeded = 0;
  let warningCount = 0;
  let failed = 0;
  let totalInputTokens = 0;
  let totalOutputTokens = 0;
  const errors = [];
  const startTime = Date.now();

  for (let i = 0; i < grapes.length; i += CONCURRENCY) {
    const batch = grapes.slice(i, i + CONCURRENCY);
    const results = await processBatch(batch, contextMap);

    for (const r of results) {
      processed++;
      const label = `${r.grape.name} (${r.grape.color || "?"})`;

      if (r.tokens) {
        totalInputTokens += r.tokens.input_tokens;
        totalOutputTokens += r.tokens.output_tokens;
      }

      if (r.error) {
        failed++;
        errors.push({ name: label, error: r.error });
        console.log(`  ❌ ${processed}/${total} ${label} — ${r.error}`);
        continue;
      }

      if (r.warnings && r.warnings.length > 0) {
        warningCount++;
        console.log(`  ⚠️ ${processed}/${total} ${label} (conf: ${r.parsed.confidence}) — ${r.warnings.join("; ")}`);
      } else {
        console.log(`  ✅ ${processed}/${total} ${label} (conf: ${r.parsed.confidence})`);
      }

      // Write to DB
      if (!DRY_RUN) {
        try {
          await writeInsight(r.grape.id, r.parsed);
          succeeded++;
        } catch (e) {
          failed++;
          errors.push({ name: label, error: e.message });
          console.log(`     ❌ DB write failed: ${e.message}`);
        }
      } else {
        succeeded++;
      }
    }

    // Progress summary every 30
    if (processed % 30 === 0 && processed < total) {
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = processed / elapsed;
      const remaining = Math.round((total - processed) / rate);
      console.log(`\n  ── ${processed}/${total} done | ${succeeded} ok, ${failed} failed | ~${remaining}s remaining ──\n`);
    }
  }

  // Final summary
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log("\n" + "=".repeat(60));
  console.log("📊 SUMMARY");
  console.log("=".repeat(60));
  console.log(`  Total processed: ${processed}`);
  console.log(`  Succeeded:       ${succeeded}`);
  console.log(`  Warnings:        ${warningCount}`);
  console.log(`  Failed:          ${failed}`);
  console.log(`  Tokens:          ${totalInputTokens.toLocaleString()} in / ${totalOutputTokens.toLocaleString()} out`);
  console.log(`  Est. cost:       $${((totalInputTokens * 3 + totalOutputTokens * 15) / 1_000_000).toFixed(2)}`);
  console.log(`  Time:            ${elapsed}s`);
  if (DRY_RUN) console.log(`  ⚠️ DRY RUN — nothing written to database`);

  if (errors.length > 0) {
    console.log(`\n❌ ERRORS (${errors.length}):`);
    for (const e of errors) {
      console.log(`  - ${e.name}: ${e.error}`);
    }
  }
}

main().catch(e => {
  console.error("Fatal error:", e);
  process.exit(1);
});
