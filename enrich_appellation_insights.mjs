#!/usr/bin/env node
/**
 * enrich_appellation_insights.mjs
 *
 * Enriches all 752 appellations with AI-generated insights using Claude Sonnet.
 * Writes results to the appellation_insights table via Supabase.
 *
 * Usage:
 *   node enrich_appellation_insights.mjs              # Run all unenriched
 *   node enrich_appellation_insights.mjs --force      # Re-run all (overwrite)
 *   node enrich_appellation_insights.mjs --dry-run    # Preview without writing
 *   node enrich_appellation_insights.mjs --limit 10   # Process only N
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
const CONCURRENCY = 3;  // parallel API calls
const MAX_GRAPES = 15;  // top grapes per appellation
const MAX_TOKENS = 1500;
const EXPECTED_KEYS = ["ai_overview", "ai_climate_profile", "ai_soil_profile", "ai_signature_style", "ai_key_grapes", "ai_aging_generalization", "confidence"];
const BANNED_WORDS = ["prestigious", "world-class", "exceptional", "unparalleled", "legendary", "iconic", "finest", "renowned"];

const SYSTEM_PROMPT = `You are a wine expert writing for Loam, a wine intelligence platform. You write with genuine love for wine and deep knowledge — the way a great winemaker talks about their craft. Be specific, grounded, and real. Never use marketing fluff or generic wine-magazine language.

When you write about an appellation, write like someone who has walked the vineyards and tasted the wines. Use specific details — soil types, elevations, microclimates, grape varieties.

HANDLING UNCERTAINTY: If you don't know specific details about a lesser-known appellation:
- Write shorter entries (1 sentence is fine).
- State the general climate zone and likely soil family rather than guessing specifics.
- Set confidence to 0.5 or lower.
- A honest one-sentence entry is always better than a padded three-sentence guess.

Return a JSON object with EXACTLY these keys (no others):

{
  "ai_overview": "...",
  "ai_climate_profile": "...",
  "ai_soil_profile": "...",
  "ai_signature_style": "...",
  "ai_key_grapes": "...",
  "ai_aging_generalization": "...",
  "confidence": 0.9
}

Field guidelines:
- ai_overview (2-4 sentences): What this place is and why it matters. Lead with what makes it distinctive.
- ai_climate_profile (2-3 sentences): The climate that shapes the wines. Be specific about the weather patterns that matter for grape growing.
- ai_soil_profile (2-3 sentences): What's in the ground and why it matters. Name actual soil types and parent rock.
- ai_signature_style (2-3 sentences): What wines from here taste and feel like. Sensory language rooted in the place.
- ai_key_grapes (1-2 sentences): The varieties that define this appellation and why they work here.
- ai_aging_generalization (1-2 sentences): How wines from here typically age.
- confidence: Your honest self-assessment. 0.9 = major appellation you know deeply. 0.7 = you know it moderately well. 0.5 = you know basics only. 0.3 = you're mostly guessing.

CRITICAL RULES:
- Do NOT reference database counts, wine counts, or producer counts.
- You may mention a producer by name ONLY if they genuinely defined or shaped the appellation (e.g., a pioneer who put the region on the map). Keep it to 1-2 names max, woven naturally into the narrative — never a list.
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

// ── Fetch appellation data ──────────────────────────────────
async function fetchAppellations() {
  // Get all appellations with region/country
  const { data: appellations, error: appErr } = await supabase
    .from("appellations")
    .select("id, name, designation_type, region:regions(name, country:countries(name))")
    .order("name");

  if (appErr) throw new Error(`Failed to fetch appellations: ${appErr.message}`);

  // Get already enriched IDs (unless --force)
  let enrichedIds = new Set();
  if (!FORCE) {
    const { data: existing, error: exErr } = await supabase
      .from("appellation_insights")
      .select("appellation_id");
    if (exErr) throw new Error(`Failed to fetch existing insights: ${exErr.message}`);
    enrichedIds = new Set(existing.map(e => e.appellation_id));
  }

  // Filter to unenriched
  const toProcess = appellations.filter(a => !enrichedIds.has(a.id));

  return { toProcess };
}

// ── Fetch top grapes per appellation ────────────────────────
async function fetchGrapeMap(appellationIds) {
  const grapesByAppellation = new Map();

  // Process in batches of 50 appellations to stay within query limits
  const BATCH_SIZE = 50;
  for (let i = 0; i < appellationIds.length; i += BATCH_SIZE) {
    const batchIds = appellationIds.slice(i, i + BATCH_SIZE);

    // Fetch wines + grapes for this batch (limit 10000 rows)
    const { data, error } = await supabase
      .from("wine_grapes")
      .select("wine:wines!inner(appellation_id), grape:grapes(name)")
      .in("wine.appellation_id", batchIds)
      .limit(10000);

    if (error) {
      // Fallback: fetch per-appellation
      console.log(`  ⚠️ Batch grape fetch failed, trying per-appellation...`);
      for (const id of batchIds) {
        const { data: rows } = await supabase
          .from("wine_grapes")
          .select("grape:grapes(name)")
          .eq("wine.appellation_id", id)
          .limit(2000);
        if (rows) {
          const counts = new Map();
          for (const r of rows) {
            const name = r.grape?.name;
            if (name) counts.set(name, (counts.get(name) || 0) + 1);
          }
          if (counts.size > 0) {
            grapesByAppellation.set(id, [...counts.entries()]
              .sort((a, b) => b[1] - a[1])
              .slice(0, MAX_GRAPES)
              .map(([name]) => name));
          }
        }
      }
      continue;
    }

    if (data) {
      const counts = new Map(); // appId -> Map<grapeName, count>
      for (const row of data) {
        const appId = row.wine?.appellation_id;
        const grapeName = row.grape?.name;
        if (!appId || !grapeName) continue;
        if (!counts.has(appId)) counts.set(appId, new Map());
        const gc = counts.get(appId);
        gc.set(grapeName, (gc.get(grapeName) || 0) + 1);
      }
      for (const [appId, gc] of counts) {
        grapesByAppellation.set(appId, [...gc.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, MAX_GRAPES)
          .map(([name]) => name));
      }
    }
  }

  return grapesByAppellation;
}

// ── Process a single appellation ────────────────────────────
async function processAppellation(app, grapes) {
  const country = app.region?.country?.name || "Unknown";
  const region = app.region?.name || "Unknown";

  const userMsg = `Write appellation insights for:

Name: ${app.name}
Designation: ${app.designation_type || "Unknown"}
Country: ${country}
Region: ${region}${grapes && grapes.length > 0 ? `\nKey grapes: ${grapes.slice(0, MAX_GRAPES).join(", ")}` : ""}`;

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
async function writeInsight(appellationId, parsed) {
  const row = {
    appellation_id: appellationId,
    ai_overview: parsed.ai_overview,
    ai_climate_profile: parsed.ai_climate_profile,
    ai_soil_profile: parsed.ai_soil_profile,
    ai_signature_style: parsed.ai_signature_style,
    ai_key_grapes: parsed.ai_key_grapes,
    ai_aging_generalization: parsed.ai_aging_generalization,
    confidence: parsed.confidence,
    enriched_at: new Date().toISOString(),
    refresh_after: new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString(), // 90 days
  };

  const { error } = await supabase
    .from("appellation_insights")
    .upsert(row, { onConflict: "appellation_id" });

  if (error) throw new Error(`Supabase write failed: ${error.message}`);
}

// ── Process batch with concurrency ──────────────────────────
async function processBatch(batch, grapesByAppellation) {
  const results = await Promise.all(
    batch.map(async (app) => {
      const grapes = grapesByAppellation.get(app.id) || [];
      const result = await processAppellation(app, grapes);
      return { app, ...result };
    })
  );
  return results;
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log("🍇 Appellation Insights Enrichment Pipeline");
  console.log(`   Model: Claude Sonnet | Concurrency: ${CONCURRENCY}`);
  console.log(`   Force: ${FORCE} | Dry run: ${DRY_RUN}${LIMIT ? ` | Limit: ${LIMIT}` : ""}`);
  console.log();

  // Fetch appellation list
  const { toProcess } = await fetchAppellations();
  const total = LIMIT ? Math.min(toProcess.length, LIMIT) : toProcess.length;
  const appellations = toProcess.slice(0, total);

  console.log(`📋 ${total} appellations to process (${toProcess.length} unenriched total)\n`);

  if (total === 0) {
    console.log("Nothing to do!");
    return;
  }

  // Fetch grapes for all appellations
  console.log("🔍 Fetching grape data...");
  const grapesByAppellation = await fetchGrapeMap(appellations.map(a => a.id));
  console.log(`   ${grapesByAppellation.size} appellations have grape data\n`);

  // Process in batches
  let processed = 0;
  let succeeded = 0;
  let warnings = 0;
  let failed = 0;
  let totalInputTokens = 0;
  let totalOutputTokens = 0;
  const errors = [];
  const startTime = Date.now();

  for (let i = 0; i < appellations.length; i += CONCURRENCY) {
    const batch = appellations.slice(i, i + CONCURRENCY);
    const results = await processBatch(batch, grapesByAppellation);

    for (const r of results) {
      processed++;
      const country = r.app.region?.country?.name || "?";
      const label = `${r.app.name} (${country})`;

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
        warnings++;
        console.log(`  ⚠️ ${processed}/${total} ${label} (conf: ${r.parsed.confidence}) — ${r.warnings.join("; ")}`);
      } else {
        console.log(`  ✅ ${processed}/${total} ${label} (conf: ${r.parsed.confidence})`);
      }

      // Write to DB
      if (!DRY_RUN) {
        try {
          await writeInsight(r.app.id, r.parsed);
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
  console.log(`  Warnings:        ${warnings}`);
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
