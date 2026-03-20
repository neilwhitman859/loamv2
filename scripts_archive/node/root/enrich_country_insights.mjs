#!/usr/bin/env node
/**
 * enrich_country_insights.mjs
 *
 * Enriches all 62 countries with AI-generated insights using Claude Sonnet.
 * Writes results to the country_insights table via Supabase.
 *
 * Usage:
 *   node enrich_country_insights.mjs              # Run all unenriched
 *   node enrich_country_insights.mjs --force      # Re-run all (overwrite)
 *   node enrich_country_insights.mjs --dry-run    # Preview without writing
 *   node enrich_country_insights.mjs --limit 10   # Process only N
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
  process.env[trimmed.slice(0, eqIdx)] = trimmed.slice(eqIdx + 1);
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
const EXPECTED_KEYS = [
  "ai_overview", "ai_wine_history", "ai_key_regions",
  "ai_signature_styles", "ai_regulatory_overview", "confidence"
];
const BANNED_WORDS = [
  "prestigious", "world-class", "exceptional", "unparalleled",
  "legendary", "iconic", "finest", "renowned"
];

const SYSTEM_PROMPT = `You are a wine expert writing for Loam, a wine intelligence platform. You write with genuine love for wine and deep knowledge — the way a great winemaker talks about their craft. Be specific, grounded, and real. Never use marketing fluff or generic wine-magazine language.

When you write about a wine country, write like someone who has traveled extensively through its regions, understands its traditions and modern evolution, and can speak to both the classics and the emerging stories.

HANDLING UNCERTAINTY: If you don't know specific details about a lesser-known wine country:
- Write shorter entries (1-2 sentences is fine).
- State what's generally known rather than guessing specifics.
- Set confidence to 0.5 or lower.
- An honest short entry is always better than a padded guess.

Return a JSON object with EXACTLY these keys (no others):

{
  "ai_overview": "...",
  "ai_wine_history": "...",
  "ai_key_regions": "...",
  "ai_signature_styles": "...",
  "ai_regulatory_overview": "...",
  "confidence": 0.9
}

Field guidelines:
- ai_overview (2-4 sentences): This country's wine identity — what defines it, what role it plays in the global wine landscape.
- ai_wine_history (2-4 sentences): The arc of wine in this country. Ancient roots, key turning points, modern evolution. What shaped it?
- ai_key_regions (3-5 sentences): The major wine regions and what distinguishes each. Focus on the regions that matter most and how they differ from each other.
- ai_signature_styles (2-4 sentences): The wines this country is known for. What are the flagship styles? What should someone expect when they pick up a bottle?
- ai_regulatory_overview (2-3 sentences): How wine is classified and labeled in this country. What system(s) govern quality tiers, geographic designations, and labeling rules?
- confidence: Your honest self-assessment. 0.9+ = major wine country you know deeply. 0.7-0.8 = well-known wine country. 0.5-0.6 = you know basics. 0.3-0.4 = mostly guessing.

CRITICAL RULES:
- Do NOT reference database counts, wine counts, or producer counts from the context data.
- The context data helps ground your response — use it to inform which regions and grapes you discuss, but don't quote numbers.
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
        body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: maxTokens, messages }),
      });
      if (res.status === 429 || res.status === 529) {
        const wait = Math.min(2 ** attempt * 2000, 30000);
        console.log(`  ⏳ Rate limited (${res.status}), waiting ${wait / 1000}s...`);
        await new Promise(r => setTimeout(r, wait));
        continue;
      }
      if (!res.ok) throw new Error(`Sonnet ${res.status}: ${await res.text()}`);
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
  if (typeof parsed.confidence !== "number" || parsed.confidence < 0 || parsed.confidence > 1)
    warnings.push(`Bad confidence: ${parsed.confidence}`);
  for (const key of EXPECTED_KEYS.filter(k => k !== "confidence")) {
    if (typeof parsed[key] === "string" && parsed[key].trim().length === 0)
      warnings.push(`Empty field: ${key}`);
  }
  const allText = EXPECTED_KEYS.filter(k => k !== "confidence").map(k => parsed[k] || "").join(" ").toLowerCase();
  const found = BANNED_WORDS.filter(w => allText.includes(w));
  if (found.length) warnings.push(`Banned words: ${found.join(", ")}`);
  return warnings;
}

// ── Fetch countries ─────────────────────────────────────────
async function fetchCountries() {
  const { data: countries, error } = await supabase
    .from("countries")
    .select("id, name")
    .is("deleted_at", null)
    .order("name");

  if (error) throw new Error(`Failed to fetch countries: ${error.message}`);

  let enrichedIds = new Set();
  if (!FORCE) {
    const { data: existing, error: exErr } = await supabase
      .from("country_insights")
      .select("country_id");
    if (exErr) throw new Error(`Failed to fetch existing: ${exErr.message}`);
    enrichedIds = new Set(existing.map(e => e.country_id));
  }

  return countries.filter(c => !enrichedIds.has(c.id));
}

// ── Fetch context per country ───────────────────────────────
async function fetchCountryContext(countryIds) {
  const contextMap = new Map();

  for (const cid of countryIds) {
    // Top regions (non-catch-all)
    const { data: regionData } = await supabase
      .from("regions")
      .select("name, slug")
      .eq("country_id", cid)
      .is("deleted_at", null)
      .order("name");

    const regions = (regionData || []).filter(r => !r.slug.endsWith("-country")).map(r => r.name);

    // Top appellations
    const { data: appData } = await supabase
      .from("appellations")
      .select("name")
      .eq("country_id", cid)
      .limit(10);

    const appellations = (appData || []).map(a => a.name);

    // Top grapes
    const { data: grapeData } = await supabase
      .from("wine_grapes")
      .select("grape:grapes(name), wine:wines!inner(country_id)")
      .eq("wine.country_id", cid)
      .limit(5000);

    const grapeCounts = new Map();
    if (grapeData) {
      for (const row of grapeData) {
        const name = row.grape?.name;
        if (name) grapeCounts.set(name, (grapeCounts.get(name) || 0) + 1);
      }
    }
    const topGrapes = [...grapeCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map(([n]) => n);

    contextMap.set(cid, { regions, appellations, grapes: topGrapes });
  }

  return contextMap;
}

// ── Process a single country ────────────────────────────────
async function processCountry(country, context) {
  let userMsg = `Write country insights for:\n\nCountry: ${country.name}`;
  if (context.regions.length > 0) userMsg += `\nMajor regions: ${context.regions.join(", ")}`;
  if (context.appellations.length > 0) userMsg += `\nTop appellations: ${context.appellations.join(", ")}`;
  if (context.grapes.length > 0) userMsg += `\nTop grapes: ${context.grapes.join(", ")}`;

  const result = await callSonnet([
    { role: "user", content: SYSTEM_PROMPT + "\n\n" + userMsg },
    { role: "assistant", content: "{" }
  ], MAX_TOKENS);

  if (result.stop_reason === "max_tokens") {
    return { error: "TRUNCATED", tokens: result.usage };
  }

  let text = "{" + result.content[0].text.trim();
  if (text.includes("```")) text = text.replace(/```(?:json)?\s*/g, "").replace(/\s*```/g, "");

  try {
    const parsed = JSON.parse(text);
    return { parsed, warnings: validateResponse(parsed), tokens: result.usage };
  } catch (e) {
    return { error: `JSON parse failed: ${e.message}`, raw: text.slice(0, 200), tokens: result.usage };
  }
}

// ── Write to Supabase ───────────────────────────────────────
async function writeInsight(countryId, parsed) {
  const row = {
    country_id: countryId,
    ai_overview: parsed.ai_overview,
    ai_wine_history: parsed.ai_wine_history,
    ai_key_regions: parsed.ai_key_regions,
    ai_signature_styles: parsed.ai_signature_styles,
    ai_regulatory_overview: parsed.ai_regulatory_overview,
    confidence: parsed.confidence,
    enriched_at: new Date().toISOString(),
    refresh_after: new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString(),
  };
  const { error } = await supabase.from("country_insights").upsert(row, { onConflict: "country_id" });
  if (error) throw new Error(`Supabase write failed: ${error.message}`);
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log("🌍 Country Insights Enrichment Pipeline");
  console.log(`   Model: Claude Sonnet | Concurrency: ${CONCURRENCY}`);
  console.log(`   Force: ${FORCE} | Dry run: ${DRY_RUN}${LIMIT ? ` | Limit: ${LIMIT}` : ""}`);
  console.log();

  const toProcess = await fetchCountries();
  const total = LIMIT ? Math.min(toProcess.length, LIMIT) : toProcess.length;
  const countries = toProcess.slice(0, total);

  console.log(`📋 ${total} countries to process (${toProcess.length} unenriched)\n`);
  if (total === 0) { console.log("Nothing to do!"); return; }

  console.log("🔍 Fetching country context data...");
  const contextMap = await fetchCountryContext(countries.map(c => c.id));
  const withContext = [...contextMap.values()].filter(c => c.regions.length > 0).length;
  console.log(`   ${withContext}/${countries.length} countries have region context\n`);

  let processed = 0, succeeded = 0, warningCount = 0, failed = 0;
  let totalIn = 0, totalOut = 0;
  const errors = [];
  const startTime = Date.now();

  for (let i = 0; i < countries.length; i += CONCURRENCY) {
    const batch = countries.slice(i, i + CONCURRENCY);
    const results = await Promise.all(batch.map(async (country) => {
      const ctx = contextMap.get(country.id) || { regions: [], appellations: [], grapes: [] };
      return { country, ...(await processCountry(country, ctx)) };
    }));

    for (const r of results) {
      processed++;
      const label = r.country.name;

      if (r.tokens) { totalIn += r.tokens.input_tokens; totalOut += r.tokens.output_tokens; }

      if (r.error) {
        failed++;
        errors.push({ name: label, error: r.error });
        console.log(`  ❌ ${processed}/${total} ${label} — ${r.error}`);
        continue;
      }

      if (r.warnings?.length > 0) {
        warningCount++;
        console.log(`  ⚠️ ${processed}/${total} ${label} (conf: ${r.parsed.confidence}) — ${r.warnings.join("; ")}`);
      } else {
        console.log(`  ✅ ${processed}/${total} ${label} (conf: ${r.parsed.confidence})`);
      }

      if (!DRY_RUN) {
        try { await writeInsight(r.country.id, r.parsed); succeeded++; }
        catch (e) { failed++; errors.push({ name: label, error: e.message }); console.log(`     ❌ DB write: ${e.message}`); }
      } else { succeeded++; }
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log("\n" + "=".repeat(60));
  console.log("📊 SUMMARY");
  console.log("=".repeat(60));
  console.log(`  Total processed: ${processed}`);
  console.log(`  Succeeded:       ${succeeded}`);
  console.log(`  Warnings:        ${warningCount}`);
  console.log(`  Failed:          ${failed}`);
  console.log(`  Tokens:          ${totalIn.toLocaleString()} in / ${totalOut.toLocaleString()} out`);
  console.log(`  Est. cost:       $${((totalIn * 3 + totalOut * 15) / 1_000_000).toFixed(2)}`);
  console.log(`  Time:            ${elapsed}s`);
  if (DRY_RUN) console.log(`  ⚠️ DRY RUN — nothing written to database`);
  if (errors.length > 0) {
    console.log(`\n❌ ERRORS (${errors.length}):`);
    for (const e of errors) console.log(`  - ${e.name}: ${e.error}`);
  }
}

main().catch(e => { console.error("Fatal error:", e); process.exit(1); });
