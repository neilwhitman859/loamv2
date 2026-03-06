#!/usr/bin/env node
/**
 * enrich_region_insights.mjs
 *
 * Enriches all 328 regions with AI-generated insights using Claude Sonnet.
 * Includes country catch-all regions (e.g., france-country) — treated as
 * broadly representative everyday wines from that country.
 *
 * Usage:
 *   node enrich_region_insights.mjs              # Run all unenriched
 *   node enrich_region_insights.mjs --force      # Re-run all (overwrite)
 *   node enrich_region_insights.mjs --dry-run    # Preview without writing
 *   node enrich_region_insights.mjs --limit 10   # Process only N
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
  "ai_overview", "ai_climate_profile", "ai_sub_region_comparison",
  "ai_signature_style", "ai_history", "confidence"
];
const BANNED_WORDS = [
  "prestigious", "world-class", "exceptional", "unparalleled",
  "legendary", "iconic", "finest", "renowned"
];

const SYSTEM_PROMPT = `You are a wine expert writing for Loam, a wine intelligence platform. You write with genuine love for wine and deep knowledge — the way a great winemaker talks about their craft. Be specific, grounded, and real. Never use marketing fluff or generic wine-magazine language.

When you write about a wine region, write like someone who has traveled there, tasted across the range, and understands how the landscape shapes what ends up in the glass.

HANDLING UNCERTAINTY: If you don't know specific details about a lesser-known region:
- Write shorter entries (1-2 sentences is fine).
- State the general climate zone and likely character rather than guessing specifics.
- Set confidence to 0.5 or lower.
- An honest short entry is always better than a padded guess.

CATCH-ALL REGIONS: Some entries are country-level catch-alls (flagged in the context). These represent wines labeled broadly under the country name rather than a specific region — often entry-level, everyday wines. Write about the general character and range of these wines honestly. Don't pretend they're from a specific place.

Return a JSON object with EXACTLY these keys (no others):

{
  "ai_overview": "...",
  "ai_climate_profile": "...",
  "ai_sub_region_comparison": "...",
  "ai_signature_style": "...",
  "ai_history": "...",
  "confidence": 0.9
}

Field guidelines:
- ai_overview (2-4 sentences): What this region is and why it matters in the wine world. Lead with what makes it distinctive.
- ai_climate_profile (2-3 sentences): The climate patterns that shape the wines. Be specific about temperature, rainfall, maritime/continental influence, altitude — whatever matters most here.
- ai_sub_region_comparison (1-4 sentences): How the sub-regions or zones within this area differ. If the region has no meaningful sub-regions, write 1 short sentence acknowledging this. Don't force a comparison that doesn't exist.
- ai_signature_style (2-3 sentences): What wines from here taste and feel like. Sensory language rooted in the place.
- ai_history (2-3 sentences): The wine history of this region. Key turning points, traditions, how it got to where it is today.
- confidence: Your honest self-assessment. 0.9+ = major region you know deeply. 0.7-0.8 = well-known regional area. 0.5-0.6 = you know basics. 0.3-0.4 = mostly guessing.

CRITICAL RULES:
- Do NOT reference database counts, wine counts, or producer counts from the context data.
- The context data helps ground your response — use it to inform which appellations and grapes you discuss, but don't quote numbers.
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

// ── Fetch regions ───────────────────────────────────────────
async function fetchRegions() {
  const { data: regions, error } = await supabase
    .from("regions")
    .select("id, name, slug, parent_id, country:countries(name)")
    .is("deleted_at", null)
    .order("name");

  if (error) throw new Error(`Failed to fetch regions: ${error.message}`);

  // Build parent name lookup from the same data
  const nameById = new Map(regions.map(r => [r.id, r.name]));
  for (const r of regions) {
    r.parentName = r.parent_id ? nameById.get(r.parent_id) || null : null;
  }

  let enrichedIds = new Set();
  if (!FORCE) {
    const { data: existing, error: exErr } = await supabase
      .from("region_insights")
      .select("region_id");
    if (exErr) throw new Error(`Failed to fetch existing insights: ${exErr.message}`);
    enrichedIds = new Set(existing.map(e => e.region_id));
  }

  return regions.filter(r => !enrichedIds.has(r.id));
}

// ── Fetch context data ──────────────────────────────────────
async function fetchRegionContext(regionIds) {
  const contextMap = new Map();
  const BATCH = 50;

  for (let i = 0; i < regionIds.length; i += BATCH) {
    const batchIds = regionIds.slice(i, i + BATCH);

    // Child regions
    const { data: children } = await supabase
      .from("regions")
      .select("parent_id, name")
      .in("parent_id", batchIds)
      .is("deleted_at", null);

    const childMap = new Map();
    if (children) {
      for (const c of children) {
        if (!childMap.has(c.parent_id)) childMap.set(c.parent_id, []);
        childMap.get(c.parent_id).push(c.name);
      }
    }

    // Top appellations per region
    const { data: appData } = await supabase
      .from("appellations")
      .select("region_id, name")
      .in("region_id", batchIds);

    const appMap = new Map();
    if (appData) {
      for (const a of appData) {
        if (!appMap.has(a.region_id)) appMap.set(a.region_id, []);
        appMap.get(a.region_id).push(a.name);
      }
    }

    // Top grapes per region
    const { data: grapeData } = await supabase
      .from("wine_grapes")
      .select("grape:grapes(name), wine:wines!inner(region_id)")
      .in("wine.region_id", batchIds)
      .limit(10000);

    const grapeMap = new Map();
    if (grapeData) {
      for (const row of grapeData) {
        const rid = row.wine?.region_id;
        const gname = row.grape?.name;
        if (!rid || !gname) continue;
        if (!grapeMap.has(rid)) grapeMap.set(rid, new Map());
        const gc = grapeMap.get(rid);
        gc.set(gname, (gc.get(gname) || 0) + 1);
      }
    }

    for (const id of batchIds) {
      const topGrapes = grapeMap.has(id)
        ? [...grapeMap.get(id).entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map(([n]) => n)
        : [];
      contextMap.set(id, {
        children: childMap.get(id) || [],
        appellations: (appMap.get(id) || []).slice(0, 10),
        grapes: topGrapes,
      });
    }
  }

  return contextMap;
}

// ── Process a single region ─────────────────────────────────
async function processRegion(region, context) {
  const country = region.country?.name || "Unknown";
  const parentRegion = region.parentName || null;
  const isCatchAll = region.slug.endsWith("-country");

  let userMsg = `Write region insights for:\n\nRegion: ${region.name}\nCountry: ${country}`;
  if (parentRegion) userMsg += `\nParent region: ${parentRegion}`;
  if (context.children.length > 0) userMsg += `\nSub-regions: ${context.children.join(", ")}`;
  if (isCatchAll) userMsg += `\n⚠️ This is a CATCH-ALL region — wines here are labeled under the country name, not a specific region. They are broadly representative of the country's everyday output.`;
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
async function writeInsight(regionId, parsed) {
  const row = {
    region_id: regionId,
    ai_overview: parsed.ai_overview,
    ai_climate_profile: parsed.ai_climate_profile,
    ai_sub_region_comparison: parsed.ai_sub_region_comparison,
    ai_signature_style: parsed.ai_signature_style,
    ai_history: parsed.ai_history,
    confidence: parsed.confidence,
    enriched_at: new Date().toISOString(),
    refresh_after: new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString(),
  };
  const { error } = await supabase.from("region_insights").upsert(row, { onConflict: "region_id" });
  if (error) throw new Error(`Supabase write failed: ${error.message}`);
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log("🗺️  Region Insights Enrichment Pipeline");
  console.log(`   Model: Claude Sonnet | Concurrency: ${CONCURRENCY}`);
  console.log(`   Force: ${FORCE} | Dry run: ${DRY_RUN}${LIMIT ? ` | Limit: ${LIMIT}` : ""}`);
  console.log();

  const toProcess = await fetchRegions();
  const total = LIMIT ? Math.min(toProcess.length, LIMIT) : toProcess.length;
  const regions = toProcess.slice(0, total);

  console.log(`📋 ${total} regions to process (${toProcess.length} unenriched)\n`);
  if (total === 0) { console.log("Nothing to do!"); return; }

  console.log("🔍 Fetching region context data...");
  const contextMap = await fetchRegionContext(regions.map(r => r.id));
  const withContext = [...contextMap.values()].filter(c => c.appellations.length > 0 || c.grapes.length > 0).length;
  console.log(`   ${withContext}/${regions.length} regions have context data\n`);

  let processed = 0, succeeded = 0, warningCount = 0, failed = 0;
  let totalIn = 0, totalOut = 0;
  const errors = [];
  const startTime = Date.now();

  for (let i = 0; i < regions.length; i += CONCURRENCY) {
    const batch = regions.slice(i, i + CONCURRENCY);
    const results = await Promise.all(batch.map(async (region) => {
      const ctx = contextMap.get(region.id) || { children: [], appellations: [], grapes: [] };
      return { region, ...(await processRegion(region, ctx)) };
    }));

    for (const r of results) {
      processed++;
      const country = r.region.country?.name || "?";
      const label = `${r.region.name} (${country})`;

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
        try { await writeInsight(r.region.id, r.parsed); succeeded++; }
        catch (e) { failed++; errors.push({ name: label, error: e.message }); console.log(`     ❌ DB write: ${e.message}`); }
      } else { succeeded++; }
    }

    if (processed % 30 === 0 && processed < total) {
      const elapsed = (Date.now() - startTime) / 1000;
      const remaining = Math.round((total - processed) / (processed / elapsed));
      console.log(`\n  ── ${processed}/${total} done | ${succeeded} ok, ${failed} failed | ~${remaining}s remaining ──\n`);
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
