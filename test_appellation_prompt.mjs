#!/usr/bin/env node
/**
 * test_appellation_prompt.mjs
 *
 * Tests the appellation_insights prompt against a few appellations
 * to nail the voice before running the full pipeline.
 */

import { readFileSync } from "fs";

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

async function callClaude(messages, { model = "haiku", maxTokens = 4096 } = {}) {
  const modelId = model === "sonnet" ? "claude-sonnet-4-20250514" : "claude-haiku-4-5-20251001";
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: modelId,
      max_tokens: maxTokens,
      messages,
    }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`${model} ${res.status}: ${err}`);
  }
  return res.json();
}

// ── Test appellations with real producer data ────────────────
const testAppellations = [
  {
    name: "Napa Valley",
    designation_type: "AVA",
    country: "United States",
    region: "Napa Valley",
    grapes: ["Cabernet Sauvignon", "Merlot", "Chardonnay", "Pinot Noir", "Sauvignon Blanc", "Zinfandel", "Syrah", "Petite Sirah", "Cabernet Franc", "Petit Verdot", "Viognier"],
    producers: ["Abreu", "Alpha Omega", "Beaulieu Vineyard (BV)", "Beringer", "Bond", "Bryant Family Vineyard", "Cade", "Cain", "Cakebread", "Caymus", "Chappellet", "Charles Krug", "Chateau Montelena", "Cliff Lede", "Clos du Val", "Colgin", "Corison", "Dalla Valle", "Dana", "Darioush", "David Arthur", "Diamond Creek", "Dominus", "Duckhorn", "Dunn", "Far Niente", "Favia", "Flora Springs", "Frank Family", "Freemark Abbey", "Frog's Leap", "Grace Family Vineyards", "Grgich Hills", "Groth", "Hall", "Harlan Estate", "Heitz Cellar", "Honig", "Hundred Acre", "Inglenook", "Joseph Phelps", "Kapcsandy", "Kongsgaard", "La Jota", "Lail Vineyards", "Larkmead", "Lewis Cellars", "Lokoya", "Long Meadow Ranch", "Louis M. Martini", "Matthiasson", "Memento Mori", "Merryvale", "Miner", "Newton", "Nickel & Nickel", "Opus One", "Orin Swift", "Ovid", "Pahlmeyer", "Paul Hobbs", "Philip Togni", "Pine Ridge", "PlumpJack", "Pride Mountain Vineyards", "Promontory", "Realm", "Robert Mondavi", "Rombauer Vineyards", "Round Pond Estate", "Rutherford Hill", "Schrader", "Schramsberg", "Shafer", "Silver Oak", "Silverado Vineyards", "Spottswoode", "Spring Mountain Vineyard", "Stag's Leap Wine Cellars", "Staglin", "Sterling Vineyards", "Trefethen", "Turley", "Turnbull", "Vineyard 29", "William Hill", "ZD Wines"]
  },
  {
    name: "Rüdesheim",
    designation_type: "Weinbaugebiet",
    country: "Germany",
    region: "Rheingau",
    grapes: ["Riesling", "Pinot Noir"],
    producers: ["Weingut Carl Ehrhard", "Georg Breuer", "Johannishof", "Hammond"]
  },
  {
    name: "Crémant de Limoux",
    designation_type: "AOC",
    country: "France",
    region: "Languedoc",
    grapes: ["Chardonnay", "Chenin Blanc", "Mauzac", "Pinot Noir"],
    producers: ["Domaine Rosier", "Gérard Bertrand", "Antech", "Philippe Collin", "Domaine Delmas", "Michel Olivier", "Domaine de Tholomies", "Domaine de la Baume", "La Louvière", "Château Beausoleil"]
  }
];

// ── MODE 1: Producer knowledge test ─────────────────────────
const PRODUCER_TEST_PROMPT = `You are a wine expert. For each producer listed below, categorize your knowledge level as one of:
- "strong": You know specific details about this producer — their flagship wines, style, history, or reputation.
- "moderate": You recognize the name and can say something general about them.
- "weak": You've maybe heard the name but can't say anything specific.
- "unknown": You don't recognize this producer at all.

Be honest. Do not inflate your knowledge. Return ONLY a raw JSON object (no markdown fences) with producer names as keys and knowledge levels as values.`;

async function testProducerKnowledge() {
  for (const app of testAppellations) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`Producer knowledge test: ${app.name} (${app.producers.length} producers)`);
    console.log("=".repeat(60));

    const userMsg = `Categorize your knowledge of these producers from ${app.name}, ${app.country}:\n\n${app.producers.join("\n")}\n\nReturn only the raw JSON object.`;

    const result = await callClaude([
      { role: "user", content: PRODUCER_TEST_PROMPT + "\n\n" + userMsg }
    ], { model: "haiku", maxTokens: 4096 });

    let text = result.content[0].text.trim();
    if (text.startsWith("```")) {
      text = text.replace(/^```(?:json)?\s*/, "").replace(/\s*```$/, "");
    }

    try {
      const parsed = JSON.parse(text);
      const buckets = { strong: [], moderate: [], weak: [], unknown: [] };
      for (const [producer, level] of Object.entries(parsed)) {
        if (buckets[level]) buckets[level].push(producer);
        else console.log(`  ⚠️ Unexpected level "${level}" for ${producer}`);
      }

      console.log(`\n  Strong (${buckets.strong.length}): ${buckets.strong.join(", ") || "none"}`);
      console.log(`  Moderate (${buckets.moderate.length}): ${buckets.moderate.join(", ") || "none"}`);
      console.log(`  Weak (${buckets.weak.length}): ${buckets.weak.join(", ") || "none"}`);
      console.log(`  Unknown (${buckets.unknown.length}): ${buckets.unknown.join(", ") || "none"}`);
    } catch (e) {
      console.log("\n--- RAW RESPONSE ---");
      console.log(text);
      console.log("\n⚠️ Failed to parse JSON:", e.message);
    }

    console.log(`\nTokens: ${result.usage.input_tokens} in, ${result.usage.output_tokens} out`);
  }
}

// ── MODE 2: Full appellation insights prompt ────────────────
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

function validateResponse(parsed, name) {
  const warnings = [];

  // Check all expected keys present
  const missing = EXPECTED_KEYS.filter(k => !(k in parsed));
  if (missing.length) warnings.push(`Missing keys: ${missing.join(", ")}`);

  // Check no unexpected keys
  const extra = Object.keys(parsed).filter(k => !EXPECTED_KEYS.includes(k));
  if (extra.length) warnings.push(`Unexpected keys: ${extra.join(", ")}`);

  // Check confidence is a reasonable number
  if (typeof parsed.confidence !== "number" || parsed.confidence < 0 || parsed.confidence > 1) {
    warnings.push(`Bad confidence value: ${parsed.confidence}`);
  }

  // Check no empty string fields
  for (const key of EXPECTED_KEYS.filter(k => k !== "confidence")) {
    if (typeof parsed[key] === "string" && parsed[key].trim().length === 0) {
      warnings.push(`Empty field: ${key}`);
    }
  }

  // Check for banned marketing words
  const allText = EXPECTED_KEYS.filter(k => k !== "confidence").map(k => parsed[k] || "").join(" ").toLowerCase();
  const found = BANNED_WORDS.filter(w => allText.includes(w));
  if (found.length) warnings.push(`Banned words found: ${found.join(", ")}`);

  return warnings;
}

async function testPrompt() {
  for (const app of testAppellations) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`Testing: ${app.name} (${app.designation_type}, ${app.country})`);
    console.log(`  Grapes: ${app.grapes.join(", ")}`);
    console.log("=".repeat(60));

    const userMsg = `Write appellation insights for:

Name: ${app.name}
Designation: ${app.designation_type}
Country: ${app.country}
Region: ${app.region}
Key grapes: ${app.grapes.join(", ")}`;

    const result = await callClaude([
      { role: "user", content: SYSTEM_PROMPT + "\n\n" + userMsg },
      { role: "assistant", content: "{" }
    ], { model: "sonnet", maxTokens: 1500 });

    // Check for truncation
    if (result.stop_reason === "max_tokens") {
      console.log("\n⚠️ TRUNCATED — response hit max_tokens limit");
    }

    let text = "{" + result.content[0].text.trim();
    // Strip markdown fences if they somehow appear
    if (text.includes("```")) {
      text = text.replace(/```(?:json)?\s*/g, "").replace(/\s*```/g, "");
    }

    try {
      const parsed = JSON.parse(text);

      // Validate
      const warnings = validateResponse(parsed, app.name);
      if (warnings.length) {
        console.log("\n⚠️ VALIDATION WARNINGS:");
        warnings.forEach(w => console.log(`  - ${w}`));
      } else {
        console.log("\n✅ Validation passed");
      }

      console.log("\n--- PARSED FIELDS ---");
      for (const [key, val] of Object.entries(parsed)) {
        console.log(`\n${key}:`);
        console.log(`  ${val}`);
      }
    } catch (e) {
      console.log("\n--- RAW RESPONSE ---");
      console.log(text);
      console.log("\n❌ Failed to parse JSON:", e.message);
    }

    console.log(`\nTokens: ${result.usage.input_tokens} in, ${result.usage.output_tokens} out`);
  }
}

// ── Run selected mode ───────────────────────────────────────
const mode = process.argv[2] || "producers";
if (mode === "producers") {
  testProducerKnowledge().catch(console.error);
} else if (mode === "insights") {
  testPrompt().catch(console.error);
} else {
  console.log("Usage: node test_appellation_prompt.mjs [producers|insights]");
}
