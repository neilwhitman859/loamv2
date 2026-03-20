#!/usr/bin/env node
/**
 * test_region_prompt.mjs
 *
 * Tests the region_insights prompt against 4 regions spanning
 * major/moderate/small/catch-all before running the full pipeline.
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
  process.env[trimmed.slice(0, eqIdx)] = trimmed.slice(eqIdx + 1);
}

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

async function callClaude(messages, maxTokens = 1500) {
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
  if (!res.ok) throw new Error(`Sonnet ${res.status}: ${await res.text()}`);
  return res.json();
}

// ── Test regions with real DB data ────────────────────────────
const testRegions = [
  {
    name: "Bordeaux",
    country: "France",
    parentRegion: null,
    childRegions: ["Left Bank", "Right Bank"],
    isCatchAll: false,
    topAppellations: ["Bordeaux", "Bordeaux Supérieur", "Côtes de Bourg", "Puisseguin-Saint-Émilion", "Blaye-Côtes de Bordeaux", "Montagne-Saint-Émilion"],
    topGrapes: ["Merlot", "Cabernet Sauvignon", "Cabernet Franc", "Malbec", "Sauvignon Blanc", "Petit Verdot", "Sémillon"],
    wineCount: 1900,
  },
  {
    name: "Willamette Valley",
    country: "United States",
    parentRegion: "Oregon",
    childRegions: [],
    isCatchAll: false,
    topAppellations: ["Willamette Valley", "Dundee Hills", "Eola-Amity Hills", "Chehalem Mountains", "Ribbon Ridge", "McMinnville"],
    topGrapes: ["Pinot Noir", "Chardonnay", "Pinot Gris", "Riesling", "Pinot Blanc"],
    wineCount: 517,
  },
  {
    name: "Kamptal",
    country: "Austria",
    parentRegion: null,
    childRegions: [],
    isCatchAll: false,
    topAppellations: ["Kamptal"],
    topGrapes: ["Grüner Veltliner", "Riesling", "Zweigelt", "Chardonnay"],
    wineCount: 143,
  },
  {
    name: "France",
    country: "France",
    parentRegion: null,
    childRegions: [],
    isCatchAll: true,
    topAppellations: ["Vin de France", "Vin de Pays"],
    topGrapes: ["Chardonnay", "Syrah", "Grenache", "Pinot Noir", "Cabernet Sauvignon", "Merlot", "Cabernet Franc", "Sauvignon Blanc"],
    wineCount: 1126,
  },
];

// ── Prompt ────────────────────────────────────────────────────
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

function buildUserMessage(region) {
  let msg = `Write region insights for:\n\nRegion: ${region.name}\nCountry: ${region.country}`;
  if (region.parentRegion) msg += `\nParent region: ${region.parentRegion}`;
  if (region.childRegions.length > 0) msg += `\nSub-regions: ${region.childRegions.join(", ")}`;
  if (region.isCatchAll) msg += `\n⚠️ This is a CATCH-ALL region — wines here are labeled under the country name, not a specific region. They are broadly representative of the country's everyday output.`;
  if (region.topAppellations.length > 0) msg += `\nTop appellations: ${region.topAppellations.join(", ")}`;
  if (region.topGrapes.length > 0) msg += `\nTop grapes: ${region.topGrapes.join(", ")}`;
  return msg;
}

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

// ── Run ───────────────────────────────────────────────────────
async function main() {
  let totalIn = 0, totalOut = 0;

  for (const region of testRegions) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`Testing: ${region.name} (${region.country})${region.isCatchAll ? " [CATCH-ALL]" : ""}`);
    console.log(`  Appellations: ${region.topAppellations.join(", ")}`);
    console.log(`  Grapes: ${region.topGrapes.join(", ")}`);
    console.log("=".repeat(60));

    const userMsg = buildUserMessage(region);
    const result = await callClaude([
      { role: "user", content: SYSTEM_PROMPT + "\n\n" + userMsg },
      { role: "assistant", content: "{" },
    ], 1500);

    totalIn += result.usage.input_tokens;
    totalOut += result.usage.output_tokens;

    if (result.stop_reason === "max_tokens") {
      console.log("\n⚠️ TRUNCATED");
    }

    let text = "{" + result.content[0].text.trim();
    if (text.includes("```")) {
      text = text.replace(/```(?:json)?\s*/g, "").replace(/\s*```/g, "");
    }

    try {
      const parsed = JSON.parse(text);
      const warnings = validateResponse(parsed);
      if (warnings.length) {
        console.log("\n⚠️ VALIDATION WARNINGS:");
        warnings.forEach(w => console.log(`  - ${w}`));
      } else {
        console.log("\n✅ Validation passed");
      }
      console.log("\n--- PARSED FIELDS ---");
      for (const [key, val] of Object.entries(parsed)) {
        console.log(`\n${key}:\n  ${val}`);
      }
    } catch (e) {
      console.log("\n--- RAW ---\n" + text.slice(0, 500));
      console.log("\n❌ JSON parse failed:", e.message);
    }

    console.log(`\nTokens: ${result.usage.input_tokens} in, ${result.usage.output_tokens} out`);
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log(`TOTAL: ${totalIn} in, ${totalOut} out`);
  console.log(`Est. cost: $${((totalIn * 3 + totalOut * 15) / 1_000_000).toFixed(4)}`);
}

main().catch(console.error);
