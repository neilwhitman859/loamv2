#!/usr/bin/env node
/**
 * test_country_prompt.mjs
 *
 * Tests the country_insights prompt against 3 countries spanning
 * major/moderate/small before running the full pipeline.
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

// ── Test countries with real DB data ──────────────────────────
const testCountries = [
  {
    name: "France",
    regionCount: 19,
    appellationCount: 207,
    producerCount: 8820,
    topRegions: ["Burgundy", "Champagne", "Bordeaux", "Loire Valley", "Southern Rhône", "Alsace", "Languedoc-Roussillon"],
    topAppellations: ["Bourgogne", "Champagne", "Alsace", "Bordeaux", "Chablis", "Côte de Beaune", "Languedoc", "Meursault"],
    topGrapes: ["Chardonnay", "Pinot Noir", "Merlot", "Cabernet Sauvignon", "Syrah", "Cabernet Franc", "Grenache", "Mourvèdre"],
  },
  {
    name: "Uruguay",
    regionCount: 2,
    appellationCount: 5,
    producerCount: 82,
    topRegions: ["Canelones", "Maldonado"],
    topAppellations: ["Canelones", "Maldonado", "Cerro Chapeu", "Juanico", "San José"],
    topGrapes: ["Tannat", "Cabernet Sauvignon", "Merlot", "Cabernet Franc", "Chardonnay", "Syrah", "Sauvignon Blanc", "Pinot Noir"],
  },
  {
    name: "Hungary",
    regionCount: 3,
    appellationCount: 5,
    producerCount: 133,
    topRegions: ["Tokaj", "Villány", "Eger"],
    topAppellations: ["Tokaj", "Villány", "Szekszárd", "Eger", "Egri Bikavér"],
    topGrapes: ["Furmint", "Cabernet Sauvignon", "Cabernet Franc", "Merlot", "Hárslevelű", "Blaufränkisch", "Pinot Noir", "Welschriesling"],
  },
];

// ── Prompt ────────────────────────────────────────────────────
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

function buildUserMessage(country) {
  let msg = `Write country insights for:\n\nCountry: ${country.name}`;
  msg += `\nMajor regions: ${country.topRegions.join(", ")}`;
  msg += `\nTop appellations: ${country.topAppellations.join(", ")}`;
  msg += `\nTop grapes: ${country.topGrapes.join(", ")}`;
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

  for (const country of testCountries) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`Testing: ${country.name}`);
    console.log(`  Regions: ${country.topRegions.join(", ")}`);
    console.log(`  Grapes: ${country.topGrapes.join(", ")}`);
    console.log("=".repeat(60));

    const userMsg = buildUserMessage(country);
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
