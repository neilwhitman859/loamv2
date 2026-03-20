#!/usr/bin/env node
/**
 * test_grape_prompt.mjs
 *
 * Tests the grape_insights prompt against 4 grapes spanning
 * major/moderate/niche profiles before running the full pipeline.
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

async function callClaude(messages, { model = "sonnet", maxTokens = 4096 } = {}) {
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

// ── Test grapes with real DB data ─────────────────────────────
const testGrapes = [
  {
    name: "Pinot Noir",
    color: "red",
    topAppellations: [
      { appellation: "Champagne", country: "France", count: 1445 },
      { appellation: "Bourgogne", country: "France", count: 1151 },
      { appellation: "Baden", country: "Germany", count: 267 },
      { appellation: "Côte de Beaune", country: "France", count: 261 },
      { appellation: "Gevrey-Chambertin", country: "France", count: 241 },
      { appellation: "Willamette Valley", country: "United States", count: 228 },
      { appellation: "Russian River Valley", country: "United States", count: 220 },
      { appellation: "Pfalz", country: "Germany", count: 214 },
    ],
    topCountries: [
      { country: "France", count: 5527 },
      { country: "United States", count: 2124 },
      { country: "Germany", count: 923 },
      { country: "Australia", count: 527 },
      { country: "Italy", count: 499 },
    ],
  },
  {
    name: "Grüner Veltliner",
    color: "white",
    topAppellations: [
      { appellation: "Niederösterreich", country: "Austria", count: 98 },
      { appellation: "Wachau", country: "Austria", count: 93 },
      { appellation: "Kamptal", country: "Austria", count: 67 },
      { appellation: "Kremstal", country: "Austria", count: 57 },
      { appellation: "Burgenland", country: "Austria", count: 32 },
      { appellation: "Weinviertel", country: "Austria", count: 31 },
      { appellation: "Wagram", country: "Austria", count: 27 },
    ],
    topCountries: [
      { country: "Austria", count: 469 },
      { country: "Hungary", count: 12 },
      { country: "United States", count: 9 },
    ],
  },
  {
    name: "Tannat",
    color: "red",
    topAppellations: [
      { appellation: "Canelones", country: "Uruguay", count: 94 },
      { appellation: "Serra Gaúcha", country: "Brazil", count: 70 },
      { appellation: "Vale dos Vinhedos", country: "Brazil", count: 32 },
      { appellation: "Campanha Gaúcha", country: "Brazil", count: 22 },
      { appellation: "Maldonado", country: "Uruguay", count: 16 },
      { appellation: "Madiran", country: "France", count: 14 },
      { appellation: "San José", country: "Uruguay", count: 13 },
      { appellation: "Mendoza", country: "Argentina", count: 11 },
    ],
    topCountries: [
      { country: "Uruguay", count: 209 },
      { country: "Brazil", count: 140 },
      { country: "France", count: 54 },
      { country: "Argentina", count: 40 },
      { country: "United States", count: 26 },
    ],
  },
  {
    name: "Assyrtiko",
    color: "white",
    topAppellations: [
      { appellation: "Santorini", country: "Greece", count: 42 },
      { appellation: "Chalkidiki", country: "Greece", count: 8 },
      { appellation: "Attiki", country: "Greece", count: 5 },
      { appellation: "Crete", country: "Greece", count: 5 },
      { appellation: "Drama", country: "Greece", count: 4 },
    ],
    topCountries: [
      { country: "Greece", count: 137 },
      { country: "Cyprus", count: 1 },
    ],
  },
];

// ── Prompt ────────────────────────────────────────────────────
const EXPECTED_KEYS = [
  "ai_overview",
  "ai_flavor_profile",
  "ai_growing_conditions",
  "ai_food_pairing",
  "ai_regions_of_note",
  "ai_aging_characteristics",
  "confidence",
];

const BANNED_WORDS = [
  "prestigious", "world-class", "exceptional", "unparalleled",
  "legendary", "iconic", "finest", "renowned",
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

function validateResponse(parsed, name) {
  const warnings = [];

  const missing = EXPECTED_KEYS.filter((k) => !(k in parsed));
  if (missing.length) warnings.push(`Missing keys: ${missing.join(", ")}`);

  const extra = Object.keys(parsed).filter((k) => !EXPECTED_KEYS.includes(k));
  if (extra.length) warnings.push(`Unexpected keys: ${extra.join(", ")}`);

  if (
    typeof parsed.confidence !== "number" ||
    parsed.confidence < 0 ||
    parsed.confidence > 1
  ) {
    warnings.push(`Bad confidence value: ${parsed.confidence}`);
  }

  for (const key of EXPECTED_KEYS.filter((k) => k !== "confidence")) {
    if (typeof parsed[key] === "string" && parsed[key].trim().length === 0) {
      warnings.push(`Empty field: ${key}`);
    }
  }

  const allText = EXPECTED_KEYS.filter((k) => k !== "confidence")
    .map((k) => parsed[k] || "")
    .join(" ")
    .toLowerCase();
  const found = BANNED_WORDS.filter((w) => allText.includes(w));
  if (found.length) warnings.push(`Banned words found: ${found.join(", ")}`);

  return warnings;
}

// ── Run ───────────────────────────────────────────────────────
async function testGrapeInsights() {
  let totalIn = 0, totalOut = 0;

  for (const grape of testGrapes) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`Testing: ${grape.name} (${grape.color})`);
    console.log(
      `  Top appellations: ${grape.topAppellations.map((a) => a.appellation).join(", ")}`
    );
    console.log(
      `  Top countries: ${grape.topCountries.map((c) => c.country).join(", ")}`
    );
    console.log("=".repeat(60));

    const appellationList = grape.topAppellations
      .map((a) => `${a.appellation} (${a.country})`)
      .join(", ");
    const countryList = grape.topCountries
      .map((c) => c.country)
      .join(", ");

    const userMsg = `Write grape insights for:

Grape: ${grape.name}
Color: ${grape.color}
Top appellations in our database: ${appellationList}
Top countries: ${countryList}`;

    const result = await callClaude(
      [
        { role: "user", content: SYSTEM_PROMPT + "\n\n" + userMsg },
        { role: "assistant", content: "{" },
      ],
      { model: "sonnet", maxTokens: 2000 }
    );

    totalIn += result.usage.input_tokens;
    totalOut += result.usage.output_tokens;

    if (result.stop_reason === "max_tokens") {
      console.log("\n⚠️ TRUNCATED — response hit max_tokens limit");
    }

    let text = "{" + result.content[0].text.trim();
    if (text.includes("```")) {
      text = text.replace(/```(?:json)?\s*/g, "").replace(/\s*```/g, "");
    }

    try {
      const parsed = JSON.parse(text);

      const warnings = validateResponse(parsed, grape.name);
      if (warnings.length) {
        console.log("\n⚠️ VALIDATION WARNINGS:");
        warnings.forEach((w) => console.log(`  - ${w}`));
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

    console.log(
      `\nTokens: ${result.usage.input_tokens} in, ${result.usage.output_tokens} out`
    );
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log(`TOTAL: ${totalIn} in, ${totalOut} out`);
  const cost = (totalIn * 3 + totalOut * 15) / 1_000_000;
  console.log(`Est. cost: $${cost.toFixed(4)}`);
}

testGrapeInsights().catch(console.error);
