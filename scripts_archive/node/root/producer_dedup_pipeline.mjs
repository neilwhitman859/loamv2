#!/usr/bin/env node
/**
 * Producer dedup pipeline for Loam v2 (Node.js port).
 * Sends fuzzy-matched producer name pairs to Claude Haiku for merge/keep_separate verdicts.
 * Reads from and writes to the producer_dedup_pairs table in Supabase.
 */

import Anthropic from "@anthropic-ai/sdk";
import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "fs";

// Load .env manually (no dotenv dependency)
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

// Config
const SUPABASE_URL = process.env.SUPABASE_URL || "https://vgbppjhmvbggfjztzobl.supabase.co";
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;
const BATCH_SIZE = 50;
const MODEL = "claude-haiku-4-5-20251001";

const SYSTEM_PROMPT = `You are a wine industry data expert helping deduplicate wine producer names.

For each numbered pair of producer names (from the same country), decide if they are the SAME producer or DIFFERENT producers.

Key rules:
- "Château X" and "Chateau X" = SAME (just accent difference)
- "Château X" and "Domaine X" = DIFFERENT (different business types in France)
- "Tenuta X" and "Fattoria X" = DIFFERENT (different estate types in Italy)
- "Cantina X" and "Cantine X" = SAME (singular vs plural)
- "X Winery" and "X Vineyards" = probably SAME (just suffix)
- "X" and "X Wines" = probably SAME
- "Domaine de X" and "Domaine X" = probably SAME
- "Cascina Ca' Rossa" and "Cascina Rossa" = DIFFERENT (Ca' Rossa is a specific name)
- "Castello di Gabiano" and "Castello di Gabbiano" = DIFFERENT (different places)
- Short names that are common words (e.g., "Aurora", "Carmen") matching longer names = usually DIFFERENT

Respond with ONLY a JSON array. Each element: {"pair": <number>, "verdict": "merge" or "separate", "reason": "<brief reason>"}
No other text, no markdown fences, just the JSON array.`;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getPendingPairs(supabase, limit) {
  // Supabase defaults to 1000 rows max — paginate to get all
  const allData = [];
  const pageSize = 1000;
  let offset = 0;
  const maxRows = limit || 100000;

  while (allData.length < maxRows) {
    const fetchSize = Math.min(pageSize, maxRows - allData.length);
    const { data, error } = await supabase
      .from("producer_dedup_pairs")
      .select("id, name_a, name_b, country, similarity")
      .eq("verdict", "pending")
      .order("similarity", { ascending: false })
      .range(offset, offset + fetchSize - 1);

    if (error) throw new Error(`Supabase fetch error: ${error.message}`);
    if (!data || data.length === 0) break;

    allData.push(...data);
    offset += data.length;
    if (data.length < fetchSize) break; // no more rows
  }
  return allData;
}

function buildBatchPrompt(pairs) {
  return pairs
    .map(
      (p, i) =>
        `${i + 1}. "${p.name_a}" vs "${p.name_b}" (country: ${p.country}, similarity: ${p.similarity})`
    )
    .join("\n");
}

async function callHaiku(client, prompt) {
  const response = await client.messages.create({
    model: MODEL,
    max_tokens: 4096,
    system: SYSTEM_PROMPT,
    messages: [{ role: "user", content: prompt }],
  });

  let text = response.content[0].text.trim();
  // Clean potential markdown fences
  if (text.startsWith("```")) {
    text = text.includes("\n") ? text.split("\n").slice(1).join("\n") : text.slice(3);
  }
  if (text.endsWith("```")) {
    text = text.slice(0, -3);
  }
  text = text.trim();

  try {
    const verdicts = JSON.parse(text);
    return { verdicts, usage: response.usage };
  } catch (e) {
    console.log(`  WARNING: Failed to parse Haiku response: ${e.message}`);
    console.log(`  Response was: ${text.slice(0, 200)}...`);
    return { verdicts: null, usage: response.usage };
  }
}

async function updateVerdicts(supabase, pairs, verdicts) {
  if (!verdicts) return 0;

  let updated = 0;
  for (const v of verdicts) {
    const pairIdx = v.pair - 1;
    if (pairIdx < 0 || pairIdx >= pairs.length) continue;

    const pair = pairs[pairIdx];
    const verdict = v.verdict === "merge" ? "merge" : "keep_separate";

    const { error } = await supabase
      .from("producer_dedup_pairs")
      .update({ verdict, verdict_source: "haiku" })
      .eq("id", pair.id);

    if (error) {
      console.log(`  WARNING: Failed to update pair ${pair.id}: ${error.message}`);
      continue;
    }
    updated++;
  }
  return updated;
}

async function runPipeline(dryRun = false, limit = null) {
  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  const client = new Anthropic(); // Uses ANTHROPIC_API_KEY env var

  // Fetch pending pairs
  const pairs = await getPendingPairs(supabase, limit);
  const total = pairs.length;
  console.log(`Fetched ${total} pending pairs`);

  if (total === 0) {
    console.log("Nothing to process!");
    return;
  }

  // Process in batches
  let totalInputTokens = 0;
  let totalOutputTokens = 0;
  let totalMerges = 0;
  let totalSeparates = 0;
  let totalUpdated = 0;
  let batchNum = 0;
  const totalBatches = Math.ceil(total / BATCH_SIZE);
  const failedBatches = [];

  for (let i = 0; i < total; i += BATCH_SIZE) {
    const batch = pairs.slice(i, i + BATCH_SIZE);
    batchNum++;
    const batchCount = batch.length;

    const prompt = buildBatchPrompt(batch);

    if (dryRun) {
      console.log(`  Batch ${batchNum}: ${batchCount} pairs (dry run, skipping API call)`);
      continue;
    }

    process.stdout.write(
      `  Batch ${batchNum}/${totalBatches}: ${batchCount} pairs...`
    );

    let attempts = 0;
    const maxAttempts = 2;
    while (attempts < maxAttempts) {
      attempts++;
      try {
        const { verdicts, usage } = await callHaiku(client, prompt);
        totalInputTokens += usage.input_tokens;
        totalOutputTokens += usage.output_tokens;

        if (verdicts) {
          const merges = verdicts.filter((v) => v.verdict === "merge").length;
          const separates = verdicts.filter((v) => v.verdict !== "merge").length;
          totalMerges += merges;
          totalSeparates += separates;

          const updated = await updateVerdicts(supabase, batch, verdicts);
          totalUpdated += updated;

          console.log(` ✓ merge:${merges} separate:${separates} (updated:${updated})`);
          break;
        } else if (attempts < maxAttempts) {
          process.stdout.write(` retry...`);
          await sleep(1000);
        } else {
          console.log(` ✗ parse error (giving up)`);
          failedBatches.push({ batchNum, startIdx: i, count: batchCount });
        }
      } catch (e) {
        if (attempts < maxAttempts) {
          process.stdout.write(` retry(${e.message})...`);
          await sleep(2000);
        } else {
          console.log(` ✗ error: ${e.message}`);
          failedBatches.push({ batchNum, startIdx: i, count: batchCount });
        }
      }
    }

    // Rate limiting
    await sleep(500);
  }

  // Summary
  console.log(`\n${"=".repeat(50)}`);
  console.log("PIPELINE COMPLETE");
  console.log("=".repeat(50));
  console.log(`Total pairs processed: ${totalUpdated}/${total}`);
  console.log(`Merges: ${totalMerges}`);
  console.log(`Separates: ${totalSeparates}`);
  console.log(`Input tokens: ${totalInputTokens.toLocaleString()}`);
  console.log(`Output tokens: ${totalOutputTokens.toLocaleString()}`);

  const inputCost = (totalInputTokens / 1_000_000) * 0.25;
  const outputCost = (totalOutputTokens / 1_000_000) * 1.25;
  console.log(`Cost: $${(inputCost + outputCost).toFixed(2)}`);
  if (failedBatches.length > 0) {
    console.log(`\nFailed batches (${failedBatches.length}): ${failedBatches.map(b => `#${b.batchNum}`).join(", ")}`);
    console.log("Re-run the pipeline to retry these (they remain 'pending').");
  }
}

// CLI
const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
let limit = null;
for (const arg of args) {
  if (arg.startsWith("--limit=")) {
    limit = parseInt(arg.split("=")[1], 10);
  }
}

if (dryRun) console.log("DRY RUN MODE - no API calls will be made");

runPipeline(dryRun, limit).catch((e) => {
  console.error("Pipeline failed:", e);
  process.exit(1);
});
