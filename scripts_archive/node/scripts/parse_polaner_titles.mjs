#!/usr/bin/env node
/**
 * parse_polaner_titles.mjs — Uses Claude Haiku to parse Polaner wine titles
 * into structured {producer, wine_name} fields.
 *
 * The Polaner catalog has no separate producer field — everything is mashed
 * into a single title string like "Cascina delle Rose Barbaresco Tre Stelle".
 *
 * Usage:
 *   node scripts/parse_polaner_titles.mjs                # dry-run (print, don't update DB)
 *   node scripts/parse_polaner_titles.mjs --apply        # parse and UPDATE source_polaner
 *   node scripts/parse_polaner_titles.mjs --limit 50     # process only 50 rows
 *   node scripts/parse_polaner_titles.mjs --stats        # show current parse coverage
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// ─── Env ──────────────────────────────────────────────────────────
const envPath = resolve(process.cwd(), '.env');
const vars = {};
try {
  readFileSync(envPath, 'utf8').replace(/\r/g, '').split('\n').forEach(line => {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) vars[m[1].trim()] = m[2].trim().replace(/^["']|["']$/g, '');
  });
} catch {}

const ANTHROPIC_API_KEY = vars.ANTHROPIC_API_KEY;
if (!ANTHROPIC_API_KEY) { console.error('Missing ANTHROPIC_API_KEY in .env'); process.exit(1); }

const supabase = createClient(vars.SUPABASE_URL, vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY);

// ─── Args ─────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const apply = args.includes('--apply');
const statsOnly = args.includes('--stats');
const limitArg = args.find(a => a.startsWith('--limit'));
const limit = limitArg ? parseInt(limitArg.includes('=') ? limitArg.split('=')[1] : args[args.indexOf('--limit') + 1]) : null;

const BATCH_SIZE = 20; // Titles per Haiku call (keeps prompt manageable)

// ─── Stats ────────────────────────────────────────────────────────
async function showStats() {
  const { count: total } = await supabase.from('source_polaner').select('*', { count: 'exact', head: true });
  const { count: parsed } = await supabase.from('source_polaner').select('*', { count: 'exact', head: true }).not('producer', 'is', null);
  const { count: unparsed } = await supabase.from('source_polaner').select('*', { count: 'exact', head: true }).is('producer', null);
  console.log(`source_polaner: ${total} total, ${parsed} parsed, ${unparsed} remaining`);
  return { total, parsed, unparsed };
}

if (statsOnly) {
  await showStats();
  process.exit(0);
}

// ─── Add producer/wine_name columns if missing ────────────────────
// (These may not exist yet on source_polaner)

// ─── Fetch unparsed rows ──────────────────────────────────────────
console.log('=== Polaner Title Parser (Claude Haiku) ===\n');
await showStats();

let query = supabase.from('source_polaner')
  .select('id, title, country, region, appellation')
  .is('producer', null)
  .order('title');

if (limit) query = query.limit(limit);

const { data: rows, error } = await query;
if (error) { console.error('Query error:', error.message); process.exit(1); }
if (!rows.length) { console.log('\nAll titles already parsed!'); process.exit(0); }

console.log(`\nProcessing ${rows.length} unparsed titles in batches of ${BATCH_SIZE}...\n`);

// ─── Haiku API call ───────────────────────────────────────────────
async function callHaiku(messages) {
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 4096,
      messages,
    }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Haiku API ${resp.status}: ${text}`);
  }

  const data = await resp.json();
  return data.content[0].text;
}

// ─── Parse batch ──────────────────────────────────────────────────
async function parseBatch(batch) {
  const titlesBlock = batch.map((r, i) =>
    `${i + 1}. "${r.title}" [country: ${r.country || '?'}, region: ${r.region || '?'}, appellation: ${r.appellation || '?'}]`
  ).join('\n');

  const prompt = `You are a wine data expert. Parse each wine title into producer name and wine name.

The title format is: "{Producer Name} {Wine/Cuvee Name} {Region/Appellation}".
The appellation/region context is provided in brackets to help you identify where the producer name ends.

Rules:
- The producer is typically the first part of the title (a person's name, estate name, or domaine name)
- The wine name includes the cuvee/vineyard/designation and often the appellation
- If the title IS just "Producer Appellation" with no cuvee, the wine_name should be the appellation (e.g., "Nervi-Conterno Gattinara" → wine_name: "Gattinara")
- Remove "[base YYYY.x]", "GIFT BOX", "[lieu dit]" suffixes — they are not part of the wine name
- Preserve accents and special characters exactly as they appear
- For reversed names like "Sigaut Anne & Herve", normalize to "Anne & Herve Sigaut"

Return ONLY a JSON array (no markdown, no explanation) with objects: {"i": <1-based index>, "producer": "...", "wine_name": "..."}

Titles:
${titlesBlock}`;

  const response = await callHaiku([{ role: 'user', content: prompt }]);

  // Parse JSON from response (handle possible markdown wrapping)
  let json = response.trim();
  if (json.startsWith('```')) {
    json = json.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
  }

  try {
    return JSON.parse(json);
  } catch (e) {
    console.error('  JSON parse error:', e.message);
    console.error('  Response:', response.slice(0, 200));
    return null;
  }
}

// ─── Main loop ────────────────────────────────────────────────────
let totalParsed = 0;
let totalErrors = 0;
let totalTokens = 0;

for (let i = 0; i < rows.length; i += BATCH_SIZE) {
  const batch = rows.slice(i, i + BATCH_SIZE);
  const batchNum = Math.floor(i / BATCH_SIZE) + 1;
  const totalBatches = Math.ceil(rows.length / BATCH_SIZE);

  process.stdout.write(`  Batch ${batchNum}/${totalBatches} (${batch.length} titles)...`);

  try {
    const results = await parseBatch(batch);
    if (!results) {
      console.log(' PARSE ERROR');
      totalErrors += batch.length;
      continue;
    }

    let batchParsed = 0;
    for (const result of results) {
      const idx = result.i - 1;
      if (idx < 0 || idx >= batch.length) continue;

      const row = batch[idx];
      const producer = result.producer?.trim();
      const wineName = result.wine_name?.trim();

      if (!producer || !wineName) {
        console.error(`\n    Missing field for "${row.title}": producer=${producer}, wine_name=${wineName}`);
        totalErrors++;
        continue;
      }

      if (apply) {
        const { error: updateErr } = await supabase.from('source_polaner')
          .update({ producer, wine_name: wineName })
          .eq('id', row.id);

        if (updateErr) {
          console.error(`\n    Update error for "${row.title}":`, updateErr.message);
          totalErrors++;
          continue;
        }
      } else {
        // Dry-run: print
        if (batchParsed === 0) console.log('');
        console.log(`    "${row.title}" → producer: "${producer}" | wine: "${wineName}"`);
      }

      batchParsed++;
    }

    totalParsed += batchParsed;
    if (apply) process.stdout.write(` ${batchParsed} parsed\n`);

    // Small delay between batches to be polite
    if (i + BATCH_SIZE < rows.length) {
      await new Promise(r => setTimeout(r, 200));
    }
  } catch (err) {
    console.error(` ERROR: ${err.message}`);
    totalErrors += batch.length;

    // Rate limit: back off
    if (err.message.includes('429')) {
      console.log('  Rate limited, waiting 30s...');
      await new Promise(r => setTimeout(r, 30000));
      i -= BATCH_SIZE; // Retry this batch
    }
  }
}

console.log(`\n=== Done ===`);
console.log(`Parsed: ${totalParsed}, Errors: ${totalErrors}`);
if (!apply) console.log('(dry-run — use --apply to update database)');

await showStats();
