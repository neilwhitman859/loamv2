#!/usr/bin/env node
/**
 * extract_drinking_windows.mjs
 *
 * Extracts drinking window data from Ridge vintage notes already in the JSONL,
 * then updates the wine_vintages table with producer_drinking_window_start/end.
 *
 * Usage:
 *   node extract_drinking_windows.mjs          # Dry run (show what would update)
 *   node extract_drinking_windows.mjs --apply  # Actually update DB
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

// ── Load .env ───────────────────────────────────────────────
const envPath = new URL('.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
for (const line of envContent.split('\n')) {
  const trimmed = line.replace(/\r/g, '').trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx === -1) continue;
  const key = trimmed.slice(0, eqIdx);
  const val = trimmed.slice(eqIdx + 1);
  if (!process.env[key]) process.env[key] = val;
}

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY);
const APPLY = process.argv.includes('--apply');

const NUMBER_WORDS = {
  'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'seven':7,'eight':8,'nine':9,'ten':10,
  'eleven':11,'twelve':12,'thirteen':13,'fourteen':14,'fifteen':15,'sixteen':16,'seventeen':17,
  'eighteen':18,'nineteen':19,'twenty':20,'twenty-one':21,'twenty-two':22,'twenty-three':23,
  'twenty-four':24,'twenty-five':25,'twenty-six':26,'twenty-seven':27,'twenty-eight':28,
  'twenty-nine':29,'thirty':30,'thirty-five':35,'forty':40,'fifty':50,
};

function parseWord(w) {
  return NUMBER_WORDS[w.toLowerCase()] || parseInt(w) || null;
}

function extractDrinkingWindow(notes, vintage) {
  if (!notes || !vintage) return null;

  // Pattern 1: "over/for the next X (to Y) years"
  let m = notes.match(/(?:over|for)\s+the\s+next\s+([\w-]+)\s+(?:to\s+([\w-]+)\s+)?years?/i);
  if (m) {
    const hi = m[2] ? parseWord(m[2]) : parseWord(m[1]);
    if (hi) return { start: vintage, end: vintage + hi, source: m[0] };
  }

  // Pattern 2: "develop/improve/evolve over/for (the next) X (to Y) years"
  m = notes.match(/(?:develop|improve|evolve|age)\s+(?:over|for)\s+(?:the\s+next\s+)?([\w-]+)\s+(?:to\s+([\w-]+)\s+)?years?/i);
  if (m) {
    const hi = m[2] ? parseWord(m[2]) : parseWord(m[1]);
    if (hi) return { start: vintage, end: vintage + hi, source: m[0] };
  }

  // Pattern 3: "enjoy now and over the next X years"
  m = notes.match(/enjoy\s+(?:now\s+and\s+)?(?:over|for)\s+(?:the\s+next\s+)?([\w-]+)\s+(?:to\s+([\w-]+)\s+)?years?/i);
  if (m) {
    const hi = m[2] ? parseWord(m[2]) : parseWord(m[1]);
    if (hi) return { start: vintage, end: vintage + hi, source: m[0] };
  }

  // Pattern 4: "drink now and over the next X years" or "drink now and for the next X years"
  m = notes.match(/drink\s+now\s+and\s+(?:over|for)\s+(?:the\s+next\s+)?([\w-]+)\s+(?:to\s+([\w-]+)\s+)?years?/i);
  if (m) {
    const hi = m[2] ? parseWord(m[2]) : parseWord(m[1]);
    if (hi) return { start: vintage, end: vintage + hi, source: m[0] };
  }

  // Pattern 5: "best from YYYY" or "best after YYYY"
  m = notes.match(/best\s+(?:from|after)\s+(\d{4})/i);
  if (m) {
    return { start: parseInt(m[1]), end: parseInt(m[1]) + 15, source: m[0] };
  }

  // Pattern 6: "YYYY-YYYY" or "YYYY–YYYY" drinking window range
  m = notes.match(/(\d{4})\s*[-–]\s*(\d{4})/);
  if (m) {
    const s = parseInt(m[1]), e = parseInt(m[2]);
    if (s >= 1990 && s <= 2050 && e >= s && e <= 2100) {
      return { start: s, end: e, source: m[0] };
    }
  }

  return null;
}

async function main() {
  console.log(`Mode: ${APPLY ? 'APPLY (will update DB)' : 'DRY RUN (preview only)'}\n`);

  // Load JSONL
  const lines = readFileSync('ridge_wines.jsonl', 'utf8').trim().split('\n');
  const wines = lines.map(l => JSON.parse(l));
  console.log(`Loaded ${wines.length} entries from JSONL\n`);

  // Get Ridge producer + wine IDs
  const { data: prod } = await sb.from('producers').select('id').eq('slug', 'ridge-vineyards').single();
  if (!prod) { console.error('Ridge producer not found'); process.exit(1); }

  const { data: dbWines } = await sb.from('wines').select('id,name').eq('producer_id', prod.id);
  const wineNameToId = new Map(dbWines.map(w => [w.name, w.id]));

  let extracted = 0;
  let updated = 0;
  let skipped = 0;

  for (const w of wines) {
    if (!w.vintage || !w.wineName) continue;

    const notes = (w.vintageNotes || '') + ' ' + (w.winemakerNotes || '');
    const dw = extractDrinkingWindow(notes, w.vintage);

    if (!dw) continue;
    extracted++;

    const wineId = wineNameToId.get(w.wineName);
    if (!wineId) {
      console.warn(`  No DB wine for "${w.wineName}"`);
      skipped++;
      continue;
    }

    console.log(`  ${w.vintage} ${w.wineName}: ${dw.start}–${dw.end} (from: "${dw.source}")`);

    if (APPLY) {
      const { error } = await sb.from('wine_vintages')
        .update({
          producer_drinking_window_start: dw.start,
          producer_drinking_window_end: dw.end,
        })
        .eq('wine_id', wineId)
        .eq('vintage_year', w.vintage);

      if (error) {
        console.error(`    UPDATE ERROR: ${error.message}`);
      } else {
        updated++;
      }
    }
  }

  console.log(`\n========================================`);
  console.log(`  Drinking Window Extraction`);
  console.log(`========================================`);
  console.log(`  Scanned: ${wines.length} entries`);
  console.log(`  Extracted: ${extracted}`);
  console.log(`  Skipped: ${skipped}`);
  if (APPLY) console.log(`  Updated in DB: ${updated}`);
  else console.log(`  (Dry run — use --apply to update DB)`);
}

main().catch(console.error);
