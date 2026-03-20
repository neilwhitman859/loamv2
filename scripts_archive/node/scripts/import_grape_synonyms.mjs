#!/usr/bin/env node
/**
 * Import grape synonyms from VIVC cache into grape_synonyms table.
 *
 * Source: data/vivc_grapes_cache.json (synonyms arrays per grape)
 * Target: grape_synonyms table (grape_id, synonym, source, synonym_type)
 *
 * Joins VIVC cache → DB grapes via vivc_number.
 * Imports all synonyms for grapes that exist in our DB.
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

// .env loading (no dotenv dependency)
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const env = {};
for (const l of readFileSync(envPath, 'utf8').split('\n')) {
  const m = l.replace(/\r/g, '').match(/^(\w+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE);

const BATCH_SIZE = 500;

async function main() {
  // Load VIVC cache
  const cache = JSON.parse(readFileSync('data/vivc_grapes_cache.json', 'utf-8'));
  const vivcGrapes = Object.values(cache.grapes);

  console.log(`VIVC cache: ${vivcGrapes.length} grapes`);

  // Filter to grapes with synonyms
  const withSynonyms = vivcGrapes.filter(g => g.synonyms && g.synonyms.length > 0);
  console.log(`Grapes with synonyms: ${withSynonyms.length}`);

  // Load all grape IDs from DB, keyed by vivc_number
  const grapeMap = new Map();
  let offset = 0;
  while (true) {
    const { data, error } = await supabase
      .from('grapes')
      .select('id, vivc_number, name, display_name')
      .not('deleted_at', 'is', null)  // include all
      .range(offset, offset + 999);

    if (error) {
      // Try without deleted_at filter
      break;
    }
    if (!data || data.length === 0) break;
    for (const g of data) {
      if (g.vivc_number) grapeMap.set(String(g.vivc_number), g);
    }
    offset += data.length;
    if (data.length < 1000) break;
  }

  // Retry loading all grapes without filter
  if (grapeMap.size === 0) {
    offset = 0;
    while (true) {
      const { data, error } = await supabase
        .from('grapes')
        .select('id, vivc_number, name, display_name')
        .range(offset, offset + 999);

      if (error) throw error;
      if (!data || data.length === 0) break;
      for (const g of data) {
        if (g.vivc_number) grapeMap.set(String(g.vivc_number), g);
      }
      offset += data.length;
      if (data.length < 1000) break;
    }
  }

  console.log(`DB grapes with VIVC numbers: ${grapeMap.size}`);

  // Build synonym rows
  const rows = [];
  let skippedNoMatch = 0;
  let skippedSelfName = 0;

  for (const vivcGrape of withSynonyms) {
    const dbGrape = grapeMap.get(String(vivcGrape.vivc_number));
    if (!dbGrape) {
      skippedNoMatch++;
      continue;
    }

    for (const syn of vivcGrape.synonyms) {
      const trimmed = syn.trim();
      if (!trimmed) continue;

      // Skip if synonym is identical to the grape's own name (case-insensitive)
      if (trimmed.toUpperCase() === vivcGrape.name.toUpperCase()) {
        skippedSelfName++;
        continue;
      }

      rows.push({
        grape_id: dbGrape.id,
        synonym: trimmed,
        synonym_type: 'vivc_synonym',
        source: 'VIVC'
      });
    }
  }

  console.log(`Synonym rows to insert: ${rows.length}`);
  console.log(`Skipped (no DB match): ${skippedNoMatch}`);
  console.log(`Skipped (self-name): ${skippedSelfName}`);

  // Check for existing synonyms
  const { count: existingCount } = await supabase
    .from('grape_synonyms')
    .select('*', { count: 'exact', head: true });

  if (existingCount > 0) {
    console.log(`\nWARNING: grape_synonyms already has ${existingCount} rows.`);
    console.log('Clearing existing VIVC synonyms before re-import...');

    // Delete in batches to avoid timeout
    let deleted = 0;
    while (true) {
      const { data, error } = await supabase
        .from('grape_synonyms')
        .delete()
        .eq('source', 'VIVC')
        .limit(2000)
        .select('id');

      if (error) throw error;
      if (!data || data.length === 0) break;
      deleted += data.length;
      console.log(`  Deleted ${deleted}...`);
    }
  }

  // Insert in batches
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const { error } = await supabase
      .from('grape_synonyms')
      .insert(batch);

    if (error) {
      console.error(`Error at batch ${i / BATCH_SIZE}:`, error.message);
      // Try smaller batches
      for (const row of batch) {
        const { error: singleError } = await supabase
          .from('grape_synonyms')
          .insert(row);
        if (singleError) {
          console.error(`  Failed: ${row.synonym} for grape ${row.grape_id}: ${singleError.message}`);
        } else {
          inserted++;
        }
      }
      continue;
    }

    inserted += batch.length;
    if ((i / BATCH_SIZE) % 10 === 0) {
      console.log(`  Inserted ${inserted}/${rows.length}...`);
    }
  }

  console.log(`\nDone! Inserted ${inserted} synonyms.`);

  // Verify with some well-known examples
  const examples = ['SYRAH', 'TEMPRANILLO TINTO', 'PINOT NOIR'];
  for (const name of examples) {
    const grape = [...grapeMap.values()].find(g => g.name === name);
    if (!grape) continue;

    const { data } = await supabase
      .from('grape_synonyms')
      .select('synonym')
      .eq('grape_id', grape.id)
      .limit(10);

    console.log(`\n${grape.display_name}: ${data?.map(s => s.synonym).join(', ')}`);
  }
}

main().catch(console.error);
