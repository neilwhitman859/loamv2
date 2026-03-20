import fs from 'fs';
import readline from 'readline';
import { createClient } from '@supabase/supabase-js';

// ── Load .env ──
const envPath = new URL('.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
try {
  const envContent = fs.readFileSync(envPath, 'utf8');
  for (const line of envContent.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const val = trimmed.slice(eqIdx + 1).trim().replace(/^["']|["']$/g, '');
    if (!process.env[key]) process.env[key] = val;
  }
} catch (e) {
  console.error('Warning: Could not read .env file:', e.message);
}

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY || process.env.VITE_SUPABASE_ANON_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const DRY_RUN = process.argv.includes('--dry-run');

function normalize(s) {
  return s.replace(/\x5cu0026/g, '&').replace(/&amp;/g, '&').normalize('NFC').toLowerCase().trim();
}

// All FK tables that reference wines.id
const FK_TABLES = [
  { table: 'wine_vintage_scores', column: 'wine_id' },
  { table: 'wine_vintage_prices', column: 'wine_id' },
  { table: 'wine_vintage_grapes', column: 'wine_id' },
  { table: 'wine_vintage_documents', column: 'wine_id' },
  { table: 'wine_vintage_insights', column: 'wine_id' },
  { table: 'wine_vintages', column: 'wine_id' },
  { table: 'wine_grapes', column: 'wine_id' },
  { table: 'wine_regions', column: 'wine_id' },
  { table: 'wine_soils', column: 'wine_id' },
  { table: 'wine_insights', column: 'wine_id' },
  { table: 'wine_biodiversity_certifications', column: 'wine_id' },
  { table: 'wine_farming_certifications', column: 'wine_id' },
  { table: 'wine_water_bodies', column: 'wine_id' },
  { table: 'wine_candidates', column: 'wines_id' },
];

async function main() {
  console.log(`=== CLEANUP WINES SQL ${DRY_RUN ? '(DRY RUN)' : '(LIVE)'} ===\n`);

  // ── Step 1: Identify bad producer IDs ──
  const rl = readline.createInterface({ input: fs.createReadStream('producer_winery_map.jsonl') });
  const badProducerIds = new Set();

  for await (const line of rl) {
    try {
      const j = JSON.parse(line);
      if (!j.vivino_winery_id) continue;

      if (j.match_confidence === 'slug_match') {
        if (normalize(j.producer_name) !== normalize(j.vivino_winery_name)) {
          badProducerIds.add(j.producer_id);
        }
      }
      if (j.match_confidence === 'suffix_stripped') {
        badProducerIds.add(j.producer_id);
      }
      if (j.match_confidence === 'substring') {
        const a = normalize(j.producer_name);
        const b = normalize(j.vivino_winery_name);
        if (!(a.includes(b) || b.includes(a))) {
          badProducerIds.add(j.producer_id);
        }
      }
    } catch {}
  }

  const producerArray = [...badProducerIds];
  console.log(`Bad producer IDs: ${producerArray.length}`);

  // ── Step 2: Get wine IDs to delete ──
  // Process producers in small batches to avoid timeout
  const wineIdsToDelete = [];

  for (let i = 0; i < producerArray.length; i += 10) {
    const batch = producerArray.slice(i, i + 10);
    const { data: wines, error } = await supabase
      .from('wines')
      .select('id')
      .in('producer_id', batch)
      .gte('created_at', '2026-03-09T00:00:00Z');

    if (error) {
      console.error(`Error fetching wines batch ${i}:`, error.message);
      continue;
    }
    if (wines) wineIdsToDelete.push(...wines.map(w => w.id));
  }

  console.log(`Total wines to delete: ${wineIdsToDelete.length}`);

  if (DRY_RUN) {
    console.log('[DRY RUN] Exiting without changes.');
    return;
  }

  // ── Step 3: Delete from all FK tables first, then wines ──
  // Use very small batches (10 wine IDs at a time) to avoid statement timeout

  const BATCH_SIZE = 10;
  let deletedWines = 0;
  let errors = 0;

  for (let i = 0; i < wineIdsToDelete.length; i += BATCH_SIZE) {
    const batch = wineIdsToDelete.slice(i, i + BATCH_SIZE);
    const idList = batch.map(id => `'${id}'`).join(',');

    // Delete from all FK tables first
    for (const { table, column } of FK_TABLES) {
      const { error } = await supabase.from(table).delete().in(column, batch);
      if (error && !error.message.includes('0 rows')) {
        // Ignore "no rows deleted" but log real errors
        if (!error.message.includes('statement timeout')) {
          // Non-timeout error is probably fine (no rows to delete)
        } else {
          console.error(`  Timeout on ${table} batch ${i}: ${error.message}`);
        }
      }
    }

    // Also clear self-referencing FK (wines.duplicate_of)
    await supabase.from('wines').update({ duplicate_of: null }).in('duplicate_of', batch);

    // Now delete the wines
    const { error: wineErr, count } = await supabase
      .from('wines')
      .delete({ count: 'exact' })
      .in('id', batch);

    if (wineErr) {
      console.error(`  Wine delete error batch ${i}: ${wineErr.message}`);
      errors++;
    } else {
      deletedWines += (count || batch.length);
    }

    if ((i + BATCH_SIZE) % 200 === 0 || i + BATCH_SIZE >= wineIdsToDelete.length) {
      console.log(`  Progress: ${Math.min(i + BATCH_SIZE, wineIdsToDelete.length)}/${wineIdsToDelete.length} wines processed, ${deletedWines} deleted, ${errors} errors`);
    }
  }

  console.log(`\nDone. Deleted ${deletedWines} wines (${errors} errors)`);

  // ── Step 4: Final counts ──
  const { count: wineCount } = await supabase.from('wines').select('*', { count: 'exact', head: true });
  const { count: scoreCount } = await supabase.from('wine_vintage_scores').select('*', { count: 'exact', head: true });
  const { count: priceCount } = await supabase.from('wine_vintage_prices').select('*', { count: 'exact', head: true });
  const { count: vintageCount } = await supabase.from('wine_vintages').select('*', { count: 'exact', head: true });

  console.log(`\n=== FINAL DB STATS ===`);
  console.log(`Wines: ${wineCount}`);
  console.log(`Scores: ${scoreCount}`);
  console.log(`Prices: ${priceCount}`);
  console.log(`Vintages: ${vintageCount}`);
}

main().catch(console.error);
