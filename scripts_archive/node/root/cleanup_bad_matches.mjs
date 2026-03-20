import fs from 'fs';
import readline from 'readline';
import { createClient } from '@supabase/supabase-js';

// ── Load .env manually (no dotenv dependency) ──────────────────────────
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

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const VIVINO_PUB_ID = 'ed228eae-c3bf-41e6-9a90-d78c8efaf97e';
const VIVINO_SOURCE_TYPE_ID = 'f4c5a61d-3921-4cd0-a32c-9363a4549f70';

const DRY_RUN = process.argv.includes('--dry-run');

function normalize(s) {
  return s.replace(/\x5cu0026/g, '&').replace(/&amp;/g, '&').normalize('NFC').toLowerCase().trim();
}

async function main() {
  console.log(`=== CLEANUP BAD MATCHES ${DRY_RUN ? '(DRY RUN)' : '(LIVE)'} ===\n`);

  // ── Step 1: Identify bad producer mappings ──
  const rl = readline.createInterface({ input: fs.createReadStream('producer_winery_map.jsonl') });
  const badProducers = [];

  for await (const line of rl) {
    try {
      const j = JSON.parse(line);
      if (!j.vivino_winery_id) continue;

      // slug_match false positives
      if (j.match_confidence === 'slug_match') {
        if (normalize(j.producer_name) !== normalize(j.vivino_winery_name)) {
          badProducers.push(j);
        }
      }
      // suffix_stripped - all risky
      if (j.match_confidence === 'suffix_stripped') {
        badProducers.push(j);
      }
      // substring risky (not fully contained)
      if (j.match_confidence === 'substring') {
        const a = normalize(j.producer_name);
        const b = normalize(j.vivino_winery_name);
        if (!(a.includes(b) || b.includes(a))) {
          badProducers.push(j);
        }
      }
    } catch {}
  }

  console.log(`Bad producers identified: ${badProducers.length}`);
  const badProducerIds = [...new Set(badProducers.map(p => p.producer_id))];
  const badWineryIds = [...new Set(badProducers.map(p => p.vivino_winery_id))];
  console.log(`Unique bad producer IDs: ${badProducerIds.length}`);
  console.log(`Unique bad Vivino winery IDs: ${badWineryIds.length}`);

  // ── Step 2: Get Vivino wine names from Phase 2 data for bad wineries ──
  // This tells us exactly what wine names were fetched from the wrong winery
  console.log('\nLoading Phase 2 data for bad wineries...');
  const rl2 = readline.createInterface({ input: fs.createReadStream('producer_wines_data.jsonl') });
  const badWineryWineNames = new Map(); // winery_id → Set of wine names
  let phase2BadCount = 0;

  for await (const line of rl2) {
    try {
      const j = JSON.parse(line);
      if (badWineryIds.includes(j.winery_id)) {
        if (!badWineryWineNames.has(j.winery_id)) badWineryWineNames.set(j.winery_id, new Set());
        badWineryWineNames.get(j.winery_id).add(j.name);
        phase2BadCount++;
      }
    } catch {}
  }
  console.log(`Phase 2 wine records from bad wineries: ${phase2BadCount}`);

  // ── Step 3: For each bad producer, find wines created by the crawl ──
  // Strategy: wines created on/after 2026-03-09 for these producer_ids
  // The crawl ran Phase 3 on 2026-03-09/10
  console.log('\nQuerying DB for wines from bad producers...');

  let totalWinesToDelete = 0;
  let totalScoresToDelete = 0;
  let totalPricesToDelete = 0;
  let totalGrapesToDelete = 0;
  let totalVintagesToDelete = 0;
  const wineIdsToDelete = [];

  // Process in batches of 20 producer IDs
  for (let i = 0; i < badProducerIds.length; i += 20) {
    const batch = badProducerIds.slice(i, i + 20);

    // Get wines for these producers that were created during/after crawl
    const { data: wines, error } = await supabase
      .from('wines')
      .select('id, name, producer_id, created_at')
      .in('producer_id', batch)
      .gte('created_at', '2026-03-09T00:00:00Z')
      .order('created_at', { ascending: true });

    if (error) {
      console.error(`Error fetching wines for batch ${i}:`, error.message);
      continue;
    }

    if (wines && wines.length > 0) {
      for (const w of wines) {
        wineIdsToDelete.push(w.id);
      }
      totalWinesToDelete += wines.length;
    }

    if (i % 100 === 0 && i > 0) {
      process.stdout.write(`  Checked ${i}/${badProducerIds.length} producers, found ${totalWinesToDelete} wines to delete\r`);
    }
  }

  console.log(`\nWines to delete (created >= 2026-03-09 for bad producers): ${totalWinesToDelete}`);

  // ── Step 4: Also find Vivino scores/prices that were applied to EXISTING wines ──
  // For the bad producers, there might be scores from the wrong Vivino winery
  // applied to existing wines (created before 2026-03-09)
  console.log('\nChecking for bad scores on existing wines...');

  const existingWineIds = [];
  for (let i = 0; i < badProducerIds.length; i += 20) {
    const batch = badProducerIds.slice(i, i + 20);
    const { data: wines, error } = await supabase
      .from('wines')
      .select('id')
      .in('producer_id', batch)
      .lt('created_at', '2026-03-09T00:00:00Z');

    if (error) continue;
    if (wines) existingWineIds.push(...wines.map(w => w.id));
  }

  console.log(`Pre-existing wines for bad producers: ${existingWineIds.length}`);

  // Check for Vivino scores on these pre-existing wines that were inserted during crawl
  let badScoresOnExisting = 0;
  const scoreIdsToDelete = [];

  for (let i = 0; i < existingWineIds.length; i += 50) {
    const batch = existingWineIds.slice(i, i + 50);
    const { data: scores, error } = await supabase
      .from('wine_vintage_scores')
      .select('id, wine_id, vintage_year')
      .in('wine_id', batch)
      .eq('publication_id', VIVINO_PUB_ID)
      .gte('created_at', '2026-03-09T00:00:00Z');

    if (error) continue;
    if (scores) {
      scoreIdsToDelete.push(...scores.map(s => s.id));
      badScoresOnExisting += scores.length;
    }
  }
  console.log(`Bad Vivino scores on pre-existing wines: ${badScoresOnExisting}`);

  // Check for Vivino prices on pre-existing wines
  let badPricesOnExisting = 0;
  const priceIdsToDelete = [];

  for (let i = 0; i < existingWineIds.length; i += 50) {
    const batch = existingWineIds.slice(i, i + 50);
    const { data: prices, error } = await supabase
      .from('wine_vintage_prices')
      .select('id, wine_id')
      .in('wine_id', batch)
      .eq('source_id', VIVINO_SOURCE_TYPE_ID)
      .gte('created_at', '2026-03-09T00:00:00Z');

    if (error) continue;
    if (prices) {
      priceIdsToDelete.push(...prices.map(p => p.id));
      badPricesOnExisting += prices.length;
    }
  }
  console.log(`Bad Vivino prices on pre-existing wines: ${badPricesOnExisting}`);

  // ── Step 5: Summary ──
  console.log('\n=== CLEANUP SUMMARY ===');
  console.log(`Wines to delete (new wines from wrong winery): ${wineIdsToDelete.length}`);
  console.log(`Scores to delete on pre-existing wines: ${scoreIdsToDelete.length}`);
  console.log(`Prices to delete on pre-existing wines: ${priceIdsToDelete.length}`);
  console.log(`(Cascading deletes will also remove scores, prices, grapes, vintages for deleted wines)`);

  if (DRY_RUN) {
    console.log('\n[DRY RUN] No changes made. Run without --dry-run to execute.');
    // Show some examples
    if (wineIdsToDelete.length > 0) {
      const sample = wineIdsToDelete.slice(0, 5);
      for (const id of sample) {
        const { data: w } = await supabase.from('wines').select('name, producer_id').eq('id', id).single();
        if (w) {
          const producer = badProducers.find(p => p.producer_id === w.producer_id);
          console.log(`  Wine: "${w.name}" (producer "${producer?.producer_name}" was matched to "${producer?.vivino_winery_name}")`);
        }
      }
    }
    return;
  }

  // ── Step 6: Execute deletions ──
  console.log('\nExecuting deletions...');

  // 6a: Delete scores on pre-existing wines
  if (scoreIdsToDelete.length > 0) {
    let deleted = 0;
    for (let i = 0; i < scoreIdsToDelete.length; i += 100) {
      const batch = scoreIdsToDelete.slice(i, i + 100);
      const { error } = await supabase.from('wine_vintage_scores').delete().in('id', batch);
      if (error) console.error('Score delete error:', error.message);
      else deleted += batch.length;
    }
    console.log(`  Deleted ${deleted} bad scores on pre-existing wines`);
  }

  // 6b: Delete prices on pre-existing wines
  if (priceIdsToDelete.length > 0) {
    let deleted = 0;
    for (let i = 0; i < priceIdsToDelete.length; i += 100) {
      const batch = priceIdsToDelete.slice(i, i + 100);
      const { error } = await supabase.from('wine_vintage_prices').delete().in('id', batch);
      if (error) console.error('Price delete error:', error.message);
      else deleted += batch.length;
    }
    console.log(`  Deleted ${deleted} bad prices on pre-existing wines`);
  }

  // 6c: Delete new wines (cascading will handle scores, prices, grapes, vintages)
  // First delete dependent data, then wines
  if (wineIdsToDelete.length > 0) {
    let deletedScores = 0, deletedPrices = 0, deletedGrapes = 0, deletedVintages = 0, deletedWines = 0;

    for (let i = 0; i < wineIdsToDelete.length; i += 100) {
      const batch = wineIdsToDelete.slice(i, i + 100);

      // Delete scores for these wines
      const { error: e1, count: c1 } = await supabase.from('wine_vintage_scores').delete({ count: 'exact' }).in('wine_id', batch);
      if (e1) console.error('Score delete error:', e1.message);
      else deletedScores += (c1 || 0);

      // Delete prices for these wines
      const { error: e2, count: c2 } = await supabase.from('wine_vintage_prices').delete({ count: 'exact' }).in('wine_id', batch);
      if (e2) console.error('Price delete error:', e2.message);
      else deletedPrices += (c2 || 0);

      // Delete grape links
      const { error: e3, count: c3 } = await supabase.from('wine_grapes').delete({ count: 'exact' }).in('wine_id', batch);
      if (e3) console.error('Grape delete error:', e3.message);
      else deletedGrapes += (c3 || 0);

      // Delete vintages
      const { error: e4, count: c4 } = await supabase.from('wine_vintages').delete({ count: 'exact' }).in('wine_id', batch);
      if (e4) console.error('Vintage delete error:', e4.message);
      else deletedVintages += (c4 || 0);

      // Delete wines
      const { error: e5, count: c5 } = await supabase.from('wines').delete({ count: 'exact' }).in('id', batch);
      if (e5) console.error('Wine delete error:', e5.message);
      else deletedWines += (c5 || 0);

      if (i % 500 === 0 && i > 0) {
        console.log(`  Progress: ${i}/${wineIdsToDelete.length} wines processed`);
      }
    }

    console.log(`  Deleted ${deletedWines} wines`);
    console.log(`  Deleted ${deletedScores} scores (from deleted wines)`);
    console.log(`  Deleted ${deletedPrices} prices (from deleted wines)`);
    console.log(`  Deleted ${deletedGrapes} grape links (from deleted wines)`);
    console.log(`  Deleted ${deletedVintages} vintages (from deleted wines)`);
  }

  // ── Step 7: Final counts ──
  console.log('\nFetching final DB stats...');
  const { count: wineCount } = await supabase.from('wines').select('*', { count: 'exact', head: true });
  const { count: scoreCount } = await supabase.from('wine_vintage_scores').select('*', { count: 'exact', head: true });
  const { count: priceCount } = await supabase.from('wine_vintage_prices').select('*', { count: 'exact', head: true });

  console.log(`\n=== FINAL DB STATS ===`);
  console.log(`Wines: ${wineCount}`);
  console.log(`Scores: ${scoreCount}`);
  console.log(`Prices: ${priceCount}`);
}

main().catch(console.error);
