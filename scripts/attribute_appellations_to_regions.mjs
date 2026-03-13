/**
 * Appellation → Region Attribution Script
 *
 * Attributes appellations from catch-all regions to their proper named regions
 * using a three-pass strategy:
 *   Pass 1: Containment hierarchy trace → bridge table lookup
 *   Pass 2: Name-pattern matching (regex)
 *   Pass 3: Direct slug-to-region lookup
 *
 * Usage:
 *   node scripts/attribute_appellations_to_regions.mjs --pass 1           # dry-run pass 1
 *   node scripts/attribute_appellations_to_regions.mjs --pass 1 --apply   # apply pass 1
 *   node scripts/attribute_appellations_to_regions.mjs --pass 2           # dry-run pass 2
 *   node scripts/attribute_appellations_to_regions.mjs --pass 3           # dry-run pass 3
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

// ── Load .env (same pattern as rebuild_regions.mjs) ─────────────────
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
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

const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY
);

const DATA_FILE = new URL('../data/appellation_region_attributions.json', import.meta.url);

// ── CLI args ────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const passArg = args.indexOf('--pass');
const passNum = passArg >= 0 ? parseInt(args[passArg + 1]) : null;
const applyMode = args.includes('--apply');

if (!passNum || ![1, 2, 3].includes(passNum)) {
  console.error('Usage: node scripts/attribute_appellations_to_regions.mjs --pass <1|2|3> [--apply]');
  process.exit(1);
}

console.log(`\n${'='.repeat(60)}`);
console.log(`APPELLATION → REGION ATTRIBUTION — Pass ${passNum}`);
console.log(`Mode: ${applyMode ? '🔴 APPLY' : '🔵 DRY-RUN'}`);
console.log(`${'='.repeat(60)}\n`);

// ── Load data ───────────────────────────────────────────────────────
const data = JSON.parse(readFileSync(DATA_FILE, 'utf-8'));

// ── Load DB state ───────────────────────────────────────────────────
async function loadCatchAllAppellations() {
  const all = [];
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data: batch, error } = await sb
      .from('appellations')
      .select('id, name, slug, country_id, region_id, regions!inner(is_catch_all, slug), countries!inner(slug, name)')
      .eq('regions.is_catch_all', true)
      .range(from, from + pageSize - 1);
    if (error) throw error;
    all.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }
  return all.map(a => ({
    id: a.id,
    name: a.name,
    slug: a.slug,
    country_id: a.country_id,
    country_slug: a.countries.slug,
    country_name: a.countries.name,
    region_id: a.region_id,
  }));
}

async function loadRegions() {
  const all = [];
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data: batch, error } = await sb
      .from('regions')
      .select('id, slug, name, country_id, is_catch_all, countries!inner(slug)')
      .range(from, from + pageSize - 1);
    if (error) throw error;
    all.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }
  // Build slug→id lookup (non-catch-all only for target regions)
  const regionBySlug = new Map();
  for (const r of all) {
    if (!r.is_catch_all) {
      regionBySlug.set(r.slug, { id: r.id, name: r.name, country_slug: r.countries.slug });
    }
  }
  return regionBySlug;
}

// ── Pass 1: Containment Trace ───────────────────────────────────────
async function runPass1(catchAllApps, regionBySlug) {
  console.log('Pass 1: Containment Trace → Bridge Lookup\n');

  // Build bridge lookup: appellation_slug → region_slug
  const bridgeMap = new Map();
  for (const entry of data.containment_bridges.entries) {
    bridgeMap.set(entry.appellation_slug, entry.region_slug);
  }

  // Run recursive CTE to find ALL ancestors for each catch-all appellation
  // We get: original_id, ancestor_slug, depth (ordered by depth ASC = closest first)
  const { data: ancestorData, error } = await sb.rpc('get_catchall_ancestors', {});

  // Since RPC might not exist, fall back to raw SQL via a different approach
  // We'll query containment in batches and build the tree in JS
  const ancestors = await traceCatchAllAncestors();

  const attributions = []; // { appellation_id, appellation_name, region_slug, region_id, method }
  const skipped = []; // { appellation, reason }

  const catchAllSet = new Set(catchAllApps.map(a => a.id));

  for (const app of catchAllApps) {
    // Step 1: Check if this appellation's own slug matches a bridge entry
    if (bridgeMap.has(app.slug)) {
      const regionSlug = bridgeMap.get(app.slug);
      if (regionSlug === null) {
        skipped.push({ name: app.name, slug: app.slug, country: app.country_name, reason: 'Bridge entry explicitly null (stays catch-all)' });
        continue;
      }
      const region = regionBySlug.get(regionSlug);
      if (region) {
        attributions.push({
          appellation_id: app.id,
          appellation_name: app.name,
          appellation_slug: app.slug,
          country: app.country_name,
          region_slug: regionSlug,
          region_name: region.name,
          region_id: region.id,
          method: 'bridge-self',
        });
        continue;
      }
    }

    // Step 2: Check ancestors (closest first)
    const appAncestors = ancestors.get(app.id) || [];
    let matched = false;
    for (const anc of appAncestors) {
      if (bridgeMap.has(anc.slug)) {
        const regionSlug = bridgeMap.get(anc.slug);
        if (regionSlug === null) {
          skipped.push({ name: app.name, slug: app.slug, country: app.country_name, reason: `Ancestor '${anc.name}' bridge is null (stays catch-all)` });
          matched = true;
          break;
        }
        const region = regionBySlug.get(regionSlug);
        if (region) {
          attributions.push({
            appellation_id: app.id,
            appellation_name: app.name,
            appellation_slug: app.slug,
            country: app.country_name,
            region_slug: regionSlug,
            region_name: region.name,
            region_id: region.id,
            method: `bridge-ancestor(${anc.name})`,
          });
          matched = true;
          break;
        }
      }
    }
    if (!matched && appAncestors.length > 0) {
      // Has ancestors but none matched a bridge
      const rootAnc = appAncestors[appAncestors.length - 1];
      skipped.push({
        name: app.name,
        slug: app.slug,
        country: app.country_name,
        reason: `Has containment (root: '${rootAnc.name}' / ${rootAnc.slug}) but no bridge match`,
      });
    }
    // If no ancestors at all, silently skip (handled by Pass 2/3)
  }

  return { attributions, skipped };
}

// ── Trace containment hierarchy for all catch-all appellations ──────
async function traceCatchAllAncestors() {
  // Get all containment rows
  const allContainment = [];
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data: batch, error } = await sb
      .from('appellation_containment')
      .select('child_id, parent_id')
      .range(from, from + pageSize - 1);
    if (error) throw error;
    allContainment.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }
  console.log(`  Loaded ${allContainment.length} containment rows`);

  // Get all appellation slugs/names for lookup
  const appLookup = new Map();
  let afrom = 0;
  while (true) {
    const { data: batch, error } = await sb
      .from('appellations')
      .select('id, name, slug')
      .range(afrom, afrom + pageSize - 1);
    if (error) throw error;
    for (const a of batch) appLookup.set(a.id, { name: a.name, slug: a.slug });
    if (batch.length < pageSize) break;
    afrom += pageSize;
  }
  console.log(`  Loaded ${appLookup.size} appellation lookups`);

  // Build parent map: child_id → [parent_id, ...]
  const parentMap = new Map();
  for (const row of allContainment) {
    if (!parentMap.has(row.child_id)) parentMap.set(row.child_id, []);
    parentMap.get(row.child_id).push(row.parent_id);
  }

  // For each appellation, trace upward to find all ancestors (BFS, closest first)
  // Returns Map<appellation_id, [{slug, name, depth}, ...]> sorted by depth ASC
  const result = new Map();

  for (const [childId, parentIds] of parentMap) {
    const ancestors = [];
    const visited = new Set([childId]);
    let frontier = parentIds.map(pid => ({ id: pid, depth: 1 }));

    while (frontier.length > 0) {
      const nextFrontier = [];
      for (const { id: ancId, depth } of frontier) {
        if (visited.has(ancId)) continue;
        visited.add(ancId);
        const info = appLookup.get(ancId);
        if (info) {
          ancestors.push({ slug: info.slug, name: info.name, depth });
        }
        const grandparents = parentMap.get(ancId) || [];
        for (const gp of grandparents) {
          if (!visited.has(gp)) {
            nextFrontier.push({ id: gp, depth: depth + 1 });
          }
        }
      }
      frontier = nextFrontier;
    }

    ancestors.sort((a, b) => a.depth - b.depth);
    result.set(childId, ancestors);
  }

  console.log(`  Built ancestor chains for ${result.size} appellations\n`);
  return result;
}

// ── Pass 2: Name Patterns ───────────────────────────────────────────
async function runPass2(catchAllApps, regionBySlug, alreadyAttributedIds) {
  console.log('Pass 2: Name-Pattern Matching\n');

  const patterns = data.name_patterns.entries.map(e => ({
    country: e.country,
    regex: new RegExp(e.pattern, 'i'),
    region_slug: e.region_slug,
    source: e.source,
  }));

  if (patterns.length === 0) {
    console.log('  No patterns defined yet. Add entries to name_patterns in the data file.\n');
    return { attributions: [], skipped: [] };
  }

  const remaining = catchAllApps.filter(a => !alreadyAttributedIds.has(a.id));
  const attributions = [];
  const skipped = [];

  for (const app of remaining) {
    let matched = false;
    for (const pat of patterns) {
      if (pat.country !== app.country_name) continue;
      if (pat.regex.test(app.name)) {
        const region = regionBySlug.get(pat.region_slug);
        if (region) {
          attributions.push({
            appellation_id: app.id,
            appellation_name: app.name,
            appellation_slug: app.slug,
            country: app.country_name,
            region_slug: pat.region_slug,
            region_name: region.name,
            region_id: region.id,
            method: `pattern(${pat.regex.source})`,
          });
          matched = true;
          break;
        }
      }
    }
    // Don't add to skipped — remaining unmatched handled by Pass 3
  }

  return { attributions, skipped };
}

// ── Pass 3: Direct Lookup ───────────────────────────────────────────
async function runPass3(catchAllApps, regionBySlug, alreadyAttributedIds) {
  console.log('Pass 3: Direct Slug-to-Region Lookup\n');

  const directMap = new Map();
  for (const entry of data.direct_attributions.entries) {
    directMap.set(entry.appellation_slug, entry.region_slug);
  }

  if (directMap.size === 0) {
    console.log('  No direct attributions defined yet. Add entries to direct_attributions in the data file.\n');
    return { attributions: [], skipped: [] };
  }

  const remaining = catchAllApps.filter(a => !alreadyAttributedIds.has(a.id));
  const attributions = [];

  for (const app of remaining) {
    if (directMap.has(app.slug)) {
      const regionSlug = directMap.get(app.slug);
      if (regionSlug === null) continue; // Explicitly stays catch-all
      const region = regionBySlug.get(regionSlug);
      if (region) {
        attributions.push({
          appellation_id: app.id,
          appellation_name: app.name,
          appellation_slug: app.slug,
          country: app.country_name,
          region_slug: regionSlug,
          region_name: region.name,
          region_id: region.id,
          method: 'direct-lookup',
        });
      }
    }
  }

  return { attributions, skipped: [] };
}

// ── Apply attributions ──────────────────────────────────────────────
async function applyAttributions(attributions) {
  console.log(`\nApplying ${attributions.length} attributions...`);
  const batchSize = 50;
  let applied = 0;

  for (let i = 0; i < attributions.length; i += batchSize) {
    const batch = attributions.slice(i, i + batchSize);
    for (const attr of batch) {
      const { error } = await sb
        .from('appellations')
        .update({ region_id: attr.region_id })
        .eq('id', attr.appellation_id);
      if (error) {
        console.error(`  ERROR updating ${attr.appellation_name}: ${error.message}`);
      } else {
        applied++;
      }
    }
    if (i + batchSize < attributions.length) {
      process.stdout.write(`  ${applied}/${attributions.length}...\r`);
    }
  }
  console.log(`  ✅ Applied ${applied}/${attributions.length} attributions`);
}

// ── Report ──────────────────────────────────────────────────────────
function printReport(attributions, skipped, totalCatchAll) {
  // Group by country
  const byCountry = new Map();
  for (const attr of attributions) {
    if (!byCountry.has(attr.country)) byCountry.set(attr.country, []);
    byCountry.get(attr.country).push(attr);
  }

  console.log('\n' + '─'.repeat(60));
  console.log('ATTRIBUTION SUMMARY');
  console.log('─'.repeat(60));

  const sorted = [...byCountry.entries()].sort((a, b) => b[1].length - a[1].length);
  for (const [country, attrs] of sorted) {
    console.log(`\n  ${country}: ${attrs.length} attributions`);
    // Group by target region
    const byRegion = new Map();
    for (const a of attrs) {
      const key = `${a.region_name} (${a.region_slug})`;
      if (!byRegion.has(key)) byRegion.set(key, 0);
      byRegion.set(key, byRegion.get(key) + 1);
    }
    for (const [region, count] of [...byRegion.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`    → ${region}: ${count}`);
    }
  }

  if (skipped.length > 0) {
    console.log('\n' + '─'.repeat(60));
    console.log(`SKIPPED (${skipped.length}):`);
    console.log('─'.repeat(60));
    // Group by reason
    const byReason = new Map();
    for (const s of skipped) {
      const key = s.reason;
      if (!byReason.has(key)) byReason.set(key, []);
      byReason.get(key).push(s);
    }
    for (const [reason, items] of byReason) {
      console.log(`\n  ${reason}`);
      for (const item of items.slice(0, 5)) {
        console.log(`    - ${item.name} (${item.country})`);
      }
      if (items.length > 5) console.log(`    ... and ${items.length - 5} more`);
    }
  }

  // Unmatched roots (containment roots with no bridge)
  const unmatchedRoots = skipped.filter(s => s.reason.includes('no bridge match'));
  if (unmatchedRoots.length > 0) {
    console.log('\n' + '─'.repeat(60));
    console.log('⚠️  UNMATCHED CONTAINMENT ROOTS (need bridge entries):');
    console.log('─'.repeat(60));
    const rootSet = new Set();
    for (const s of unmatchedRoots) {
      const match = s.reason.match(/root: '(.+?)' \/ (.+?)\)/);
      if (match) rootSet.add(`${s.country} | ${match[1]} (${match[2]})`);
    }
    for (const r of [...rootSet].sort()) {
      console.log(`  - ${r}`);
    }
  }

  console.log('\n' + '─'.repeat(60));
  console.log(`TOTALS: ${attributions.length} attributed, ${skipped.length} skipped, ${totalCatchAll - attributions.length - skipped.length} untouched (no containment data for Pass 1)`);
  console.log('─'.repeat(60) + '\n');
}

// ── Main ────────────────────────────────────────────────────────────
async function main() {
  const catchAllApps = await loadCatchAllAppellations();
  console.log(`Loaded ${catchAllApps.length} catch-all appellations\n`);

  const regionBySlug = await loadRegions();
  console.log(`Loaded ${regionBySlug.size} named regions\n`);

  let attributions = [];
  let skipped = [];

  if (passNum === 1) {
    const result = await runPass1(catchAllApps, regionBySlug);
    attributions = result.attributions;
    skipped = result.skipped;
  } else if (passNum === 2) {
    // For Pass 2, we'd need to know which were already attributed by Pass 1
    // Since Pass 1 is applied first, those won't be in catch-all anymore
    const result = await runPass2(catchAllApps, regionBySlug, new Set());
    attributions = result.attributions;
    skipped = result.skipped;
  } else if (passNum === 3) {
    const result = await runPass3(catchAllApps, regionBySlug, new Set());
    attributions = result.attributions;
    skipped = result.skipped;
  }

  printReport(attributions, skipped, catchAllApps.length);

  if (applyMode && attributions.length > 0) {
    await applyAttributions(attributions);
  } else if (applyMode && attributions.length === 0) {
    console.log('Nothing to apply.');
  } else {
    console.log('Dry-run complete. Use --apply to execute changes.');
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
