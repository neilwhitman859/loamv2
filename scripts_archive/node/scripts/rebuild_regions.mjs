#!/usr/bin/env node
/**
 * rebuild_regions.mjs
 *
 * Rebuilds the regions table from data/regions_rebuild.json.
 * Two modes:
 *   --dry-run   Validate JSON, check countries exist in DB, report what would be inserted (default)
 *   --insert    Actually insert new regions into the DB (does NOT delete old regions)
 *
 * The appellation remapping and old region cleanup are deferred to a future session.
 *
 * Usage:
 *   node scripts/rebuild_regions.mjs              # Dry run
 *   node scripts/rebuild_regions.mjs --insert     # Insert new regions
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

// ── Load .env ───────────────────────────────────────────────
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

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

const INSERT_MODE = process.argv.includes('--insert');

// ── Step 1: Load and validate JSON ──────────────────────────
console.log('Step 1: Loading and validating data/regions_rebuild.json...\n');

const dataPath = new URL('../data/regions_rebuild.json', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const regions = JSON.parse(readFileSync(dataPath, 'utf8'));

const catchAll = regions.filter(r => r.is_catch_all);
const l1 = regions.filter(r => !r.is_catch_all && !r.parent);
const l2 = regions.filter(r => !r.is_catch_all && r.parent);

console.log(`  Total entries: ${regions.length}`);
console.log(`  Catch-all: ${catchAll.length}`);
console.log(`  L1: ${l1.length}`);
console.log(`  L2: ${l2.length}`);

// Check duplicate slugs
const slugs = regions.map(r => r.slug);
const seen = new Set();
const dupes = [];
for (const s of slugs) {
  if (seen.has(s)) dupes.push(s);
  seen.add(s);
}
if (dupes.length > 0) {
  console.error(`\n  ERROR: Duplicate slugs found: ${dupes.join(', ')}`);
  process.exit(1);
}
console.log('  Duplicate slugs: none ✓');

// Check L2 parents reference valid L1 slugs
const l1Slugs = new Set(l1.map(r => r.slug));
const badParents = l2.filter(r => !l1Slugs.has(r.parent));
if (badParents.length > 0) {
  console.error(`\n  ERROR: L2 regions with invalid parents:`);
  for (const r of badParents) console.error(`    ${r.slug} -> ${r.parent}`);
  process.exit(1);
}
console.log('  L2 parent references: all valid ✓');

// Check each country has exactly one catch-all
const countriesInData = new Set(regions.map(r => r.country));
const catchAllByCountry = {};
for (const r of catchAll) {
  catchAllByCountry[r.country] = (catchAllByCountry[r.country] || 0) + 1;
}
for (const c of countriesInData) {
  if (!catchAllByCountry[c]) {
    console.error(`\n  ERROR: Country "${c}" has no catch-all region`);
    process.exit(1);
  }
  if (catchAllByCountry[c] > 1) {
    console.error(`\n  ERROR: Country "${c}" has ${catchAllByCountry[c]} catch-all regions`);
    process.exit(1);
  }
}
console.log('  Catch-all per country: exactly one each ✓');

// ── Step 2: Check countries exist in DB ─────────────────────
console.log('\nStep 2: Verifying countries exist in database...\n');

// Fetch all countries from DB
const allCountries = [];
let from = 0;
const PAGE = 1000;
while (true) {
  const { data, error } = await sb.from('countries').select('id, name, slug').range(from, from + PAGE - 1);
  if (error) { console.error('  DB error:', error.message); process.exit(1); }
  allCountries.push(...data);
  if (data.length < PAGE) break;
  from += PAGE;
}

const countryByName = new Map(allCountries.map(c => [c.name, c]));
const missing = [];
for (const c of countriesInData) {
  if (!countryByName.has(c)) missing.push(c);
}
if (missing.length > 0) {
  console.error(`  ERROR: Countries not found in DB: ${missing.join(', ')}`);
  process.exit(1);
}
console.log(`  All ${countriesInData.size} countries found in DB ✓`);

// ── Step 3: Check for existing regions with same slugs ──────
console.log('\nStep 3: Checking for slug conflicts with existing regions...\n');

const existingRegions = [];
from = 0;
while (true) {
  const { data, error } = await sb.from('regions').select('id, name, slug, country_id, is_catch_all, parent_id').range(from, from + PAGE - 1);
  if (error) { console.error('  DB error:', error.message); process.exit(1); }
  existingRegions.push(...data);
  if (data.length < PAGE) break;
  from += PAGE;
}
console.log(`  Existing regions in DB: ${existingRegions.length}`);

const existingSlugSet = new Set(existingRegions.map(r => r.slug));
const conflicts = regions.filter(r => existingSlugSet.has(r.slug));
if (conflicts.length > 0) {
  const catchAllConflicts = conflicts.filter(r => r.is_catch_all);
  const nonCatchAllConflicts = conflicts.filter(r => !r.is_catch_all);
  console.log(`  Slug conflicts: ${conflicts.length} (${catchAllConflicts.length} catch-all, ${nonCatchAllConflicts.length} non-catch-all)`);
  if (nonCatchAllConflicts.length > 0) {
    console.log('  Non-catch-all conflicts:');
    for (const r of nonCatchAllConflicts.slice(0, 10)) {
      console.log(`    ${r.slug} (${r.country}/${r.name})`);
    }
    if (nonCatchAllConflicts.length > 10) console.log(`    ... and ${nonCatchAllConflicts.length - 10} more`);
  }
} else {
  console.log('  No slug conflicts ✓');
}

// ── Summary ─────────────────────────────────────────────────
console.log('\n' + '═'.repeat(60));
console.log('SUMMARY');
console.log('═'.repeat(60));
console.log(`  Would insert: ${regions.length - conflicts.length} new regions`);
console.log(`  Would skip (slug exists): ${conflicts.length}`);
console.log(`  Countries with new regions: ${[...countriesInData].filter(c => {
  return regions.filter(r => r.country === c && !r.is_catch_all && !existingSlugSet.has(r.slug)).length > 0;
}).length}`);

// Per-country breakdown
console.log('\n  Per-country breakdown:');
for (const c of [...countriesInData].sort()) {
  const regs = regions.filter(r => r.country === c && !r.is_catch_all);
  if (regs.length === 0) continue;
  const newRegs = regs.filter(r => !existingSlugSet.has(r.slug));
  const existingRegs = regs.filter(r => existingSlugSet.has(r.slug));
  console.log(`    ${c}: ${regs.length} regions (${newRegs.length} new, ${existingRegs.length} existing)`);
}

if (!INSERT_MODE) {
  console.log('\n  Mode: DRY RUN — no changes made');
  console.log('  Run with --insert to insert new regions into DB');
  process.exit(0);
}

// ── Step 4: Insert new regions ──────────────────────────────
console.log('\n' + '═'.repeat(60));
console.log('INSERTING NEW REGIONS');
console.log('═'.repeat(60));

// Build country_id lookup
const countryIdByName = new Map(allCountries.map(c => [c.name, c.id]));

// Insert catch-all regions first (skip existing)
let inserted = 0;
let skipped = 0;

console.log('\n  Inserting catch-all regions...');
for (const r of catchAll) {
  if (existingSlugSet.has(r.slug)) {
    skipped++;
    continue;
  }
  const { error } = await sb.from('regions').insert({
    name: r.name,
    slug: r.slug,
    country_id: countryIdByName.get(r.country),
    is_catch_all: true,
    parent_id: null,
  });
  if (error) {
    console.error(`    ERROR inserting catch-all ${r.slug}: ${error.message}`);
    continue;
  }
  inserted++;
}
console.log(`    Inserted: ${inserted}, Skipped (existing): ${skipped}`);

// Insert L1 regions
console.log('\n  Inserting L1 regions...');
let l1Inserted = 0;
let l1Skipped = 0;
const newL1IdBySlug = new Map();

for (const r of l1) {
  if (existingSlugSet.has(r.slug)) {
    // Fetch existing ID for L2 parent resolution
    const existing = existingRegions.find(e => e.slug === r.slug);
    if (existing) newL1IdBySlug.set(r.slug, existing.id);
    l1Skipped++;
    continue;
  }
  const { data, error } = await sb.from('regions').insert({
    name: r.name,
    slug: r.slug,
    country_id: countryIdByName.get(r.country),
    is_catch_all: false,
    parent_id: null,
  }).select('id');
  if (error) {
    console.error(`    ERROR inserting L1 ${r.slug}: ${error.message}`);
    continue;
  }
  newL1IdBySlug.set(r.slug, data[0].id);
  l1Inserted++;
}
console.log(`    Inserted: ${l1Inserted}, Skipped (existing): ${l1Skipped}`);

// Insert L2 regions
console.log('\n  Inserting L2 regions...');
let l2Inserted = 0;
let l2Skipped = 0;
let l2Errors = 0;

for (const r of l2) {
  if (existingSlugSet.has(r.slug)) {
    l2Skipped++;
    continue;
  }
  const parentId = newL1IdBySlug.get(r.parent);
  if (!parentId) {
    console.error(`    ERROR: No parent ID found for ${r.slug} -> ${r.parent}`);
    l2Errors++;
    continue;
  }
  const { error } = await sb.from('regions').insert({
    name: r.name,
    slug: r.slug,
    country_id: countryIdByName.get(r.country),
    is_catch_all: false,
    parent_id: parentId,
  });
  if (error) {
    console.error(`    ERROR inserting L2 ${r.slug}: ${error.message}`);
    l2Errors++;
    continue;
  }
  l2Inserted++;
}
console.log(`    Inserted: ${l2Inserted}, Skipped (existing): ${l2Skipped}, Errors: ${l2Errors}`);

// Final summary
console.log('\n' + '═'.repeat(60));
console.log('DONE');
console.log('═'.repeat(60));
console.log(`  Total inserted: ${inserted + l1Inserted + l2Inserted}`);
console.log(`  Total skipped: ${skipped + l1Skipped + l2Skipped}`);
console.log(`  Total errors: ${l2Errors}`);

// Verify final count
const { count } = await sb.from('regions').select('*', { count: 'exact', head: true });
console.log(`  Total regions in DB now: ${count}`);
