#!/usr/bin/env node
/**
 * review_region_boundaries.mjs
 *
 * Phase 4: Wine Expert Sonnet Review of all region boundaries.
 * Uses Claude Sonnet to review every region as a wine expert would,
 * checking for attribution issues, missing areas, and boundary sanity.
 *
 * Batches by country for efficiency. Outputs a detailed report and
 * can automatically apply corrections (with logging).
 *
 * Usage:
 *   node scripts/review_region_boundaries.mjs --dry-run     # Review only, no changes
 *   node scripts/review_region_boundaries.mjs --apply        # Review + apply corrections
 *   node scripts/review_region_boundaries.mjs --country=FR   # Review single country
 */

import { readFileSync } from 'fs';
import { writeFileSync } from 'fs';
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
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

const APPLY = process.argv.includes('--apply');
const MODE = APPLY ? 'APPLY' : 'DRY RUN';
const COUNTRY_FILTER = process.argv.find(a => a.startsWith('--country='))?.split('=')[1];

// ── Anthropic API ───────────────────────────────────────────
async function callSonnet(systemPrompt, userMessage, maxTokens = 4096) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: maxTokens,
          system: systemPrompt,
          messages: [{ role: 'user', content: userMessage }],
        }),
      });

      if (res.status === 429 || res.status === 529) {
        const wait = Math.min(30000, 5000 * (attempt + 1));
        console.log(`    Rate limited, waiting ${wait / 1000}s...`);
        await new Promise(r => setTimeout(r, wait));
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`API ${res.status}: ${body.slice(0, 200)}`);
      }

      const data = await res.json();
      return data.content[0].text;
    } catch (err) {
      if (attempt === 2) throw err;
      console.log(`    Retry ${attempt + 1}: ${err.message}`);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

// ── Load all data ───────────────────────────────────────────
async function loadData() {
  // All named regions with boundaries
  const allRegions = [];
  let from = 0;
  while (true) {
    const { data, error } = await sb
      .from('regions')
      .select('id, slug, name, parent_id, country_id, is_catch_all')
      .is('deleted_at', null)
      .eq('is_catch_all', false)
      .range(from, from + 999);
    if (error) throw error;
    allRegions.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }

  // Countries
  const { data: countries, error: cErr } = await sb
    .from('countries')
    .select('id, name, iso_code')
    .is('deleted_at', null);
  if (cErr) throw cErr;
  const countryById = {};
  for (const c of countries) countryById[c.id] = c;

  // Region boundaries
  const allBounds = [];
  from = 0;
  while (true) {
    const { data, error } = await sb
      .from('geographic_boundaries')
      .select('region_id, boundary_confidence, boundary_source, centroid, boundary')
      .not('region_id', 'is', null)
      .range(from, from + 999);
    if (error) throw error;
    allBounds.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }
  const boundByRegion = {};
  for (const b of allBounds) boundByRegion[b.region_id] = b;

  // All appellations with region attribution
  const allApps = [];
  from = 0;
  while (true) {
    const { data, error } = await sb
      .from('appellations')
      .select('id, slug, name, region_id, country_id, classification_level')
      .is('deleted_at', null)
      .range(from, from + 999);
    if (error) throw error;
    allApps.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }

  // Catch-all regions (to find appellations still on catch-all)
  const { data: catchAlls, error: caErr } = await sb
    .from('regions')
    .select('id, country_id')
    .is('deleted_at', null)
    .eq('is_catch_all', true);
  if (caErr) throw caErr;
  const catchAllByCountry = {};
  for (const ca of catchAlls) catchAllByCountry[ca.country_id] = ca.id;

  return { allRegions, countryById, boundByRegion, allApps, catchAllByCountry };
}

// ── Build country review data ───────────────────────────────
function buildCountryReview(countryId, data) {
  const { allRegions, countryById, boundByRegion, allApps, catchAllByCountry } = data;
  const country = countryById[countryId];

  const regions = allRegions.filter(r => r.country_id === countryId);
  const regionById = {};
  for (const r of regions) regionById[r.id] = r;

  // Build region hierarchy
  const l1Regions = regions.filter(r => !r.parent_id);
  const l2Regions = regions.filter(r => r.parent_id);

  // Appellations per region
  const appsByRegion = {};
  const catchAllApps = [];
  for (const a of allApps) {
    if (a.country_id !== countryId) continue;
    if (a.region_id === catchAllByCountry[countryId]) {
      catchAllApps.push(a);
    } else if (a.region_id && regionById[a.region_id]) {
      if (!appsByRegion[a.region_id]) appsByRegion[a.region_id] = [];
      appsByRegion[a.region_id].push(a);
    }
  }

  // Build summary for each region
  const regionSummaries = regions.map(r => {
    const bound = boundByRegion[r.id];
    const apps = appsByRegion[r.id] || [];
    const level = r.parent_id ? 'L2' : 'L1';
    const parent = r.parent_id ? regionById[r.parent_id] : null;
    const children = l2Regions.filter(l2 => l2.parent_id === r.id);

    return {
      slug: r.slug,
      name: r.name,
      level,
      parent: parent?.name || null,
      children: children.map(c => c.name),
      boundary: bound ? {
        confidence: bound.boundary_confidence,
        source: bound.boundary_source,
        hasPolygon: !!bound.boundary,
      } : null,
      appellations: apps.map(a => ({
        name: a.name,
        level: a.classification_level,
      })),
    };
  });

  return {
    country: country.name,
    iso: country.iso_code,
    regionCount: regions.length,
    catchAllApps: catchAllApps.map(a => ({
      name: a.name,
      level: a.classification_level,
    })),
    regions: regionSummaries,
  };
}

const SYSTEM_PROMPT = `You are a Master of Wine reviewing a wine database's region boundaries and appellation attributions. Your role is to ensure every region is correctly structured and bounded as a wine professional would expect.

Context: This is a two-level region hierarchy (L1 parent, L2 child). Each region has appellations attributed to it. Region boundaries were derived from child appellation polygon unions, copied from matching appellations, or fetched from administrative boundary databases.

Key principle: Appellations should be on the LOWEST-LEVEL region they can accurately be attributed to. They roll up naturally from L2 → L1.

Key distinction: Appellations are legally defined. Regions are qualitative approximations of wine-producing areas. It's OK for region boundaries to be somewhat approximate.

For each country, review:
1. ATTRIBUTION: Are appellations on the right regions? Should any catch-all appellations move to a named region?
2. MISSING REGIONS: Are there important wine regions missing from the hierarchy?
3. BOUNDARY SANITY: Do the boundary sources make sense? Any obviously wrong boundaries?
4. NAMING: Are regions named as a WSET L3 student or MW would expect?

Respond with JSON only. Format:
{
  "country": "Country Name",
  "overall_assessment": "pass" | "minor_issues" | "major_issues",
  "corrections": [
    {
      "type": "move_appellation" | "rename_region" | "flag_review" | "smooth_boundary",
      "description": "What and why",
      "region_slug": "affected-region",
      "appellation_name": "if applicable",
      "severity": "auto_fix" | "needs_review"
    }
  ],
  "notes": "Brief overall assessment"
}

Rules:
- Only suggest corrections you're confident about as a wine expert
- "move_appellation" = move from catch-all to a named region (must specify which)
- "smooth_boundary" = suggest using ST_ConvexHull to fill gaps in derived boundaries
- "flag_review" = something looks off but you're not sure enough to auto-fix
- Keep corrections actionable and specific
- If everything looks good, return empty corrections array`;

// ── Main ────────────────────────────────────────────────────
async function main() {
  console.log(`\n=== Wine Expert Region Review — Phase 4 (${MODE}) ===\n`);

  const data = await loadData();
  const { allRegions, countryById } = data;

  // Group regions by country
  const countriesWithRegions = new Map();
  for (const r of allRegions) {
    if (!countriesWithRegions.has(r.country_id)) {
      countriesWithRegions.set(r.country_id, []);
    }
    countriesWithRegions.get(r.country_id).push(r);
  }

  // Filter if --country specified
  let countryIds = [...countriesWithRegions.keys()];
  if (COUNTRY_FILTER) {
    countryIds = countryIds.filter(id =>
      countryById[id].iso_code === COUNTRY_FILTER.toUpperCase()
    );
    if (countryIds.length === 0) {
      console.log(`No regions found for country: ${COUNTRY_FILTER}`);
      return;
    }
  }

  // Sort by region count (biggest first for most impactful review)
  countryIds.sort((a, b) =>
    countriesWithRegions.get(b).length - countriesWithRegions.get(a).length
  );

  console.log(`Reviewing ${allRegions.length} regions across ${countryIds.length} countries\n`);

  const allResults = [];
  let totalCorrections = 0;
  let autoFixCount = 0;
  let reviewCount = 0;

  for (let i = 0; i < countryIds.length; i++) {
    const countryId = countryIds[i];
    const country = countryById[countryId];
    const regionCount = countriesWithRegions.get(countryId).length;

    console.log(`[${i + 1}/${countryIds.length}] ${country.name} (${regionCount} regions)...`);

    const reviewData = buildCountryReview(countryId, data);
    const userMessage = JSON.stringify(reviewData, null, 2);

    try {
      const response = await callSonnet(SYSTEM_PROMPT, userMessage);

      // Parse JSON from response (handle markdown code blocks)
      let jsonStr = response;
      const jsonMatch = response.match(/```(?:json)?\s*([\s\S]*?)```/);
      if (jsonMatch) jsonStr = jsonMatch[1];
      jsonStr = jsonStr.trim();

      const result = JSON.parse(jsonStr);
      allResults.push(result);

      const corrections = result.corrections || [];
      totalCorrections += corrections.length;

      const autoFixes = corrections.filter(c => c.severity === 'auto_fix');
      const reviews = corrections.filter(c => c.severity === 'needs_review');
      autoFixCount += autoFixes.length;
      reviewCount += reviews.length;

      if (corrections.length === 0) {
        console.log(`  ✅ ${result.overall_assessment}`);
      } else {
        console.log(`  ${result.overall_assessment === 'major_issues' ? '❌' : '⚠️'} ${result.overall_assessment}: ${corrections.length} items`);
        for (const c of corrections) {
          const icon = c.severity === 'auto_fix' ? '🔧' : '👁️';
          console.log(`    ${icon} [${c.type}] ${c.description}`);
        }
      }

      if (result.notes) {
        console.log(`  📝 ${result.notes}`);
      }
    } catch (err) {
      console.log(`  ❌ Error: ${err.message}`);
      allResults.push({
        country: country.name,
        overall_assessment: 'error',
        corrections: [],
        notes: err.message,
      });
    }
  }

  // Write full report
  const reportPath = new URL('../data/region_review_report.json', import.meta.url)
    .pathname.replace(/^\/([A-Z]:)/, '$1');
  writeFileSync(reportPath, JSON.stringify(allResults, null, 2));

  console.log(`\n${'='.repeat(60)}`);
  console.log(`REVIEW COMPLETE`);
  console.log(`${'='.repeat(60)}`);
  console.log(`Countries reviewed: ${countryIds.length}`);
  console.log(`Total corrections found: ${totalCorrections}`);
  console.log(`  Auto-fixable: ${autoFixCount}`);
  console.log(`  Needs review: ${reviewCount}`);
  console.log(`\nFull report: data/region_review_report.json`);

  // Apply auto-fixes if in apply mode
  if (APPLY && autoFixCount > 0) {
    console.log(`\nApplying ${autoFixCount} auto-fixes...`);
    await applyCorrections(allResults, data);
  }

  console.log(`\nDone!`);
}

async function applyCorrections(results, data) {
  const { allRegions, catchAllByCountry, countryById, allApps } = data;

  const regionBySlug = {};
  for (const r of allRegions) regionBySlug[r.slug] = r;

  for (const result of results) {
    const autoFixes = (result.corrections || []).filter(c => c.severity === 'auto_fix');
    if (autoFixes.length === 0) continue;

    for (const fix of autoFixes) {
      if (fix.type === 'move_appellation' && fix.region_slug && fix.appellation_name) {
        const region = regionBySlug[fix.region_slug];
        if (!region) {
          console.log(`  ⚠️ Region not found: ${fix.region_slug}`);
          continue;
        }

        // Find the appellation
        const app = allApps.find(a =>
          a.name === fix.appellation_name && a.country_id === region.country_id
        );
        if (!app) {
          console.log(`  ⚠️ Appellation not found: ${fix.appellation_name}`);
          continue;
        }

        // Move it
        const { error } = await sb
          .from('appellations')
          .update({ region_id: region.id })
          .eq('id', app.id);

        if (error) {
          console.log(`  ❌ Failed to move ${fix.appellation_name}: ${error.message}`);
        } else {
          console.log(`  ✅ Moved ${fix.appellation_name} → ${region.name}`);
        }
      } else if (fix.type === 'smooth_boundary' && fix.region_slug) {
        console.log(`  📋 Smooth boundary noted for ${fix.region_slug} (applied in batch)`);
      }
    }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
