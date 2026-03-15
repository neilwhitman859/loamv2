#!/usr/bin/env node
/**
 * fetch_kl_catalog.mjs — Full Kermit Lynch catalog extraction via their API
 *
 * Endpoints discovered on kermitlynch.com:
 *   /api/v1?action=getWines         → 1,523 wines (list with IDs/SKUs)
 *   /api/v1?action=getWine&id=N     → wine detail (blend, soil, vine_age, viticulture)
 *   /api/v1?action=getGrowers       → 193 growers
 *   /api/v1?action=getGrower&slug=X → grower profile (about, founded, website, coordinates)
 *   /api/v1?action=getRegions       → 29 regions
 *   /api/v1?action=getFarming       → 9 farming types
 *   /api/v1?action=getWineTypes     → 9 wine types
 *
 * Saves: data/imports/kermit_lynch_catalog.json
 */

import { writeFileSync, readFileSync, existsSync } from 'fs';

const BASE = 'https://kermitlynch.com/api/v1';
const CACHE_FILE = 'data/imports/kl_wine_details_cache.json';
const OUT_FILE = 'data/imports/kermit_lynch_catalog.json';

// Rate-limited fetch
let lastFetch = 0;
async function apiFetch(action, params = '') {
  const delay = 100; // 100ms between requests
  const now = Date.now();
  if (now - lastFetch < delay) await new Promise(r => setTimeout(r, delay - (now - lastFetch)));
  lastFetch = Date.now();

  const url = `${BASE}?action=${action}${params}`;
  const resp = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Loam/1.0)', 'Accept': 'application/json' }
  });
  const text = await resp.text();
  if (!text) return null;
  return JSON.parse(text);
}

function stripHtml(html) {
  if (!html) return '';
  return html
    .replace(/<em[^>]*>/g, '').replace(/<\/em>/g, '')
    .replace(/<p[^>]*>/g, '\n').replace(/<\/p>/g, '')
    .replace(/<br\s*\/?>/g, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&bull;/g, '•')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function cleanWineName(name) {
  return stripHtml(name).replace(/\s+/g, ' ').trim();
}

async function main() {
  console.log('=== Kermit Lynch Full Catalog Extraction ===\n');

  // 1. Fetch reference data
  console.log('Fetching reference data...');
  const [winesList, growersList, regions, farming, wineTypes] = await Promise.all([
    apiFetch('getWines'),
    apiFetch('getGrowers'),
    apiFetch('getRegions'),
    apiFetch('getFarming'),
    apiFetch('getWineTypes'),
  ]);

  // Build lookup maps
  const regionMap = Object.fromEntries(regions.map(r => [r.id, r.name]));
  const farmingMap = Object.fromEntries(farming.map(f => [f.id, f.name]));
  const wineTypeMap = Object.fromEntries(wineTypes.map(t => [t.id, t.value]));
  // Country IDs (inferred from data: 1=France, 2=Italy based on regions)
  const countryMap = { 1: 'France', 2: 'Italy' };

  console.log(`  Wines: ${winesList.length}`);
  console.log(`  Growers: ${growersList.length}`);
  console.log(`  Regions: ${regions.length}`);

  // Filter to actual wines (exclude Grocery, Spirits, Vermouth, Liqueur)
  const wineTypeIds = new Set(
    wineTypes.filter(t => ['Red', 'White', 'Rosé', 'Sparkling', 'Dessert'].includes(t.value)).map(t => t.id)
  );
  const actualWines = winesList.filter(w => wineTypeIds.has(w.wine_type));
  console.log(`  Actual wines (excluding grocery/spirits): ${actualWines.length}`);

  // 2. Load cache of wine details (resume support)
  let detailCache = {};
  if (existsSync(CACHE_FILE)) {
    detailCache = JSON.parse(readFileSync(CACHE_FILE, 'utf-8'));
    console.log(`\n  Loaded ${Object.keys(detailCache).length} cached wine details`);
  }

  // 3. Fetch wine details for all wines
  const needFetch = actualWines.filter(w => !detailCache[w.id]);
  console.log(`\n  Need to fetch ${needFetch.length} wine details...\n`);

  let fetched = 0;
  for (const wine of needFetch) {
    try {
      const detail = await apiFetch('getWine', `&id=${wine.id}`);
      if (detail) {
        detailCache[wine.id] = detail;
        fetched++;
        if (fetched % 50 === 0) {
          console.log(`  Fetched ${fetched}/${needFetch.length} details...`);
          // Save cache periodically
          writeFileSync(CACHE_FILE, JSON.stringify(detailCache, null, 2));
        }
      }
    } catch (e) {
      console.warn(`  Error fetching wine ${wine.id}: ${e.message}`);
    }
  }

  // Save final cache
  if (fetched > 0) {
    writeFileSync(CACHE_FILE, JSON.stringify(detailCache, null, 2));
    console.log(`  Cached ${fetched} new details`);
  }

  // 4. Fetch grower profiles
  console.log('\nFetching grower profiles...');
  const growerProfiles = {};
  let growersFetched = 0;
  for (const grower of growersList) {
    try {
      const profile = await apiFetch('getGrower', `&slug=${grower.slug}`);
      if (profile) {
        growerProfiles[grower.id] = profile;
        growersFetched++;
        if (growersFetched % 20 === 0) {
          console.log(`  Fetched ${growersFetched}/${growersList.length} grower profiles...`);
        }
      }
    } catch (e) {
      console.warn(`  Error fetching grower ${grower.slug}: ${e.message}`);
    }
  }
  console.log(`  Fetched ${growersFetched} grower profiles`);

  // 5. Assemble the catalog
  console.log('\nAssembling catalog...');

  const growers = growersList.map(g => {
    const profile = growerProfiles[g.id];
    const farmingIds = String(g.farming || '').split(',').map(Number).filter(Boolean);
    return {
      kl_id: g.id,
      name: g.name,
      slug: g.slug,
      country: countryMap[g.country] || `country_${g.country}`,
      region: regionMap[g.region] || null,
      farming: farmingIds.map(id => farmingMap[id]).filter(Boolean),
      winemaker: profile?.producer || null,
      founded_year: profile?.founded || null,
      website: profile?.www || null,
      location: profile?.location || null,
      annual_production: profile?.annual_production || null,
      viticulture_notes: profile ? stripHtml(profile.viticulture) : null,
      about: profile ? stripHtml(profile.about).substring(0, 500) : null,
    };
  });

  const wines = actualWines.map(w => {
    const detail = detailCache[w.id] || {};
    const farmingIds = String(w.farming || '').split(',').map(Number).filter(Boolean);
    return {
      kl_id: w.id,
      sku: w.sku,
      wine_name: cleanWineName(w.name),
      grower_name: w.grower,
      grower_kl_id: w.grower_id,
      country: countryMap[w.country] || `country_${w.country}`,
      region: regionMap[w.region] || null,
      wine_type: wineTypeMap[w.wine_type] || null,
      blend: detail.blend || null,
      soil: detail.soil || null,
      vine_age: detail.vine_age || null,
      vineyard_area: detail.vineyard_area || null,
      vinification: detail.viticulture ? stripHtml(detail.viticulture) : null,
      farming: farmingIds.map(id => farmingMap[id]).filter(Boolean),
    };
  });

  const catalog = {
    source: 'kermitlynch.com',
    extracted: new Date().toISOString(),
    summary: {
      total_wines: wines.length,
      total_growers: growers.length,
      regions: [...new Set(wines.map(w => w.region).filter(Boolean))].sort(),
      countries: [...new Set(wines.map(w => w.country))].sort(),
      wine_types: [...new Set(wines.map(w => w.wine_type).filter(Boolean))].sort(),
    },
    growers,
    wines,
  };

  writeFileSync(OUT_FILE, JSON.stringify(catalog, null, 2));
  console.log(`\nSaved catalog to ${OUT_FILE}`);
  console.log(`  ${wines.length} wines from ${growers.length} growers`);
  console.log(`  Regions: ${catalog.summary.regions.join(', ')}`);

  // Stats on data completeness
  const hasBlend = wines.filter(w => w.blend).length;
  const hasSoil = wines.filter(w => w.soil).length;
  const hasVineAge = wines.filter(w => w.vine_age).length;
  const hasVinification = wines.filter(w => w.vinification).length;
  console.log(`\nData completeness:`);
  console.log(`  Blend: ${hasBlend}/${wines.length} (${(100*hasBlend/wines.length).toFixed(0)}%)`);
  console.log(`  Soil: ${hasSoil}/${wines.length} (${(100*hasSoil/wines.length).toFixed(0)}%)`);
  console.log(`  Vine age: ${hasVineAge}/${wines.length} (${(100*hasVineAge/wines.length).toFixed(0)}%)`);
  console.log(`  Vinification: ${hasVinification}/${wines.length} (${(100*hasVinification/wines.length).toFixed(0)}%)`);
}

main().catch(console.error);
