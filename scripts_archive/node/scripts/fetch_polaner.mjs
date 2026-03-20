#!/usr/bin/env node
/**
 * fetch_polaner.mjs — Fetch Polaner Selections catalog via WordPress REST API
 *
 * Polaner is a WordPress site with custom 'wine' post type and taxonomies.
 * The REST API exposes: wine title, country, region, appellation,
 * biodynamic/organic/natural certifications.
 *
 * The detailed wine data (grapes, soil, vinification) is not exposed via API —
 * it lives in ACF fields rendered client-side. We get taxonomy data only.
 *
 * Strategy:
 *   1. Fetch all taxonomy terms (countries, regions, appellations, certs)
 *   2. Paginate through all wines via REST API
 *   3. Resolve taxonomy IDs to names
 *   4. Parse producer name from wine title
 *   5. Save to data/imports/polaner_catalog.json
 *
 * Usage:
 *   node scripts/fetch_polaner.mjs                  # Full fetch
 *   node scripts/fetch_polaner.mjs --limit 100      # Test with 100 wines
 *
 * Output: data/imports/polaner_catalog.json
 */

import { writeFileSync } from 'fs';
import https from 'https';

const OUTPUT_FILE = 'data/imports/polaner_catalog.json';
const BASE_API = 'https://www.polanerselections.com/wp-json/wp/v2';
const DELAY_MS = 500; // 500ms delay between API calls

const args = process.argv.slice(2);
const LIMIT = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : Infinity;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
      timeout: 15000,
    }, (res) => {
      const headers = {
        total: res.headers['x-wp-total'],
        totalPages: res.headers['x-wp-totalpages'],
      };
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body, headers }));
    }).on('error', reject).on('timeout', () => reject(new Error('Timeout')));
  });
}

function decodeEntities(s) {
  return s
    .replace(/&amp;/g, '&')
    .replace(/&#8217;/g, "'")
    .replace(/&#8216;/g, "'")
    .replace(/&#8220;/g, '"')
    .replace(/&#8221;/g, '"')
    .replace(/&#8211;/g, '–')
    .replace(/&#8212;/g, '—')
    .replace(/&eacute;/g, 'é')
    .replace(/&egrave;/g, 'è')
    .replace(/&agrave;/g, 'à')
    .replace(/&uuml;/g, 'ü')
    .replace(/&ouml;/g, 'ö')
    .replace(/&oacute;/g, 'ó')
    .replace(/&ntilde;/g, 'ñ')
    .replace(/&ccedil;/g, 'ç')
    .replace(/&#\d+;/g, m => String.fromCharCode(parseInt(m.slice(2, -1))));
}

// ── Fetch all terms for a taxonomy ─────────────────────────
async function fetchAllTerms(taxonomy) {
  const terms = new Map();
  let page = 1;
  while (true) {
    const url = `${BASE_API}/${taxonomy}?per_page=100&page=${page}&_fields=id,name,slug`;
    const res = await httpsGet(url);
    if (res.status !== 200) break;
    const data = JSON.parse(res.body);
    if (data.length === 0) break;
    for (const t of data) {
      terms.set(t.id, { name: decodeEntities(t.name), slug: t.slug });
    }
    if (!res.headers.totalPages || page >= parseInt(res.headers.totalPages)) break;
    page++;
    await sleep(DELAY_MS);
  }
  return terms;
}

// ── Parse producer from wine title ─────────────────────────
// Polaner titles are typically "Producer WineName Appellation" or "Producer WineName"
// We attempt to extract the producer by looking for known producer patterns
function parseProducerFromTitle(title, appellationName) {
  // If appellation is in the title, remove it first
  let cleaned = title;
  if (appellationName && title.toLowerCase().includes(appellationName.toLowerCase())) {
    // Keep original for now, we'll use it to split
  }

  // Common patterns in Polaner: "Producer WineName Region"
  // We can't perfectly split without a producer dictionary, but we can try heuristics
  return { full_title: title };
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  // Step 1: Fetch all taxonomies
  console.log('Step 1: Fetching taxonomies...');

  const [countries, regions, appellations, biodynamics, organics, greenProps] = await Promise.all([
    fetchAllTerms('country'),
    fetchAllTerms('region'),
    fetchAllTerms('appellations'),
    fetchAllTerms('biodynamics'),
    fetchAllTerms('organics'),
    fetchAllTerms('green_properties'),
  ]);

  console.log(`  Countries: ${countries.size}`);
  console.log(`  Regions: ${regions.size}`);
  console.log(`  Appellations: ${appellations.size}`);
  console.log(`  Biodynamics: ${biodynamics.size}`);
  console.log(`  Organics: ${organics.size}`);
  console.log(`  Green properties: ${greenProps.size}`);

  // Step 2: Fetch all wines
  console.log('\nStep 2: Fetching wines...');
  const wines = [];
  let page = 1;
  let totalWines = 0;

  while (wines.length < LIMIT) {
    const url = `${BASE_API}/wine?per_page=100&page=${page}&_fields=id,slug,title,link,appellations,country,region,biodynamics,organics,green_properties`;
    const res = await httpsGet(url);
    if (res.status !== 200) {
      console.log(`  Page ${page}: status ${res.status}`);
      break;
    }

    const data = JSON.parse(res.body);
    if (data.length === 0) break;

    if (page === 1) {
      totalWines = parseInt(res.headers.total) || 0;
      console.log(`  Total wines available: ${totalWines}`);
    }

    for (const w of data) {
      if (wines.length >= LIMIT) break;

      const title = decodeEntities(w.title?.rendered || w.title || '');

      const wine = {
        wp_id: w.id,
        slug: w.slug,
        title: title,
        url: w.link,
        _source: 'polaner',
      };

      // Resolve taxonomies
      if (w.country?.length > 0) {
        const c = countries.get(w.country[0]);
        if (c) wine.country = c.name;
      }
      if (w.region?.length > 0) {
        const r = regions.get(w.region[0]);
        if (r) wine.region = r.name;
      }
      if (w.appellations?.length > 0) {
        const apps = w.appellations.map(id => appellations.get(id)?.name).filter(Boolean);
        if (apps.length > 0) wine.appellation = apps[0];
        if (apps.length > 1) wine.appellations_all = apps;
      }

      // Certifications
      const certs = [];
      if (w.biodynamics?.length > 0) {
        for (const id of w.biodynamics) {
          const term = biodynamics.get(id);
          if (term) certs.push(`biodynamic:${term.name}`);
        }
      }
      if (w.organics?.length > 0) {
        for (const id of w.organics) {
          const term = organics.get(id);
          if (term) certs.push(`organic:${term.name}`);
        }
      }
      if (w.green_properties?.length > 0) {
        for (const id of w.green_properties) {
          const term = greenProps.get(id);
          if (term) certs.push(`green:${term.name}`);
        }
      }
      if (certs.length > 0) wine.certifications = certs;

      wines.push(wine);
    }

    console.log(`  Page ${page}: ${data.length} wines (total: ${wines.length})`);
    page++;
    await sleep(DELAY_MS);
  }

  // Step 3: Save
  writeFileSync(OUTPUT_FILE, JSON.stringify(wines, null, 2));
  console.log(`\n✅ Done. ${wines.length} wines saved to ${OUTPUT_FILE}`);

  // Print sample
  if (wines.length > 0) {
    console.log('\nSample wine:');
    console.log(JSON.stringify(wines[0], null, 2));
  }

  // Stats
  const stats = {
    total: wines.length,
    withCountry: wines.filter(w => w.country).length,
    withRegion: wines.filter(w => w.region).length,
    withAppellation: wines.filter(w => w.appellation).length,
    withCerts: wines.filter(w => w.certifications?.length > 0).length,
  };
  console.log('\nField coverage:');
  Object.entries(stats).forEach(([k, v]) => {
    const pct = stats.total > 0 ? ((v / stats.total) * 100).toFixed(1) : 0;
    console.log(`  ${k}: ${v} (${pct}%)`);
  });

  // Country distribution
  const countryDist = {};
  for (const w of wines) {
    const c = w.country || 'unknown';
    countryDist[c] = (countryDist[c] || 0) + 1;
  }
  console.log('\nCountry distribution:');
  Object.entries(countryDist).sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));

  // Certification distribution
  const certDist = {};
  for (const w of wines) {
    if (w.certifications) {
      for (const c of w.certifications) {
        certDist[c] = (certDist[c] || 0) + 1;
      }
    }
  }
  if (Object.keys(certDist).length > 0) {
    console.log('\nCertification distribution:');
    Object.entries(certDist).sort((a, b) => b[1] - a[1])
      .forEach(([k, v]) => console.log(`  ${k}: ${v}`));
  }
}

main().catch(console.error);
