#!/usr/bin/env node
/**
 * import_eu_pdo.mjs
 *
 * Imports 1,177 European wine PDO boundaries from the Eurac Research EU Wine
 * Geospatial Inventory (EWaGI) into the Loam database.
 *
 * Data source: https://doi.org/10.6084/m9.figshare.c.5877659.v1
 * License: CC BY 4.0
 *
 * Three phases:
 *   Phase 0: Load & join GeoPackage (boundaries) + CSV (metadata)
 *   Phase 1: Match PDOs against existing appellations or create new ones
 *   Phase 2: Import boundary polygons (reprojected from EPSG:3035 to WGS84)
 *
 * Usage:
 *   node import_eu_pdo.mjs                # full run
 *   node import_eu_pdo.mjs --dry-run      # preview only
 *   node import_eu_pdo.mjs --country FR   # single country
 *   node import_eu_pdo.mjs --boundaries-only  # skip Phase 1 (appellations already created)
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import initSqlJs from 'sql.js';
import wkx from 'wkx';
import proj4 from 'proj4';

// ---------------------------------------------------------------------------
// .env loading
// ---------------------------------------------------------------------------
const envPath = new URL('.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envLines = readFileSync(envPath, 'utf8').split('\n');
const env = {};
for (const l of envLines) {
  const m = l.replace(/\r/g, '').match(/^(\w+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE);

const DRY_RUN = process.argv.includes('--dry-run');
const BOUNDARIES_ONLY = process.argv.includes('--boundaries-only');
const COUNTRY_FILTER = (() => {
  const idx = process.argv.indexOf('--country');
  return idx >= 0 ? process.argv[idx + 1] : null;
})();

// ---------------------------------------------------------------------------
// Projections
// ---------------------------------------------------------------------------
proj4.defs('EPSG:3035', '+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs');

// ---------------------------------------------------------------------------
// Greek transliteration map
// ---------------------------------------------------------------------------
const GREEK_TO_LATIN = {
  'Α': 'A', 'Β': 'V', 'Γ': 'G', 'Δ': 'D', 'Ε': 'E', 'Ζ': 'Z', 'Η': 'I', 'Θ': 'Th',
  'Ι': 'I', 'Κ': 'K', 'Λ': 'L', 'Μ': 'M', 'Ν': 'N', 'Ξ': 'X', 'Ο': 'O', 'Π': 'P',
  'Ρ': 'R', 'Σ': 'S', 'Τ': 'T', 'Υ': 'Y', 'Φ': 'F', 'Χ': 'Ch', 'Ψ': 'Ps', 'Ω': 'O',
  'α': 'a', 'β': 'v', 'γ': 'g', 'δ': 'd', 'ε': 'e', 'ζ': 'z', 'η': 'i', 'θ': 'th',
  'ι': 'i', 'κ': 'k', 'λ': 'l', 'μ': 'm', 'ν': 'n', 'ξ': 'x', 'ο': 'o', 'π': 'p',
  'ρ': 'r', 'σ': 's', 'ς': 's', 'τ': 't', 'υ': 'y', 'φ': 'f', 'χ': 'ch', 'ψ': 'ps', 'ω': 'o',
};

// Map of Greek PDO names to their standard English/Latin wine-world names
const GREEK_NAME_MAP = {
  'Αγχίαλος': 'Anchialos',
  'Αμύνταιο': 'Amyntaio',
  'Αρχάνες': 'Archanes',
  'Γουμένισσα': 'Goumenissa',
  'Δαφνές': 'Dafnes',
  'Ζίτσα': 'Zitsa',
  'Λήμνος': 'Lemnos',
  'Μαντινεία': 'Mantinia',
  'Μαυροδάφνη Κεφαλληνίας': 'Mavrodaphne of Cephalonia',
  'Μαυροδάφνη Πατρών': 'Mavrodaphne of Patras',
  'Μεσενικόλα': 'Messenikola',
  'Μονεμβασία- Malvasia': 'Monemvasia-Malvasia',
  'Μοσχάτο Πατρών': 'Muscat of Patras',
  'Μοσχάτος Κεφαλληνίας': 'Muscat of Cephalonia',
  'Μοσχάτος Λήμνου': 'Muscat of Lemnos',
  'Μοσχάτος Ρίου Πάτρας': 'Muscat of Rio Patras',
  'Μοσχάτος Ρόδου': 'Muscat of Rhodes',
  'Νάουσα': 'Naoussa',
  'Νεμέα': 'Nemea',
  'Πάρος': 'Paros',
  'Πάτρα': 'Patra',
  'Πεζά': 'Peza',
  'Πλαγιές Μελίτωνα': 'Slopes of Meliton',
  'Ραψάνη': 'Rapsani',
  'Ρομπόλα Κεφαλληνίας': 'Robola of Cephalonia',
  'Ρόδος': 'Rhodes',
  'Σάμος': 'Samos',
  'Σαντορίνη': 'Santorini',
  'Σητεία': 'Sitia',
  'Χάνδακας - Candia': 'Heraklion-Candia',
  'Malvasia Σητείας': 'Malvasia of Sitia',
  'Malvasia Πάρος': 'Malvasia of Paros',
  'Malvasia Χάνδακας-Candia': 'Malvasia of Candia',
};

// Cyrillic (Bulgarian) transliteration map
const CYRILLIC_TO_LATIN = {
  'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ж': 'Zh', 'З': 'Z',
  'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O', 'П': 'P',
  'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch',
  'Ш': 'Sh', 'Щ': 'Sht', 'Ъ': 'a', 'Ь': '', 'Ю': 'Yu', 'Я': 'Ya',
  'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ж': 'zh', 'з': 'z',
  'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p',
  'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch',
  'ш': 'sh', 'щ': 'sht', 'ъ': 'a', 'ь': '', 'ю': 'yu', 'я': 'ya',
};

function transliterateGreek(s) {
  return s.replace(/[Α-ωά-ώ]/g, ch => GREEK_TO_LATIN[ch] || ch);
}

function transliterateCyrillic(s) {
  return s.replace(/[А-яЁё]/g, ch => CYRILLIC_TO_LATIN[ch] || ch);
}

function transliterateAll(s) {
  return transliterateCyrillic(transliterateGreek(s));
}

// Country ISO code → local designation type (what appears on labels)
const COUNTRY_DESIGNATION = {
  'FR': 'AOC',    // Appellation d'Origine Contrôlée
  'IT': 'DOC',    // Denominazione di Origine Controllata
  'ES': 'DO',     // Denominación de Origen
  'PT': 'DOC',    // Denominação de Origem Controlada
  'DE': 'Qualitätswein',
  'AT': 'DAC',    // Districtus Austriae Controllatus
  'HU': 'OEM',    // Oltalom alatt álló eredetmegjelölés
  'RO': 'DOC',
  'CZ': 'VOC',    // Vína originální certifikace
  'GR': 'PDO',    // Greece uses PDO/ΠΟΠ on labels
  'HR': 'PDO',
  'SI': 'PDO',
  'SK': 'PDO',
  'BG': 'PDO',
  'BE': 'PDO',
  'NL': 'PDO',
  'GB': 'PDO',
  'MT': 'PDO',
  'CY': 'PDO',
  'DK': 'PDO',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function slugify(s) {
  return transliterateAll(s).toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/['']/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function normalizeName(s) {
  return transliterateAll(s).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * Reproject all coordinates in a GeoJSON geometry from EPSG:3035 to WGS84.
 * Returns a new geometry object (does not mutate input).
 */
function reprojectGeometry(geojson) {
  function reprojectCoords(coords) {
    if (typeof coords[0] === 'number') {
      return proj4('EPSG:3035', 'EPSG:4326', coords);
    }
    return coords.map(c => reprojectCoords(c));
  }

  return {
    type: geojson.type,
    coordinates: reprojectCoords(geojson.coordinates),
  };
}

/**
 * Compute centroid from GeoJSON geometry by averaging all coordinates.
 */
function computeCentroid(geojson) {
  let totalLat = 0, totalLng = 0, count = 0;
  function extract(coords) {
    if (typeof coords[0] === 'number') {
      totalLng += coords[0];
      totalLat += coords[1];
      count++;
    } else {
      coords.forEach(c => extract(c));
    }
  }
  extract(geojson.coordinates);
  return count > 0 ? { lat: totalLat / count, lng: totalLng / count } : null;
}

/**
 * Parse GeoPackage Binary geometry blob to GeoJSON.
 * GeoPackage Binary = GP header (magic + version + flags + SRS + envelope) + WKB
 */
function gpkgBlobToGeoJSON(blob) {
  // Parse header
  const flags = blob[3];
  const envelopeType = (flags >> 1) & 7;
  const envSizes = [0, 32, 48, 48, 64];
  const wkbStart = 8 + envSizes[envelopeType];

  // Extract WKB and parse
  const wkbBuf = Buffer.from(blob.slice(wkbStart));
  const geom = wkx.Geometry.parse(wkbBuf);
  return geom.toGeoJSON();
}

/**
 * Simplify polygon by reducing coordinate precision to ~1m accuracy.
 * This reduces GeoJSON size without visible quality loss.
 */
function simplifyPrecision(geojson) {
  function roundCoords(coords) {
    if (typeof coords[0] === 'number') {
      return [Math.round(coords[0] * 100000) / 100000, Math.round(coords[1] * 100000) / 100000];
    }
    return coords.map(c => roundCoords(c));
  }
  return {
    type: geojson.type,
    coordinates: roundCoords(geojson.coordinates),
  };
}

// ---------------------------------------------------------------------------
// CSV Parser (handles BOM and quoted fields)
// ---------------------------------------------------------------------------
function parseCSV(text) {
  // Remove BOM
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);

  const lines = text.split('\n').filter(l => l.trim());
  const headers = lines[0].split(',').map(h => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const values = [];
    let current = '';
    let inQuotes = false;

    for (const ch of lines[i]) {
      if (ch === '"') { inQuotes = !inQuotes; continue; }
      if (ch === ',' && !inQuotes) { values.push(current.trim()); current = ''; continue; }
      current += ch;
    }
    values.push(current.trim());

    const obj = {};
    headers.forEach((h, idx) => { obj[h] = values[idx] || ''; });
    rows.push(obj);
  }
  return rows;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('=== EU PDO Boundary Import ===');
  if (DRY_RUN) console.log('[DRY RUN MODE]');
  if (COUNTRY_FILTER) console.log(`[COUNTRY FILTER: ${COUNTRY_FILTER}]`);

  // -----------------------------------------------------------------------
  // Phase 0: Load data sources
  // -----------------------------------------------------------------------
  console.log('\n--- Phase 0: Loading data ---');

  // Load GeoPackage
  const SQL = await initSqlJs();
  const gpkgBuf = readFileSync('data/geo/EU_PDO.gpkg');
  const db = new SQL.Database(gpkgBuf);

  // Read all features from GeoPackage
  const gpkgFeatures = {};
  const stmt = db.prepare('SELECT PDOid, Shape FROM EU_PDO');
  while (stmt.step()) {
    const row = stmt.get();
    gpkgFeatures[row[0]] = row[1]; // PDOid -> Shape blob
  }
  stmt.free();
  db.close();
  console.log(`  GeoPackage: ${Object.keys(gpkgFeatures).length} features loaded`);

  // Load CSV metadata
  const csvText = readFileSync('data/geo/PDO_EU_id.csv', 'utf8');
  const csvRows = parseCSV(csvText);
  console.log(`  CSV: ${csvRows.length} rows loaded`);

  // Build PDO lookup: PDOid -> metadata + geometry
  const pdoMap = new Map();
  for (const row of csvRows) {
    const pdoId = row.PDOid;
    if (!pdoId) continue;

    // Skip if country filter active
    if (COUNTRY_FILTER && row.Country !== COUNTRY_FILTER) continue;

    // Merge CSV metadata with GeoPackage geometry
    if (!pdoMap.has(pdoId)) {
      pdoMap.set(pdoId, {
        pdoId,
        country: row.Country,
        name: row.PDOnam,
        registration: row.Registration,
        category: row.Category_of_wine_product,
        varietiesOIV: row.Varieties_OIV,
        varietiesOther: row.Varieties_Other,
        maxYieldHl: row.Maximum_yield_hl !== 'na' ? parseFloat(row.Maximum_yield_hl) : null,
        maxYieldKg: row.Maximum_yield_kg !== 'na' ? parseFloat(row.Maximum_yield_kg) : null,
        pdoInfoUrl: row.PDOinfo,
        shapeBlob: gpkgFeatures[pdoId] || null,
      });
    }
  }
  console.log(`  Joined: ${pdoMap.size} PDOs to process`);

  // -----------------------------------------------------------------------
  // Load DB reference data
  // -----------------------------------------------------------------------
  console.log('\n--- Loading DB reference data ---');

  // Countries by ISO code
  const { data: countries } = await sb.from('countries').select('id, name, iso_code');
  const countryByIso = new Map();
  for (const c of countries) countryByIso.set(c.iso_code, c);
  console.log(`  ${countries.length} countries loaded`);

  // Regions (catch-all) by country_id
  const { data: catchAllRegions } = await sb.from('regions')
    .select('id, name, country_id')
    .eq('is_catch_all', true);
  const catchAllByCountryId = new Map();
  for (const r of catchAllRegions) catchAllByCountryId.set(r.country_id, r);

  // All regions by country_id
  const { data: allRegions } = await sb.from('regions')
    .select('id, name, country_id, is_catch_all');
  const regionsByCountryId = new Map();
  for (const r of allRegions) {
    if (!regionsByCountryId.has(r.country_id)) regionsByCountryId.set(r.country_id, []);
    regionsByCountryId.get(r.country_id).push(r);
  }

  // All existing appellations (paginated to avoid Supabase 1000-row default limit)
  let existingApps = [];
  let offset = 0;
  const PAGE_SIZE = 1000;
  while (true) {
    const { data: page, error: pageErr } = await sb.from('appellations')
      .select('id, name, slug, country_id, region_id, designation_type, max_yield_hl_ha, allowed_grapes_description, regulatory_url')
      .range(offset, offset + PAGE_SIZE - 1);
    if (pageErr) throw new Error(`Failed to fetch appellations: ${pageErr.message}`);
    existingApps = existingApps.concat(page);
    if (page.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  console.log(`  ${existingApps.length} existing appellations loaded`);

  // Build normalized name lookup: country_id + normalized_name -> appellation
  const appLookup = new Map();
  for (const app of existingApps) {
    const key = `${app.country_id}::${normalizeName(app.name)}`;
    appLookup.set(key, app);
  }

  // Also build slug lookup for fallback matching
  const appSlugLookup = new Map();
  for (const app of existingApps) {
    const key = `${app.country_id}::${app.slug}`;
    appSlugLookup.set(key, app);
  }

  // -----------------------------------------------------------------------
  // Phase 1: Match / Create Appellations
  // -----------------------------------------------------------------------
  if (!BOUNDARIES_ONLY) {
    console.log('\n--- Phase 1: Matching appellations ---');
  }

  const stats = {
    matched: 0,
    created: 0,
    skippedNoCountry: 0,
    boundariesUpdated: 0,
    boundariesCreated: 0,
    enriched: 0,
    errors: 0,
  };

  // Track matched appellation IDs for Phase 2
  const pdoToAppId = new Map(); // PDOid -> appellation_id

  for (const [pdoId, pdo] of pdoMap) {
    const country = countryByIso.get(pdo.country);
    if (!country) {
      console.log(`  [SKIP] No country for ISO code: ${pdo.country} (${pdo.name})`);
      stats.skippedNoCountry++;
      continue;
    }

    // Resolve non-Latin-script names to standard Latin-script wine names
    // Greek: use curated wine-world names; Bulgarian/Cyrillic: auto-transliterate
    const resolvedName = GREEK_NAME_MAP[pdo.name]
      || (/[А-я]/.test(pdo.name) ? transliterateCyrillic(pdo.name) : pdo.name);

    // Try to match by normalized name — check both original and resolved names
    const normName = normalizeName(resolvedName);
    const lookupKey = `${country.id}::${normName}`;
    const slugKey = `${country.id}::${slugify(resolvedName)}`;

    // Also try original name (handles existing DB entries stored in Greek script)
    const origNormName = normalizeName(pdo.name);
    const origLookupKey = `${country.id}::${origNormName}`;
    const origSlugKey = `${country.id}::${slugify(pdo.name)}`;

    let app = appLookup.get(lookupKey) || appSlugLookup.get(slugKey)
           || appLookup.get(origLookupKey) || appSlugLookup.get(origSlugKey);

    if (app) {
      // Matched existing appellation
      pdoToAppId.set(pdoId, app.id);
      stats.matched++;

      // Enrich with metadata if missing
      if (!BOUNDARIES_ONLY && !DRY_RUN) {
        const updates = {};
        if (!app.max_yield_hl_ha && pdo.maxYieldHl) updates.max_yield_hl_ha = pdo.maxYieldHl;
        if (!app.allowed_grapes_description && pdo.varietiesOIV && pdo.varietiesOIV !== 'na') {
          // Clean variety names: "Cabernet Sauvignon N/Merlot N" -> "Cabernet Sauvignon, Merlot"
          updates.allowed_grapes_description = pdo.varietiesOIV
            .split('/')
            .map(v => v.replace(/ [BNRG]r?s?g?$/, '').trim())
            .filter(v => v)
            .join(', ');
        }
        if (!app.regulatory_url && pdo.pdoInfoUrl) updates.regulatory_url = pdo.pdoInfoUrl;
        if (!app.designation_type) updates.designation_type = COUNTRY_DESIGNATION[pdo.country] || 'PDO';

        if (Object.keys(updates).length > 0) {
          const { error } = await sb.from('appellations').update(updates).eq('id', app.id);
          if (error) {
            console.log(`  [ERROR] Enriching ${pdo.name}: ${error.message}`);
          } else {
            stats.enriched++;
          }
        }
      }
    } else if (!BOUNDARIES_ONLY) {
      // Create new appellation
      const catchAll = catchAllByCountryId.get(country.id);
      if (!catchAll) {
        console.log(`  [SKIP] No catch-all region for ${country.name} (${pdo.name})`);
        stats.skippedNoCountry++;
        continue;
      }

      // Compute centroid from geometry if available
      let lat = null, lng = null;
      if (pdo.shapeBlob) {
        try {
          const rawGeom = gpkgBlobToGeoJSON(pdo.shapeBlob);
          const wgs84Geom = reprojectGeometry(rawGeom);
          const centroid = computeCentroid(wgs84Geom);
          if (centroid) { lat = centroid.lat; lng = centroid.lng; }
        } catch (e) {
          // centroid computation failed, continue without
        }
      }

      // Determine hemisphere and growing season
      const hemisphere = lat && lat < 0 ? 'south' : 'north';

      // Parse registration year
      let establishedYear = null;
      if (pdo.registration && pdo.registration.match(/^\d{4}/)) {
        establishedYear = parseInt(pdo.registration.slice(0, 4));
      }

      // Clean variety string
      let grapesDesc = null;
      if (pdo.varietiesOIV && pdo.varietiesOIV !== 'na') {
        grapesDesc = pdo.varietiesOIV
          .split('/')
          .map(v => v.replace(/ [BNRG]r?s?g?$/, '').trim())
          .filter(v => v)
          .join(', ');
      }

      const designationType = COUNTRY_DESIGNATION[pdo.country] || 'PDO';

      const newApp = {
        name: resolvedName,
        slug: slugify(resolvedName),
        country_id: country.id,
        region_id: catchAll.id,
        designation_type: designationType,
        latitude: lat ? Math.round(lat * 100000) / 100000 : null,
        longitude: lng ? Math.round(lng * 100000) / 100000 : null,
        hemisphere,
        growing_season_start_month: hemisphere === 'northern' ? 4 : 10,
        growing_season_end_month: hemisphere === 'northern' ? 10 : 4,
        max_yield_hl_ha: pdo.maxYieldHl,
        allowed_grapes_description: grapesDesc,
        regulatory_url: pdo.pdoInfoUrl || null,
        established_year: establishedYear,
      };

      if (DRY_RUN) {
        console.log(`  [DRY] Would create: ${resolvedName} (${country.name}) in ${catchAll.name}`);
        stats.created++;
        continue;
      }

      const { data: inserted, error } = await sb.from('appellations')
        .insert(newApp)
        .select('id')
        .single();

      if (error) {
        if (error.code === '23505') {
          // Slug conflict — try with country suffix
          newApp.slug = slugify(`${resolvedName}-${country.name}`);
          const { data: retry, error: retryErr } = await sb.from('appellations')
            .insert(newApp)
            .select('id')
            .single();
          if (retryErr) {
            console.log(`  [ERROR] Creating ${pdo.name}: ${retryErr.message}`);
            stats.errors++;
            continue;
          }
          pdoToAppId.set(pdoId, retry.id);
          // Add to lookups so duplicate PDOids with same name match
          appLookup.set(lookupKey, { id: retry.id, ...newApp });
          appSlugLookup.set(`${country.id}::${newApp.slug}`, { id: retry.id, ...newApp });
        } else {
          console.log(`  [ERROR] Creating ${pdo.name}: ${error.message}`);
          stats.errors++;
          continue;
        }
      } else {
        pdoToAppId.set(pdoId, inserted.id);
        // Add to lookups so duplicate PDOids with same name match
        appLookup.set(lookupKey, { id: inserted.id, ...newApp });
        appSlugLookup.set(slugKey, { id: inserted.id, ...newApp });
      }

      stats.created++;
      if (stats.created % 50 === 0) {
        console.log(`  Created ${stats.created} appellations...`);
      }
    }
  }

  if (!BOUNDARIES_ONLY) {
    console.log(`\n  Phase 1 complete: ${stats.matched} matched, ${stats.created} created, ${stats.enriched} enriched, ${stats.errors} errors`);
  }

  // -----------------------------------------------------------------------
  // Phase 2: Import Boundary Polygons
  // -----------------------------------------------------------------------
  console.log('\n--- Phase 2: Importing boundaries ---');

  let processed = 0;
  for (const [pdoId, pdo] of pdoMap) {
    const appId = pdoToAppId.get(pdoId);
    if (!appId) {
      // Try to find by matching again (for --boundaries-only mode)
      const country = countryByIso.get(pdo.country);
      if (country) {
        const resolved = GREEK_NAME_MAP[pdo.name] || pdo.name;
        const normName = normalizeName(resolved);
        const key = `${country.id}::${normName}`;
        const slugKey = `${country.id}::${slugify(resolved)}`;
        const existing = appLookup.get(key) || appSlugLookup.get(slugKey);
        if (existing) {
          pdoToAppId.set(pdoId, existing.id);
        }
      }
    }

    const finalAppId = pdoToAppId.get(pdoId);
    if (!finalAppId) continue;
    if (!pdo.shapeBlob) {
      console.log(`  [SKIP] No geometry for ${pdo.name}`);
      continue;
    }

    try {
      // Parse and reproject geometry
      const rawGeom = gpkgBlobToGeoJSON(pdo.shapeBlob);
      const wgs84Geom = reprojectGeometry(rawGeom);
      const simplified = simplifyPrecision(wgs84Geom);

      if (DRY_RUN) {
        const centroid = computeCentroid(simplified);
        console.log(`  [DRY] Would import boundary for ${pdo.name} (${simplified.type}, centroid: ${centroid?.lat?.toFixed(2)}, ${centroid?.lng?.toFixed(2)})`);
        stats.boundariesCreated++;
        processed++;
        continue;
      }

      // Upsert boundary via RPC
      const { error } = await sb.rpc('upsert_appellation_boundary', {
        p_appellation_id: finalAppId,
        p_geojson: JSON.stringify(simplified),
        p_source_id: `eu-pdo/${pdoId}`,
        p_confidence: 'approximate',  // municipality-level, not parcel-level
      });

      if (error) {
        console.log(`  [ERROR] Boundary for ${pdo.name}: ${error.message}`);
        stats.errors++;
      } else {
        stats.boundariesUpdated++;
      }

      processed++;
      if (processed % 50 === 0) {
        console.log(`  Processed ${processed}/${pdoMap.size} boundaries...`);
      }

      // Small delay to avoid overwhelming Supabase
      if (processed % 20 === 0) await sleep(100);

    } catch (e) {
      console.log(`  [ERROR] Processing ${pdo.name}: ${e.message}`);
      stats.errors++;
    }
  }

  // -----------------------------------------------------------------------
  // Summary
  // -----------------------------------------------------------------------
  console.log('\n=== Import Complete ===');
  console.log(`  Matched existing: ${stats.matched}`);
  console.log(`  Created new:      ${stats.created}`);
  console.log(`  Enriched:         ${stats.enriched}`);
  console.log(`  Boundaries:       ${stats.boundariesUpdated} updated`);
  console.log(`  Skipped (no country): ${stats.skippedNoCountry}`);
  console.log(`  Errors:           ${stats.errors}`);
  if (DRY_RUN) console.log('\n  [DRY RUN - no changes made]');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
