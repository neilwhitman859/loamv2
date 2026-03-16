#!/usr/bin/env node
/**
 * lib/import.mjs — Shared producer import library
 *
 * Takes a standardized JSON file and inserts producer, wines, vintages,
 * scores, grape compositions, and label designations into Supabase.
 *
 * Usage:
 *   node lib/import.mjs data/imports/moone-tsai.json [--dry-run]
 *
 * JSON format: see docs or data/imports/_template.json
 */

import { readFileSync, existsSync } from 'fs';
import { createClient } from '@supabase/supabase-js';
import { randomUUID } from 'crypto';

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

// ── CLI ─────────────────────────────────────────────────────
const args = process.argv.slice(2);
const jsonPath = args.find(a => !a.startsWith('--'));
const DRY_RUN = args.includes('--dry-run');
const REPLACE = args.includes('--replace');

if (!jsonPath) {
  console.error('Usage: node lib/import.mjs <path-to-json> [--dry-run] [--replace]');
  process.exit(1);
}

// ── Helpers ─────────────────────────────────────────────────
function normalize(s) {
  return s
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function slugify(s) {
  return s
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

const MONTHS = { january: '01', february: '02', march: '03', april: '04', may: '05', june: '06',
  july: '07', august: '08', september: '09', october: '10', november: '11', december: '12' };

function parseDate(val) {
  if (!val) return null;
  const s = String(val).trim();
  // Already ISO-ish: 2024-08-19
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  // "August 2024" → "2024-08-01"
  const match = s.match(/^(\w+)\s+(\d{4})$/);
  if (match) {
    const mm = MONTHS[match[1].toLowerCase()];
    if (mm) return `${match[2]}-${mm}-01`;
  }
  // Can't parse — skip it (will be stored in metadata)
  return null;
}

async function fetchAll(table, columns = '*', filter = {}, batchSize = 1000) {
  const all = [];
  let offset = 0;
  while (true) {
    let query = sb.from(table).select(columns).range(offset, offset + batchSize - 1);
    for (const [k, v] of Object.entries(filter)) {
      query = query.eq(k, v);
    }
    const { data, error } = await query;
    if (error) throw new Error(`fetchAll ${table}: ${error.message}`);
    all.push(...data);
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return all;
}

// ── Grape Name Resolution ───────────────────────────────────
// Common aliases: producer label name → DB display_name or VIVC name
const GRAPE_ALIASES = {
  // US names
  'petite sirah': 'Durif',
  'petit sirah': 'Durif',
  'petite syrah': 'Durif',
  'cab sauv': 'Cabernet Sauvignon',
  'cab franc': 'Cabernet Franc',
  'cab': 'Cabernet Sauvignon',
  'zin': 'Zinfandel',
  'petit verdot': 'Petit Verdot',
  'petite verdot': 'Petit Verdot',
  // Spanish names
  'mazuelo': 'Carignan',
  'mazuela': 'Carignan',
  'cariñena': 'Carignan',
  'garnacho': 'Grenache',
  'garnacho tinto': 'Grenache',
  'garnacha': 'Grenache',
  'garnacha tinta': 'Grenache',
  'malvasía': 'Malvasia',
  'malvasia': 'Malvasia',
  // French names
  'mourvèdre': 'Mourvèdre',
  'mourvedre': 'Mourvèdre',
  'mataro': 'Mourvèdre',
  'mataró': 'Mourvèdre',
  'syrah': 'Syrah',
  'shiraz': 'Syrah',
  'grenache blanc': 'Grenache Blanc',
  'grenache noir': 'Grenache',
  'sémillon': 'Sémillon',
  'semillon': 'Sémillon',
  // Hungarian
  'sárgamuskotály': 'Muscat Blanc à Petits Grains',
  'sargamuskotaly': 'Muscat Blanc à Petits Grains',
  'yellow muscat': 'Muscat Blanc à Petits Grains',
  'hárslevelű': 'Hárslevelü',
  'harslevelu': 'Hárslevelü',
  // Italian
  'sangiovese grosso': 'Sangiovese',
  'brunello': 'Sangiovese',
  'primitivo': 'Zinfandel',
  'nebbiolo': 'Nebbiolo',
  // German/Austrian
  'müller-thurgau': 'Müller-Thurgau',
  'muller-thurgau': 'Müller-Thurgau',
  'muller thurgau': 'Müller-Thurgau',
  'mueller thurgau': 'Müller-Thurgau',
  'grüner veltliner': 'Grüner Veltliner',
  'gruner veltliner': 'Grüner Veltliner',
  'blaufränkisch': 'Blaufränkisch',
  'blaufrankisch': 'Blaufränkisch',
  'lemberger': 'Blaufränkisch',
  'zweigelt': 'Zweigelt',
  'st. laurent': 'Sankt Laurent',
  'saint laurent': 'Sankt Laurent',
  // Italian
  'pinot bianco': 'Pinot Blanc',
  'pinot grigio': 'Pinot Gris',
  'verdicchio': 'Verdicchio Bianco',
  'lagrein': 'Lagrein',
  'schiava': 'Schiava Grossa',
  'schiava gentile': 'Schiava Grossa',
  'vernatsch': 'Schiava Grossa',
  'teroldego': 'Teroldego',
  'nosiola': 'Nosiola',
  // Portuguese
  'touriga nacional': 'Touriga Nacional',
  'touriga franca': 'Touriga Franca',
  'tinta roriz': 'Tempranillo',
  'aragonez': 'Tempranillo',
  'tinto cão': 'Tinto Cao',
  'tinto cao': 'Tinto Cao',
  'códega': 'Codega do Larinho',
  'codega': 'Codega do Larinho',
  'tinta barroca': 'Tinta Barroca',
  // Champagne
  'meunier': 'Pinot Meunier',
  'pinot meunier': 'Pinot Meunier',
  'pm': 'Pinot Meunier',
  'pn': 'Pinot Noir',
  'ch': 'Chardonnay',
  // Friulian / Slovenian
  'ribolla gialla': 'Ribolla Gialla',
  'ribolla': 'Ribolla Gialla',
  'friulano': 'Sauvignonasse',
  'sauvignonasse': 'Sauvignonasse',
  'tocai friulano': 'Sauvignonasse',
  'tocai': 'Sauvignonasse',
  'picolit': 'Picolit',
  'pignolo': 'Pignolo',
  'malvasia istriana': 'Malvasia Istriana',
  'schioppettino': 'Schioppettino',
  'refosco dal peduncolo rosso': 'Refosco dal Peduncolo Rosso',
  'refosco': 'Refosco dal Peduncolo Rosso',
  // Southern French / Languedoc
  'picpoul': 'Piquepoul Blanc',
  'piquepoul': 'Piquepoul Blanc',
  'picpoul de pinet': 'Piquepoul Blanc',
  'piquepoul blanc': 'Piquepoul Blanc',
  'clairette': 'Clairette',
  'clairette blanche': 'Clairette',
  'bourboulenc': 'Bourboulenc',
  'rolle': 'Vermentino',
  // Greek
  'assyrtiko': 'Assyrtiko',
  'xinomavro': 'Xinomavro',
  'agiorgitiko': 'Agiorgitiko',
  'moschofilero': 'Moschofilero',
  // South American
  'país': 'País',
  'pais': 'País',
  'criolla': 'Criolla Grande',
  'torrontés': 'Torrontés Riojano',
  'torrontes': 'Torrontés Riojano',
  'bonarda': 'Bonarda',
};

// ── Publication Name Resolution ─────────────────────────────
const PUB_ALIASES = {
  'robert parker': 'Wine Advocate',
  "robert parker's wine advocate": 'Wine Advocate',
  'the wine advocate': 'Wine Advocate',
  'parker': 'Wine Advocate',
  'wine advocate': 'Wine Advocate',
  'jamessuckling.com': 'James Suckling',
  'james suckling': 'James Suckling',
  'vinous media': 'Vinous',
  'vinous': 'Vinous',
  'antonio galloni': 'Vinous',
  'wine spectator': 'Wine Spectator',
  'wine enthusiast': 'Wine Enthusiast',
  'decanter': 'Decanter',
  'decanter magazine': 'Decanter',
  'jancis robinson': 'Jancis Robinson',
  'jancisrobinson.com': 'Jancis Robinson',
  'guía peñín': 'Guía Peñín',
  'guia penin': 'Guía Peñín',
  'penin': 'Guía Peñín',
  'tim atkin': 'Tim Atkin MW',
  'tim atkin mw': 'Tim Atkin MW',
  'burghound': 'Burghound',
  'allen meadows': 'Burghound',
  "allen meadows' burghound": 'Burghound',
  'gambero rosso': 'Gambero Rosso',
  'prince of pinot': 'Prince of Pinot',
  'international wine review': 'International Wine Review',
  'view from the cellar': 'View From the Cellar',
  'john gilman': 'View From the Cellar',
  'jeb dunnuck': 'Jeb Dunnuck',
  'jd': 'Jeb Dunnuck',
  'jasper morris': 'Jasper Morris MW',
  'jasper morris mw': 'Jasper Morris MW',
  'js': 'James Suckling',
  'ws': 'Wine Spectator',
  'wa': 'Wine Advocate',
  'we': 'Wine Enthusiast',
};

// ── Region Name Aliases ─────────────────────────────────────
// Maps English/alternative names to DB canonical names
const REGION_ALIASES = {
  // Italian regions (English → Italian)
  'piedmont': 'Piemonte',
  'tuscany': 'Toscana',
  'lombardy': 'Lombardia',
  'sicily': 'Sicilia',
  'sardinia': 'Sardegna',
  'veneto': 'Veneto',
  'friuli-venezia giulia': 'Friuli-Venezia Giulia',
  'friuli venezia giulia': 'Friuli-Venezia Giulia',
  'friuli': 'Friuli-Venezia Giulia',
  'trentino-alto adige': 'Trentino-Alto Adige',
  'trentino alto adige': 'Trentino-Alto Adige',
  'trentino': 'Trentino-Alto Adige',
  'alto adige': 'Trentino-Alto Adige',
  'südtirol': 'Trentino-Alto Adige',
  'abruzzo': 'Abruzzo',
  'campania': 'Campania',
  'puglia': 'Puglia',
  'apulia': 'Puglia',
  // French regions (English → French/DB)
  'burgundy': 'Burgundy',
  'bourgogne': 'Burgundy',
  'rhone': 'Rhône Valley',
  'rhône': 'Rhône Valley',
  'rhone valley': 'Rhône Valley',
  'rhône valley': 'Rhône Valley',
  'northern rhone': 'Northern Rhône',
  'northern rhône': 'Northern Rhône',
  'southern rhone': 'Southern Rhône',
  'southern rhône': 'Southern Rhône',
  'bordeaux': 'Bordeaux',
  'left bank': 'Left Bank',
  'right bank': 'Right Bank',
  'loire': 'Loire Valley',
  'loire valley': 'Loire Valley',
  'alsace': 'Alsace',
  'champagne': 'Champagne',
  'languedoc': 'Languedoc',
  'roussillon': 'Roussillon',
  'provence': 'Provence',
  'southwest france': 'Southwest France',
  'south west france': 'Southwest France',
  // Spanish regions
  'rioja': 'Rioja',
  'catalonia': 'Catalunya',
  'catalunya': 'Catalunya',
  'castile and leon': 'Castilla y León',
  'castile and león': 'Castilla y León',
  'castilla y leon': 'Castilla y León',
  // German regions
  'mosel': 'Mosel',
  'moselle': 'Mosel',
  'rheingau': 'Rheingau',
  'nahe': 'Nahe',
  'pfalz': 'Pfalz',
  'palatinate': 'Pfalz',
  // Portuguese regions
  'douro': 'Douro',
  'dão': 'Dão',
  'dao': 'Dão',
  'alentejo': 'Alentejo',
  // Other
  'central otago': 'Central Otago',
  'marlborough': 'Marlborough',
  'hawke\'s bay': 'Hawke\'s Bay',
  'hawkes bay': 'Hawke\'s Bay',
  'barossa': 'Barossa',
  'barossa valley': 'Barossa Valley',
  'mclaren vale': 'McLaren Vale',
  'mendoza': 'Mendoza',
  'stellenbosch': 'Stellenbosch',
};

// ── Reference Data Loader ───────────────────────────────────
class ReferenceData {
  constructor() {
    this.countries = new Map();
    this.regions = new Map();
    this.appellations = new Map();
    this.grapes = new Map();        // display_name lowercase → id
    this.grapesByName = new Map();   // VIVC name lowercase → id
    this.grapeSynonyms = new Map();  // synonym lowercase → grape_id
    this.varietalCategories = new Map();
    this.publications = new Map();
    this.labelDesignations = new Map();
    this.sourceTypes = new Map();
    this.bottleFormats = new Map();  // name lowercase → {id, volume_ml}
    this.winemakers = new Map();    // slug → id
    this.classificationLevels = new Map(); // "system|level" → {level_id, system_name}
  }

  async load() {
    console.log('Loading reference data...');

    // Countries
    const countries = await fetchAll('countries', 'id,name,iso_code');
    for (const c of countries) {
      this.countries.set(c.name.toLowerCase(), c.id);
      if (c.iso_code) this.countries.set(c.iso_code.toLowerCase(), c.id);
    }
    console.log(`  Countries: ${countries.length}`);

    // Regions
    const regions = await fetchAll('regions', 'id,name,country_id,parent_id,is_catch_all');
    for (const r of regions) {
      this.regions.set(r.name.toLowerCase(), r);
      // Also key by "name|country_id" for disambiguation
      this.regions.set(`${r.name.toLowerCase()}|${r.country_id}`, r);
      // Also key by normalized (accent-stripped) name
      const norm = normalize(r.name);
      if (norm !== r.name.toLowerCase()) {
        this.regions.set(norm, r);
        this.regions.set(`${norm}|${r.country_id}`, r);
      }
    }
    console.log(`  Regions: ${regions.length}`);

    // Appellations
    const appellations = await fetchAll('appellations', 'id,name,designation_type,country_id,region_id');
    for (const a of appellations) {
      this.appellations.set(a.name.toLowerCase(), a);
    }
    console.log(`  Appellations: ${appellations.length}`);

    // Appellation aliases
    const aliases = await fetchAll('appellation_aliases', 'appellation_id,alias_normalized');
    let aliasCount = 0;
    for (const al of aliases) {
      if (!this.appellations.has(al.alias_normalized)) {
        const app = appellations.find(a => a.id === al.appellation_id);
        if (app) {
          this.appellations.set(al.alias_normalized, app);
          aliasCount++;
        }
      }
    }
    console.log(`  Appellation aliases: ${aliases.length} (${aliasCount} new keys)`);

    // Grapes — by display_name and VIVC name
    const grapes = await fetchAll('grapes', 'id,name,display_name,color');
    for (const g of grapes) {
      if (g.display_name) {
        this.grapes.set(g.display_name.toLowerCase(), g);
      }
      this.grapesByName.set(g.name.toLowerCase(), g);
    }
    console.log(`  Grapes: ${grapes.length}`);

    // Grape synonyms
    const synonyms = await fetchAll('grape_synonyms', 'grape_id,synonym');
    for (const s of synonyms) {
      this.grapeSynonyms.set(s.synonym.toLowerCase(), s.grape_id);
    }
    console.log(`  Grape synonyms: ${synonyms.length}`);

    // Varietal categories
    const varietals = await fetchAll('varietal_categories', 'id,name,slug');
    for (const v of varietals) {
      this.varietalCategories.set(v.name.toLowerCase(), v.id);
      this.varietalCategories.set(v.slug, v.id);
    }
    console.log(`  Varietal categories: ${varietals.length}`);

    // Publications
    const pubs = await fetchAll('publications', 'id,name,slug');
    for (const p of pubs) {
      this.publications.set(p.name.toLowerCase(), p.id);
      this.publications.set(p.slug, p.id);
    }
    // Register aliases
    for (const [alias, canonical] of Object.entries(PUB_ALIASES)) {
      const id = this.publications.get(canonical.toLowerCase());
      if (id) this.publications.set(alias.toLowerCase(), id);
    }
    console.log(`  Publications: ${pubs.length}`);

    // Label designations
    const lds = await fetchAll('label_designations', 'id,canonical_name,local_name,category,country_id');
    for (const ld of lds) {
      // Key by canonical_name|country_id for disambiguation
      this.labelDesignations.set(`${ld.canonical_name.toLowerCase()}|${ld.country_id || 'null'}`, ld.id);
      if (ld.local_name) {
        this.labelDesignations.set(`${ld.local_name.toLowerCase()}|${ld.country_id || 'null'}`, ld.id);
      }
    }
    console.log(`  Label designations: ${lds.length}`);

    // Source types
    const sts = await fetchAll('source_types', 'id,slug');
    for (const s of sts) {
      this.sourceTypes.set(s.slug, s.id);
    }
    console.log(`  Source types: ${sts.length}`);

    // Bottle formats
    const formats = await fetchAll('bottle_formats', 'id,name,volume_ml');
    for (const f of formats) {
      this.bottleFormats.set(f.name.toLowerCase(), f);
      this.bottleFormats.set(String(f.volume_ml), f);
    }
    console.log(`  Bottle formats: ${formats.length}`);

    // Classification levels
    const clLevels = await fetchAll('classification_levels', 'id,classification_id,level_name,level_rank');
    const clSystems = await fetchAll('classifications', 'id,name,country_id');
    const systemMap = new Map(clSystems.map(s => [s.id, s]));
    for (const cl of clLevels) {
      const sys = systemMap.get(cl.classification_id);
      if (!sys) continue;
      // Key: "system name|level name" lowercase
      const key = `${sys.name.toLowerCase()}|${cl.level_name.toLowerCase()}`;
      const entry = { levelId: cl.id, systemName: sys.name, levelName: cl.level_name, rank: cl.level_rank };
      this.classificationLevels.set(key, entry);
      // Also register short aliases for common systems
      const shortNames = {
        "langton's classification of australian wine": ["langton's classification", "langtons classification", "langtons", "langton's"],
        "bordeaux 1855 classification (sauternes)": ["bordeaux 1855 sauternes classification", "1855 sauternes", "sauternes classification", "sauternes 1855"],
        "bordeaux 1855 classification (médoc)": ["bordeaux 1855 classification", "1855 medoc", "medoc classification", "1855 classification"],
        "burgundy vineyard classification": ["burgundy classification", "burgundy vineyard"],
        "champagne cru classification": ["champagne classification", "champagne cru"],
        "saint-émilion classification": ["saint-emilion classification", "st-emilion classification", "saint emilion"],
        "vdp classification": ["vdp"],
        "cru bourgeois du médoc": ["cru bourgeois"],
        "ötw erste lagen": ["otw erste lagen", "otw"],
      };
      const sysLower = sys.name.toLowerCase();
      const aliases = shortNames[sysLower] || [];
      for (const alias of aliases) {
        this.classificationLevels.set(`${alias}|${cl.level_name.toLowerCase()}`, entry);
      }
      // Also key by just level name for single-system contexts (e.g., "Grand Cru" when system is specified)
    }
    console.log(`  Classification levels: ${clLevels.length} (${clSystems.length} systems)`);

    // Winemakers
    const wms = await fetchAll('winemakers', 'id,slug,name');
    for (const w of wms) {
      this.winemakers.set(w.slug, w);
      this.winemakers.set(w.name.toLowerCase(), w);
    }
    console.log(`  Winemakers: ${wms.length}`);

    console.log('Reference data loaded.\n');
  }

  resolveGrape(name) {
    const lower = name.toLowerCase().trim();
    const norm = normalize(lower);
    // 1. Check alias table (both accented and accent-stripped)
    const aliased = GRAPE_ALIASES[lower] || GRAPE_ALIASES[norm];
    if (aliased) {
      const g = this.grapes.get(aliased.toLowerCase()) || this.grapesByName.get(aliased.toLowerCase());
      if (g) return g;
    }
    // 2. Check display_name (accented then normalized)
    const byDisplay = this.grapes.get(lower) || this.grapes.get(norm);
    if (byDisplay) return byDisplay;
    // 3. Check VIVC name (accented then normalized)
    const byVivc = this.grapesByName.get(lower) || this.grapesByName.get(norm);
    if (byVivc) return byVivc;
    // 4. Check synonyms table (accented then normalized)
    const synId = this.grapeSynonyms.get(lower) || this.grapeSynonyms.get(norm);
    if (synId) {
      return { id: synId };
    }
    // 5. Try with common suffixes
    for (const suffix of [' noir', ' blanc', ' tinto', ' tinta', ' blanco']) {
      const withSuffix = this.grapes.get(lower + suffix) || this.grapes.get(norm + suffix);
      if (withSuffix) return withSuffix;
    }
    return null;
  }

  resolveAppellation(name) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    // Try exact match first
    const a = this.appellations.get(lower);
    if (a) return a;
    // Try normalized (accent-stripped) match
    const norm = normalize(lower);
    const b = this.appellations.get(norm);
    return b || null;
  }

  resolveRegion(name, countryId) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    // Try alias first
    const aliased = REGION_ALIASES[lower];
    const candidates = aliased ? [lower, aliased.toLowerCase()] : [lower];
    for (const c of candidates) {
      if (countryId) {
        const r = this.regions.get(`${c}|${countryId}`);
        if (r) return r;
      }
      const r = this.regions.get(c);
      if (r) return r;
      // Try normalized (accent-stripped)
      const norm = normalize(c);
      if (countryId) {
        const r2 = this.regions.get(`${norm}|${countryId}`);
        if (r2) return r2;
      }
      const r3 = this.regions.get(norm);
      if (r3) return r3;
    }
    return null;
  }

  resolveCountry(name) {
    if (!name) return null;
    return this.countries.get(name.toLowerCase().trim()) || null;
  }

  resolvePublication(name) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    return this.publications.get(lower) || null;
  }

  resolveVarietalCategory(name) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    return this.varietalCategories.get(lower) ||
      this.varietalCategories.get(slugify(name)) || null;
  }

  resolveLabelDesignation(name, countryId) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    // Try country-specific first
    if (countryId) {
      const id = this.labelDesignations.get(`${lower}|${countryId}`);
      if (id) return id;
    }
    // Try universal (null country)
    return this.labelDesignations.get(`${lower}|null`) || null;
  }

  resolveBottleFormat(nameOrMl) {
    if (!nameOrMl) return null;
    const key = String(nameOrMl).toLowerCase().trim();
    return this.bottleFormats.get(key) || null;
  }

  resolveClassification(system, level) {
    if (!system || !level) return null;
    const key = `${system.toLowerCase().trim()}|${level.toLowerCase().trim()}`;
    const cl = this.classificationLevels.get(key);
    return cl || null;
  }

  resolveWinemaker(name) {
    if (!name) return null;
    return this.winemakers.get(name.toLowerCase().trim()) ||
      this.winemakers.get(slugify(name)) || null;
  }
}

// ── Import Engine ───────────────────────────────────────────
class ProducerImporter {
  constructor(data, refs, dryRun = false, replace = false) {
    this.data = data;
    this.refs = refs;
    this.dryRun = dryRun;
    this.replace = replace;
    this.stats = {
      producer: 0,
      wines: 0,
      vintages: 0,
      scores: 0,
      wineGrapes: 0,
      vintageGrapes: 0,
      labelDesignations: 0,
      certifications: 0,
      winemakers: 0,
      bottleFormats: 0,
      classifications: 0,
      aliases: 0,
      vineyards: 0,
      warnings: [],
    };
    this.producerId = null;
    this.countryId = null;
    this.regionId = null;
    this.sourceId = null;
    this.wineIdMap = new Map(); // wine name → wine_id
  }

  warn(msg) {
    this.stats.warnings.push(msg);
    console.warn(`  ⚠ ${msg}`);
  }

  async run() {
    console.log(`\n${'='.repeat(50)}`);
    console.log(`  IMPORTING: ${this.data.producer.name}`);
    console.log(`  ${this.dryRun ? '(DRY RUN)' : ''}`);
    console.log(`${'='.repeat(50)}\n`);

    // Resolve source type
    this.sourceId = this.refs.sourceTypes.get('producer-website');
    if (!this.sourceId) {
      throw new Error('source_type "producer-website" not found');
    }

    // Resolve country and region
    this.countryId = this.refs.resolveCountry(this.data.producer.country);
    if (!this.countryId) {
      throw new Error(`Country not found: "${this.data.producer.country}"`);
    }

    const regionData = this.refs.resolveRegion(this.data.producer.region, this.countryId);
    this.regionId = regionData?.id || null;
    if (!this.regionId) {
      this.warn(`Region not found: "${this.data.producer.region}"`);
    }

    // --replace: delete existing producer data before re-importing
    if (this.replace) {
      await this.deleteExisting();
    }

    await this.importProducer();
    await this.importWines();
    this.printSummary();
  }

  async deleteExisting() {
    const p = this.data.producer;
    const producerSlug = p.slug || slugify(p.name);

    const { data: existing } = await sb.from('producers')
      .select('id').eq('slug', producerSlug).single();

    if (!existing) {
      console.log('No existing producer to replace.\n');
      return;
    }

    const producerId = existing.id;
    console.log(`\n🔄 REPLACE MODE: Deleting existing data for ${p.name}...`);

    if (this.dryRun) {
      console.log('[DRY RUN] Would delete all data for this producer.\n');
      return;
    }

    // Get all wine IDs for this producer
    const wines = await fetchAll('wines', 'id', { producer_id: producerId });
    const wineIds = wines.map(w => w.id);

    if (wineIds.length > 0) {
      // Delete in dependency order (children first)
      for (const table of [
        'wine_vintage_scores', 'wine_vintage_grapes', 'wine_vintage_formats',
        'wine_vintage_vineyards', 'wine_vintage_descriptors', 'wine_vintage_nv_components',
        'wine_vintage_tasting_insights', 'wine_vintage_prices',
      ]) {
        const { error } = await sb.from(table).delete().in('wine_id', wineIds);
        if (error && !error.message.includes('does not exist')) {
          console.log(`  Cleared ${table}: ${error?.message || 'ok'}`);
        }
      }

      // Delete wine_vintages
      const { error: vintErr } = await sb.from('wine_vintages').delete().in('wine_id', wineIds);
      if (vintErr) console.log(`  wine_vintages error: ${vintErr.message}`);

      // Delete wine-level joins
      for (const table of ['wine_grapes', 'wine_label_designations', 'wine_vineyards', 'wine_appellations', 'wine_aliases']) {
        const { error } = await sb.from(table).delete().in('wine_id', wineIds);
        if (error && !error.message.includes('does not exist')) {
          console.log(`  Cleared ${table}: ${error?.message || 'ok'}`);
        }
      }

      // Delete entity_classifications for wines
      for (const wineId of wineIds) {
        await sb.from('entity_classifications').delete()
          .eq('entity_type', 'wine').eq('entity_id', wineId);
      }

      // Delete wines
      const { error: wineErr } = await sb.from('wines').delete().eq('producer_id', producerId);
      if (wineErr) console.log(`  wines error: ${wineErr.message}`);
    }

    // Delete producer-level joins
    for (const table of ['producer_winemakers', 'producer_farming_certifications', 'producer_biodiversity_certifications', 'producer_importers']) {
      const { error } = await sb.from(table).delete().eq('producer_id', producerId);
      if (error && !error.message.includes('does not exist')) {
        // Silently continue
      }
    }

    // Delete the producer itself
    const { error: prodErr } = await sb.from('producers').delete().eq('id', producerId);
    if (prodErr) console.log(`  producer error: ${prodErr.message}`);

    console.log(`  Deleted: ${wines.length} wines + producer record.\n`);
  }

  async importProducer() {
    const p = this.data.producer;
    const producerSlug = p.slug || slugify(p.name);

    // Check if producer already exists
    const { data: existing } = await sb.from('producers')
      .select('id').eq('slug', producerSlug).single();

    if (existing) {
      this.producerId = existing.id;
      console.log(`Producer already exists: ${p.name} (${this.producerId})`);
      this.stats.producer = 0;
      return;
    }

    this.producerId = randomUUID();
    const row = {
      id: this.producerId,
      slug: producerSlug,
      name: p.name,
      name_normalized: normalize(p.name),
      country_id: this.countryId,
      region_id: this.regionId,
      website_url: p.website || null,
      year_established: p.founded_year || p.year_established || null,
      producer_type: p.producer_type || null,
      parent_company: p.parent_company || null,
      hectares_under_vine: p.hectares_under_vine || null,
      total_production_cases: p.total_production_cases || null,
      philosophy: p.philosophy || null,
      latitude: p.latitude || null,
      longitude: p.longitude || null,
      metadata: p.metadata || null,
    };

    // Resolve parent producer if given
    if (p.parent_producer_slug) {
      const { data: parent } = await sb.from('producers')
        .select('id').eq('slug', p.parent_producer_slug).single();
      if (parent) {
        row.parent_producer_id = parent.id;
      } else {
        this.warn(`Parent producer not found: "${p.parent_producer_slug}"`);
      }
    }

    // Resolve appellation if given
    if (p.appellation) {
      const app = this.refs.resolveAppellation(p.appellation);
      if (app) row.appellation_id = app.id;
      else this.warn(`Producer appellation not found: "${p.appellation}"`);
    }

    if (this.dryRun) {
      console.log(`[DRY RUN] Would create producer: ${p.name}`);
    } else {
      const { error } = await sb.from('producers').insert(row);
      if (error) throw new Error(`Producer insert failed: ${error.message}`);
      console.log(`Created producer: ${p.name} (${this.producerId})`);
    }
    this.stats.producer = 1;

    // Farming certifications
    if (p.farming_certifications) {
      for (const cert of p.farming_certifications) {
        await this.insertFarmingCert(cert);
      }
    }

    // Winemakers
    if (p.winemakers) {
      for (const wm of p.winemakers) {
        await this.importWinemaker(wm);
      }
    }
  }

  async insertFarmingCert(cert) {
    // Look up certification by name (accept both 'name' and 'certification' fields)
    const certName = cert.name || cert.certification;
    const { data: certRow } = await sb.from('farming_certifications')
      .select('id').ilike('name', certName).single();
    if (!certRow) {
      this.warn(`Farming certification not found: "${certName}"`);
      return;
    }
    if (this.dryRun) {
      console.log(`[DRY RUN] Would add certification: ${certName}`);
      return;
    }
    // certified_since is integer (year) — extract year from dates like "2021-03-01"
    let certSince = cert.since || cert.year || null;
    if (certSince && typeof certSince === 'string' && certSince.includes('-')) {
      certSince = parseInt(certSince.split('-')[0], 10);
    }
    const { error } = await sb.from('producer_farming_certifications').insert({
      producer_id: this.producerId,
      farming_certification_id: certRow.id,
      certified_since: certSince,
      certifying_body: cert.body || null,
      certification_status: cert.status || 'certified',
      source_id: this.sourceId,
    });
    if (error && !error.message.includes('duplicate')) {
      this.warn(`Certification insert error: ${error.message}`);
    } else {
      this.stats.certifications++;
    }
  }

  async importWinemaker(wm) {
    const wmSlug = slugify(wm.name);
    // Check if winemaker exists
    let existing = this.refs.resolveWinemaker(wm.name);
    let wmId;
    if (existing) {
      wmId = existing.id;
    } else {
      wmId = randomUUID();
      if (this.dryRun) {
        console.log(`[DRY RUN] Would create winemaker: ${wm.name}`);
      } else {
        const { error } = await sb.from('winemakers').insert({
          id: wmId,
          name: wm.name,
          slug: wmSlug,
          country_id: wm.country ? this.refs.resolveCountry(wm.country) : null,
          metadata: wm.metadata || null,
        });
        if (error) {
          if (error.message.includes('duplicate')) {
            // Slug collision — look up by slug
            const { data: bySlug } = await sb.from('winemakers')
              .select('id').eq('slug', wmSlug).single();
            if (bySlug) wmId = bySlug.id;
          } else {
            this.warn(`Winemaker insert error: ${error.message}`);
            return;
          }
        }
        console.log(`  Created winemaker: ${wm.name}`);
      }
      // Cache for future use
      this.refs.winemakers.set(wmSlug, { id: wmId, slug: wmSlug, name: wm.name });
      this.refs.winemakers.set(wm.name.toLowerCase(), { id: wmId, slug: wmSlug, name: wm.name });
    }

    // Link to producer
    if (!this.dryRun) {
      const { error } = await sb.from('producer_winemakers').insert({
        producer_id: this.producerId,
        winemaker_id: wmId,
        role: wm.role || 'head',
        start_year: wm.start_year || null,
        end_year: wm.end_year || null,
      });
      if (error && !error.message.includes('duplicate')) {
        this.warn(`Producer-winemaker link error: ${error.message}`);
      } else {
        this.stats.winemakers++;
      }
    }
  }

  async importWines() {
    console.log(`\nImporting ${this.data.wines.length} wines...\n`);

    for (const wine of this.data.wines) {
      await this.importWine(wine);
    }
  }

  async importWine(wine) {
    const wineSlug = wine.slug || slugify(`${this.data.producer.name} ${wine.name}`);

    // Check if wine already exists
    const { data: existing } = await sb.from('wines')
      .select('id').eq('slug', wineSlug).single();

    let wineId;
    if (existing) {
      wineId = existing.id;
      console.log(`  Wine exists: ${wine.name} (${wineId})`);
    } else {
      wineId = randomUUID();

      // Resolve appellation
      let appellationId = null;
      if (wine.appellation) {
        const app = this.refs.resolveAppellation(wine.appellation);
        if (app) appellationId = app.id;
        else this.warn(`Wine appellation not found: "${wine.appellation}" for ${wine.name}`);
      }

      // Resolve varietal category
      let varietalCategoryId = null;
      if (wine.varietal_category) {
        varietalCategoryId = this.refs.resolveVarietalCategory(wine.varietal_category);
        if (!varietalCategoryId) {
          this.warn(`Varietal category not found: "${wine.varietal_category}" for ${wine.name}`);
          // Fall back to Red Blend or White Blend
          const fallback = (wine.color === 'white') ? 'white-blend' : 'red-blend';
          varietalCategoryId = this.refs.resolveVarietalCategory(fallback);
        }
      }
      // varietal_category_id is nullable — better null than wrong default
      if (!varietalCategoryId && wine.varietal_category) {
        this.warn(`Varietal category not resolved for ${wine.name}: "${wine.varietal_category}"`);
      }

      // Resolve wine-level region (may differ from producer)
      let wineRegionId = this.regionId;
      if (wine.region) {
        const r = this.refs.resolveRegion(wine.region, this.countryId);
        if (r) wineRegionId = r.id;
      }

      const row = {
        id: wineId,
        slug: wineSlug,
        name: wine.name,
        name_normalized: normalize(wine.name),
        producer_id: this.producerId,
        country_id: this.countryId,
        region_id: wineRegionId,
        appellation_id: appellationId,
        varietal_category_id: varietalCategoryId,
        varietal_category_source: this.sourceId,
        color: wine.color === 'rosé' ? 'rose' : (wine.color || null),
        wine_type: (wine.wine_type === 'still' ? 'table' : wine.wine_type) || 'table',
        sweetness_level: wine.sweetness_level || null,
        effervescence: wine.effervescence || 'still',
        vinification_notes: wine.vinification_notes || null,
        food_pairings: wine.food_pairings || null,
        is_nv: wine.is_nv || false,
        style: wine.style || null,
        first_vintage_year: wine.first_vintage_year || null,
        description: wine.description || null,
        metadata: wine.metadata || null,
      };

      if (this.dryRun) {
        console.log(`  [DRY RUN] Would create wine: ${wine.name}`);
      } else {
        const { error } = await sb.from('wines').insert(row);
        if (error) {
          this.warn(`Wine insert error for "${wine.name}": ${error.message}`);
          return;
        }
        console.log(`  Created wine: ${wine.name}`);
      }
      this.stats.wines++;
    }

    this.wineIdMap.set(wine.name, wineId);

    // Wine-level grapes (default composition)
    if (wine.grapes && wine.grapes.length > 0) {
      await this.importWineGrapes(wineId, wine.name, wine.grapes);
    }

    // Label designations
    if (wine.label_designations) {
      for (const ld of wine.label_designations) {
        await this.importLabelDesignation(wineId, ld);
      }
    }

    // Classification (e.g., Burgundy Grand Cru, Saint-Émilion Premier Grand Cru Classé)
    if (wine.classification) {
      await this.importClassification(wineId, wine.name, wine.classification);
    }

    // Wine aliases (previous names, alternate labels)
    if (wine.aliases) {
      for (const alias of wine.aliases) {
        await this.importWineAlias(wineId, alias);
      }
    }

    // Vineyard sourcing
    if (wine.vineyards) {
      for (const vy of wine.vineyards) {
        await this.importWineVineyard(wineId, wine.name, vy);
      }
    }

    // Vintages
    if (wine.vintages) {
      for (const v of wine.vintages) {
        await this.importVintage(wineId, wine, v);
      }
    }
  }

  async importWineGrapes(wineId, wineName, grapes) {
    for (const g of grapes) {
      const grapeName = g.name || g.grape;
      const grape = this.refs.resolveGrape(grapeName);
      if (!grape) {
        this.warn(`Unknown grape: "${grapeName}" for ${wineName}`);
        continue;
      }
      if (this.dryRun) continue;

      const { error } = await sb.from('wine_grapes').insert({
        wine_id: wineId,
        grape_id: grape.id,
        percentage: g.percentage || null,
        percentage_source: this.sourceId,
      });
      if (error && !error.message.includes('duplicate')) {
        this.warn(`Wine grape error for ${wineName}/${grapeName}: ${error.message}`);
      } else if (!error) {
        this.stats.wineGrapes++;
      }
    }
  }

  async importLabelDesignation(wineId, designationName) {
    const ldId = this.refs.resolveLabelDesignation(designationName, this.countryId);
    if (!ldId) {
      this.warn(`Label designation not found: "${designationName}"`);
      return;
    }
    if (this.dryRun) return;

    const { error } = await sb.from('wine_label_designations').insert({
      wine_id: wineId,
      label_designation_id: ldId,
    });
    if (error && !error.message.includes('duplicate')) {
      this.warn(`Label designation error: ${error.message}`);
    } else if (!error) {
      this.stats.labelDesignations++;
    }
  }

  async importClassification(wineId, wineName, classification) {
    const cl = this.refs.resolveClassification(classification.system, classification.level);
    if (!cl) {
      this.warn(`Classification not found: "${classification.system} / ${classification.level}" for ${wineName}`);
      return;
    }
    if (this.dryRun) {
      console.log(`    [DRY RUN] Would classify ${wineName} as ${cl.systemName} ${cl.levelName}`);
      return;
    }
    const { error } = await sb.from('entity_classifications').insert({
      classification_level_id: cl.levelId,
      entity_type: 'wine',
      entity_id: wineId,
      year_classified: classification.year_classified || null,
      year_declassified: classification.year_declassified || null,
      notes: classification.notes || null,
    });
    if (error && !error.message.includes('duplicate')) {
      this.warn(`Classification insert error for ${wineName}: ${error.message}`);
    } else if (!error) {
      this.stats.classifications++;
    }
  }

  async importWineAlias(wineId, alias) {
    if (this.dryRun) {
      console.log(`    [DRY RUN] Would add alias: ${alias.name}`);
      return;
    }
    const { error } = await sb.from('wine_aliases').insert({
      wine_id: wineId,
      name: alias.name || alias.alias,
      alias_type: alias.alias_type || 'previous_name',
      start_year: alias.start_year || null,
      end_year: alias.end_year || null,
      notes: alias.notes || null,
    });
    if (error) {
      this.warn(`Wine alias insert error: ${error.message}`);
    } else {
      this.stats.aliases++;
    }
  }

  async importWineVineyard(wineId, wineName, vineyard) {
    // Look up vineyard by name
    const vyName = vineyard.name || vineyard;
    const { data: vyRow } = await sb.from('vineyards')
      .select('id').ilike('name', vyName).single();
    if (!vyRow) {
      this.warn(`Vineyard not found: "${vyName}" for ${wineName}`);
      return;
    }
    if (this.dryRun) {
      console.log(`    [DRY RUN] Would link vineyard: ${vyName}`);
      return;
    }
    const { error } = await sb.from('wine_vineyards').insert({
      wine_id: wineId,
      vineyard_id: vyRow.id,
    });
    if (error && !error.message.includes('duplicate')) {
      this.warn(`Wine vineyard link error: ${error.message}`);
    } else if (!error) {
      this.stats.vineyards++;
    }
  }

  async importVintage(wineId, wine, v) {
    // Check if vintage already exists
    const { data: existing } = await sb.from('wine_vintages')
      .select('id')
      .eq('wine_id', wineId)
      .eq('vintage_year', v.year)
      .single();

    if (existing) {
      // Still import scores for existing vintages
      if (v.scores) {
        for (const s of v.scores) {
          await this.importScore(wineId, v.year, s);
        }
      }
      return;
    }

    const row = {
      wine_id: wineId,
      vintage_year: v.year != null ? v.year : null,  // preserve 0 for NV wines
      abv: v.abv || null,
      ph: v.ph || null,
      ta_g_l: v.ta_g_l || null,
      rs_g_l: v.rs_g_l != null ? v.rs_g_l : null,
      brix_at_harvest: v.brix || null,
      duration_in_oak_months: v.oak_duration_months || v.oak_months || null,
      new_oak_pct: v.oak_new_pct || v.new_oak_pct || null,
      neutral_oak_pct: v.neutral_oak_pct || null,
      whole_cluster_pct: v.whole_cluster_pct || null,
      bottle_aging_months: v.bottle_aging_months || null,
      mlf: v.mlf || null,
      maceration_days: v.maceration_days || null,
      lees_aging_months: v.lees_aging_months || null,
      batonnage: v.batonnage ?? null,
      skin_contact_days: v.skin_contact_days || null,
      aging_vessel: v.aging_vessel || null,
      yield_hl_ha: v.yield_hl_ha || null,
      winemaker_notes: v.winemaker_notes || null,
      vintage_notes: v.vintage_notes || null,
      cases_produced: v.production_cases || v.cases_produced || null,
      bottling_date: parseDate(v.bottling_date),
      harvest_start_date: parseDate(v.harvest_start || v.harvest_date),
      harvest_end_date: parseDate(v.harvest_end),
      release_price_usd: v.release_price_usd || null,
      release_price_original: v.release_price_original || null,
      release_price_currency: v.release_price_currency || null,
      release_price_source: v.release_price_usd ? this.sourceId : null,
      producer_drinking_window_start: v.drink_window_start || null,
      producer_drinking_window_end: v.drink_window_end || null,
      availability_status: v.availability || null,
      // Per-vintage overrides
      oak_origin: v.oak_origin || null,
      yeast_type: v.yeast_type || null,
      fermentation_vessel: v.fermentation_vessel || null,
      closure: v.closure || null,
      fining: v.fining || null,
      filtration: v.filtration ?? null,
      disgorgement_date: parseDate(v.disgorgement_date),
      release_date: parseDate(v.release_date),
      pradikat: v.pradikat || wine.pradikat || this._detectPradikat(wine) || null,
      solera_system: v.solera_system ?? null,
      age_statement_years: v.age_statement_years || null,
      label_image_url: v.label_image_url || null,
      winemaking_source: this.sourceId,
      metadata: v.metadata || null,
    };

    if (this.dryRun) {
      console.log(`    [DRY RUN] Would create vintage: ${v.year}`);
    } else {
      const { error } = await sb.from('wine_vintages').insert(row);
      if (error) {
        this.warn(`Vintage insert error for ${wine.name} ${v.year}: ${error.message}`);
        return;
      }
    }
    this.stats.vintages++;

    // Per-vintage grape percentages (if different from wine-level)
    if (v.grapes && v.grapes.length > 0) {
      for (const g of v.grapes) {
        const grapeName = g.name || g.grape;
        const grape = this.refs.resolveGrape(grapeName);
        if (!grape) {
          this.warn(`Unknown vintage grape: "${grapeName}" for ${wine.name} ${v.year}`);
          continue;
        }
        if (this.dryRun) continue;

        const { error } = await sb.from('wine_vintage_grapes').insert({
          wine_id: wineId,
          vintage_year: v.year,
          grape_id: grape.id,
          percentage: g.percentage || null,
          percentage_source: this.sourceId,
        });
        if (error && !error.message.includes('duplicate')) {
          this.warn(`Vintage grape error: ${error.message}`);
        } else if (!error) {
          this.stats.vintageGrapes++;
        }
      }
    }

    // Scores
    if (v.scores) {
      for (const s of v.scores) {
        await this.importScore(wineId, v.year, s);
      }
    }

    // Bottle formats
    if (v.formats) {
      for (const fmt of v.formats) {
        await this.importBottleFormat(wineId, v.year, fmt);
      }
    }

    // Per-vintage vineyard sourcing
    if (v.vineyards) {
      for (const vy of v.vineyards) {
        await this.importVintageVineyard(wineId, v.year, wine.name, vy);
      }
    }
  }

  _detectPradikat(wine) {
    const PRADIKATS = ['Trockenbeerenauslese', 'Beerenauslese', 'Eiswein', 'Auslese', 'Spätlese', 'Kabinett'];
    // Check label_designations first
    if (wine.label_designations) {
      for (const p of PRADIKATS) {
        if (wine.label_designations.some(ld => ld.toLowerCase() === p.toLowerCase())) return p;
      }
    }
    // Check wine name
    const name = wine.name || '';
    for (const p of PRADIKATS) {
      if (name.toLowerCase().includes(p.toLowerCase())) return p;
      // Also check ASCII version
      if (p === 'Spätlese' && name.toLowerCase().includes('spatlese')) return p;
    }
    return null;
  }

  async importVintageVineyard(wineId, vintageYear, wineName, vineyard) {
    const vyName = vineyard.name || vineyard;
    const { data: vyRow } = await sb.from('vineyards')
      .select('id').ilike('name', vyName).single();
    if (!vyRow) {
      this.warn(`Vineyard not found: "${vyName}" for ${wineName} ${vintageYear}`);
      return;
    }
    if (this.dryRun) return;
    const { error } = await sb.from('wine_vintage_vineyards').insert({
      wine_id: wineId,
      vintage_year: vintageYear,
      vineyard_id: vyRow.id,
      percentage: vineyard.percentage || null,
    });
    if (error && !error.message.includes('duplicate')) {
      this.warn(`Vintage vineyard link error: ${error.message}`);
    }
  }

  async importBottleFormat(wineId, vintageYear, fmt) {
    const format = this.refs.resolveBottleFormat(fmt.name || fmt.volume_ml);
    if (!format) {
      this.warn(`Bottle format not found: "${fmt.name || fmt.volume_ml}"`);
      return;
    }
    if (this.dryRun) return;

    const { error } = await sb.from('wine_vintage_formats').insert({
      wine_id: wineId,
      vintage_year: vintageYear,
      bottle_format_id: format.id,
      cases_produced: fmt.cases_produced || null,
      release_price_usd: fmt.release_price_usd || null,
    });
    if (error && !error.message.includes('duplicate')) {
      this.warn(`Bottle format insert error: ${error.message}`);
    } else if (!error) {
      this.stats.bottleFormats++;
    }
  }

  async importScore(wineId, vintageYear, s) {
    const pubId = this.refs.resolvePublication(s.publication);
    if (!pubId) {
      this.warn(`Publication not found: "${s.publication}" (score: ${s.score})`);
      return;
    }
    if (this.dryRun) return;

    const { error } = await sb.from('wine_vintage_scores').insert({
      wine_id: wineId,
      vintage_year: vintageYear,
      score: s.score,
      score_low: s.score_low || null,
      score_high: s.score_high || null,
      score_scale: s.scale || '100',
      publication_id: pubId,
      critic: s.reviewer || s.critic || null,
      tasting_note: s.tasting_note || null,
      review_date: s.review_date || null,
      critic_drink_window_start: s.drinking_window_start || s.drink_from || null,
      critic_drink_window_end: s.drinking_window_end || s.drink_to || null,
      source_id: this.sourceId,
      discovered_at: new Date().toISOString(),
    });
    if (error) {
      if (!error.message.includes('duplicate')) {
        this.warn(`Score insert error: ${error.message}`);
      }
    } else {
      this.stats.scores++;
    }
  }

  printSummary() {
    console.log(`\n${'='.repeat(50)}`);
    console.log(`  IMPORT COMPLETE: ${this.data.producer.name}`);
    console.log(`${'='.repeat(50)}`);
    console.log(`  Producer: ${this.stats.producer ? 'created' : 'already existed'}`);
    console.log(`  Wines: ${this.stats.wines}`);
    console.log(`  Vintages: ${this.stats.vintages}`);
    console.log(`  Scores: ${this.stats.scores}`);
    console.log(`  Wine grapes: ${this.stats.wineGrapes}`);
    console.log(`  Vintage grapes: ${this.stats.vintageGrapes}`);
    console.log(`  Label designations: ${this.stats.labelDesignations}`);
    console.log(`  Certifications: ${this.stats.certifications}`);
    console.log(`  Winemakers: ${this.stats.winemakers}`);
    console.log(`  Bottle formats: ${this.stats.bottleFormats}`);
    console.log(`  Classifications: ${this.stats.classifications}`);
    console.log(`  Aliases: ${this.stats.aliases}`);
    console.log(`  Vineyards: ${this.stats.vineyards}`);
    if (this.stats.warnings.length > 0) {
      console.log(`\n  Warnings (${this.stats.warnings.length}):`);
      for (const w of this.stats.warnings) {
        console.log(`    - ${w}`);
      }
    }
    console.log('');
  }
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  if (!existsSync(jsonPath)) {
    console.error(`File not found: ${jsonPath}`);
    process.exit(1);
  }

  const data = JSON.parse(readFileSync(jsonPath, 'utf-8'));
  console.log(`Loaded: ${jsonPath}`);
  console.log(`Producer: ${data.producer.name}`);
  console.log(`Wines: ${data.wines.length}`);
  console.log(`Total vintages: ${data.wines.reduce((s, w) => s + (w.vintages?.length || 0), 0)}`);

  const refs = new ReferenceData();
  await refs.load();

  const importer = new ProducerImporter(data, refs, DRY_RUN, REPLACE);
  await importer.run();
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
