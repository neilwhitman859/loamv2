/**
 * lib/merge.mjs — Core merge library for the Loam wine database
 *
 * Provides multi-source matching, additive field merging, and data grade
 * calculation. Designed to be imported by: import_lwin.mjs, fetch_*.mjs
 * importers, COLA enricher, state DB importers, and any future pipeline
 * that needs to resolve or create producers/wines from external data.
 *
 * Usage:
 *   import { MergeEngine } from '../lib/merge.mjs';
 *   const engine = new MergeEngine();
 *   await engine.init();
 *   const producer = await engine.matchProducer('Domaine de la Romanee-Conti', countryId);
 *   const wine = await engine.matchWine(producer.id, 'La Tache');
 */

import { readFileSync } from 'fs';
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

// ── Text Helpers ────────────────────────────────────────────

/**
 * Normalize a string for matching: strip accents, lowercase, collapse whitespace.
 * @param {string} s
 * @returns {string}
 */
export function normalize(s) {
  if (!s) return '';
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

/**
 * Generate a URL-safe slug from a string.
 * @param {string} s
 * @returns {string}
 */
export function slugify(s) {
  if (!s) return '';
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

// ── Supabase Pagination Helper ──────────────────────────────

/**
 * Fetch all rows from a Supabase table, paginating in batches.
 * @param {string} table - Table name
 * @param {string} columns - Column selection string
 * @param {Object} [filter] - Optional eq filters as key-value pairs
 * @param {number} [batchSize=1000]
 * @returns {Promise<Array>}
 */
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

// ── Grape Aliases (in-code) ─────────────────────────────────
// Common label names -> canonical DB display_name.
// Duplicated from lib/import.mjs to keep merge.mjs self-contained.
const GRAPE_ALIASES = {
  // US names
  'petite sirah': 'Durif', 'petit sirah': 'Durif', 'petite syrah': 'Durif',
  'cab sauv': 'Cabernet Sauvignon', 'cab franc': 'Cabernet Franc',
  'cab': 'Cabernet Sauvignon', 'zin': 'Zinfandel',
  'petit verdot': 'Petit Verdot', 'petite verdot': 'Petit Verdot',
  // Spanish
  'mazuelo': 'Carignan', 'mazuela': 'Carignan', 'cariñena': 'Carignan',
  'garnacha': 'Grenache', 'garnacha tinta': 'Grenache',
  'garnacho': 'Grenache', 'garnacho tinto': 'Grenache',
  'malvasía': 'Malvasia', 'malvasia': 'Malvasia',
  // French
  'mourvèdre': 'Mourvèdre', 'mourvedre': 'Mourvèdre',
  'mataro': 'Mourvèdre', 'mataró': 'Mourvèdre',
  'syrah': 'Syrah', 'shiraz': 'Syrah',
  'grenache blanc': 'Grenache Blanc', 'grenache noir': 'Grenache',
  'sémillon': 'Sémillon', 'semillon': 'Sémillon',
  // Hungarian
  'sárgamuskotály': 'Muscat Blanc à Petits Grains',
  'sargamuskotaly': 'Muscat Blanc à Petits Grains',
  'yellow muscat': 'Muscat Blanc à Petits Grains',
  'hárslevelű': 'Hárslevelü', 'harslevelu': 'Hárslevelü',
  // Italian
  'sangiovese grosso': 'Sangiovese', 'brunello': 'Sangiovese',
  'primitivo': 'Zinfandel', 'nebbiolo': 'Nebbiolo',
  'pinot bianco': 'Pinot Blanc', 'pinot grigio': 'Pinot Gris',
  'verdicchio': 'Verdicchio Bianco', 'lagrein': 'Lagrein',
  // German/Austrian
  'müller-thurgau': 'Müller-Thurgau', 'muller-thurgau': 'Müller-Thurgau',
  'muller thurgau': 'Müller-Thurgau', 'mueller thurgau': 'Müller-Thurgau',
  'grüner veltliner': 'Grüner Veltliner', 'gruner veltliner': 'Grüner Veltliner',
  'blaufränkisch': 'Blaufränkisch', 'blaufrankisch': 'Blaufränkisch',
  'lemberger': 'Blaufränkisch', 'zweigelt': 'Zweigelt',
  'st. laurent': 'Sankt Laurent', 'saint laurent': 'Sankt Laurent',
  // Portuguese
  'touriga nacional': 'Touriga Nacional', 'touriga franca': 'Touriga Franca',
  'tinta roriz': 'Tempranillo', 'aragonez': 'Tempranillo',
  'tinto cão': 'Tinto Cao', 'tinto cao': 'Tinto Cao',
  'tinta barroca': 'Tinta Barroca',
  // Champagne
  'meunier': 'Pinot Meunier', 'pinot meunier': 'Pinot Meunier',
  // Georgian
  'rkatsiteli': 'Rkatsiteli', 'saperavi': 'Saperavi',
  'mtsvane': 'Mtsvane Kakhuri', 'mtsvane kakhuri': 'Mtsvane Kakhuri',
  // Lebanese
  'obaideh': 'Obaideh', 'merwah': 'Merwah',
  // Madeira
  'sercial': 'Sercial', 'verdelho': 'Verdelho',
  'boal': 'Boal', 'bual': 'Boal', 'terrantez': 'Terrantez',
  'tinta negra': 'Tinta Negra Mole', 'tinta negra mole': 'Tinta Negra Mole',
  // Common
  'sauv blanc': 'Sauvignon Blanc', 'sauvignon': 'Sauvignon Blanc',
  'chard': 'Chardonnay', 'pinot noir': 'Pinot Noir',
  'pinot gris': 'Pinot Gris', 'pinot blanc': 'Pinot Blanc',
  'gewurztraminer': 'Gewürztraminer', 'gewürztraminer': 'Gewürztraminer',
  'riesling': 'Riesling', 'merlot': 'Merlot',
  'cabernet sauvignon': 'Cabernet Sauvignon', 'cabernet franc': 'Cabernet Franc',
  'malbec': 'Malbec', 'cot': 'Malbec', 'côt': 'Malbec',
  'tempranillo': 'Tempranillo', 'tinta de toro': 'Tempranillo',
  'sangiovese': 'Sangiovese', 'morellino': 'Sangiovese',
  'prugnolo gentile': 'Sangiovese', 'nielluccio': 'Sangiovese',
  'chenin blanc': 'Chenin Blanc', 'chenin': 'Chenin Blanc', 'steen': 'Chenin Blanc',
  'viognier': 'Viognier', 'verdejo': 'Verdejo',
  'albariño': 'Albariño', 'albarino': 'Albariño',
  'pinotage': 'Pinotage', 'colombard': 'Colombard',
  'cinsaut': 'Cinsaut', 'cinsault': 'Cinsaut',
  'palomino': 'Palomino Fino', 'pedro ximenez': 'Pedro Ximenez',
  'pedro ximénez': 'Pedro Ximenez', 'px': 'Pedro Ximenez',
  'marsanne': 'Marsanne', 'roussanne': 'Roussanne',
  'trebbiano': 'Trebbiano Toscano', 'ugni blanc': 'Trebbiano Toscano',
  'melon de bourgogne': 'Melon', 'muscadet': 'Melon',
  'cortese': 'Cortese', 'arneis': 'Arneis',
  'dolcetto': 'Dolcetto', 'barbera': 'Barbera',
  'nerello mascalese': 'Nerello Mascalese', 'carricante': 'Carricante',
  "nero d'avola": "Nero d'Avola", 'nero davola': "Nero d'Avola",
  'aglianico': 'Aglianico', 'fiano': 'Fiano', 'greco': 'Greco',
  'falanghina': 'Falanghina',
  'picpoul': 'Piquepoul Blanc', 'piquepoul': 'Piquepoul Blanc',
  'clairette': 'Clairette', 'bourboulenc': 'Bourboulenc',
  'rolle': 'Vermentino',
  'assyrtiko': 'Assyrtiko', 'xinomavro': 'Xinomavro',
  'agiorgitiko': 'Agiorgitiko', 'moschofilero': 'Moschofilero',
  'país': 'País', 'pais': 'País',
  'torrontés': 'Torrontés Riojano', 'torrontes': 'Torrontés Riojano',
  'bonarda': 'Bonarda',
  'ribolla gialla': 'Ribolla Gialla', 'ribolla': 'Ribolla Gialla',
  'friulano': 'Sauvignonasse', 'tocai friulano': 'Sauvignonasse',
};

// ── Region Aliases (in-code) ────────────────────────────────
// Maps English/alternative names to DB canonical names.
const REGION_ALIASES = {
  'piedmont': 'Piemonte', 'tuscany': 'Toscana',
  'lombardy': 'Lombardia', 'sicily': 'Sicilia', 'sardinia': 'Sardegna',
  'friuli': 'Friuli-Venezia Giulia', 'friuli venezia giulia': 'Friuli-Venezia Giulia',
  'trentino': 'Trentino-Alto Adige', 'alto adige': 'Trentino-Alto Adige',
  'südtirol': 'Trentino-Alto Adige', 'apulia': 'Puglia',
  'burgundy': 'Burgundy', 'bourgogne': 'Burgundy',
  'rhone': 'Rhône Valley', 'rhône': 'Rhône Valley',
  'rhone valley': 'Rhône Valley', 'rhône valley': 'Rhône Valley',
  'northern rhone': 'Northern Rhône', 'northern rhône': 'Northern Rhône',
  'southern rhone': 'Southern Rhône', 'southern rhône': 'Southern Rhône',
  'loire': 'Loire Valley', 'loire valley': 'Loire Valley',
  'languedoc': 'Languedoc', 'roussillon': 'Roussillon',
  'southwest france': 'Southwest France', 'south west france': 'Southwest France',
  'catalonia': 'Catalunya', 'catalunya': 'Catalunya',
  'castile and leon': 'Castilla y León', 'castilla y leon': 'Castilla y León',
  'mosel': 'Mosel', 'moselle': 'Mosel',
  'palatinate': 'Pfalz',
  'douro': 'Douro', 'dão': 'Dão', 'dao': 'Dão',
  'bekaa valley': 'Bekaa Valley', 'bekaa': 'Bekaa Valley',
  'kakheti': 'Kakheti', 'kartli': 'Kartli', 'imereti': 'Imereti',
  'barossa': 'Barossa', 'barossa valley': 'Barossa Valley',
  'mclaren vale': 'McLaren Vale',
  'hawkes bay': "Hawke's Bay", "hawke's bay": "Hawke's Bay",
  'napa valley': 'Napa Valley', 'sonoma coast': 'Sonoma Coast',
  'willamette valley': 'Willamette Valley',
  'stellenbosch': 'Stellenbosch', 'swartland': 'Swartland',
  'mendoza': 'Mendoza', 'salta': 'Salta',
  'uco valley': 'Uco Valley', 'valle de uco': 'Uco Valley',
  'lujan de cuyo': 'Luján de Cuyo', 'luján de cuyo': 'Luján de Cuyo',
  'maipo valley': 'Maipo Valley', 'colchagua valley': 'Colchagua Valley',
  'casablanca valley': 'Casablanca Valley',
};

// ── Publication Aliases (in-code) ───────────────────────────
const PUB_ALIASES = {
  'robert parker': 'Wine Advocate', "robert parker's wine advocate": 'Wine Advocate',
  'the wine advocate': 'Wine Advocate', 'parker': 'Wine Advocate',
  'wine advocate': 'Wine Advocate', 'wa': 'Wine Advocate',
  'jamessuckling.com': 'James Suckling', 'james suckling': 'James Suckling', 'js': 'James Suckling',
  'vinous media': 'Vinous', 'vinous': 'Vinous', 'antonio galloni': 'Vinous',
  'wine spectator': 'Wine Spectator', 'ws': 'Wine Spectator',
  'wine enthusiast': 'Wine Enthusiast', 'we': 'Wine Enthusiast',
  'decanter': 'Decanter', 'decanter magazine': 'Decanter',
  'jancis robinson': 'Jancis Robinson', 'jancisrobinson.com': 'Jancis Robinson',
  'guía peñín': 'Guía Peñín', 'guia penin': 'Guía Peñín', 'penin': 'Guía Peñín',
  'tim atkin': 'Tim Atkin MW', 'tim atkin mw': 'Tim Atkin MW',
  'burghound': 'Burghound', 'allen meadows': 'Burghound',
  'gambero rosso': 'Gambero Rosso',
  'jeb dunnuck': 'Jeb Dunnuck', 'jd': 'Jeb Dunnuck',
  'jasper morris': 'Jasper Morris MW', 'jasper morris mw': 'Jasper Morris MW',
};

// ── Classification Aliases ──────────────────────────────────
const CLASSIFICATION_SYSTEM_ALIASES = {
  "langton's classification": "langton's classification of australian wine",
  "langtons classification": "langton's classification of australian wine",
  "langtons": "langton's classification of australian wine",
  "langton's": "langton's classification of australian wine",
  "bordeaux 1855 sauternes classification": "bordeaux 1855 classification (sauternes)",
  "1855 sauternes": "bordeaux 1855 classification (sauternes)",
  "sauternes classification": "bordeaux 1855 classification (sauternes)",
  "bordeaux 1855 classification": "bordeaux 1855 classification (médoc)",
  "1855 medoc": "bordeaux 1855 classification (médoc)",
  "1855 classification": "bordeaux 1855 classification (médoc)",
  "burgundy classification": "burgundy vineyard classification",
  "burgundy vineyard": "burgundy vineyard classification",
  "champagne classification": "champagne cru classification",
  "champagne cru": "champagne cru classification",
  "saint-emilion classification": "saint-émilion classification",
  "st-emilion classification": "saint-émilion classification",
  "saint emilion": "saint-émilion classification",
  "vdp": "vdp classification",
  "cru bourgeois": "cru bourgeois du médoc",
  "otw erste lagen": "ötw erste lagen",
  "otw": "ötw erste lagen",
};

// ── Identity Confidence Upgrade Path ────────────────────────
const CONFIDENCE_RANK = {
  'unverified': 0,
  'lwin_matched': 1,
  'cola_matched': 2,
  'upc_matched': 3,
  'manual_verified': 4,
};

// ══════════════════════════════════════════════════════════════
// MergeEngine
// ══════════════════════════════════════════════════════════════

export class MergeEngine {
  constructor(options = {}) {
    /** @type {import('@supabase/supabase-js').SupabaseClient} */
    this.sb = options.supabaseClient || sb;
    this.verbose = options.verbose ?? true;
    this._initialized = false;

    // Reference data maps (populated by init)
    this.countries = new Map();       // name lowercase | iso_code -> id
    this.regions = new Map();         // name lowercase | "name|country_id" -> { id, name, country_id, ... }
    this.appellations = new Map();    // name lowercase | normalized -> { id, name, country_id, region_id }
    this.grapes = new Map();          // display_name lowercase -> { id, name, display_name, color }
    this.grapesByName = new Map();    // VIVC name lowercase -> { id, ... }
    this.grapeSynonyms = new Map();   // synonym lowercase -> grape_id
    this.publications = new Map();    // name lowercase | slug -> id
    this.sourceTypes = new Map();     // slug -> id
    this.classificationLevels = new Map(); // "system|level" lowercase -> { levelId, systemName, levelName }
    this.classificationSystems = new Map(); // slug -> { id, name }
  }

  // ── Initialization ──────────────────────────────────────────

  /**
   * Load all reference data into memory. Must be called before any matching.
   * @returns {Promise<void>}
   */
  async init() {
    if (this._initialized) return;
    const t0 = Date.now();
    if (this.verbose) console.log('MergeEngine: loading reference data...');

    await Promise.all([
      this._loadCountries(),
      this._loadRegions(),
      this._loadAppellations(),
      this._loadGrapes(),
      this._loadPublications(),
      this._loadSourceTypes(),
      this._loadClassifications(),
    ]);

    this._initialized = true;
    if (this.verbose) console.log(`MergeEngine: ready (${Date.now() - t0}ms)\n`);
  }

  async _loadCountries() {
    const rows = await fetchAll('countries', 'id,name,iso_code');
    for (const c of rows) {
      this.countries.set(c.name.toLowerCase(), c.id);
      if (c.iso_code) this.countries.set(c.iso_code.toLowerCase(), c.id);
    }
    // Common aliases
    const us = this.countries.get('united states');
    if (us) { this.countries.set('usa', us); this.countries.set('us', us); }
    const uk = this.countries.get('united kingdom');
    if (uk) { this.countries.set('uk', uk); this.countries.set('england', uk); }
    if (this.verbose) console.log(`  Countries: ${rows.length}`);
  }

  async _loadRegions() {
    const [regions, regionAliases] = await Promise.all([
      fetchAll('regions', 'id,name,country_id,parent_id,is_catch_all'),
      fetchAll('region_aliases', 'id,name,region_id'),
    ]);
    for (const r of regions) {
      const lower = r.name.toLowerCase();
      const norm = normalize(r.name);
      this.regions.set(lower, r);
      this.regions.set(`${lower}|${r.country_id}`, r);
      if (norm !== lower) {
        this.regions.set(norm, r);
        this.regions.set(`${norm}|${r.country_id}`, r);
      }
    }
    // DB aliases
    for (const ra of regionAliases) {
      const region = regions.find(r => r.id === ra.region_id);
      if (region) {
        const norm = normalize(ra.name);
        const lower = ra.name.toLowerCase();
        this.regions.set(`${norm}|${region.country_id}`, region);
        this.regions.set(norm, region);
        if (lower !== norm) {
          this.regions.set(`${lower}|${region.country_id}`, region);
          this.regions.set(lower, region);
        }
      }
    }
    if (this.verbose) console.log(`  Regions: ${regions.length} (+${regionAliases.length} aliases)`);
  }

  async _loadAppellations() {
    const [appellations, aliases] = await Promise.all([
      fetchAll('appellations', 'id,name,designation_type,country_id,region_id'),
      fetchAll('appellation_aliases', 'appellation_id,alias_normalized'),
    ]);
    for (const a of appellations) {
      const lower = a.name.toLowerCase();
      const norm = normalize(a.name);
      this.appellations.set(lower, a);
      if (!this.appellations.has(norm)) this.appellations.set(norm, a);
    }
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
    if (this.verbose) console.log(`  Appellations: ${appellations.length} (+${aliasCount} alias keys)`);
  }

  async _loadGrapes() {
    const [grapes, synonyms] = await Promise.all([
      fetchAll('grapes', 'id,name,display_name,color'),
      fetchAll('grape_synonyms', 'grape_id,synonym'),
    ]);
    for (const g of grapes) {
      if (g.display_name) this.grapes.set(g.display_name.toLowerCase(), g);
      this.grapesByName.set(g.name.toLowerCase(), g);
    }
    for (const s of synonyms) {
      this.grapeSynonyms.set(s.synonym.toLowerCase(), s.grape_id);
    }
    if (this.verbose) console.log(`  Grapes: ${grapes.length} (+${synonyms.length} synonyms)`);
  }

  async _loadPublications() {
    const pubs = await fetchAll('publications', 'id,name,slug');
    for (const p of pubs) {
      this.publications.set(p.name.toLowerCase(), p.id);
      this.publications.set(p.slug, p.id);
    }
    for (const [alias, canonical] of Object.entries(PUB_ALIASES)) {
      const id = this.publications.get(canonical.toLowerCase());
      if (id) this.publications.set(alias.toLowerCase(), id);
    }
    if (this.verbose) console.log(`  Publications: ${pubs.length}`);
  }

  async _loadSourceTypes() {
    const sts = await fetchAll('source_types', 'id,slug');
    for (const s of sts) this.sourceTypes.set(s.slug, s.id);
    if (this.verbose) console.log(`  Source types: ${sts.length}`);
  }

  async _loadClassifications() {
    const [levels, systems] = await Promise.all([
      fetchAll('classification_levels', 'id,classification_id,level_name,level_rank'),
      fetchAll('classifications', 'id,name,slug,country_id'),
    ]);
    const sysById = new Map(systems.map(s => [s.id, s]));
    for (const s of systems) {
      this.classificationSystems.set(s.slug, s);
      this.classificationSystems.set(s.name.toLowerCase(), s);
    }
    for (const cl of levels) {
      const sys = sysById.get(cl.classification_id);
      if (!sys) continue;
      const entry = { levelId: cl.id, classificationId: sys.id, systemName: sys.name, levelName: cl.level_name, rank: cl.level_rank };
      const key = `${sys.name.toLowerCase()}|${cl.level_name.toLowerCase()}`;
      this.classificationLevels.set(key, entry);
      // Also register slug-based key
      this.classificationLevels.set(`${sys.slug}|${cl.level_name.toLowerCase()}`, entry);
    }
    // Register classification system aliases
    for (const [alias, canonical] of Object.entries(CLASSIFICATION_SYSTEM_ALIASES)) {
      for (const cl of levels) {
        const sys = sysById.get(cl.classification_id);
        if (!sys || sys.name.toLowerCase() !== canonical) continue;
        const entry = this.classificationLevels.get(`${canonical}|${cl.level_name.toLowerCase()}`);
        if (entry) this.classificationLevels.set(`${alias}|${cl.level_name.toLowerCase()}`, entry);
      }
    }
    if (this.verbose) console.log(`  Classifications: ${systems.length} systems, ${levels.length} levels`);
  }

  /**
   * Return all loaded reference data maps (for callers that need raw access).
   * @returns {Object}
   */
  getReferenceData() {
    return {
      countries: this.countries,
      regions: this.regions,
      appellations: this.appellations,
      grapes: this.grapes,
      grapesByName: this.grapesByName,
      grapeSynonyms: this.grapeSynonyms,
      publications: this.publications,
      sourceTypes: this.sourceTypes,
      classificationLevels: this.classificationLevels,
      classificationSystems: this.classificationSystems,
    };
  }

  // ══════════════════════════════════════════════════════════════
  // Resolution Helpers
  // ══════════════════════════════════════════════════════════════

  /**
   * Resolve a country name or ISO code to a country UUID.
   * @param {string} name
   * @returns {string|null} country UUID
   */
  resolveCountry(name) {
    if (!name) return null;
    return this.countries.get(name.toLowerCase().trim()) || null;
  }

  /**
   * Resolve a region name to a region object { id, name, country_id, ... }.
   * Checks in-code aliases, DB aliases, and normalized forms.
   * @param {string} name
   * @param {string} [countryId] - Disambiguate when the same name exists in multiple countries
   * @returns {{ id: string, name: string, country_id: string }|null}
   */
  resolveRegion(name, countryId) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    const aliased = REGION_ALIASES[lower];
    const candidates = aliased ? [lower, aliased.toLowerCase()] : [lower];
    for (const c of candidates) {
      if (countryId) {
        const r = this.regions.get(`${c}|${countryId}`);
        if (r) return r;
      }
      const r = this.regions.get(c);
      if (r) return r;
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

  /**
   * Resolve an appellation name to an appellation object { id, name, country_id, region_id }.
   * Checks direct name, normalized form, and alias table.
   * @param {string} name
   * @param {string} [countryId] - Not currently used for filtering but reserved
   * @returns {{ id: string, name: string, country_id: string, region_id: string }|null}
   */
  resolveAppellation(name, countryId) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    const a = this.appellations.get(lower);
    if (a) return a;
    const norm = normalize(lower);
    return this.appellations.get(norm) || null;
  }

  /**
   * Resolve a grape name to a grape object { id, name?, display_name?, color? }.
   * Checks in-code aliases, display_name, VIVC name, synonyms table, and common suffixes.
   * @param {string} name
   * @returns {{ id: string }|null}
   */
  resolveGrape(name) {
    if (!name) return null;
    const lower = name.toLowerCase().trim();
    const norm = normalize(lower);

    // 1. In-code alias
    const aliased = GRAPE_ALIASES[lower] || GRAPE_ALIASES[norm];
    if (aliased) {
      const g = this.grapes.get(aliased.toLowerCase()) || this.grapesByName.get(aliased.toLowerCase());
      if (g) return g;
    }
    // 2. Display name
    const byDisplay = this.grapes.get(lower) || this.grapes.get(norm);
    if (byDisplay) return byDisplay;
    // 3. VIVC name
    const byVivc = this.grapesByName.get(lower) || this.grapesByName.get(norm);
    if (byVivc) return byVivc;
    // 4. Synonyms table
    const synId = this.grapeSynonyms.get(lower) || this.grapeSynonyms.get(norm);
    if (synId) return { id: synId };
    // 5. Common suffixes
    for (const suffix of [' noir', ' blanc', ' tinto', ' tinta', ' blanco']) {
      const withSuffix = this.grapes.get(lower + suffix) || this.grapes.get(norm + suffix);
      if (withSuffix) return withSuffix;
    }
    return null;
  }

  /**
   * Resolve a publication name to a publication UUID.
   * @param {string} name
   * @returns {string|null}
   */
  resolvePublication(name) {
    if (!name) return null;
    return this.publications.get(name.toLowerCase().trim()) || null;
  }

  /**
   * Resolve a classification system + level to { levelId, classificationId, systemName, levelName }.
   * @param {string} system - System name, slug, or alias
   * @param {string} level - Level name
   * @returns {{ levelId: string, classificationId: string, systemName: string, levelName: string }|null}
   */
  resolveClassification(system, level) {
    if (!system || !level) return null;
    const sysLower = system.toLowerCase().trim();
    const lvlLower = level.toLowerCase().trim();
    // Try direct
    let cl = this.classificationLevels.get(`${sysLower}|${lvlLower}`);
    if (cl) return cl;
    // Try normalized
    const sysNorm = normalize(sysLower);
    cl = this.classificationLevels.get(`${sysNorm}|${lvlLower}`);
    if (cl) return cl;
    // Try alias
    const aliased = CLASSIFICATION_SYSTEM_ALIASES[sysNorm] || CLASSIFICATION_SYSTEM_ALIASES[sysLower];
    if (aliased) {
      cl = this.classificationLevels.get(`${aliased}|${lvlLower}`);
      if (cl) return cl;
    }
    return null;
  }

  /**
   * Resolve a source type slug to its UUID.
   * @param {string} slug
   * @returns {string|null}
   */
  resolveSourceType(slug) {
    if (!slug) return null;
    return this.sourceTypes.get(slug) || null;
  }

  // ══════════════════════════════════════════════════════════════
  // Producer Matching
  // ══════════════════════════════════════════════════════════════

  /**
   * Match a producer by name with three-tier matching.
   *
   * - Tier 1: Exact normalized match on producers.name_normalized
   * - Tier 2: Alias match via producer_aliases table
   * - Tier 3: Fuzzy match via pg_trgm similarity (> threshold, same country)
   *
   * @param {string} name - Producer name from source
   * @param {string} [countryId] - Country UUID for disambiguation
   * @param {Object} [options]
   * @param {boolean} [options.fuzzy=true] - Enable tier 3 fuzzy matching
   * @param {number} [options.fuzzyThreshold=0.4] - pg_trgm similarity threshold
   * @returns {Promise<{ id: string, name: string, confidence: number, match_tier: number }|null>}
   */
  async matchProducer(name, countryId, options = {}) {
    if (!name) return null;
    const norm = normalize(name);
    const fuzzy = options.fuzzy ?? true;
    const threshold = options.fuzzyThreshold ?? 0.4;

    // Tier 1: Exact normalized match
    let query = this.sb.from('producers').select('id,name,name_normalized,country_id')
      .eq('name_normalized', norm).is('deleted_at', null).limit(5);
    const { data: exactMatches, error: err1 } = await query;
    if (err1) throw new Error(`matchProducer tier1: ${err1.message}`);

    if (exactMatches && exactMatches.length > 0) {
      // Prefer same-country match
      const sameCountry = countryId ? exactMatches.find(p => p.country_id === countryId) : null;
      const best = sameCountry || exactMatches[0];
      return { id: best.id, name: best.name, confidence: 1.0, match_tier: 1 };
    }

    // Tier 2: Alias match
    const { data: aliasMatches, error: err2 } = await this.sb
      .from('producer_aliases').select('producer_id,name')
      .eq('name_normalized', norm).limit(5);
    if (err2) throw new Error(`matchProducer tier2: ${err2.message}`);

    if (aliasMatches && aliasMatches.length > 0) {
      const producerId = aliasMatches[0].producer_id;
      const { data: producer } = await this.sb.from('producers')
        .select('id,name,country_id').eq('id', producerId).is('deleted_at', null).single();
      if (producer) {
        return { id: producer.id, name: producer.name, confidence: 0.9, match_tier: 2 };
      }
    }

    // Tier 3: Fuzzy match via pg_trgm
    if (fuzzy) {
      let sql = `
        SELECT id, name, country_id, similarity(name_normalized, '${norm.replace(/'/g, "''")}') AS sim
        FROM producers
        WHERE deleted_at IS NULL
          AND similarity(name_normalized, '${norm.replace(/'/g, "''")}') > ${threshold}
      `;
      if (countryId) {
        sql += ` AND country_id = '${countryId}'`;
      }
      sql += ` ORDER BY sim DESC LIMIT 1`;

      const { data: fuzzyMatches, error: err3 } = await this.sb.rpc('exec_sql', { query: sql }).maybeSingle();
      // Fallback: if the RPC doesn't exist, query directly with ilike as rough approximation
      if (err3) {
        // pg_trgm via direct Supabase is not natively supported in PostgREST filters,
        // so we fall back to a textSearch or skip fuzzy
        if (this.verbose && err3.message && !err3.message.includes('exec_sql')) {
          console.warn(`  MergeEngine: fuzzy match unavailable (${err3.message})`);
        }
        return null;
      }
      if (fuzzyMatches) {
        return {
          id: fuzzyMatches.id,
          name: fuzzyMatches.name,
          confidence: parseFloat(fuzzyMatches.sim) || 0.5,
          match_tier: 3,
        };
      }
    }

    return null;
  }

  // ══════════════════════════════════════════════════════════════
  // Wine Matching
  // ══════════════════════════════════════════════════════════════

  /**
   * Match a wine with three-tier matching.
   *
   * - Tier 1: Key match (LWIN, barcode, or external ID)
   * - Tier 2: Exact normalized name match within the same producer
   * - Tier 3: Fuzzy name match within the same producer
   *
   * @param {string} producerId - UUID of the matched producer
   * @param {string} wineName - Wine name from source
   * @param {Object} [options]
   * @param {string} [options.lwin] - LWIN-7 code
   * @param {string} [options.barcode] - GTIN/EAN barcode
   * @param {string} [options.externalId] - External ID (e.g., COLA ID)
   * @param {string} [options.externalSystem] - System name for external ID lookup (e.g., 'cola_cloud')
   * @param {boolean} [options.fuzzy=true] - Enable tier 3 fuzzy matching
   * @param {number} [options.fuzzyThreshold=0.4] - pg_trgm similarity threshold
   * @returns {Promise<{ id: string, name: string, lwin: string|null, confidence: number, match_tier: number }|null>}
   */
  async matchWine(producerId, wineName, options = {}) {
    if (!producerId) return null;
    const fuzzy = options.fuzzy ?? true;
    const threshold = options.fuzzyThreshold ?? 0.4;

    // Tier 1: Key match — LWIN
    if (options.lwin) {
      const { data } = await this.sb.from('wines')
        .select('id,name,lwin').eq('lwin', options.lwin).is('deleted_at', null).limit(1);
      if (data && data.length > 0) {
        return { id: data[0].id, name: data[0].name, lwin: data[0].lwin, confidence: 1.0, match_tier: 1 };
      }
    }

    // Tier 1: Key match — barcode
    if (options.barcode) {
      const { data } = await this.sb.from('wines')
        .select('id,name,lwin').eq('barcode', options.barcode).is('deleted_at', null).limit(1);
      if (data && data.length > 0) {
        return { id: data[0].id, name: data[0].name, lwin: data[0].lwin, confidence: 1.0, match_tier: 1 };
      }
    }

    // Tier 1: Key match — external ID
    if (options.externalId && options.externalSystem) {
      const { data } = await this.sb.from('external_ids')
        .select('entity_id')
        .eq('entity_type', 'wine')
        .eq('system', options.externalSystem)
        .eq('external_id', options.externalId)
        .limit(1);
      if (data && data.length > 0) {
        const wineId = data[0].entity_id;
        const { data: wine } = await this.sb.from('wines')
          .select('id,name,lwin').eq('id', wineId).is('deleted_at', null).single();
        if (wine) {
          return { id: wine.id, name: wine.name, lwin: wine.lwin, confidence: 1.0, match_tier: 1 };
        }
      }
    }

    // Tier 2: Exact normalized name match within producer
    if (wineName) {
      const norm = normalize(wineName);
      const { data } = await this.sb.from('wines')
        .select('id,name,lwin,name_normalized')
        .eq('producer_id', producerId)
        .eq('name_normalized', norm)
        .is('deleted_at', null)
        .limit(5);
      if (data && data.length > 0) {
        return { id: data[0].id, name: data[0].name, lwin: data[0].lwin, confidence: 0.95, match_tier: 2 };
      }
    }

    // Tier 3: Fuzzy name match within producer
    if (fuzzy && wineName) {
      const norm = normalize(wineName);
      // Use the wines table with pg_trgm — requires raw SQL via RPC
      const sql = `
        SELECT id, name, lwin, similarity(name_normalized, '${norm.replace(/'/g, "''")}') AS sim
        FROM wines
        WHERE deleted_at IS NULL
          AND producer_id = '${producerId}'
          AND similarity(name_normalized, '${norm.replace(/'/g, "''")}') > ${threshold}
        ORDER BY sim DESC
        LIMIT 1
      `;
      const { data, error } = await this.sb.rpc('exec_sql', { query: sql }).maybeSingle();
      if (!error && data) {
        return {
          id: data.id,
          name: data.name,
          lwin: data.lwin,
          confidence: parseFloat(data.sim) || 0.5,
          match_tier: 3,
        };
      }
    }

    return null;
  }

  // ══════════════════════════════════════════════════════════════
  // Additive Field Merge
  // ══════════════════════════════════════════════════════════════

  /**
   * Merge fields into an existing wine record additively.
   * Fills nulls but never overwrites existing data. Logs conflicts.
   *
   * @param {string} wineId - UUID of the wine to update
   * @param {Object} fields - Map of column_name -> new_value
   * @param {string} source - Source identifier for logging (e.g., 'cola_cloud', 'skurnik')
   * @returns {Promise<{ updated: string[], conflicts: Array<{ field: string, existing: any, incoming: any }>, skipped: string[] }>}
   */
  async mergeWineFields(wineId, fields, source) {
    const { data: wine, error } = await this.sb.from('wines')
      .select('*').eq('id', wineId).single();
    if (error) throw new Error(`mergeWineFields: ${error.message}`);
    if (!wine) throw new Error(`mergeWineFields: wine ${wineId} not found`);

    const updates = {};
    const updated = [];
    const conflicts = [];
    const skipped = [];

    // Protected fields: only set if null, never overwrite
    const protectedFields = new Set(['lwin', 'barcode', 'identity_confidence']);

    for (const [field, newValue] of Object.entries(fields)) {
      if (newValue === null || newValue === undefined || newValue === '') {
        skipped.push(field);
        continue;
      }

      // Skip non-column fields
      if (field === 'data_grade') continue; // Recalculated separately
      if (!(field in wine)) {
        skipped.push(field);
        continue;
      }

      const existing = wine[field];

      // identity_confidence: only upgrade, never downgrade
      if (field === 'identity_confidence') {
        const existingRank = CONFIDENCE_RANK[existing] ?? -1;
        const newRank = CONFIDENCE_RANK[newValue] ?? -1;
        if (newRank > existingRank) {
          updates[field] = newValue;
          updated.push(field);
        } else {
          skipped.push(field);
        }
        continue;
      }

      // Protected fields: only set if null
      if (protectedFields.has(field)) {
        if (existing === null || existing === undefined) {
          updates[field] = newValue;
          updated.push(field);
        } else if (String(existing) !== String(newValue)) {
          conflicts.push({ field, existing, incoming: newValue, source });
        } else {
          skipped.push(field);
        }
        continue;
      }

      // Standard fields: fill null, log conflict on difference
      if (existing === null || existing === undefined || existing === '') {
        updates[field] = newValue;
        updated.push(field);
      } else if (String(existing) !== String(newValue)) {
        conflicts.push({ field, existing, incoming: newValue, source });
      } else {
        skipped.push(field);
      }
    }

    // Write updates
    if (Object.keys(updates).length > 0) {
      const { error: updateErr } = await this.sb.from('wines')
        .update(updates).eq('id', wineId);
      if (updateErr) throw new Error(`mergeWineFields update: ${updateErr.message}`);
    }

    // Recalculate grade after merge
    await this.calculateGrade(wineId);

    if (this.verbose && conflicts.length > 0) {
      for (const c of conflicts) {
        console.warn(`  Conflict on wine ${wineId} field '${c.field}': existing='${c.existing}' vs incoming='${c.incoming}' from ${source}`);
      }
    }

    return { updated, conflicts, skipped };
  }

  /**
   * Merge fields into an existing producer record additively.
   * Same fill-nulls-only logic as mergeWineFields.
   *
   * @param {string} producerId
   * @param {Object} fields
   * @param {string} source
   * @returns {Promise<{ updated: string[], conflicts: Array<{ field: string, existing: any, incoming: any }>, skipped: string[] }>}
   */
  async mergeProducerFields(producerId, fields, source) {
    const { data: producer, error } = await this.sb.from('producers')
      .select('*').eq('id', producerId).single();
    if (error) throw new Error(`mergeProducerFields: ${error.message}`);
    if (!producer) throw new Error(`mergeProducerFields: producer ${producerId} not found`);

    const updates = {};
    const updated = [];
    const conflicts = [];
    const skipped = [];

    for (const [field, newValue] of Object.entries(fields)) {
      if (newValue === null || newValue === undefined || newValue === '') {
        skipped.push(field);
        continue;
      }
      if (!(field in producer)) { skipped.push(field); continue; }

      const existing = producer[field];
      if (existing === null || existing === undefined || existing === '') {
        updates[field] = newValue;
        updated.push(field);
      } else if (String(existing) !== String(newValue)) {
        conflicts.push({ field, existing, incoming: newValue, source });
      } else {
        skipped.push(field);
      }
    }

    if (Object.keys(updates).length > 0) {
      const { error: updateErr } = await this.sb.from('producers')
        .update(updates).eq('id', producerId);
      if (updateErr) throw new Error(`mergeProducerFields update: ${updateErr.message}`);
    }

    return { updated, conflicts, skipped };
  }

  // ══════════════════════════════════════════════════════════════
  // Create Wine
  // ══════════════════════════════════════════════════════════════

  /**
   * Create a new wine record.
   *
   * @param {Object} data
   * @param {string} data.name - Wine name (required)
   * @param {string} data.producerId - Producer UUID (required)
   * @param {string} [data.countryId]
   * @param {string} [data.regionId]
   * @param {string} [data.appellationId]
   * @param {string} [data.color]
   * @param {string} [data.wineType] - table, sparkling, fortified, dessert
   * @param {string} [data.effervescence] - still, sparkling, petillant
   * @param {boolean} [data.isNv]
   * @param {string} [data.lwin]
   * @param {string} [data.barcode]
   * @param {number} [data.firstVintageYear]
   * @param {string} [data.sweetnessLevel]
   * @param {string} [data.sparklingMethod]
   * @param {string} [data.vinificationNotes]
   * @param {string} [data.soilDescription]
   * @param {string} [data.style]
   * @param {string} [data.description]
   * @param {Object} [data.metadata]
   * @param {string} source - Source identifier
   * @returns {Promise<string>} New wine UUID
   */
  async createWine(data, source) {
    if (!data.name) throw new Error('createWine: name is required');
    if (!data.producerId) throw new Error('createWine: producerId is required');

    const id = randomUUID();
    const slug = slugify(`${data.producerName || data.name}-${data.name}-${id.slice(0, 8)}`);
    const nameNormalized = normalize(data.name);

    // Determine initial identity confidence
    let identityConfidence = 'unverified';
    if (data.lwin) identityConfidence = 'lwin_matched';
    else if (data.barcode) identityConfidence = 'upc_matched';

    // Determine initial data grade based on available data
    const grade = this._computeInitialGrade(data);

    const wine = {
      id,
      slug,
      name: data.name,
      name_normalized: nameNormalized,
      producer_id: data.producerId,
      country_id: data.countryId || null,
      region_id: data.regionId || null,
      appellation_id: data.appellationId || null,
      color: data.color || null,
      wine_type: data.wineType || 'table',
      effervescence: data.effervescence || 'still',
      is_nv: data.isNv || false,
      lwin: data.lwin || null,
      barcode: data.barcode || null,
      first_vintage_year: data.firstVintageYear || null,
      sweetness_level: data.sweetnessLevel || null,
      sparkling_method: data.sparklingMethod || null,
      vinification_notes: data.vinificationNotes || null,
      soil_description: data.soilDescription || null,
      style: data.style || null,
      description: data.description || null,
      metadata: data.metadata || null,
      data_grade: grade,
      identity_confidence: identityConfidence,
      lookup_count: 0,
    };

    const { error } = await this.sb.from('wines').insert(wine);
    if (error) throw new Error(`createWine: ${error.message}`);
    return id;
  }

  /**
   * Compute the initial data grade for a new wine based on what data is provided.
   * @param {Object} data - Wine data fields
   * @returns {string} 'F' or 'D'
   */
  _computeInitialGrade(data) {
    // D requires at least one of: scores, prices, grapes, or ABV (checked at vintage level)
    // At creation time we can only check wine-level data. Caller should call calculateGrade
    // after adding vintages/scores/grapes if they want an accurate grade.
    return 'F';
  }

  // ══════════════════════════════════════════════════════════════
  // Create Producer
  // ══════════════════════════════════════════════════════════════

  /**
   * Create a new producer record.
   *
   * @param {Object} data
   * @param {string} data.name - Producer name (required)
   * @param {string} [data.countryId]
   * @param {string} [data.regionId]
   * @param {string} [data.producerType] - estate, negociant, cooperative, virtual, corporate
   * @param {string} [data.websiteUrl]
   * @param {string} [data.address]
   * @param {string} [data.philosophy]
   * @param {number} [data.latitude]
   * @param {number} [data.longitude]
   * @param {number} [data.hectaresUnderVine]
   * @param {number} [data.totalProductionCases]
   * @param {string} [data.parentProducerId]
   * @param {Object} [data.metadata]
   * @param {string} source - Source identifier
   * @returns {Promise<string>} New producer UUID
   */
  async createProducer(data, source) {
    if (!data.name) throw new Error('createProducer: name is required');

    const id = randomUUID();
    const slug = slugify(data.name);
    const nameNormalized = normalize(data.name);

    const producer = {
      id,
      slug,
      name: data.name,
      name_normalized: nameNormalized,
      country_id: data.countryId || null,
      region_id: data.regionId || null,
      producer_type: data.producerType || 'estate',
      website_url: data.websiteUrl || null,
      address: data.address || null,
      philosophy: data.philosophy || null,
      latitude: data.latitude || null,
      longitude: data.longitude || null,
      hectares_under_vine: data.hectaresUnderVine || null,
      total_production_cases: data.totalProductionCases || null,
      parent_producer_id: data.parentProducerId || null,
      metadata: data.metadata || null,
    };

    const { error } = await this.sb.from('producers').insert(producer);
    if (error) {
      // Handle slug collision by appending a short UUID suffix
      if (error.message.includes('duplicate') && error.message.includes('slug')) {
        producer.slug = `${slug}-${id.slice(0, 8)}`;
        const { error: retryErr } = await this.sb.from('producers').insert(producer);
        if (retryErr) throw new Error(`createProducer retry: ${retryErr.message}`);
      } else {
        throw new Error(`createProducer: ${error.message}`);
      }
    }
    return id;
  }

  // ══════════════════════════════════════════════════════════════
  // External ID Management
  // ══════════════════════════════════════════════════════════════

  /**
   * Store an external identifier for an entity (wine, producer, wine_vintage).
   * Upserts based on the unique constraint (entity_type, entity_id, system).
   *
   * @param {string} entityType - 'wine', 'producer', or 'wine_vintage'
   * @param {string} entityId - UUID of the entity
   * @param {string} system - Identifier system (e.g., 'lwin', 'cola_cloud', 'kansas', 'upc')
   * @param {string} externalId - The external identifier value
   * @param {Object} [options]
   * @param {string} [options.sourceSlug] - Source type slug for source_id FK
   * @param {string} [options.notes] - Optional notes
   * @returns {Promise<void>}
   */
  async storeExternalId(entityType, entityId, system, externalId, options = {}) {
    if (!entityType || !entityId || !system || !externalId) return;

    const row = {
      id: randomUUID(),
      entity_type: entityType,
      entity_id: entityId,
      system,
      external_id: externalId,
      notes: options.notes || null,
    };

    // Resolve source_id if slug provided
    if (options.sourceSlug) {
      row.source_id = this.sourceTypes.get(options.sourceSlug) || null;
    }

    const { error } = await this.sb.from('external_ids')
      .upsert(row, { onConflict: 'entity_type,entity_id,system' });
    if (error) throw new Error(`storeExternalId: ${error.message}`);
  }

  // ══════════════════════════════════════════════════════════════
  // Data Grade Calculator
  // ══════════════════════════════════════════════════════════════

  /**
   * Calculate the data grade for a wine based on what data exists.
   *
   * - F: Has name + producer + country. No scores, grapes, or winemaking.
   * - D: Has at least one of: scores, prices, grapes, or ABV from any vintage.
   * - C: Has wine_insights.ai_hook (batch Haiku enrichment).
   * - B: Has full Sonnet enrichment (ai_hook + ai_vinification_summary).
   * - A: Manually curated (is_verified = true).
   *
   * Updates wines.data_grade in place.
   *
   * @param {string} wineId
   * @returns {Promise<string>} The calculated grade (F/D/C/B/A)
   */
  async calculateGrade(wineId) {
    // Parallel queries to check each grade criterion
    const [scoresRes, pricesRes, grapesRes, vintagesRes, insightsRes] = await Promise.all([
      this.sb.from('wine_vintage_scores').select('id', { count: 'exact', head: true }).eq('wine_id', wineId),
      this.sb.from('wine_vintage_prices').select('id', { count: 'exact', head: true }).eq('wine_id', wineId),
      this.sb.from('wine_grapes').select('id', { count: 'exact', head: true }).eq('wine_id', wineId),
      this.sb.from('wine_vintages').select('id,abv', { count: 'exact', head: false }).eq('wine_id', wineId).limit(1),
      this.sb.from('wine_insights').select('ai_hook,ai_vinification_summary,is_verified').eq('wine_id', wineId).maybeSingle(),
    ]);

    let grade = 'F';

    // Check D: scores, prices, grapes, or ABV
    const hasScores = (scoresRes.count || 0) > 0;
    const hasPrices = (pricesRes.count || 0) > 0;
    const hasGrapes = (grapesRes.count || 0) > 0;
    const hasAbv = vintagesRes.data && vintagesRes.data.some(v => v.abv != null);
    if (hasScores || hasPrices || hasGrapes || hasAbv) {
      grade = 'D';
    }

    // Check C/B/A from wine_insights
    const insights = insightsRes.data;
    if (insights) {
      if (insights.is_verified) {
        grade = 'A';
      } else if (insights.ai_hook && insights.ai_vinification_summary) {
        grade = 'B';
      } else if (insights.ai_hook) {
        grade = 'C';
      }
    }

    // Update if changed
    const { data: current } = await this.sb.from('wines').select('data_grade').eq('id', wineId).single();
    if (current && current.data_grade !== grade) {
      await this.sb.from('wines').update({ data_grade: grade }).eq('id', wineId);
    }

    return grade;
  }

  // ══════════════════════════════════════════════════════════════
  // Batch Helpers
  // ══════════════════════════════════════════════════════════════

  /**
   * Match or create a producer. Convenience wrapper that tries matching first,
   * then creates if no match found.
   *
   * @param {string} name
   * @param {string} [countryId]
   * @param {Object} [createData] - Additional fields for createProducer if needed
   * @param {string} source
   * @returns {Promise<{ id: string, name: string, created: boolean, match_tier: number|null }>}
   */
  async matchOrCreateProducer(name, countryId, createData = {}, source = 'unknown') {
    const match = await this.matchProducer(name, countryId);
    if (match) {
      return { id: match.id, name: match.name, created: false, match_tier: match.match_tier };
    }

    const id = await this.createProducer({
      name,
      countryId,
      ...createData,
    }, source);

    return { id, name, created: true, match_tier: null };
  }

  /**
   * Match or create a wine. Convenience wrapper that tries matching first,
   * then creates if no match found.
   *
   * @param {string} producerId
   * @param {string} wineName
   * @param {Object} [matchOptions] - Options for matchWine (lwin, barcode, etc.)
   * @param {Object} [createData] - Additional fields for createWine if needed
   * @param {string} source
   * @returns {Promise<{ id: string, name: string, created: boolean, match_tier: number|null }>}
   */
  async matchOrCreateWine(producerId, wineName, matchOptions = {}, createData = {}, source = 'unknown') {
    const match = await this.matchWine(producerId, wineName, matchOptions);
    if (match) {
      return { id: match.id, name: match.name, created: false, match_tier: match.match_tier };
    }

    const id = await this.createWine({
      name: wineName,
      producerId,
      ...createData,
    }, source);

    return { id, name: wineName, created: true, match_tier: null };
  }

  /**
   * Get the Supabase client (for callers that need direct access).
   * @returns {import('@supabase/supabase-js').SupabaseClient}
   */
  getClient() {
    return this.sb;
  }
}

// ── Standalone Reference Data Loader ────────────────────────

/**
 * Load all reference data and return the maps.
 * Convenience function for scripts that need reference data but not the full engine.
 *
 * @returns {Promise<Object>} All reference data maps
 */
export async function loadReferenceData() {
  const engine = new MergeEngine({ verbose: false });
  await engine.init();
  return engine.getReferenceData();
}

// ── Named Exports ───────────────────────────────────────────

export { fetchAll, GRAPE_ALIASES, REGION_ALIASES, PUB_ALIASES, CLASSIFICATION_SYSTEM_ALIASES, CONFIDENCE_RANK };
