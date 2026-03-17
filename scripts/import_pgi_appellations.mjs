/**
 * import_pgi_appellations.mjs
 *
 * Imports ~454 PGI/IGP/IGT wine appellations from eAmbrosia API data
 * plus ~5 base-tier table wine designations (Vin de France, Vino d'Italia, etc.)
 *
 * Source: eAmbrosia EU Geographical Indications Register (official EU data)
 * Data file: data/eambrosia_pgi_wines.json
 *
 * Usage:
 *   node scripts/import_pgi_appellations.mjs --analyze     # show what would be imported
 *   node scripts/import_pgi_appellations.mjs --dry-run     # full run without DB writes
 *   node scripts/import_pgi_appellations.mjs --import      # actually import
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

// .env loading
const envContent = readFileSync('.env', 'utf8');
const getEnv = (key) => envContent.match(new RegExp(`${key}=(.+)`))?.[1]?.trim();
const supabase = createClient(getEnv('SUPABASE_URL'), getEnv('SUPABASE_SERVICE_ROLE'));

const MODE = process.argv.includes('--import') ? 'import'
  : process.argv.includes('--dry-run') ? 'dry-run'
  : 'analyze';

// ─── Country code → designation type mapping ────────────────────────────────
const DESIGNATION_TYPES = {
  IT: 'IGT',        // Indicazione Geografica Tipica
  FR: 'IGP',        // Indication Géographique Protégée
  ES: 'VdlT',       // Vino de la Tierra
  PT: 'VR',         // Vinho Regional
  DE: 'Landwein',   // Landwein
  AT: 'Landwein',   // Landwein (Austria uses same term)
  GR: 'PGI',        // Protected Geographical Indication
  RO: 'PGI',
  HU: 'PGI',
  BG: 'PGI',
  CZ: 'PGI',
  SI: 'PGI',
  CY: 'PGI',
  NL: 'PGI',
  DK: 'PGI',
  BE: 'PGI',
  GB: 'PGI',
  MT: 'PGI',
  SK: 'PGI',
  CN: 'PGI',
  US: 'PGI',
  HR: 'PGI',
};

// ─── Greek transliteration map ──────────────────────────────────────────────
// Common Greek wine PGI names → Latin transliteration
const GREEK_TRANSLITERATIONS = {
  'Άβδηρα': 'Avdira',
  'Άγιο Όρος': 'Agio Oros',
  'Αγορά': 'Agora',
  'Αιγαίο Πέλαγος': 'Aegean Sea',
  'Ανάβυσσος': 'Anavyssos',
  'Αργολίδα': 'Argolida',
  'Αρκαδία': 'Arkadia',
  'Αττική': 'Attiki',
  'Αχαΐα': 'Achaia',
  'Γρεβενά': 'Grevena',
  'Δράμα': 'Drama',
  'Δωδεκάνησος': 'Dodecanese',
  'Έβρος': 'Evros',
  'Εύβοια': 'Evia',
  'Ζάκυνθος': 'Zakynthos',
  'Ηλεία': 'Ilia',
  'Ημαθία': 'Imathia',
  'Ηράκλειο': 'Heraklion',
  'Θάσος': 'Thasos',
  'Θεσσαλονίκη': 'Thessaloniki',
  'Ιωάννινα': 'Ioannina',
  'Καβάλα': 'Kavala',
  'Καρδίτσα': 'Karditsa',
  'Καστοριά': 'Kastoria',
  'Κέρκυρα': 'Corfu',
  'Κοζάνη': 'Kozani',
  'Κορινθία': 'Korinthia',
  'Κρήτη': 'Crete',
  'Κυκλάδες': 'Cyclades',
  'Λακωνία': 'Lakonia',
  'Λασίθι': 'Lasithi',
  'Λέσβος': 'Lesvos',
  'Λευκάδα': 'Lefkada',
  'Μαγνησία': 'Magnisia',
  'Μακεδονία': 'Macedonia',
  'Μεσσηνία': 'Messinia',
  'Πέλλα': 'Pella',
  'Πελοπόννησος': 'Peloponnese',
  'Πιερία': 'Pieria',
  'Ρέθυμνο': 'Rethymno',
  'Σέρρες': 'Serres',
  'Στερεά Ελλάδα': 'Central Greece',
  'Τριφυλία': 'Trifilia',
  'Φλώρινα': 'Florina',
  'Χαλκιδική': 'Halkidiki',
  'Χανιά': 'Chania',
  'Χίος': 'Chios',
  'Ήπειρος': 'Epirus',
  'Θεσσαλία': 'Thessalia',
  'Θράκη': 'Thrace',
  'Αδριανή': 'Adriani',
  'Αιγιαλεία': 'Aigialia',
  'Γεράνεια': 'Geraneia',
  'Ισμαρός': 'Ismaros',
  'Κλημέντι': 'Klimenti',
  'Κραννώνα': 'Krannona',
  'Κρανιά': 'Krania',
  'Κωπαΐδα': 'Kopaida',
  'Λαρισαία': 'Larisaia',
  'Μετέωρα': 'Meteora',
  'Μεταξάτα': 'Metaxata',
  'Μουζάκι': 'Mouzaki',
  'Νήσος Κως': 'Island of Kos',
  'Νήσος Ρόδος': 'Island of Rhodes',
  'Όπουντία Λοκρίδα': 'Opountia Lokrída',
  'Παγγαίο': 'Pangaio',
  'Πλαγιές Αιγιαλείας': 'Slopes of Aigialia',
  'Πλαγιές Αμπέλου': 'Slopes of Ampelos',
  'Πλαγιές Βερτίσκου': 'Slopes of Vertiskos',
  'Πλαγιές Κιθαιρώνα': 'Slopes of Kithairon',
  'Πλαγιές Κνημίδας': 'Slopes of Knimida',
  'Πλαγιές Πάρνηθας': 'Slopes of Parnitha',
  'Πλαγιές Παρνασσού': 'Slopes of Parnassos',
  'Πλαγιές Πεντελικού': 'Slopes of Penteliko',
  'Πλαγιές Πετρωτού': 'Slopes of Petroto',
  'Πλαγιές του Αίνου': 'Slopes of Ainos',
  'Πυλία': 'Pylia',
  'Σιάτιστα': 'Siatista',
  'Σπάτα': 'Spata',
  'Στεφανοβίκειο': 'Stefanovikio',
  'Τεγέα': 'Tegea',
  'Τύρναβος': 'Tyrnavos',
  'Χαλίκουνα': 'Halikouna',
  'Πλαγιές Πάικου': 'Slopes of Paiko',
  'Φωκίδα': 'Fokida',
  'Πρέβεζα': 'Preveza',
  'Ιονίων Νήσων': 'Ionian Islands',
  'Ζίτσα': 'Zitsa',
  'Κεφαλληνία': 'Kefalonia',
  'Μεσογεία Αττικής': 'Mesogia Attikis',
  'Λετρίνοι': 'Letrinoi',
  'Βελβεντό': 'Velvento',
  'Κοιλάδα Αταλάντης': 'Valley of Atalanti',
  'Παιανία': 'Paiania',
  'Μαρτίνο': 'Martino',
  'Πλαγιές Ολύμπου': 'Slopes of Olympus',
  'Αγαθονήσι': 'Agathonisi',
  'Κάρπαθος': 'Karpathos',
  'Λήμνος': 'Limnos',
  'Λέρος': 'Leros',
  'Νάξος': 'Naxos',
  'Πάρος': 'Paros',
  'Σάμος': 'Samos',
  'Αλεξανδρούπολη': 'Alexandroupoli',
  'Κρήτη — Ηρακλείου': 'Crete — Heraklion',
};

// ─── Chinese transliteration map ────────────────────────────────────────────
const CHINESE_TRANSLITERATIONS = {
  '贺兰山东麓葡萄酒': 'Helan Mountain East',
  '桓仁冰酒': 'Huanren Ice Wine',
  '烟台葡萄酒': 'Yantai Wine',
  '沙城葡萄酒': 'Shacheng Wine',
};

// ─── Italian name corrections ───────────────────────────────────────────────
// eAmbrosia uses "Toscano" but the market uses "Toscana" — add as alias
const ITALIAN_NAME_ALIASES = {
  'Toscano': ['Toscana', 'Toscana IGT', 'IGT Toscana', 'IGT Toscano'],
  'Terre Siciliane': ['Sicilia IGT', 'IGT Sicilia', 'IGT Terre Siciliane'],
  'Trevenezie': ['delle Venezie', 'Tre Venezie', 'IGT Trevenezie'],
};

// ─── Base-tier (table wine) designations ────────────────────────────────────
const BASE_TIER = [
  { name: 'Vin de France', country: 'FR', designation_type: 'VdF' },
  { name: 'Vino d\'Italia', country: 'IT', designation_type: 'VdI' },
  { name: 'Vino de España', country: 'ES', designation_type: 'VdE' },
  { name: 'Vinho de Portugal', country: 'PT', designation_type: 'VdP' },
  { name: 'Deutscher Wein', country: 'DE', designation_type: 'VdT' },
];

// ─── Region mapping helpers ─────────────────────────────────────────────────
// Italian regions in our DB → common IGT names that map to them
const ITALIAN_REGION_MAP = {
  'Tuscany': ['Toscano', 'Colli della Toscana centrale', 'Costa Toscana', 'Alta Valle della Greve', 'Costa Etrusco Romana', 'Montecastelli'],
  'Sicily': ['Terre Siciliane', 'Avola', 'Camarro', 'Fontanarossa di Cerda', 'Salemi', 'Salina', 'Valle Belice'],
  'Veneto': ['Veneto', 'Veneto Orientale', 'Marca Trevigiana', 'Colli Trevigiani', 'Conselvano', 'Verona'],
  'Piedmont': [],
  'Lombardy': ['Collina del Milanese', 'Benaco Bresciano', 'Montenetto di Brescia', 'Ronchi di Brescia', 'Provincia di Mantova', 'Provincia di Pavia', 'Quistello', 'Sabbioneta', 'Sebino', 'Alto Mincio', 'Bergamasca', 'Terre Lariane', 'Valcamonica'],
  'Puglia': ['Puglia', 'Daunia', 'Murgia', 'Salento', 'Tarantino', 'Valle d\'Itria'],
  'Campania': ['Campania', 'Catalanesca del Monte Somma', 'Colli di Salerno', 'Dugenta', 'Epomeo', 'Paestum', 'Pompeiano', 'Roccamonfina', 'Terre del Volturno'],
  'Emilia-Romagna': ['dell\'Emilia', 'Bianco del Sillaro', 'Castelfranco Emilia', 'Fortana del Taro', 'Forlì', 'Ravenna', 'Rubicone', 'Terre di Veleja', 'Val Tidone'],
  'Calabria': ['Calabria', 'Arghillà', 'Costa Viola', 'Lipuda', 'Locride', 'Palizzi', 'Pellaro', 'Scilla', 'Val di Neto', 'Valdamato'],
  'Sardinia': ['Barbagia', 'Colli del Limbara', 'Isola dei Nuraghi', 'Marmilla', 'Nurra', 'Ogliastra', 'Parteolla', 'Planargia', 'Provincia di Nuoro', 'Romangia', 'Sibiola', 'Tharros', 'Trexenta', 'Valle del Tirso', 'Valli di Porto Pino'],
  'Lazio': ['Lazio', 'Anagni', 'Civitella d\'Agliano', 'Colli Cimini', 'Costa Etrusco Romana', 'Frusinate', 'Rotae'],
  'Marche': ['Marche'],
  'Umbria': ['Umbria', 'Allerona', 'Bettona', 'Cannara', 'Narni', 'Spello'],
  'Abruzzo': ['Colli Aprutini', 'Colli del Sangro', 'Colline Frentane', 'Colline Pescaresi', 'Colline Teatine', 'del Vastese', 'Terre Abruzzesi', 'Terre Aquilane', 'Terre di Chieti'],
  'Friuli Venezia Giulia': ['Venezia Giulia', 'Alto Livenza'],
  'Liguria': ['Colline del Genovesato', 'Colline Savonesi', 'Liguria di Levante', 'Terrazze dell\'Imperiese'],
  'Trentino-Alto Adige': ['Mitterberg', 'Vallagarina', 'Vigneti delle Dolomiti'],
  'Basilicata': ['Basilicata'],
  'Molise': ['Osco', 'Rotae'],
};

// French region mapping
const FRENCH_REGION_MAP = {
  'Languedoc-Roussillon': ['Pays d\'Oc', 'Cité de Carcassonne', 'Coteaux d\'Ensérune', 'Coteaux de Béziers', 'Coteaux de Narbonne', 'Coteaux de Peyriac', 'Côtes Catalanes', 'Côte Vermeille', 'Côtes de Thau', 'Côtes de Thongue', 'Haute Vallée de l\'Aude', 'Haute Vallée de l\'Orb', 'Le Pays Cathare', 'Pays d\'Hérault', 'Saint-Guilhem-le-Désert', 'Vallée du Paradis', 'Vallée du Torgan', 'Vicomté d\'Aumelas', 'Cévennes', 'Aude', 'Gard'],
  'Provence': ['Alpilles', 'Maures', 'Mont Caume', 'Var', 'Pays des Bouches-du-Rhône', 'Méditerranée'],
  'Rhône Valley': ['Ardèche', 'Collines Rhodaniennes', 'Comtés Rhodaniens', 'Coteaux des Baronnies', 'Drôme', 'Vaucluse'],
  'South West France': ['Agenais', 'Ariège', 'Aveyron', 'Comté Tolosan', 'Côtes de Gascogne', 'Côtes du Lot', 'Côtes du Tarn', 'Gers', 'Lavilledieu', 'Périgord', 'Thézac-Perricard', 'Pays de Brive'],
  'Loire Valley': ['Val de Loire', 'Coteaux du Cher et de l\'Arnon', 'Urfé'],
  'Bordeaux': ['Atlantique'],
  'Corsica': ['Île de Beauté'],
  'Burgundy': ['Coteaux de l\'Auxois', 'Saône-et-Loire', 'Coteaux de Tannay', 'Yonne', 'Côtes de la Charité'],
  'Jura': ['Franche-Comté'],
  'Savoie': ['Vin des Allobroges', 'Coteaux de l\'Ain', 'Isère'],
  'Alsace': [],
  'Champagne': ['Haute-Marne', 'Coteaux de Coiffy', 'Sainte-Marie-la-Blanche'],
};

// ─── Normalize function ─────────────────────────────────────────────────────
function normalize(str) {
  return str?.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim() || '';
}

function slugify(str) {
  return str?.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase().trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '') || '';
}

// ─── Helper to fetch all rows ───────────────────────────────────────────────
async function fetchAll(table, select = '*') {
  const rows = [];
  let from = 0;
  const PAGE = 1000;
  while (true) {
    const { data, error } = await supabase.from(table).select(select).range(from, from + PAGE - 1);
    if (error) throw new Error(`${table}: ${error.message}`);
    rows.push(...data);
    if (data.length < PAGE) break;
    from += PAGE;
  }
  return rows;
}

// ─── Main ───────────────────────────────────────────────────────────────────
async function main() {
  console.log(`Mode: ${MODE}\n`);

  // Load reference data
  console.log('Loading reference data...');
  const countries = await fetchAll('countries', 'id,name,iso_code');
  const regions = await fetchAll('regions', 'id,name,country_id,parent_id,is_catch_all');
  const existingApps = await fetchAll('appellations', 'id,name,country_id,region_id,designation_type');

  const countryByCode = {};
  countries.forEach(c => { countryByCode[c.iso_code] = c; });

  const countryById = {};
  countries.forEach(c => { countryById[c.id] = c; });

  // Build region lookup: normalized name → region
  const regionByNormName = {};
  regions.forEach(r => {
    regionByNormName[normalize(r.name)] = r;
  });

  // Build existing appellation lookup
  const existingByNormName = {};
  existingApps.forEach(a => {
    const key = normalize(a.name) + '|' + a.country_id;
    existingByNormName[key] = a;
  });

  // Load eAmbrosia PGI data
  const pgiData = JSON.parse(readFileSync('data/eambrosia_pgi_wines.json', 'utf8'));
  console.log(`eAmbrosia PGI entries: ${pgiData.length}`);

  // Process entries
  const toInsert = [];
  const skipped = [];
  const aliases = [];

  for (const entry of pgiData) {
    const country = countryByCode[entry.country];
    if (!country) {
      skipped.push({ name: entry.name, reason: `Unknown country code: ${entry.country}` });
      continue;
    }

    // Determine display name
    let displayName = entry.name;

    // Transliterate Greek
    if (entry.country === 'GR' && GREEK_TRANSLITERATIONS[entry.name]) {
      displayName = GREEK_TRANSLITERATIONS[entry.name];
      aliases.push({ original: entry.name, display: displayName, type: 'greek_original' });
    }

    // Transliterate Chinese
    if (entry.country === 'CN' && CHINESE_TRANSLITERATIONS[entry.name]) {
      displayName = CHINESE_TRANSLITERATIONS[entry.name];
      aliases.push({ original: entry.name, display: displayName, type: 'chinese_original' });
    }

    // Check if already exists
    const existKey = normalize(displayName) + '|' + country.id;
    if (existingByNormName[existKey]) {
      skipped.push({ name: displayName, reason: 'Already exists in appellations' });
      continue;
    }

    // Determine designation type
    const designationType = DESIGNATION_TYPES[entry.country] || 'PGI';

    // Resolve region
    let regionId = null;
    const regionMaps = entry.country === 'IT' ? ITALIAN_REGION_MAP
      : entry.country === 'FR' ? FRENCH_REGION_MAP : null;

    if (regionMaps) {
      for (const [regionName, igtNames] of Object.entries(regionMaps)) {
        if (igtNames.some(n => normalize(n) === normalize(entry.name))) {
          const region = regions.find(r => normalize(r.name) === normalize(regionName) && r.country_id === country.id);
          if (region) {
            regionId = region.id;
            break;
          }
        }
      }
    }

    // Fallback: try direct name match against regions
    if (!regionId) {
      const directMatch = regions.find(r =>
        r.country_id === country.id && normalize(r.name) === normalize(displayName)
      );
      if (directMatch) {
        regionId = directMatch.id;
      }
    }

    // For Portugal VRs, try matching known patterns
    if (!regionId && entry.country === 'PT') {
      const ptMap = {
        'Minho': 'Minho', 'Transmontano': 'Trás-os-Montes', 'Duriense': 'Douro',
        'Lisboa': 'Lisboa', 'Tejo': 'Tejo', 'Alentejano': 'Alentejo',
        'Algarve': 'Algarve', 'Açores': 'Azores', 'Península de Setúbal': 'Setúbal',
        'Terras Madeirenses': 'Madeira',
      };
      if (ptMap[entry.name]) {
        const region = regions.find(r => normalize(r.name) === normalize(ptMap[entry.name]) && r.country_id === country.id);
        if (region) regionId = region.id;
      }
    }

    // Fall back to catch-all region for the country
    if (!regionId) {
      const catchAll = regions.find(r =>
        r.country_id === country.id && r.is_catch_all === true && r.parent_id === null
      );
      if (catchAll) regionId = catchAll.id;
    }

    toInsert.push({
      name: displayName,
      country_id: country.id,
      region_id: regionId,
      designation_type: designationType,
      hemisphere: country.iso_code === 'CN' ? 'north' : // all EU + CN are northern
        ['AR', 'CL', 'ZA', 'AU', 'NZ'].includes(entry.country) ? 'south' : 'north',
      eambrosia_id: entry.gi_identifier,
      eambrosia_file: entry.file_number,
      original_name: entry.name !== displayName ? entry.name : null,
    });

    // Generate aliases
    const entryAliases = [];

    // Accent-stripped version
    const stripped = normalize(displayName);
    if (stripped !== displayName.toLowerCase()) {
      entryAliases.push(displayName.normalize('NFD').replace(/[\u0300-\u036f]/g, ''));
    }

    // With designation type suffix
    const dt = designationType;
    entryAliases.push(`${displayName} ${dt}`);
    entryAliases.push(`${dt} ${displayName}`);

    // Italian-specific aliases
    if (ITALIAN_NAME_ALIASES[entry.name]) {
      entryAliases.push(...ITALIAN_NAME_ALIASES[entry.name]);
    }

    aliases.push(...entryAliases.map(a => ({
      display: displayName,
      alias: a,
      type: 'generated',
    })));
  }

  // Add base-tier designations
  for (const bt of BASE_TIER) {
    const country = countryByCode[bt.country];
    if (!country) continue;
    const existKey = normalize(bt.name) + '|' + country.id;
    if (existingByNormName[existKey]) {
      skipped.push({ name: bt.name, reason: 'Already exists' });
      continue;
    }
    toInsert.push({
      name: bt.name,
      country_id: country.id,
      region_id: null,  // national-level, no region
      designation_type: bt.designation_type,
      hemisphere: 'north',
      eambrosia_id: null,
      eambrosia_file: null,
      original_name: null,
    });
  }

  // ─── Analysis output ──────────────────────────────────────────────────────
  console.log(`\n=== ANALYSIS ===`);
  console.log(`To insert: ${toInsert.length}`);
  console.log(`Skipped: ${skipped.length}`);
  console.log(`Aliases to create: ${aliases.length}`);

  // By country
  const byCountry = {};
  toInsert.forEach(r => {
    const c = countryById[r.country_id]?.name || 'Unknown';
    byCountry[c] = (byCountry[c] || 0) + 1;
  });
  console.log('\nBy country:');
  Object.entries(byCountry).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log(`  ${k}: ${v}`));

  // Region resolution rate
  const withRegion = toInsert.filter(r => r.region_id).length;
  console.log(`\nRegion resolved: ${withRegion}/${toInsert.length} (${(withRegion / toInsert.length * 100).toFixed(1)}%)`);

  // Show unresolved
  const unresolved = toInsert.filter(r => !r.region_id);
  if (unresolved.length > 0) {
    console.log(`\nUnresolved regions (${unresolved.length}):`);
    for (const u of unresolved.slice(0, 30)) {
      const c = countryById[u.country_id]?.iso_code || '??';
      console.log(`  [${c}] ${u.name}`);
    }
    if (unresolved.length > 30) console.log(`  ... and ${unresolved.length - 30} more`);
  }

  if (skipped.length > 0) {
    console.log(`\nSkipped (${skipped.length}):`);
    skipped.forEach(s => console.log(`  ${s.name}: ${s.reason}`));
  }

  if (MODE === 'analyze') {
    console.log('\nRun with --dry-run or --import to proceed.');
    return;
  }

  // ─── Import ───────────────────────────────────────────────────────────────
  if (MODE === 'dry-run') {
    console.log('\n--- DRY RUN — no DB writes ---');
    console.log('Sample records:');
    toInsert.slice(0, 5).forEach(r => console.log(JSON.stringify(r, null, 2)));
    return;
  }

  // Insert appellations
  console.log('\nInserting appellations...');
  let inserted = 0;
  const BATCH = 50;
  const insertedIds = {}; // name → id

  for (let i = 0; i < toInsert.length; i += BATCH) {
    const batch = toInsert.slice(i, i + BATCH).map(r => {
      let slug = slugify(r.name);
      // If slug is empty (non-Latin chars), use original_name or gi_identifier
      if (!slug || slug.length < 2) {
        slug = slugify(r.original_name || '') || r.eambrosia_id?.toLowerCase() || `pgi-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      }
      return {
        name: r.name,
        slug,
        country_id: r.country_id,
        region_id: r.region_id,
        designation_type: r.designation_type,
        hemisphere: r.hemisphere,
      };
    });

    const { data, error } = await supabase.from('appellations').insert(batch).select('id,name');
    if (error) {
      console.error(`Batch error at ${i}: ${error.message}`);
      // Row by row fallback
      for (const row of batch) {
        const { data: d, error: e } = await supabase.from('appellations').insert(row).select('id,name');
        if (e) {
          console.error(`  Row error (${row.name}): ${e.message}`);
        } else {
          inserted++;
          if (d?.[0]) insertedIds[d[0].name] = d[0].id;
        }
      }
    } else {
      inserted += batch.length;
      data?.forEach(d => { insertedIds[d.name] = d.id; });
    }
  }
  console.log(`Inserted ${inserted} appellations.`);

  // Insert aliases
  console.log('\nInserting aliases...');
  let aliasCount = 0;
  const aliasRows = aliases
    .filter(a => a.alias && insertedIds[a.display])
    .map(a => ({
      appellation_id: insertedIds[a.display],
      alias: a.alias,
      alias_normalized: normalize(a.alias),
    }));

  for (let i = 0; i < aliasRows.length; i += BATCH) {
    const batch = aliasRows.slice(i, i + BATCH);
    const { error } = await supabase.from('appellation_aliases').insert(batch);
    if (error) {
      // Skip duplicates silently
      for (const row of batch) {
        const { error: e } = await supabase.from('appellation_aliases').insert(row);
        if (!e) aliasCount++;
      }
    } else {
      aliasCount += batch.length;
    }
  }
  console.log(`Inserted ${aliasCount} aliases.`);

  // Final count
  const { count } = await supabase.from('appellations').select('*', { count: 'exact', head: true });
  console.log(`\nTotal appellations in DB: ${count}`);
}

main().catch(console.error);
