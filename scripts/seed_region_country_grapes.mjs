/**
 * Seed region_grapes and country_grapes from Anderson & Aryal dataset
 *
 * Source: Anderson, Nelgen & Puga (2023). "Database of Regional, National and
 * Global Winegrape Bearing Areas by Variety, 2000 to 2023."
 * University of Adelaide Wine Economics Research Centre.
 * https://economics.adelaide.edu.au/wine-economics/databases
 *
 * Strategy:
 * 1. Parse the regional Excel file for grape plantings by region
 * 2. Match Anderson regions to our Loam regions (fuzzy matching)
 * 3. Match Anderson grape names to our VIVC-based grapes (synonym matching)
 * 4. Generate SQL inserts for region_grapes and country_grapes
 * 5. Clear existing data first (we're rebuilding from authoritative source)
 */

import XLSX from 'xlsx';
import { createClient } from '@supabase/supabase-js';
import { writeFileSync } from 'fs';

const REGIONAL_FILE = 'C:/Users/neilw/.claude/projects/C--Users-neilw-Documents-GitHub-loamv2/e9288814-96ee-4aa2-b7f9-6d0f936fdefd/tool-results/webfetch-1773525788884-c3gex1.xlsx';

const supabase = createClient(
  'https://vgbppjhmvbggfjztzobl.supabase.co',
  process.env.SUPABASE_ANON_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZnYnBwamhtdmJnZ2ZqenR6b2JsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI1ODU1NDYsImV4cCI6MjA4ODE2MTU0Nn0.KHZiqk6B7XYDnkFcDNJtMIKoT-hf7s8MGkmpOsjgVDk'
);

// Thresholds
const MIN_HA_REGION = 50;
const MIN_SHARE_REGION = 0.02; // 2% of region total
const MAX_GRAPES_REGION = 15;
const MIN_HA_COUNTRY = 200;
const MIN_SHARE_COUNTRY = 0.015; // 1.5% of country total
const MAX_GRAPES_COUNTRY = 25;

// Anderson country name -> our ISO code mapping
const COUNTRY_MAP = {
  'Albania': 'AL', 'Algeria': null, 'Argentina': 'AR', 'Armenia': 'AM',
  'Australia': 'AU', 'Austria': 'AT', 'Belgium': 'BE', 'Brazil': 'BR',
  'Bulgaria': 'BG', 'Cambodia': null, 'Canada': 'CA', 'Chile': 'CL',
  'China': 'CN', 'Croatia': 'HR', 'Cyprus': 'CY', 'Czechia': 'CZ',
  'Ethiopia': null, 'France': 'FR', 'Georgia': 'GE', 'Germany': 'DE',
  'Greece': 'GR', 'Hungary': 'HU', 'India': 'IN', 'Israel': 'IL',
  'Italy': 'IT', 'Japan': 'JP', 'Kazakhstan': null, 'Korea, Rep.': null,
  'Lebanon': 'LB', 'Lithuania': null, 'Luxembourg': 'LU', 'Mexico': 'MX',
  'Moldova': 'MD', 'Morocco': 'MA', 'Myanmar': 'MM', 'New Zealand': 'NZ',
  'North Macedonia': 'MK', 'Norway': null, 'Peru': 'PE', 'Poland': 'PL',
  'Portugal': 'PT', 'Romania': 'RO', 'Russia': 'RU', 'Serbia': 'RS',
  'Slovakia': 'SK', 'Slovenia': 'SI', 'South Africa': 'ZA', 'Spain': 'ES',
  'Sweden': 'SE', 'Switzerland': 'CH', 'Taiwan': null, 'Thailand': 'TH',
  'Tunisia': 'TN', 'Turkiye': 'TR', 'Turkmenistan': null, 'Ukraine': 'UA',
  'United Kingdom': 'GB', 'United States': 'US', 'Uruguay': 'UY'
};

// Anderson region name -> our region name mapping (where they differ)
const REGION_MAP = {
  // France — Anderson uses admin regions, we use wine regions
  'FR|Alsace': 'Alsace',
  'FR|Aquitaine': 'Bordeaux', // Bordeaux is in Aquitaine
  'FR|Bourgogne': 'Burgundy',
  'FR|Champagne': 'Champagne',
  'FR|Centre': 'Loire Valley', // Sancerre, Pouilly-Fumé are in Centre
  'FR|Pays De Loire': 'Loire Valley', // Muscadet, Anjou, Saumur
  'FR|Corse': 'Corsica',
  'FR|Languedoc-Roussillon': 'Southern France',
  'FR|Provence -C. D\'Azur': 'Southern France',
  'FR|Rhone-Alpes': 'Rhône Valley',
  'FR|Midi-Pyrenees': 'Southwest France',
  'FR|Mourvedre N': 'Southwest France', // Likely SW France / Basque
  'FR|Poitou-Charentes': 'Cognac',
  // Italy — Anderson uses admin regions, map to our names
  'IT|Lombardia': 'Lombardy',
  'IT|Piemonte': 'Piemonte',
  'IT|Toscana': 'Tuscany',
  'IT|Sicilia': 'Sicily',
  'IT|Sardegna': 'Sardinia',
  'IT|Provincia Autonoma di Bolzano/Bozen': 'Trentino-Alto Adige',
  'IT|Provincia Autonoma di Trento': 'Trentino-Alto Adige',
  'IT|Puglia': 'Puglia',
  'IT|Veneto': 'Veneto',
  'IT|Emilia-Romagna': 'Emilia-Romagna',
  'IT|Friuli-Venezia Giulia': 'Friuli-Venezia Giulia',
  'IT|Campania': 'Campania',
  'IT|Abruzzo': 'Abruzzo',
  'IT|Lazio': 'Lazio',
  'IT|Calabria': 'Calabria',
  'IT|Basilicata': 'Basilicata',
  'IT|Liguria': 'Liguria',
  'IT|Umbria': 'Umbria',
  'IT|Marche': 'Marche',
  'IT|Molise': 'Molise',
  // Germany — Anderson uses admin districts, map to Anbaugebiete
  'DE|Trier': 'Mosel',
  'DE|Koblenz': 'Mosel', // Also Mittelrhein, but mostly Mosel
  'DE|Rheinhessen-Pfalz': 'Rheinhessen', // Combined, we'll split notes
  'DE|Freiburg': 'Baden',
  'DE|Karlsruhe': 'Baden',
  'DE|Stuttgart': 'Württemberg',
  'DE|Tübingen': 'Württemberg',
  'DE|Darmstadt': 'Rheingau', // Hessische Bergstraße is also there
  'DE|Mittelfranken': 'Franken',
  'DE|Unterfranken': 'Franken',
  'DE|Sachsen-Anhalt': 'Saale-Unstrut',
  // Spain
  'ES|La Rioja': 'The Upper Ebro',
  'ES|Navarra': 'The Upper Ebro',
  'ES|Galicia': 'The North West',
  'ES|Cataluña': 'Catalunya',
  'ES|Castilla Y León': 'Castilla y León',
  'ES|Castilla La\nMancha': 'Castilla-La Mancha',
  'ES|Aragón': 'Aragón',
  'ES|Andalucía': 'Andalucía',
  'ES|Valencia': 'The Levante',
  'ES|Murcia': 'The Levante',
  'ES|Madrid': 'Madrid',
  'ES|Extremadura': 'Extremadura',
  'ES|País Vasco': 'The Upper Ebro',
  'ES|Baleares': 'Balearic Islands',
  'ES|Canarias': 'Canary Islands',
  'ES|Castilla La Mancha': 'Castilla-La Mancha', // normalized newline version
  'ES|Asturias': null, // too small
  'ES|Cantabria': null, // too small
  // Chile — map to our regions
  'CL|Valparaiso': 'Aconcagua Region', // Casablanca, San Antonio are in Valparaíso
  'CL|Metropolitana': 'Aconcagua Region', // Maipo is in Metropolitana
  'CL|L.B.O\'Higgins': 'Central Valley Region', // Rapel, Colchagua
  'CL|Maule': 'Central Valley Region',
  'CL|Coquimbo': 'Coquimbo Region',
  'CL|Bio Bio': 'Southern Region',
  'CL|Ñuble': 'Southern Region',
  'CL|Araucania': 'Southern Region',
  'CL|Atacama': 'Atacama',
  // Greece — map admin regions to our wine regions
  'GR|Kentriki Makedonia': 'Macedonia',
  'GR|Dytiki Makedonia': 'Macedonia',
  'GR|Anatoliki Makedonia, Thraki': 'Macedonia',
  'GR|Peloponnisos': 'Peloponnese',
  'GR|Dytiki Elláda': 'Peloponnese',
  'GR|Attiki': 'Central Greece',
  'GR|Sterea Elláda': 'Central Greece',
  'GR|Thessalia': 'Central Greece',
  'GR|Kriti': 'Crete',
  'GR|Notio Aigaio': 'Aegean Islands',
  'GR|Voreio Aigaio': 'Aegean Islands',
  'GR|Ionia Nisia': 'Ionian Islands',
  'GR|Ipeiros': 'Epirus',
  // Hungary — map to our regions where possible
  'HU|Észak-Magyarország': null, // Tokaj+Eger mixed
  'HU|Dél-Dunántúl': null, // Villány+Szekszárd
  'HU|Nyugat-Dunántúl': null, // Sopron
  'HU|Dél-Alföld': null,
  'HU|Észak-Alföld': null,
  'HU|Közép-Dunántúl': 'Lake Balaton',
  'HU|Pest': null,
  'HU|Budapest': null,
  // Slovenia — map to our regions (we don't have L2)
  'SI|Goriška Brda': null, // would need L2
  'SI|Vipavska Dolina': null,
  'SI|Slovenska Istra': null,
  'SI|Štajerska Slovenija': null,
  'SI|Kras': null,
  'SI|Dolenjska': null,
  'SI|Bizeljsko-Sremič': null,
  'SI|Bela Krajina': null,
  'SI|Prekmurje': null,
  'SI|Ni Okoliš': null,
  // Portugal
  'PT|Douro': 'Douro',
  'PT|Alentejo': 'Alentejo',
  'PT|Minho': 'Vinho Verde',
  'PT|Lisboa': 'Lisboa',
  'PT|Tejo': 'Tejo',
  'PT|Beira Atlântico': 'Bairrada',
  'PT|Terras Do Dão': 'Dão',
  'PT|Terras De Cister': 'Dão', // Overlaps with Dão
  'PT|Terras Da Beira': 'Beira Interior',
  'PT|Trás-Os-Montes': 'Trás-os-Montes',
  'PT|Algarve': 'Algarve',
  'PT|Península De Setúbal': 'Setúbal',
  // Australia
  'AU|SA': 'South Australia',
  'AU|Vic': 'Victoria',
  'AU|NSW': 'New South Wales',
  'AU|WA': 'Western Australia',
  'AU|Tas': 'Tasmania',
  'AU|Qld': 'Queensland',
  // NZ
  'NZ|Marlborough': 'South Island',
  'NZ|Central Otago': 'South Island',
  'NZ|North Canterbury': 'South Island',
  'NZ|Nelson': 'South Island',
  'NZ|Hawkes Bay': 'North Island',
  'NZ|Gisborne': 'North Island',
  'NZ|Auckland': 'North Island',
  'NZ|Wairarapa': 'North Island',
  // South Africa — handle newlines in names (both raw and normalized)
  'ZA|Stellen-\nBosch': 'Coastal Region', 'ZA|Stellen- Bosch': 'Coastal Region',
  'ZA|Paarl': 'Coastal Region',
  'ZA|Swart-\nLand': 'Coastal Region', 'ZA|Swart- Land': 'Coastal Region',
  'ZA|Cape\nTown': 'Coastal Region', 'ZA|Cape Town': 'Coastal Region',
  'ZA|Cape South Coast': 'Cape South Coast',
  'ZA|Robert-\nSon': 'Breede River Valley', 'ZA|Robert- Son': 'Breede River Valley',
  'ZA|Worces-\nTer': 'Breede River Valley', 'ZA|Worces- Ter': 'Breede River Valley',
  'ZA|Breede-\nKloof': 'Breede River Valley', 'ZA|Breede- Kloof': 'Breede River Valley',
  'ZA|Klein\nKaroo': 'Western Cape', 'ZA|Klein Karoo': 'Western Cape',
  'ZA|Olifants\nRiver': 'Western Cape', 'ZA|Olifants River': 'Western Cape',
  'ZA|Northern\nCape': 'Western Cape', 'ZA|Northern Cape': 'Western Cape',
  // US
  'US|California': 'California',
  'US|Washington': 'Washington',
  'US|Oregon': 'Oregon',
  'US|New York': 'New York',
  'US|Texas': 'Texas',
  // Switzerland
  'CH|VD': 'Vaud',
  'CH|VS': 'Valais',
  'CH|GE': 'Geneva',
  'CH|TI': 'Ticino',
  'CH|ZH': 'Zürich',
  'CH|NE': 'Neuchâtel',
  'CH|SH': 'Schaffhausen',
  'CH|TG': 'Thurgau',
  'CH|AG': 'Aargau',
  'CH|SG': 'St. Gallen',
  'CH|BE Lac de Bienne': 'Bern',
  'CH|GR übriges Gebiet': 'Graubünden',
  'CH|BL': null, // Basel-Landschaft, too small
  // Austria
  'AT|Niederösterreich': 'Niederösterreich',
  'AT|Burgenland': 'Burgenland',
  'AT|Steiermark': 'Steiermark',
  'AT|Wien': 'Wien',
  // Hungary — Anderson uses admin regions
  'HU|Észak-Magyarország': null, // Contains Tokaj + Eger, too mixed
  'HU|Dél-Dunántúl': null, // Contains Villány + Szekszárd
  'HU|Nyugat-Dunántúl': null, // Contains Sopron
  // Georgia
  'GE|Kakheti': 'Kakheti',
  'GE|Racha And Lechkhumi': 'Racha-Lechkhumi',
  // Canada
  'CA|British Columbia': 'British Columbia',
  // Argentina
  'AR|Mendoza': 'Mendoza',
  'AR|Salta': 'Salta',
  'AR|San Juan': 'San Juan',
  'AR|La Rioja': 'La Rioja',
  'AR|Rio Negro': 'Patagonia',
  'AR|Neuquen': 'Patagonia',
  'AR|Catamarca': null,
  'AR|Cordoba': 'Córdoba',
  'AR|Tucuman': null,
  'AR|La Pampa': null,
  'AR|Buenos Aires': 'Buenos Aires',
  'AR|Jujuy': 'Jujuy',
  'AR|Entre Rios': 'Entre Ríos',
  'AR|San Luis': null,
  'AR|Chubut': 'Patagonia',
  'AR|Misiones': null,
  'AR|Santa Fe': null,
  'AR|S Del Estero': null,
  // US
  'US|Michigan': null, // Concord/hybrids, not vinifera
  // Japan — no matching regions in our DB currently
  'JP|Yamanashi': null,
  'JP|Hokkaido': null,
  'JP|Nagano': null,
  // Romania — uses admin counties, we don't have Romanian regions
  // Skip all Romanian county-level data
  // Bulgaria — uses admin regions, we don't have Bulgarian regions
  // Skip all Bulgarian admin regions
  // Slovakia — uses admin regions
  'SK|Bratislavský kraj': null,
  'SK|Západné Slovensko': null,
  'SK|Stredné Slovensko': null,
  'SK|Východné Slovensko': null,
  // Russia
  'RU|Crimea': null,
  'RU|Krasnodar Krai': null,
  'RU|Other regions': null,
  'RU|Rostov Oblast': null,
  // Czechia
  'CZ|Morava': null,
  'CZ|Čechy': null,
  // Poland — all admin regions, skip
};

async function main() {
  // 1. Load our DB data
  console.log('Loading DB data...');

  // Get countries
  const { data: countries, error: cErr } = await supabase.from('countries').select('id, name, iso_code');
  if (cErr) { console.error('Error loading countries:', cErr); return; }
  console.log(`  Loaded ${countries.length} countries`);
  const countryByIso = {};
  for (const c of countries) countryByIso[c.iso_code] = c;

  // Get regions
  let allRegions = [];
  let from = 0;
  while (true) {
    const { data } = await supabase.from('regions').select('id, name, country_id, is_catch_all')
      .eq('is_catch_all', false).range(from, from + 999);
    if (!data || data.length === 0) break;
    allRegions = allRegions.concat(data);
    from += 1000;
  }
  console.log(`  ${countries.length} countries, ${allRegions.length} regions`);

  // Build region lookup: { country_id: { name_lower: region } }
  const regionLookup = {};
  for (const r of allRegions) {
    if (!regionLookup[r.country_id]) regionLookup[r.country_id] = {};
    regionLookup[r.country_id][r.name.toLowerCase()] = r;
  }

  // Get grapes with display_name and synonyms
  let allGrapes = [];
  from = 0;
  while (true) {
    const { data } = await supabase.from('grapes').select('id, name, display_name')
      .range(from, from + 999);
    if (!data || data.length === 0) break;
    allGrapes = allGrapes.concat(data);
    from += 1000;
  }
  console.log(`  ${allGrapes.length} grapes`);

  // Get synonyms
  let allSynonyms = [];
  from = 0;
  while (true) {
    const { data } = await supabase.from('grape_synonyms').select('grape_id, synonym')
      .range(from, from + 999);
    if (!data || data.length === 0) break;
    allSynonyms = allSynonyms.concat(data);
    from += 1000;
  }
  console.log(`  ${allSynonyms.length} synonyms`);

  // Build grape lookup by various name forms
  const grapeLookup = {}; // lowered name -> grape_id
  for (const g of allGrapes) {
    grapeLookup[g.name.toLowerCase()] = g.id;
    if (g.display_name) grapeLookup[g.display_name.toLowerCase()] = g.id;
  }
  for (const s of allSynonyms) {
    const key = s.synonym.toLowerCase();
    if (!grapeLookup[key]) grapeLookup[key] = s.grape_id;
  }

  // Manual grape name overrides for Anderson names that don't match
  const GRAPE_OVERRIDES = {
    'côt': grapeLookup['malbec'] || grapeLookup['cot'],
    'douce noire': grapeLookup['corbeau'],
    'tribidrag': grapeLookup['primitivo'],
    'prosecco': grapeLookup['glera'],
    'syrah': grapeLookup['syrah'],
    'graševina': grapeLookup['graševina'] || grapeLookup['welschriesling'] || lookupGrape('WELSCHRIESLING'),
    'mammolo': lookupGrape('MAMMOLO'),
    'nero d\'avola': grapeLookup['nero d\'avola'] || grapeLookup['calabrese'],
    'listán prieto': grapeLookup['listan prieto'] || lookupGrape('LISTAN PRIETO'),
    'listán de huelva': grapeLookup['listán de huelva'] || lookupGrape('PALOMINO FINO'),
    'listán negro': lookupGrape('LISTAN NEGRO'),
    'muscat blanc à petits grains': grapeLookup['muscat blanc à petits grains'] || lookupGrape('MUSCAT A PETITS GRAINS BLANCS'),
    'palomino fino': lookupGrape('PALOMINO FINO'),
    'pedro ximénez': lookupGrape('PEDRO XIMENEZ'),
    'schiava grossa': lookupGrape('SCHIAVA GROSSA'),
    'mazuelo': grapeLookup['carignan noir'],
    'alicante henri bouschet': lookupGrape('ALICANTE HENRI BOUSCHET'),
    'catarratto bianco': grapeLookup['catarratto bianco comune'],
    'monastrell': grapeLookup['mourvèdre'] || grapeLookup['monastrell'],
    'macabeo': lookupGrape('MACABEO'),
    'airén': lookupGrape('AIREN'),
    'bobal': lookupGrape('BOBAL'),
    'hondarribi zuri': grapeLookup['courbu blanc'], // from earlier work
    'castelão': lookupGrape('CASTELAO'),
    'alvarinho': grapeLookup['albariño'] || grapeLookup['alvarinho'],
    'arinto de bucelas': lookupGrape('ARINTO'),
    'loureiro': lookupGrape('LOUREIRO'),
    'rufete': lookupGrape('RUFETE'),
    'malvasia fina': lookupGrape('MALVASIA FINA'),
    'mencía': grapeLookup['mencía'] || lookupGrape('MENCIA'),
    'trebbiano romagnolo': lookupGrape('TREBBIANO ROMAGNOLO'),
    'lambrusco salamino': lookupGrape('LAMBRUSCO SALAMINO'),
    'falanghina flegrea': lookupGrape('FALANGHINA'),
    'teroldego': lookupGrape('TEROLDEGO'),
    'bonarda piemontese': lookupGrape('BONARDA PIEMONTESE'),
    'grechetto di orvieto': lookupGrape('GRECHETTO'),
    'gaglioppo': lookupGrape('GAGLIOPPO'),
    'magliocco canino': lookupGrape('MAGLIOCCO CANINO'),
    'trebbiano d\'abruzzo': lookupGrape('TREBBIANO ABRUZZESE'),
    'malvasia bianca di candia': lookupGrape('MALVASIA BIANCA DI CANDIA'),
    'muscat of hamburg': lookupGrape('MUSCAT HAMBURG'),
    'fetească regală': lookupGrape('FETEASCA REGALA'),
    'fetească albă': lookupGrape('FETEASCA ALBA'),
    'vranac': lookupGrape('VRANAC'),
    'dimyat': lookupGrape('DIMIAT'),
    'pamid': lookupGrape('PAMID'),
    'muscat ottonel': lookupGrape('MUSCAT OTTONEL'),
    'isabella': lookupGrape('ISABELLA'),
    'siroka melniska': lookupGrape('SIROKA MELNISHKA LOZA'),
    'xynisteri': lookupGrape('XYNISTERI'),
    'sultaniye': lookupGrape('SULTANIYE'),
    'plavac mali crni': lookupGrape('PLAVAC MALI'),
    'malvazija istarska': lookupGrape('MALVAZIJA ISTARSKA'),
    'kotsifali': lookupGrape('KOTSIFALI'),
    'mavrodafni': lookupGrape('MAVRODAPHNI'),
    'mandilaria': lookupGrape('MANDILARIA'),
    'savatiano': lookupGrape('SAVATIANO'),
    'liatiko': lookupGrape('LIATIKO'),
    'dornfelder': lookupGrape('DORNFELDER'),
    'bacchus': lookupGrape('BACCHUS WEISS'),
    'xarello': lookupGrape('XAREL-LO'),
    'parellada': lookupGrape('PARELLADA'),
    'cayetana blanca': lookupGrape('CAYETANA BLANCA'),
    'manto negro': lookupGrape('MANTO NEGRO'),
    'verdejo': grapeLookup['verdejo'] || lookupGrape('VERDEJO BLANCO'),
    'cserszegi fűszeres': lookupGrape('CSERSZEGI FUESZERES'),
    'bianca': lookupGrape('BIANCA'),
    'ribolla gialla': lookupGrape('RIBOLLA GIALLA'),
    'žametovka': lookupGrape('ZAMETOVKA'),
    'refosco': lookupGrape('REFOSCO DAL PEDUNCOLO ROSSO'),
    'solaris': lookupGrape('SOLARIS'),
    'johanniter': lookupGrape('JOHANNITER'),
  };

  function lookupGrape(vivcName) {
    const g = allGrapes.find(g => g.name === vivcName);
    return g ? g.id : null;
  }

  function findGrapeId(andersonName) {
    const lower = andersonName.toLowerCase().trim();
    // Check overrides first
    if (GRAPE_OVERRIDES[lower] !== undefined) return GRAPE_OVERRIDES[lower];
    // Direct lookup
    if (grapeLookup[lower]) return grapeLookup[lower];
    // Try without accents (rough)
    const noAccent = lower.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    if (grapeLookup[noAccent]) return grapeLookup[noAccent];
    return null;
  }

  // 2. Parse Anderson data
  console.log('\nParsing Anderson & Aryal regional data...');
  const wb = XLSX.readFile(REGIONAL_FILE);
  const sheets = wb.SheetNames.filter(s => s !== 'Title page' && s !== 'All countries');

  // Aggregate by our region IDs
  const regionAgg = {}; // region_id -> { grape_id -> { hectares, grape_name } }
  const countryAgg = {}; // country_id -> { grape_id -> { hectares, grape_name } }
  const unmatchedGrapes = new Set();
  const unmatchedRegions = new Set();

  for (const sheet of sheets) {
    const iso = COUNTRY_MAP[sheet];
    if (!iso) continue;

    const country = countryByIso[iso];
    if (!country) continue;

    if (!countryAgg[country.id]) countryAgg[country.id] = {};

    const ws = wb.Sheets[sheet];
    const rows = XLSX.utils.sheet_to_json(ws);

    for (const row of rows) {
      const grape = row.prime;
      const area = parseFloat(row.area) || 0;
      const regionName = row.region || 'Unknown';

      if (!grape || area <= 0 || grape === 'other' || grape === 'other red' || grape === 'other white') continue;

      // Find grape
      const grapeId = findGrapeId(grape);
      if (!grapeId) {
        unmatchedGrapes.add(grape);
        continue;
      }

      // Country aggregation
      if (!countryAgg[country.id][grapeId]) countryAgg[country.id][grapeId] = { hectares: 0, name: grape };
      countryAgg[country.id][grapeId].hectares += area;

      // Region mapping - normalize newlines in region names
      const normalizedRegionName = regionName.replace(/\n/g, ' ').trim();
      const mapKey = `${iso}|${regionName}`;
      const mapKeyNorm = `${iso}|${normalizedRegionName}`;
      const mappedRegionName = REGION_MAP[mapKey] ?? REGION_MAP[mapKeyNorm];

      if (mappedRegionName === null) continue; // explicitly skipped
      if (mappedRegionName === undefined && regionName !== 'Unknown') {
        unmatchedRegions.add(`${sheet} > ${regionName}`);
        continue;
      }

      if (mappedRegionName) {
        // Find region by name in this country
        const regionsByCountry = regionLookup[country.id] || {};
        const region = regionsByCountry[mappedRegionName.toLowerCase()];
        if (region) {
          if (!regionAgg[region.id]) regionAgg[region.id] = {};
          if (!regionAgg[region.id][grapeId]) regionAgg[region.id][grapeId] = { hectares: 0, name: grape };
          regionAgg[region.id][grapeId].hectares += area;
        } else {
          unmatchedRegions.add(`${sheet} > ${regionName} -> ${mappedRegionName} (not found in DB)`);
        }
      }
    }
  }

  // 3. Generate inserts
  console.log('\nGenerating SQL...');

  // Country grapes
  const countryInserts = [];
  for (const [countryId, grapes] of Object.entries(countryAgg)) {
    const sorted = Object.entries(grapes).sort((a, b) => b[1].hectares - a[1].hectares);
    const totalHa = sorted.reduce((sum, [, g]) => sum + g.hectares, 0);

    const filtered = sorted
      .filter(([, g]) => g.hectares >= MIN_HA_COUNTRY && (g.hectares / totalHa) >= MIN_SHARE_COUNTRY)
      .slice(0, MAX_GRAPES_COUNTRY);

    for (const [grapeId, g] of filtered) {
      const share = Math.round((g.hectares / totalHa) * 1000) / 10;
      countryInserts.push({
        country_id: countryId,
        grape_id: grapeId,
        association_type: 'typical',
        notes: `${Math.round(g.hectares)} ha (${share}% of national plantings). Source: Anderson & Aryal 2023`
      });
    }
  }

  // Region grapes
  const regionInserts = [];
  for (const [regionId, grapes] of Object.entries(regionAgg)) {
    const sorted = Object.entries(grapes).sort((a, b) => b[1].hectares - a[1].hectares);
    const totalHa = sorted.reduce((sum, [, g]) => sum + g.hectares, 0);

    const filtered = sorted
      .filter(([, g]) => g.hectares >= MIN_HA_REGION && (g.hectares / totalHa) >= MIN_SHARE_REGION)
      .slice(0, MAX_GRAPES_REGION);

    for (const [grapeId, g] of filtered) {
      const share = Math.round((g.hectares / totalHa) * 1000) / 10;
      regionInserts.push({
        region_id: regionId,
        grape_id: grapeId,
        association_type: 'typical',
        notes: `${Math.round(g.hectares)} ha (${share}% of regional plantings). Source: Anderson & Aryal 2023`
      });
    }
  }

  console.log(`\nCountry grape entries: ${countryInserts.length}`);
  console.log(`Region grape entries: ${regionInserts.length}`);
  console.log(`Unmatched grapes (${unmatchedGrapes.size}): ${[...unmatchedGrapes].sort().join(', ')}`);
  console.log(`Unmatched regions (${unmatchedRegions.size}):`);
  for (const r of [...unmatchedRegions].sort()) console.log(`  ${r}`);

  // 4. Write to DB
  console.log('\nClearing existing data...');

  // Clear and re-insert country_grapes
  const { error: delCG } = await supabase.from('country_grapes').delete().neq('country_id', '00000000-0000-0000-0000-000000000000');
  if (delCG) console.error('Error clearing country_grapes:', delCG);
  else console.log('  Cleared country_grapes');

  // Clear and re-insert region_grapes
  const { error: delRG } = await supabase.from('region_grapes').delete().neq('region_id', '00000000-0000-0000-0000-000000000000');
  if (delRG) console.error('Error clearing region_grapes:', delRG);
  else console.log('  Cleared region_grapes');

  // Insert country_grapes in batches
  console.log('\nInserting country_grapes...');
  for (let i = 0; i < countryInserts.length; i += 500) {
    const batch = countryInserts.slice(i, i + 500);
    const { error } = await supabase.from('country_grapes').upsert(batch, { onConflict: 'country_id,grape_id' });
    if (error) console.error(`  Batch ${i} error:`, error);
    else console.log(`  Inserted ${Math.min(i + 500, countryInserts.length)}/${countryInserts.length}`);
  }

  // Insert region_grapes in batches
  console.log('\nInserting region_grapes...');
  for (let i = 0; i < regionInserts.length; i += 500) {
    const batch = regionInserts.slice(i, i + 500);
    const { error } = await supabase.from('region_grapes').upsert(batch, { onConflict: 'region_id,grape_id' });
    if (error) console.error(`  Batch ${i} error:`, error);
    else console.log(`  Inserted ${Math.min(i + 500, regionInserts.length)}/${regionInserts.length}`);
  }

  console.log('\nDone!');

  // Save report
  const report = {
    source: 'Anderson, Nelgen & Puga (2023)',
    url: 'https://economics.adelaide.edu.au/wine-economics/databases',
    country_grapes: countryInserts.length,
    region_grapes: regionInserts.length,
    unmatched_grapes: [...unmatchedGrapes].sort(),
    unmatched_regions: [...unmatchedRegions].sort()
  };
  writeFileSync('data/anderson_aryal_seed_report.json', JSON.stringify(report, null, 2));
  console.log('Report saved to data/anderson_aryal_seed_report.json');
}

main().catch(console.error);
