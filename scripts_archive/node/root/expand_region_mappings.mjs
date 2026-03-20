#!/usr/bin/env node
/**
 * Expand region_name_mappings to cover more wine_candidates region_names.
 *
 * Strategy:
 * 1. Exact match unmapped region_name to appellation.name
 * 2. Normalized match (strip accents, lowercase) to appellation.name
 * 3. French sub-appellation patterns (1er Cru, Grand Cru → base appellation)
 * 4. Italian sub-DOC patterns (Classico, Superiore → base DOC)
 * 5. Known manual mappings for common alternative names
 * 6. Fuzzy: map to parent region by country when we can identify the area
 *
 * Usage:
 *   node expand_region_mappings.mjs --dry-run     # Preview without inserting
 *   node expand_region_mappings.mjs               # Insert mappings + update wines
 */

import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "fs";

// ── Load .env ───────────────────────────────────────────────
const envPath = new URL(".env", import.meta.url).pathname.replace(/^\/([A-Z]:)/, "$1");
const envContent = readFileSync(envPath, "utf-8");
for (const line of envContent.split("\n")) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) continue;
  const eqIdx = trimmed.indexOf("=");
  if (eqIdx === -1) continue;
  const key = trimmed.slice(0, eqIdx);
  const val = trimmed.slice(eqIdx + 1);
  if (!process.env[key]) process.env[key] = val;
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);
const DRY_RUN = process.argv.includes("--dry-run");

function normalize(s) {
  return s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
}

async function fetchAll(table, columns = "*", batchSize = 1000) {
  const rows = [];
  let offset = 0;
  while (true) {
    const { data, error } = await sb.from(table).select(columns).range(offset, offset + batchSize - 1);
    if (error) throw new Error(`Fetch ${table}: ${error.message}`);
    rows.push(...data);
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return rows;
}

// ── Manual mappings for well-known alternative names ────────
// region_name → { region: "Region Name", appellation?: "Appellation Name" }
// These are names that won't match via normalization but we know the mapping.
const MANUAL_REGION_ALIASES = {
  // ═══ France ═══
  // Bordeaux satellites & sub-regions
  "Côtes de Bourg|France": { region: "Bordeaux" },
  "Castillon-Côtes de Bordeaux|France": { region: "Bordeaux" },
  "Puisseguin-Saint-Émilion|France": { region: "Bordeaux" },
  "Blaye-Côtes de Bordeaux|France": { region: "Bordeaux" },
  "Montagne-Saint-Émilion|France": { region: "Bordeaux" },
  "Lussac-Saint-Émilion|France": { region: "Bordeaux" },
  "Côtes de Bordeaux|France": { region: "Bordeaux" },
  "Guyenne|France": { region: "Bordeaux" },
  "Francs-Côtes de Bordeaux|France": { region: "Bordeaux" },
  "Cadillac-Côtes de Bordeaux|France": { region: "Bordeaux" },
  "Sainte-Foy-Bordeaux|France": { region: "Bordeaux" },
  "Premières Côtes de Bordeaux|France": { region: "Bordeaux" },

  // Rhône Valley
  "Ventoux|France": { region: "Southern Rhône" },
  "Luberon|France": { region: "Southern Rhône" },
  "Costières-de-Nîmes|France": { region: "Southern Rhône" },
  "Côtes-du-Rhône-Villages|France": { region: "Southern Rhône" },
  "Côtes-du-Rhône-Villages 'Plan de Dieu'|France": { region: "Southern Rhône" },
  "Côtes-du-Rhône-Villages 'Cairanne'|France": { region: "Southern Rhône" },
  "Côtes-du-Rhône-Villages 'Séguret'|France": { region: "Southern Rhône" },
  "Côtes-du-Rhône-Villages 'Sablet'|France": { region: "Southern Rhône" },
  "Vaucluse|France": { region: "Rhône Valley" },
  "Collines Rhodaniennes|France": { region: "Rhône Valley" },
  "Drôme|France": { region: "Rhône Valley" },
  "Ardèche|France": { region: "Rhône Valley" },

  // Burgundy
  "Bourgogne Hautes-Côtes de Beaune|France": { region: "Burgundy" },
  "Bourgogne Hautes-Côtes de Nuits|France": { region: "Burgundy" },
  "Saint-Aubin|France": { region: "Burgundy" },
  "Viré-Clessé|France": { region: "Burgundy" },
  "Côte de Nuits Villages|France": { region: "Burgundy" },
  "Côte Chalonnaise|France": { region: "Burgundy" },
  "Saint-Romain|France": { region: "Burgundy" },
  "Côte de Beaune|France": { region: "Burgundy" },
  "Côte de Beaune-Villages|France": { region: "Burgundy" },
  "Mâcon-Villages|France": { region: "Burgundy" },
  "Bourgogne Aligoté|France": { region: "Burgundy" },
  "Hautes-Côtes de Beaune|France": { region: "Burgundy" },
  "Hautes-Côtes de Nuits|France": { region: "Burgundy" },
  "Auxey-Duresses|France": { region: "Burgundy" },
  "Montagny|France": { region: "Burgundy" },
  "Rully|France": { region: "Burgundy" },
  "Bouzeron|France": { region: "Burgundy" },
  "Givry|France": { region: "Burgundy" },
  "Mercurey|France": { region: "Burgundy" },

  // Loire Valley
  "Muscadet-Sevre et Maine|France": { region: "Loire Valley" },
  "Muscadet-Sèvre et Maine|France": { region: "Loire Valley" },
  "Pays Nantais|France": { region: "Loire Valley" },
  "Upper Loire|France": { region: "Loire Valley" },
  "Rosé d'Anjou|France": { region: "Loire Valley" },
  "Haut-Poitou|France": { region: "Loire Valley" },
  "Val de Loire|France": { region: "Loire Valley" },
  "Cheverny|France": { region: "Loire Valley" },
  "Cour-Cheverny|France": { region: "Loire Valley" },
  "Touraine|France": { region: "Loire Valley" },
  "Anjou|France": { region: "Loire Valley" },
  "Savennières|France": { region: "Loire Valley" },

  // Languedoc-Roussillon
  "Pays d'Hérault|France": { region: "Languedoc-Roussillon" },
  "Pays d'Oc|France": { region: "Languedoc-Roussillon" },
  "Côtes de Thongue|France": { region: "Languedoc-Roussillon" },
  "Duché d'Uzès|France": { region: "Languedoc-Roussillon" },
  "Gard|France": { region: "Languedoc-Roussillon" },
  "Aude|France": { region: "Languedoc-Roussillon" },
  "Vin de Pays du Gard|France": { region: "Languedoc-Roussillon" },
  "Picpoul de Pinet|France": { region: "Languedoc" },
  "Crémant de Limoux|France": { region: "Languedoc" },
  "Côtes Catalanes|France": { region: "Roussillon" },
  "Côtes du Roussillon Villages|France": { region: "Roussillon" },

  // Provence
  "Coteaux Varois en Provence|France": { region: "Provence" },
  "Bouches-du-Rhône|France": { region: "Provence" },
  "Var|France": { region: "Provence" },
  "Méditerranée|France": { region: "Provence" },
  "Alpilles|France": { region: "Provence" },
  "Coteaux d'Aix-en-Provence|France": { region: "Provence" },

  // Southwest France
  "Côtes de Gascogne|France": { region: "Southwest France" },
  "Côtes du Lot|France": { region: "Southwest France" },
  "Comté Tolosan|France": { region: "Southwest France" },
  "Gascogne|France": { region: "Southwest France" },

  // Corsica
  "Île de Beauté|France": { region: "Corsica" },

  // ═══ Italy ═══
  // Veneto
  "Verona|Italy": { region: "Veneto" },
  "Conegliano-Valdobbiadene Prosecco|Italy": { region: "Veneto" },
  "Conegliano-Valdobbiadene Prosecco Superiore|Italy": { region: "Veneto" },
  "Bianco di Custoza|Italy": { region: "Veneto" },
  "Venezia|Italy": { region: "Veneto" },
  "Rosso Veronese|Italy": { region: "Veneto" },
  "Garda|Italy": { region: "Veneto" },
  "Trevenezie|Italy": { region: "Veneto" },

  // Friuli-Venezia Giulia
  "Colli Orientali del Friuli|Italy": { region: "Friuli-Venezia Giulia" },
  "Friuli Isonzo|Italy": { region: "Friuli-Venezia Giulia" },
  "Venezia Giulia|Italy": { region: "Friuli-Venezia Giulia" },
  "Friuli Colli Orientali|Italy": { region: "Friuli-Venezia Giulia" },
  "Collio|Italy": { region: "Friuli-Venezia Giulia" },

  // Tuscany
  "Chianti Colli Senesi|Italy": { region: "Tuscany" },
  "Maremma Toscana|Italy": { region: "Tuscany" },
  "San Gimignano|Italy": { region: "Tuscany" },
  "Vin Santo del Chianti Classico|Italy": { region: "Tuscany" },
  "Vin Santo del Chianti|Italy": { region: "Tuscany" },
  "Chianti Rufina|Italy": { region: "Tuscany" },
  "Chianti Classico|Italy": { region: "Tuscany" },
  "Morellino di Scansano|Italy": { region: "Tuscany" },
  "Rosso di Montalcino|Italy": { region: "Tuscany" },
  "Rosso di Montepulciano|Italy": { region: "Tuscany" },

  // Piedmont
  "Monferrato|Italy": { region: "Piedmont" },
  "Langhe|Italy": { region: "Piedmont" },
  "Roero|Italy": { region: "Piedmont" },
  "Gavi|Italy": { region: "Piedmont" },
  "Dogliani|Italy": { region: "Piedmont" },

  // Trentino-Alto Adige
  "Teroldego Rotaliano|Italy": { region: "Trentino-Alto Adige" },
  "Vigneti delle Dolomiti|Italy": { region: "Trentino-Alto Adige" },
  "Valdadige|Italy": { region: "Trentino-Alto Adige" },

  // Lombardy
  "Valtellina|Italy": { region: "Lombardy" },
  "Provincia di Pavia|Italy": { region: "Lombardy" },
  "Oltrepò Pavese|Italy": { region: "Lombardy" },

  // Other Italian regions
  "Rubicone|Italy": { region: "Emilia-Romagna" },
  "Terre di Chieti|Italy": { region: "Abruzzo" },
  "Terre Siciliane|Italy": { region: "Sicily" },
  "Puglia|Italy": { region: "Puglia" },
  "Salento|Italy": { region: "Puglia" },
  "Brindisi|Italy": { region: "Puglia" },
  "Umbria|Italy": { region: "Umbria" },
  "Montefalco|Italy": { region: "Umbria" },
  "Lazio|Italy": { region: "Lazio" },
  "Basilicata|Italy": { region: "Basilicata" },
  "Riviera Ligure di Ponente|Italy": { region: "Liguria" },
  "Campania|Italy": { region: "Campania" },
  "Falanghina del Beneventano|Italy": { region: "Campania" },
  "Fiano di Avellino|Italy": { region: "Campania" },
  "Greco di Tufo|Italy": { region: "Campania" },
  "Marche|Italy": { region: "Marche" },

  // ═══ Spain ═══
  // Regions that exist in our table
  "Utiel-Requena|Spain": { region: "Valencia" },
  "Alicante|Spain": { region: "Valencia" },
  "Rioja Alta|Spain": { region: "Rioja" },
  "Rioja Alavesa|Spain": { region: "Rioja" },
  "Rioja Oriental|Spain": { region: "Rioja" },
  "Oloroso Sherry|Spain": { region: "Jerez" },
  "Manzanilla|Spain": { region: "Jerez" },
  "Fino Sherry|Spain": { region: "Jerez" },
  "Amontillado Sherry|Spain": { region: "Jerez" },
  "Palo Cortado Sherry|Spain": { region: "Jerez" },
  "Pedro Ximenez Sherry|Spain": { region: "Jerez" },
  "Costers del Segre|Spain": { region: "Catalonia" },
  "Tarragona|Spain": { region: "Catalonia" },
  "Empordà|Spain": { region: "Catalonia" },
  "Terra Alta|Spain": { region: "Catalonia" },
  "Tierra de Castilla y León|Spain": { region: "Castilla y León" },
  "Ribeira Sacra|Spain": { region: "Galicia" },
  "Valdeorras|Spain": { region: "Galicia" },
  "Tierra de Castilla|Spain": { region: "La Mancha" },
  "Castilla|Spain": { region: "La Mancha" },
  "Valdepeñas|Spain": { region: "La Mancha" },
  "Manchuela|Spain": { region: "La Mancha" },
  "Almansa|Spain": { region: "La Mancha" },

  // Spain — catch-all for regions without a match
  "Cariñena|Spain": { region: "Spain" },
  "Aragón|Spain": { region: "Spain" },
  "Madrid|Spain": { region: "Spain" },
  "Vinos de Madrid|Spain": { region: "Spain" },
  "Andalucía|Spain": { region: "Spain" },
  "Bullas|Spain": { region: "Spain" },
  "Múrcia|Spain": { region: "Spain" },
  "Lanzarote|Spain": { region: "Spain" },
  "Mallorca|Spain": { region: "Spain" },

  // ═══ Portugal ═══
  "Ribatejo|Portugal": { region: "Lisboa" },
  "Duriense|Portugal": { region: "Douro" },
  "Monção e Melgaço|Portugal": { region: "Vinho Verde" },
  "Beira Interior|Portugal": { region: "Dão" },
  "Terras do Dão|Portugal": { region: "Dão" },
  "Trás-os-Montes|Portugal": { region: "Douro" },
  "Terras de Cister|Portugal": { region: "Dão" },
  "Evora|Portugal": { region: "Alentejo" },
  "Algarve|Portugal": { region: "Portugal" },

  // ═══ Germany ═══
  "Brauneberg|Germany": { region: "Mosel" },
  "Bernkastel|Germany": { region: "Mosel" },
  "Piesport|Germany": { region: "Mosel" },
  "Trittenheim|Germany": { region: "Mosel" },
  "Graach|Germany": { region: "Mosel" },
  "Wehlen|Germany": { region: "Mosel" },
  "Ürzig|Germany": { region: "Mosel" },
  "Erden|Germany": { region: "Mosel" },
  "Rüdesheim|Germany": { region: "Rheingau" },
  "Nierstein|Germany": { region: "Rheinhessen" },
  "Wachenheim|Germany": { region: "Pfalz" },
  "Deidesheim|Germany": { region: "Pfalz" },
  "Forst|Germany": { region: "Pfalz" },
  "Ruppertsberg|Germany": { region: "Pfalz" },

  // ═══ Austria ═══
  // Regions that exist
  "Thermenregion|Austria": { region: "Thermenregion" },
  "Südsteiermark|Austria": { region: "Südsteiermark" },
  "Mittelburgenland|Austria": { region: "Burgenland" },
  "Südburgenland|Austria": { region: "Burgenland" },
  "Leithaberg|Austria": { region: "Burgenland" },
  "Neusiedlersee|Austria": { region: "Burgenland" },
  "Neusiedlersee-Hügelland|Austria": { region: "Burgenland" },
  "Eisenberg|Austria": { region: "Burgenland" },
  // Austrian regions without a match → catch-all
  "Wagram|Austria": { region: "Austria" },
  "Carnuntum|Austria": { region: "Austria" },
  "Traisental|Austria": { region: "Austria" },
  "Weinviertel|Austria": { region: "Austria" },
  "Steiermark|Austria": { region: "Austria" },
  "Weststeiermark|Austria": { region: "Austria" },
  "Vulkanland Steiermark|Austria": { region: "Austria" },
  "Weinland|Austria": { region: "Austria" },

  // ═══ United States ═══
  // Napa sub-AVAs
  "Mount Veeder|United States": { region: "Napa Valley" },
  "Howell Mountain|United States": { region: "Napa Valley" },
  "Diamond Mountain|United States": { region: "Napa Valley" },
  "Spring Mountain|United States": { region: "Napa Valley" },
  "Atlas Peak|United States": { region: "Napa Valley" },
  "Coombsville|United States": { region: "Napa Valley" },
  "Oak Knoll District|United States": { region: "Napa Valley" },
  "Calistoga|United States": { region: "Napa Valley" },
  "Yountville|United States": { region: "Napa Valley" },
  "St. Helena|United States": { region: "Napa Valley" },
  "Wild Horse Valley|United States": { region: "Napa Valley" },
  "Stags Leap District|United States": { region: "Napa Valley" },
  "Rutherford|United States": { region: "Napa Valley" },
  "Oakville|United States": { region: "Napa Valley" },
  "Carneros|United States": { region: "Napa Valley" },

  // Sonoma sub-AVAs
  "Rockpile|United States": { region: "Sonoma County" },
  "Bennett Valley|United States": { region: "Sonoma County" },
  "Moon Mountain|United States": { region: "Sonoma County" },
  "Fort Ross-Seaview|United States": { region: "Sonoma County" },
  "Petaluma Gap|United States": { region: "Sonoma County" },
  "Green Valley|United States": { region: "Sonoma County" },
  "Green Valley of Russian River Valley|United States": { region: "Sonoma County" },
  "Knights Valley|United States": { region: "Sonoma County" },
  "Alexander Valley|United States": { region: "Sonoma County" },
  "Dry Creek Valley|United States": { region: "Sonoma County" },
  "Russian River Valley|United States": { region: "Sonoma County" },
  "Sonoma Coast|United States": { region: "Sonoma County" },
  "Sonoma Valley|United States": { region: "Sonoma County" },
  "Chalk Hill|United States": { region: "Sonoma County" },

  // Central Coast & Santa Barbara
  "Santa Cruz Mountains|United States": { region: "Central Coast" },
  "San Benito County|United States": { region: "Central Coast" },
  "Lime Kiln Valley|United States": { region: "Central Coast" },
  "Edna Valley|United States": { region: "Central Coast" },
  "Arroyo Grande Valley|United States": { region: "Central Coast" },
  "York Mountain|United States": { region: "Central Coast" },
  "San Luis Obispo County|United States": { region: "Central Coast" },
  "Carmel Valley|United States": { region: "Central Coast" },
  "Santa Maria Valley|United States": { region: "Santa Barbara County" },
  "Happy Canyon|United States": { region: "Santa Barbara County" },
  "Ballard Canyon|United States": { region: "Santa Barbara County" },
  "Los Olivos|United States": { region: "Santa Barbara County" },
  "Santa Ynez Valley|United States": { region: "Santa Barbara County" },
  "Sta. Rita Hills|United States": { region: "Santa Barbara County" },

  // Other California
  "Temecula Valley|United States": { region: "California" },
  "Livermore Valley|United States": { region: "California" },
  "South Coast|United States": { region: "California" },
  "San Francisco Bay|United States": { region: "California" },
  "Lake County|United States": { region: "California" },
  "Clarksburg|United States": { region: "California" },
  "Contra Costa County|United States": { region: "California" },
  "Calaveras County|United States": { region: "Sierra Foothills" },
  "El Dorado|United States": { region: "Sierra Foothills" },
  "Arroyo Seco|United States": { region: "Monterey" },
  "North Fork of Long Island|United States": { region: "Long Island" },

  // Other US states
  "Columbia Valley Oregon|United States": { region: "Oregon" },
  "Umpqua Valley|United States": { region: "Oregon" },
  "Rogue Valley|United States": { region: "Oregon" },
  "Puget Sound|United States": { region: "Washington" },
  "Wahluke Slope|United States": { region: "Washington" },
  "Red Mountain|United States": { region: "Washington" },
  "Monticello|United States": { region: "Virginia" },
  "Texas|United States": { region: "Texas Hill Country" },
  "Texas High Plains|United States": { region: "Texas Hill Country" },
  // US states → catch-all
  "Pennsylvania|United States": { region: "United States" },
  "Illinois|United States": { region: "United States" },
  "New Jersey|United States": { region: "United States" },
  "Missouri|United States": { region: "United States" },
  "Arizona|United States": { region: "United States" },
  "Colorado|United States": { region: "United States" },
  "New Mexico|United States": { region: "United States" },
  "Massachusetts|United States": { region: "United States" },
  "Maryland|United States": { region: "United States" },
  "South Coast (US)|United States": { region: "United States" },

  // ═══ Australia ═══
  "Langhorne Creek|Australia": { region: "South Australia" },
  "Fleurieu|Australia": { region: "South Australia" },
  "Wrattonbully|Australia": { region: "South Australia" },
  "Padthaway|Australia": { region: "South Australia" },
  "Currency Creek|Australia": { region: "South Australia" },
  "Riverland|Australia": { region: "South Australia" },
  "Mount Lofty Ranges|Australia": { region: "South Australia" },
  "Limestone Coast|Australia": { region: "South Australia" },
  "South West Australia|Australia": { region: "Western Australia" },
  "Pemberton|Australia": { region: "Western Australia" },
  "Frankland River|Australia": { region: "Western Australia" },
  "Great Southern|Australia": { region: "Western Australia" },
  "Port Phillip|Australia": { region: "Victoria" },
  "King Valley|Australia": { region: "Victoria" },
  "Geelong|Australia": { region: "Victoria" },
  "Beechworth|Australia": { region: "Victoria" },
  "Goulburn Valley|Australia": { region: "Victoria" },
  "Central Victoria|Australia": { region: "Victoria" },
  "Pyrenees|Australia": { region: "Victoria" },
  "Macedon Ranges|Australia": { region: "Victoria" },
  "Orange|Australia": { region: "New South Wales" },
  "Tumbarumba|Australia": { region: "New South Wales" },
  "Mudgee|Australia": { region: "New South Wales" },
  "Central Ranges|Australia": { region: "New South Wales" },
  "Southern Highlands|Australia": { region: "New South Wales" },
  // Australia catch-all
  "Granite Belt|Australia": { region: "Australia" },
  "Murray Darling|Australia": { region: "Australia" },

  // ═══ South Africa ═══
  "Wellington|South Africa": { region: "Coastal Region" },
  "Darling|South Africa": { region: "Coastal Region" },
  "Durbanville|South Africa": { region: "Coastal Region" },
  "Tulbagh|South Africa": { region: "Coastal Region" },
  "Breede River Valley|South Africa": { region: "Western Cape" },
  "Robertson|South Africa": { region: "Western Cape" },
  "Overberg|South Africa": { region: "Western Cape" },
  "Calitzdorp|South Africa": { region: "Western Cape" },
  "Elgin|South Africa": { region: "Elgin" },
  "Bot River|South Africa": { region: "Walker Bay" },
  "Hemel-en-Aarde Valley|South Africa": { region: "Walker Bay" },

  // ═══ New Zealand ═══
  "Waipara Valley|New Zealand": { region: "Canterbury" },
  "Waipara|New Zealand": { region: "Canterbury" },
  "Gimblett Gravels|New Zealand": { region: "Hawke's Bay" },
  "Wairarapa|New Zealand": { region: "Wairarapa" },
  // NZ catch-all for broad regions
  "Auckland|New Zealand": { region: "New Zealand" },
  "Nelson|New Zealand": { region: "New Zealand" },
  "Gisborne|New Zealand": { region: "New Zealand" },
  "North Island|New Zealand": { region: "New Zealand" },
  "South Island|New Zealand": { region: "New Zealand" },

  // ═══ Chile ═══
  "Loncomilla Valley|Chile": { region: "Central Valley" },
  "Aconcagua|Chile": { region: "Aconcagua Valley" },
  "San Antonio Valley|Chile": { region: "Aconcagua Valley" },
  "San Antonio Valley (CL)|Chile": { region: "Aconcagua Valley" },
  // Chile catch-all for regions without a match
  "Limarí Valley|Chile": { region: "Chile" },
  "Elqui Valley|Chile": { region: "Chile" },
  "Itata Valley|Chile": { region: "Chile" },
  "Choapa Valley|Chile": { region: "Chile" },

  // ═══ Argentina ═══
  "Paraje Altamira|Argentina": { region: "Uco Valley" },
  "Vista Flores|Argentina": { region: "Uco Valley" },
  "Gualtallary|Argentina": { region: "Uco Valley" },
  "Altamira|Argentina": { region: "Uco Valley" },
  "Tupungato|Argentina": { region: "Uco Valley" },
  "San Carlos|Argentina": { region: "Uco Valley" },
  "La Consulta|Argentina": { region: "Uco Valley" },
  "Agrelo|Argentina": { region: "Mendoza" },
  "Las Compuertas|Argentina": { region: "Mendoza" },
  "Maipú|Argentina": { region: "Mendoza" },
  "San Rafael|Argentina": { region: "Mendoza" },
  "Luján de Cuyo|Argentina": { region: "Mendoza" },
  "Calchaqui Valley|Argentina": { region: "Salta" },
  "Cafayate|Argentina": { region: "Salta" },
  "Rio Negro|Argentina": { region: "Patagonia" },

  // ═══ Canada ═══
  "Ontario|Canada": { region: "Niagara Peninsula" },
  "Niagara Lakeshore|Canada": { region: "Niagara Peninsula" },
  "British Columbia|Canada": { region: "Okanagan Valley" },

  // ═══ Greece ═══
  "Chalkidiki|Greece": { region: "Macedonia" },
  "Drama|Greece": { region: "Macedonia" },
  "Crete|Greece": { region: "Crete" },
  // Greece catch-all for broad regions
  "Peloponnesos|Greece": { region: "Greece" },
  "Attiki|Greece": { region: "Greece" },
  "Atalanti|Greece": { region: "Greece" },

  // ═══ Switzerland ═══
  "Genève|Switzerland": { region: "Geneva" },
  "La Côte|Switzerland": { region: "Vaud" },
  "Lavaux|Switzerland": { region: "Vaud" },
  "Neuchâtel|Switzerland": { region: "Switzerland" },

  // ═══ Hungary ═══
  "Szekszárd|Hungary": { region: "Hungary" },

  // ═══ Croatia ═══
  "Srednja I Juzna Dalmacija|Croatia": { region: "Dalmatia" },

  // ═══ Turkey ═══
  "Thrace (TR)|Turkey": { region: "Thrace" },

  // ═══ Japan ═══
  "Yamanashi-ken|Japan": { region: "Yamanashi" },
  "Hokkaidō-ken|Japan": { region: "Hokkaido" },

  // ═══ Mexico ═══
  "Ensenada|Mexico": { region: "Valle de Guadalupe" },
  "Valle de Parras|Mexico": { region: "Mexico" },

  // ═══ Peru ═══
  "Ica|Peru": { region: "Ica Valley" },

  // ═══ Malta ═══
  "Gozo|Malta": { region: "Malta" },

  // ═══ Moldova ═══
  "South Eastern|Moldova": { region: "Moldova" },
  "Etulia|Moldova": { region: "Moldova" },

  // ═══ United Kingdom ═══
  "England|United Kingdom": { region: "United Kingdom" },

  // ═══ Uruguay ═══
  "Progreso|Uruguay": { region: "Canelones" },
  "Juanico|Uruguay": { region: "Canelones" },
  "San José|Uruguay": { region: "Canelones" },
  "Montevideo|Uruguay": { region: "Canelones" },
  "Cerro Chapeu|Uruguay": { region: "Uruguay" },

  // ═══ Brazil ═══
  "São Paulo|Brazil": { region: "Brazil" },
  "Santa Catarina|Brazil": { region: "Planalto Catarinense" },
  "Vale do São Francisco|Brazil": { region: "São Francisco Valley" },

  // ═══ Romania ═══
  "Vrancea|Romania": { region: "Romania" },
  "Dealurile Olteniei|Romania": { region: "Romania" },

  // ═══ Russia & Ukraine ═══
  "Crimeia (Крим)|Ukraine": { region: "Ukraine" },
  "Sennoy (Сенной)|Russia": { region: "Krasnodar" },
  "Taman Peninsula (Таманский полуостров)|Russia": { region: "Krasnodar" },

  // ═══ Bulgaria ═══
  "Melnik|Bulgaria": { region: "Thracian Valley" },
};

async function main() {
  console.log("Loading reference data...");

  // Load everything we need
  const regions = await fetchAll("regions", "id,name,slug,country_id,is_catch_all");
  const appellations = await fetchAll("appellations", "id,name,region_id,country_id,designation_type");
  const countries = await fetchAll("countries", "id,name");
  const existingMappings = await fetchAll("region_name_mappings", "region_name,country");

  const countryMap = new Map();
  for (const c of countries) {
    countryMap.set(c.name, c.id);
    countryMap.set(c.id, c.name);
  }

  // Region lookup: "name|country_id" → id, also "norm_name|country_id" → id
  const regionByName = new Map();
  const regionByNorm = new Map();
  const catchAllByCountryId = new Map();
  for (const r of regions) {
    regionByName.set(`${r.name}|${r.country_id}`, r.id);
    regionByNorm.set(`${normalize(r.name)}|${r.country_id}`, r.id);
    if (r.is_catch_all) catchAllByCountryId.set(r.country_id, r.id);
  }

  // Appellation lookup: "name|country_id" → {id, region_id}
  const appellByName = new Map();
  const appellByNorm = new Map();
  for (const a of appellations) {
    appellByName.set(`${a.name}|${a.country_id}`, { id: a.id, region_id: a.region_id });
    appellByNorm.set(`${normalize(a.name)}|${a.country_id}`, { id: a.id, region_id: a.region_id });
  }

  // Existing mappings set
  const existingSet = new Set(existingMappings.map((m) => `${m.region_name}|${m.country}`));

  console.log(`  ${regions.length} regions, ${appellations.length} appellations, ${existingMappings.length} existing mappings`);

  // Get unmapped region_names from wine_candidates
  const allCandidates = await fetchAll(
    "wine_candidates",
    "region_name,country"
  );

  // Count unique unmapped region_names
  const unmappedCounts = new Map(); // "region_name|country" → count
  for (const wc of allCandidates) {
    if (!wc.region_name) continue;
    const key = `${wc.region_name}|${wc.country}`;
    if (existingSet.has(key)) continue;
    unmappedCounts.set(key, (unmappedCounts.get(key) || 0) + 1);
  }

  console.log(`  ${unmappedCounts.size} unmapped region_name|country combos (${[...unmappedCounts.values()].reduce((a, b) => a + b, 0)} wines)\n`);

  // Build new mappings
  const newMappings = []; // {region_name, country, region_id, appellation_id, match_type}
  let matchedByAppellation = 0;
  let matchedByAppellationNorm = 0;
  let matchedByRegion = 0;
  let matchedBySubAppellation = 0;
  let matchedByManual = 0;
  let unmatched = 0;

  // Sort by wine count descending for reporting
  const sortedUnmapped = [...unmappedCounts.entries()].sort((a, b) => b[1] - a[1]);

  for (const [key, wineCount] of sortedUnmapped) {
    const [regionName, country] = [key.slice(0, key.lastIndexOf("|")), key.slice(key.lastIndexOf("|") + 1)];
    const countryId = countryMap.get(country);
    if (!countryId) {
      unmatched++;
      continue;
    }

    let regionId = null;
    let appellationId = null;
    let matchType = null;

    // Strategy 1: Exact appellation match
    const appellExact = appellByName.get(`${regionName}|${countryId}`);
    if (appellExact) {
      regionId = appellExact.region_id;
      appellationId = appellExact.id;
      matchType = "appellation_exact";
      matchedByAppellation++;
    }

    // Strategy 2: Normalized appellation match
    if (!matchType) {
      const appellNorm = appellByNorm.get(`${normalize(regionName)}|${countryId}`);
      if (appellNorm) {
        regionId = appellNorm.region_id;
        appellationId = appellNorm.id;
        matchType = "appellation_norm";
        matchedByAppellationNorm++;
      }
    }

    // Strategy 3: French/Italian sub-appellation patterns
    // "Chablis 1er Cru 'Montmains'" → try "Chablis"
    // "Amarone della Valpolicella Classico" → try "Amarone della Valpolicella"
    // "Valpolicella Classico" → try "Valpolicella"
    // "Bardolino Classico" → try "Bardolino"
    // "Prosecco di Treviso" → try "Prosecco"
    if (!matchType) {
      let baseName = regionName;

      // Strip French vineyard specifics
      baseName = baseName.replace(/\s+1er\s+Cru\s+'[^']*'$/i, "");
      baseName = baseName.replace(/\s+Grand\s+Cru\s+'[^']*'$/i, "");
      baseName = baseName.replace(/\s+1er\s+Cru\s+"[^"]*"$/i, "");
      baseName = baseName.replace(/\s+Grand\s+Cru\s+"[^"]*"$/i, "");
      baseName = baseName.replace(/\s+1er\s+Cru$/i, "");
      baseName = baseName.replace(/\s+Grand\s+Cru$/i, "");

      // Strip Italian suffixes
      baseName = baseName.replace(/\s+Classico\s+Superiore$/i, "");
      baseName = baseName.replace(/\s+Classico$/i, "");
      baseName = baseName.replace(/\s+Superiore$/i, "");
      baseName = baseName.replace(/\s+Riserva$/i, "");
      baseName = baseName.replace(/\s+Ripasso\s+Classico$/i, "");
      baseName = baseName.replace(/\s+Ripasso$/i, "");
      baseName = baseName.replace(/\s+Chiaretto$/i, "");

      // Strip "di X" / "de X" regional qualifiers for Italian
      const diMatch = baseName.match(/^(.+?)\s+di\s+\w+$/i);

      if (baseName !== regionName) {
        // Try the stripped version
        const stripped = appellByName.get(`${baseName}|${countryId}`) || appellByNorm.get(`${normalize(baseName)}|${countryId}`);
        if (stripped) {
          regionId = stripped.region_id;
          appellationId = stripped.id;
          matchType = "sub_appellation";
          matchedBySubAppellation++;
        }
      }

      // For "Prosecco di Treviso" → "Prosecco"
      if (!matchType && diMatch) {
        const diBase = diMatch[1];
        const diStripped = appellByName.get(`${diBase}|${countryId}`) || appellByNorm.get(`${normalize(diBase)}|${countryId}`);
        if (diStripped) {
          regionId = diStripped.region_id;
          appellationId = diStripped.id;
          matchType = "sub_appellation";
          matchedBySubAppellation++;
        }
      }
    }

    // Strategy 4: Exact region match
    if (!matchType) {
      const regExact = regionByName.get(`${regionName}|${countryId}`);
      if (regExact) {
        regionId = regExact;
        matchType = "region_exact";
        matchedByRegion++;
      }
    }

    // Strategy 4b: Normalized region match
    if (!matchType) {
      const regNorm = regionByNorm.get(`${normalize(regionName)}|${countryId}`);
      if (regNorm) {
        regionId = regNorm;
        matchType = "region_norm";
        matchedByRegion++;
      }
    }

    // Strategy 5: Manual mapping
    if (!matchType) {
      const manual = MANUAL_REGION_ALIASES[`${regionName}|${country}`];
      if (manual) {
        // Resolve the region name to ID
        regionId = regionByName.get(`${manual.region}|${countryId}`) || regionByNorm.get(`${normalize(manual.region)}|${countryId}`);
        if (manual.appellation) {
          const app = appellByName.get(`${manual.appellation}|${countryId}`) || appellByNorm.get(`${normalize(manual.appellation)}|${countryId}`);
          if (app) appellationId = app.id;
        }
        if (regionId) {
          matchType = "manual";
          matchedByManual++;
        }
      }
    }

    if (matchType) {
      newMappings.push({
        region_name: regionName,
        country,
        region_id: regionId,
        appellation_id: appellationId,
        match_type: matchType,
      });
    } else {
      if (wineCount >= 20) {
        console.log(`  UNMATCHED (${wineCount}w): "${regionName}" [${country}]`);
      }
      unmatched++;
    }
  }

  console.log(`\n── Match Results ──`);
  console.log(`  Appellation exact:    ${matchedByAppellation}`);
  console.log(`  Appellation norm:     ${matchedByAppellationNorm}`);
  console.log(`  Sub-appellation:      ${matchedBySubAppellation}`);
  console.log(`  Region exact/norm:    ${matchedByRegion}`);
  console.log(`  Manual mapping:       ${matchedByManual}`);
  console.log(`  Total matched:        ${newMappings.length}`);
  console.log(`  Unmatched:            ${unmatched}`);

  const winesMatched = newMappings.reduce((sum, m) => sum + unmappedCounts.get(`${m.region_name}|${m.country}`), 0);
  console.log(`  Wines covered by new mappings: ${winesMatched}`);

  if (DRY_RUN) {
    console.log(`\nDRY RUN — no database changes made.`);

    // Show sample mappings by type
    for (const type of ["appellation_exact", "appellation_norm", "sub_appellation", "region_exact", "region_norm", "manual"]) {
      const samples = newMappings.filter((m) => m.match_type === type).slice(0, 5);
      if (samples.length > 0) {
        console.log(`\n  ${type} samples:`);
        for (const m of samples) {
          const wc = unmappedCounts.get(`${m.region_name}|${m.country}`);
          const regName = regions.find((r) => r.id === m.region_id)?.name || "?";
          const appName = m.appellation_id ? appellations.find((a) => a.id === m.appellation_id)?.name || "?" : "-";
          console.log(`    "${m.region_name}" [${m.country}] → region: ${regName}, appellation: ${appName} (${wc}w)`);
        }
      }
    }
    return;
  }

  // ── Map internal match types to DB-allowed values ────────
  // DB constraint: exact_region, exact_appellation, alias, parent_rollup, catch_all
  const DB_MATCH_TYPE = {
    appellation_exact: "exact_appellation",
    appellation_norm: "exact_appellation",
    sub_appellation: "alias",
    region_exact: "exact_region",
    region_norm: "exact_region",
    manual: "alias",
  };
  for (const m of newMappings) {
    m.match_type = DB_MATCH_TYPE[m.match_type] || "alias";
  }

  // ── Insert new mappings ───────────────────────────────────
  console.log(`\nInserting ${newMappings.length} new region_name_mappings...`);
  let insertErrors = 0;
  for (let i = 0; i < newMappings.length; i += 500) {
    const batch = newMappings.slice(i, i + 500);
    const { error } = await sb.from("region_name_mappings").insert(batch);
    if (error) {
      console.error(`  Batch error at ${i}: ${error.message}`);
      insertErrors++;
    }
  }
  console.log(`  Done inserting ${newMappings.length} mappings (${insertErrors} errors).`);
  console.log(`\n✅ Mappings inserted! Use MCP SQL to update wines with new region_id / appellation_id.`);
}

main().catch((err) => {
  console.error("FATAL:", err);
  process.exit(1);
});
