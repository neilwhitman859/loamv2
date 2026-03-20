"""
Expand region_name_mappings to cover more wine_candidates region_names.

5-strategy matching:
  1. Exact match unmapped region_name to appellation.name
  2. Normalized match (strip accents, lowercase) to appellation.name
  3. Sub-appellation patterns (French 1er Cru/Grand Cru, Italian Classico/Superiore)
  4. Exact/normalized region match
  5. Manual mapping via MANUAL_REGION_ALIASES dict

Usage:
    python -m pipeline.reference.expand_region_mappings --dry-run
    python -m pipeline.reference.expand_region_mappings
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all
from pipeline.lib.normalize import normalize


# ── Manual mappings for well-known alternative names ────────────────
# Key: "region_name|Country" -> { region: "Region Name", appellation?: "Appellation Name" }

MANUAL_REGION_ALIASES: dict[str, dict] = {
    # === France ===
    # Bordeaux satellites & sub-regions
    "Cotes de Bourg|France": {"region": "Bordeaux"},
    "Castillon-Cotes de Bordeaux|France": {"region": "Bordeaux"},
    "Puisseguin-Saint-Emilion|France": {"region": "Bordeaux"},
    "Blaye-Cotes de Bordeaux|France": {"region": "Bordeaux"},
    "Montagne-Saint-Emilion|France": {"region": "Bordeaux"},
    "Lussac-Saint-Emilion|France": {"region": "Bordeaux"},
    "Cotes de Bordeaux|France": {"region": "Bordeaux"},
    "Guyenne|France": {"region": "Bordeaux"},
    "Francs-Cotes de Bordeaux|France": {"region": "Bordeaux"},
    "Cadillac-Cotes de Bordeaux|France": {"region": "Bordeaux"},
    "Sainte-Foy-Bordeaux|France": {"region": "Bordeaux"},
    "Premieres Cotes de Bordeaux|France": {"region": "Bordeaux"},

    # Rhone Valley
    "Ventoux|France": {"region": "Southern Rhone"},
    "Luberon|France": {"region": "Southern Rhone"},
    "Costieres-de-Nimes|France": {"region": "Southern Rhone"},
    "Cotes-du-Rhone-Villages|France": {"region": "Southern Rhone"},
    "Cotes-du-Rhone-Villages 'Plan de Dieu'|France": {"region": "Southern Rhone"},
    "Cotes-du-Rhone-Villages 'Cairanne'|France": {"region": "Southern Rhone"},
    "Cotes-du-Rhone-Villages 'Seguret'|France": {"region": "Southern Rhone"},
    "Cotes-du-Rhone-Villages 'Sablet'|France": {"region": "Southern Rhone"},
    "Vaucluse|France": {"region": "Rhone Valley"},
    "Collines Rhodaniennes|France": {"region": "Rhone Valley"},
    "Drome|France": {"region": "Rhone Valley"},
    "Ardeche|France": {"region": "Rhone Valley"},

    # Burgundy
    "Bourgogne Hautes-Cotes de Beaune|France": {"region": "Burgundy"},
    "Bourgogne Hautes-Cotes de Nuits|France": {"region": "Burgundy"},
    "Saint-Aubin|France": {"region": "Burgundy"},
    "Vire-Clesse|France": {"region": "Burgundy"},
    "Cote de Nuits Villages|France": {"region": "Burgundy"},
    "Cote Chalonnaise|France": {"region": "Burgundy"},
    "Saint-Romain|France": {"region": "Burgundy"},
    "Cote de Beaune|France": {"region": "Burgundy"},
    "Cote de Beaune-Villages|France": {"region": "Burgundy"},
    "Macon-Villages|France": {"region": "Burgundy"},
    "Bourgogne Aligote|France": {"region": "Burgundy"},
    "Hautes-Cotes de Beaune|France": {"region": "Burgundy"},
    "Hautes-Cotes de Nuits|France": {"region": "Burgundy"},
    "Auxey-Duresses|France": {"region": "Burgundy"},
    "Montagny|France": {"region": "Burgundy"},
    "Rully|France": {"region": "Burgundy"},
    "Bouzeron|France": {"region": "Burgundy"},
    "Givry|France": {"region": "Burgundy"},
    "Mercurey|France": {"region": "Burgundy"},

    # Loire Valley
    "Muscadet-Sevre et Maine|France": {"region": "Loire Valley"},
    "Muscadet-Sevre et Maine|France": {"region": "Loire Valley"},
    "Pays Nantais|France": {"region": "Loire Valley"},
    "Upper Loire|France": {"region": "Loire Valley"},
    "Rose d'Anjou|France": {"region": "Loire Valley"},
    "Haut-Poitou|France": {"region": "Loire Valley"},
    "Val de Loire|France": {"region": "Loire Valley"},
    "Cheverny|France": {"region": "Loire Valley"},
    "Cour-Cheverny|France": {"region": "Loire Valley"},
    "Touraine|France": {"region": "Loire Valley"},
    "Anjou|France": {"region": "Loire Valley"},
    "Savennieres|France": {"region": "Loire Valley"},

    # Languedoc-Roussillon
    "Pays d'Herault|France": {"region": "Languedoc-Roussillon"},
    "Pays d'Oc|France": {"region": "Languedoc-Roussillon"},
    "Cotes de Thongue|France": {"region": "Languedoc-Roussillon"},
    "Duche d'Uzes|France": {"region": "Languedoc-Roussillon"},
    "Gard|France": {"region": "Languedoc-Roussillon"},
    "Aude|France": {"region": "Languedoc-Roussillon"},
    "Vin de Pays du Gard|France": {"region": "Languedoc-Roussillon"},
    "Picpoul de Pinet|France": {"region": "Languedoc"},
    "Cremant de Limoux|France": {"region": "Languedoc"},
    "Cotes Catalanes|France": {"region": "Roussillon"},
    "Cotes du Roussillon Villages|France": {"region": "Roussillon"},

    # Provence
    "Coteaux Varois en Provence|France": {"region": "Provence"},
    "Bouches-du-Rhone|France": {"region": "Provence"},
    "Var|France": {"region": "Provence"},
    "Mediterranee|France": {"region": "Provence"},
    "Alpilles|France": {"region": "Provence"},
    "Coteaux d'Aix-en-Provence|France": {"region": "Provence"},

    # Southwest France
    "Cotes de Gascogne|France": {"region": "Southwest France"},
    "Cotes du Lot|France": {"region": "Southwest France"},
    "Comte Tolosan|France": {"region": "Southwest France"},
    "Gascogne|France": {"region": "Southwest France"},

    # Corsica
    "Ile de Beaute|France": {"region": "Corsica"},

    # === Italy ===
    # Veneto
    "Verona|Italy": {"region": "Veneto"},
    "Conegliano-Valdobbiadene Prosecco|Italy": {"region": "Veneto"},
    "Conegliano-Valdobbiadene Prosecco Superiore|Italy": {"region": "Veneto"},
    "Bianco di Custoza|Italy": {"region": "Veneto"},
    "Venezia|Italy": {"region": "Veneto"},
    "Rosso Veronese|Italy": {"region": "Veneto"},
    "Garda|Italy": {"region": "Veneto"},
    "Trevenezie|Italy": {"region": "Veneto"},

    # Friuli-Venezia Giulia
    "Colli Orientali del Friuli|Italy": {"region": "Friuli-Venezia Giulia"},
    "Friuli Isonzo|Italy": {"region": "Friuli-Venezia Giulia"},
    "Venezia Giulia|Italy": {"region": "Friuli-Venezia Giulia"},
    "Friuli Colli Orientali|Italy": {"region": "Friuli-Venezia Giulia"},
    "Collio|Italy": {"region": "Friuli-Venezia Giulia"},

    # Tuscany
    "Chianti Colli Senesi|Italy": {"region": "Tuscany"},
    "Maremma Toscana|Italy": {"region": "Tuscany"},
    "San Gimignano|Italy": {"region": "Tuscany"},
    "Vin Santo del Chianti Classico|Italy": {"region": "Tuscany"},
    "Vin Santo del Chianti|Italy": {"region": "Tuscany"},
    "Chianti Rufina|Italy": {"region": "Tuscany"},
    "Chianti Classico|Italy": {"region": "Tuscany"},
    "Morellino di Scansano|Italy": {"region": "Tuscany"},
    "Rosso di Montalcino|Italy": {"region": "Tuscany"},
    "Rosso di Montepulciano|Italy": {"region": "Tuscany"},

    # Piedmont
    "Monferrato|Italy": {"region": "Piedmont"},
    "Langhe|Italy": {"region": "Piedmont"},
    "Roero|Italy": {"region": "Piedmont"},
    "Gavi|Italy": {"region": "Piedmont"},
    "Dogliani|Italy": {"region": "Piedmont"},

    # Trentino-Alto Adige
    "Teroldego Rotaliano|Italy": {"region": "Trentino-Alto Adige"},
    "Vigneti delle Dolomiti|Italy": {"region": "Trentino-Alto Adige"},
    "Valdadige|Italy": {"region": "Trentino-Alto Adige"},

    # Lombardy
    "Valtellina|Italy": {"region": "Lombardy"},
    "Provincia di Pavia|Italy": {"region": "Lombardy"},
    "Oltrepo Pavese|Italy": {"region": "Lombardy"},

    # Other Italian regions
    "Rubicone|Italy": {"region": "Emilia-Romagna"},
    "Terre di Chieti|Italy": {"region": "Abruzzo"},
    "Terre Siciliane|Italy": {"region": "Sicily"},
    "Puglia|Italy": {"region": "Puglia"},
    "Salento|Italy": {"region": "Puglia"},
    "Brindisi|Italy": {"region": "Puglia"},
    "Umbria|Italy": {"region": "Umbria"},
    "Montefalco|Italy": {"region": "Umbria"},
    "Lazio|Italy": {"region": "Lazio"},
    "Basilicata|Italy": {"region": "Basilicata"},
    "Riviera Ligure di Ponente|Italy": {"region": "Liguria"},
    "Campania|Italy": {"region": "Campania"},
    "Falanghina del Beneventano|Italy": {"region": "Campania"},
    "Fiano di Avellino|Italy": {"region": "Campania"},
    "Greco di Tufo|Italy": {"region": "Campania"},
    "Marche|Italy": {"region": "Marche"},

    # === Spain ===
    "Utiel-Requena|Spain": {"region": "Valencia"},
    "Alicante|Spain": {"region": "Valencia"},
    "Rioja Alta|Spain": {"region": "Rioja"},
    "Rioja Alavesa|Spain": {"region": "Rioja"},
    "Rioja Oriental|Spain": {"region": "Rioja"},
    "Oloroso Sherry|Spain": {"region": "Jerez"},
    "Manzanilla|Spain": {"region": "Jerez"},
    "Fino Sherry|Spain": {"region": "Jerez"},
    "Amontillado Sherry|Spain": {"region": "Jerez"},
    "Palo Cortado Sherry|Spain": {"region": "Jerez"},
    "Pedro Ximenez Sherry|Spain": {"region": "Jerez"},
    "Costers del Segre|Spain": {"region": "Catalonia"},
    "Tarragona|Spain": {"region": "Catalonia"},
    "Emporda|Spain": {"region": "Catalonia"},
    "Terra Alta|Spain": {"region": "Catalonia"},
    "Tierra de Castilla y Leon|Spain": {"region": "Castilla y Leon"},
    "Ribeira Sacra|Spain": {"region": "Galicia"},
    "Valdeorras|Spain": {"region": "Galicia"},
    "Tierra de Castilla|Spain": {"region": "La Mancha"},
    "Castilla|Spain": {"region": "La Mancha"},
    "Valdepenas|Spain": {"region": "La Mancha"},
    "Manchuela|Spain": {"region": "La Mancha"},
    "Almansa|Spain": {"region": "La Mancha"},
    # Spain catch-all
    "Carinena|Spain": {"region": "Spain"},
    "Aragon|Spain": {"region": "Spain"},
    "Madrid|Spain": {"region": "Spain"},
    "Vinos de Madrid|Spain": {"region": "Spain"},
    "Andalucia|Spain": {"region": "Spain"},
    "Bullas|Spain": {"region": "Spain"},
    "Murcia|Spain": {"region": "Spain"},
    "Lanzarote|Spain": {"region": "Spain"},
    "Mallorca|Spain": {"region": "Spain"},

    # === Portugal ===
    "Ribatejo|Portugal": {"region": "Lisboa"},
    "Duriense|Portugal": {"region": "Douro"},
    "Moncao e Melgaco|Portugal": {"region": "Vinho Verde"},
    "Beira Interior|Portugal": {"region": "Dao"},
    "Terras do Dao|Portugal": {"region": "Dao"},
    "Tras-os-Montes|Portugal": {"region": "Douro"},
    "Terras de Cister|Portugal": {"region": "Dao"},
    "Evora|Portugal": {"region": "Alentejo"},
    "Algarve|Portugal": {"region": "Portugal"},

    # === Germany ===
    "Brauneberg|Germany": {"region": "Mosel"},
    "Bernkastel|Germany": {"region": "Mosel"},
    "Piesport|Germany": {"region": "Mosel"},
    "Trittenheim|Germany": {"region": "Mosel"},
    "Graach|Germany": {"region": "Mosel"},
    "Wehlen|Germany": {"region": "Mosel"},
    "Urzig|Germany": {"region": "Mosel"},
    "Erden|Germany": {"region": "Mosel"},
    "Rudesheim|Germany": {"region": "Rheingau"},
    "Nierstein|Germany": {"region": "Rheinhessen"},
    "Wachenheim|Germany": {"region": "Pfalz"},
    "Deidesheim|Germany": {"region": "Pfalz"},
    "Forst|Germany": {"region": "Pfalz"},
    "Ruppertsberg|Germany": {"region": "Pfalz"},

    # === Austria ===
    "Thermenregion|Austria": {"region": "Thermenregion"},
    "Sudsteiermark|Austria": {"region": "Sudsteiermark"},
    "Mittelburgenland|Austria": {"region": "Burgenland"},
    "Sudburgenland|Austria": {"region": "Burgenland"},
    "Leithaberg|Austria": {"region": "Burgenland"},
    "Neusiedlersee|Austria": {"region": "Burgenland"},
    "Neusiedlersee-Hugelland|Austria": {"region": "Burgenland"},
    "Eisenberg|Austria": {"region": "Burgenland"},
    # Austria catch-all
    "Wagram|Austria": {"region": "Austria"},
    "Carnuntum|Austria": {"region": "Austria"},
    "Traisental|Austria": {"region": "Austria"},
    "Weinviertel|Austria": {"region": "Austria"},
    "Steiermark|Austria": {"region": "Austria"},
    "Weststeiermark|Austria": {"region": "Austria"},
    "Vulkanland Steiermark|Austria": {"region": "Austria"},
    "Weinland|Austria": {"region": "Austria"},

    # === United States ===
    # Napa sub-AVAs
    "Mount Veeder|United States": {"region": "Napa Valley"},
    "Howell Mountain|United States": {"region": "Napa Valley"},
    "Diamond Mountain|United States": {"region": "Napa Valley"},
    "Spring Mountain|United States": {"region": "Napa Valley"},
    "Atlas Peak|United States": {"region": "Napa Valley"},
    "Coombsville|United States": {"region": "Napa Valley"},
    "Oak Knoll District|United States": {"region": "Napa Valley"},
    "Calistoga|United States": {"region": "Napa Valley"},
    "Yountville|United States": {"region": "Napa Valley"},
    "St. Helena|United States": {"region": "Napa Valley"},
    "Wild Horse Valley|United States": {"region": "Napa Valley"},
    "Stags Leap District|United States": {"region": "Napa Valley"},
    "Rutherford|United States": {"region": "Napa Valley"},
    "Oakville|United States": {"region": "Napa Valley"},
    "Carneros|United States": {"region": "Napa Valley"},

    # Sonoma sub-AVAs
    "Rockpile|United States": {"region": "Sonoma County"},
    "Bennett Valley|United States": {"region": "Sonoma County"},
    "Moon Mountain|United States": {"region": "Sonoma County"},
    "Fort Ross-Seaview|United States": {"region": "Sonoma County"},
    "Petaluma Gap|United States": {"region": "Sonoma County"},
    "Green Valley|United States": {"region": "Sonoma County"},
    "Green Valley of Russian River Valley|United States": {"region": "Sonoma County"},
    "Knights Valley|United States": {"region": "Sonoma County"},
    "Alexander Valley|United States": {"region": "Sonoma County"},
    "Dry Creek Valley|United States": {"region": "Sonoma County"},
    "Russian River Valley|United States": {"region": "Sonoma County"},
    "Sonoma Coast|United States": {"region": "Sonoma County"},
    "Sonoma Valley|United States": {"region": "Sonoma County"},
    "Chalk Hill|United States": {"region": "Sonoma County"},

    # Central Coast & Santa Barbara
    "Santa Cruz Mountains|United States": {"region": "Central Coast"},
    "San Benito County|United States": {"region": "Central Coast"},
    "Lime Kiln Valley|United States": {"region": "Central Coast"},
    "Edna Valley|United States": {"region": "Central Coast"},
    "Arroyo Grande Valley|United States": {"region": "Central Coast"},
    "York Mountain|United States": {"region": "Central Coast"},
    "San Luis Obispo County|United States": {"region": "Central Coast"},
    "Carmel Valley|United States": {"region": "Central Coast"},
    "Santa Maria Valley|United States": {"region": "Santa Barbara County"},
    "Happy Canyon|United States": {"region": "Santa Barbara County"},
    "Ballard Canyon|United States": {"region": "Santa Barbara County"},
    "Los Olivos|United States": {"region": "Santa Barbara County"},
    "Santa Ynez Valley|United States": {"region": "Santa Barbara County"},
    "Sta. Rita Hills|United States": {"region": "Santa Barbara County"},

    # Other California
    "Temecula Valley|United States": {"region": "California"},
    "Livermore Valley|United States": {"region": "California"},
    "South Coast|United States": {"region": "California"},
    "San Francisco Bay|United States": {"region": "California"},
    "Lake County|United States": {"region": "California"},
    "Clarksburg|United States": {"region": "California"},
    "Contra Costa County|United States": {"region": "California"},
    "Calaveras County|United States": {"region": "Sierra Foothills"},
    "El Dorado|United States": {"region": "Sierra Foothills"},
    "Arroyo Seco|United States": {"region": "Monterey"},
    "North Fork of Long Island|United States": {"region": "Long Island"},

    # Other US states
    "Columbia Valley Oregon|United States": {"region": "Oregon"},
    "Umpqua Valley|United States": {"region": "Oregon"},
    "Rogue Valley|United States": {"region": "Oregon"},
    "Puget Sound|United States": {"region": "Washington"},
    "Wahluke Slope|United States": {"region": "Washington"},
    "Red Mountain|United States": {"region": "Washington"},
    "Monticello|United States": {"region": "Virginia"},
    "Texas|United States": {"region": "Texas Hill Country"},
    "Texas High Plains|United States": {"region": "Texas Hill Country"},
    # US states catch-all
    "Pennsylvania|United States": {"region": "United States"},
    "Illinois|United States": {"region": "United States"},
    "New Jersey|United States": {"region": "United States"},
    "Missouri|United States": {"region": "United States"},
    "Arizona|United States": {"region": "United States"},
    "Colorado|United States": {"region": "United States"},
    "New Mexico|United States": {"region": "United States"},
    "Massachusetts|United States": {"region": "United States"},
    "Maryland|United States": {"region": "United States"},
    "South Coast (US)|United States": {"region": "United States"},

    # === Australia ===
    "Langhorne Creek|Australia": {"region": "South Australia"},
    "Fleurieu|Australia": {"region": "South Australia"},
    "Wrattonbully|Australia": {"region": "South Australia"},
    "Padthaway|Australia": {"region": "South Australia"},
    "Currency Creek|Australia": {"region": "South Australia"},
    "Riverland|Australia": {"region": "South Australia"},
    "Mount Lofty Ranges|Australia": {"region": "South Australia"},
    "Limestone Coast|Australia": {"region": "South Australia"},
    "South West Australia|Australia": {"region": "Western Australia"},
    "Pemberton|Australia": {"region": "Western Australia"},
    "Frankland River|Australia": {"region": "Western Australia"},
    "Great Southern|Australia": {"region": "Western Australia"},
    "Port Phillip|Australia": {"region": "Victoria"},
    "King Valley|Australia": {"region": "Victoria"},
    "Geelong|Australia": {"region": "Victoria"},
    "Beechworth|Australia": {"region": "Victoria"},
    "Goulburn Valley|Australia": {"region": "Victoria"},
    "Central Victoria|Australia": {"region": "Victoria"},
    "Pyrenees|Australia": {"region": "Victoria"},
    "Macedon Ranges|Australia": {"region": "Victoria"},
    "Orange|Australia": {"region": "New South Wales"},
    "Tumbarumba|Australia": {"region": "New South Wales"},
    "Mudgee|Australia": {"region": "New South Wales"},
    "Central Ranges|Australia": {"region": "New South Wales"},
    "Southern Highlands|Australia": {"region": "New South Wales"},
    # Australia catch-all
    "Granite Belt|Australia": {"region": "Australia"},
    "Murray Darling|Australia": {"region": "Australia"},

    # === South Africa ===
    "Wellington|South Africa": {"region": "Coastal Region"},
    "Darling|South Africa": {"region": "Coastal Region"},
    "Durbanville|South Africa": {"region": "Coastal Region"},
    "Tulbagh|South Africa": {"region": "Coastal Region"},
    "Breede River Valley|South Africa": {"region": "Western Cape"},
    "Robertson|South Africa": {"region": "Western Cape"},
    "Overberg|South Africa": {"region": "Western Cape"},
    "Calitzdorp|South Africa": {"region": "Western Cape"},
    "Elgin|South Africa": {"region": "Elgin"},
    "Bot River|South Africa": {"region": "Walker Bay"},
    "Hemel-en-Aarde Valley|South Africa": {"region": "Walker Bay"},

    # === New Zealand ===
    "Waipara Valley|New Zealand": {"region": "Canterbury"},
    "Waipara|New Zealand": {"region": "Canterbury"},
    "Gimblett Gravels|New Zealand": {"region": "Hawke's Bay"},
    "Wairarapa|New Zealand": {"region": "Wairarapa"},
    # NZ catch-all
    "Auckland|New Zealand": {"region": "New Zealand"},
    "Nelson|New Zealand": {"region": "New Zealand"},
    "Gisborne|New Zealand": {"region": "New Zealand"},
    "North Island|New Zealand": {"region": "New Zealand"},
    "South Island|New Zealand": {"region": "New Zealand"},

    # === Chile ===
    "Loncomilla Valley|Chile": {"region": "Central Valley"},
    "Aconcagua|Chile": {"region": "Aconcagua Valley"},
    "San Antonio Valley|Chile": {"region": "Aconcagua Valley"},
    "San Antonio Valley (CL)|Chile": {"region": "Aconcagua Valley"},
    # Chile catch-all
    "Limari Valley|Chile": {"region": "Chile"},
    "Elqui Valley|Chile": {"region": "Chile"},
    "Itata Valley|Chile": {"region": "Chile"},
    "Choapa Valley|Chile": {"region": "Chile"},

    # === Argentina ===
    "Paraje Altamira|Argentina": {"region": "Uco Valley"},
    "Vista Flores|Argentina": {"region": "Uco Valley"},
    "Gualtallary|Argentina": {"region": "Uco Valley"},
    "Altamira|Argentina": {"region": "Uco Valley"},
    "Tupungato|Argentina": {"region": "Uco Valley"},
    "San Carlos|Argentina": {"region": "Uco Valley"},
    "La Consulta|Argentina": {"region": "Uco Valley"},
    "Agrelo|Argentina": {"region": "Mendoza"},
    "Las Compuertas|Argentina": {"region": "Mendoza"},
    "Maipu|Argentina": {"region": "Mendoza"},
    "San Rafael|Argentina": {"region": "Mendoza"},
    "Lujan de Cuyo|Argentina": {"region": "Mendoza"},
    "Calchaqui Valley|Argentina": {"region": "Salta"},
    "Cafayate|Argentina": {"region": "Salta"},
    "Rio Negro|Argentina": {"region": "Patagonia"},

    # === Canada ===
    "Ontario|Canada": {"region": "Niagara Peninsula"},
    "Niagara Lakeshore|Canada": {"region": "Niagara Peninsula"},
    "British Columbia|Canada": {"region": "Okanagan Valley"},

    # === Greece ===
    "Chalkidiki|Greece": {"region": "Macedonia"},
    "Drama|Greece": {"region": "Macedonia"},
    "Crete|Greece": {"region": "Crete"},
    # Greece catch-all
    "Peloponnesos|Greece": {"region": "Greece"},
    "Attiki|Greece": {"region": "Greece"},
    "Atalanti|Greece": {"region": "Greece"},

    # === Switzerland ===
    "Geneve|Switzerland": {"region": "Geneva"},
    "La Cote|Switzerland": {"region": "Vaud"},
    "Lavaux|Switzerland": {"region": "Vaud"},
    "Neuchatel|Switzerland": {"region": "Switzerland"},

    # === Hungary ===
    "Szekszard|Hungary": {"region": "Hungary"},

    # === Croatia ===
    "Srednja I Juzna Dalmacija|Croatia": {"region": "Dalmatia"},

    # === Turkey ===
    "Thrace (TR)|Turkey": {"region": "Thrace"},

    # === Japan ===
    "Yamanashi-ken|Japan": {"region": "Yamanashi"},
    "Hokkaido-ken|Japan": {"region": "Hokkaido"},

    # === Mexico ===
    "Ensenada|Mexico": {"region": "Valle de Guadalupe"},
    "Valle de Parras|Mexico": {"region": "Mexico"},

    # === Peru ===
    "Ica|Peru": {"region": "Ica Valley"},

    # === Malta ===
    "Gozo|Malta": {"region": "Malta"},

    # === Moldova ===
    "South Eastern|Moldova": {"region": "Moldova"},
    "Etulia|Moldova": {"region": "Moldova"},

    # === United Kingdom ===
    "England|United Kingdom": {"region": "United Kingdom"},

    # === Uruguay ===
    "Progreso|Uruguay": {"region": "Canelones"},
    "Juanico|Uruguay": {"region": "Canelones"},
    "San Jose|Uruguay": {"region": "Canelones"},
    "Montevideo|Uruguay": {"region": "Canelones"},
    "Cerro Chapeu|Uruguay": {"region": "Uruguay"},

    # === Brazil ===
    "Sao Paulo|Brazil": {"region": "Brazil"},
    "Santa Catarina|Brazil": {"region": "Planalto Catarinense"},
    "Vale do Sao Francisco|Brazil": {"region": "Sao Francisco Valley"},

    # === Romania ===
    "Vrancea|Romania": {"region": "Romania"},
    "Dealurile Olteniei|Romania": {"region": "Romania"},

    # === Russia & Ukraine ===
    "Crimeia (Krim)|Ukraine": {"region": "Ukraine"},
    "Sennoy|Russia": {"region": "Krasnodar"},
    "Taman Peninsula|Russia": {"region": "Krasnodar"},

    # === Bulgaria ===
    "Melnik|Bulgaria": {"region": "Thracian Valley"},
}

# DB-allowed match_type values mapped from internal match types
DB_MATCH_TYPE = {
    "appellation_exact": "exact_appellation",
    "appellation_norm": "exact_appellation",
    "sub_appellation": "alias",
    "region_exact": "exact_region",
    "region_norm": "exact_region",
    "manual": "alias",
}


def strip_sub_appellation(region_name: str) -> tuple[str, str | None]:
    """
    Strip French/Italian sub-appellation suffixes and return (base_name, di_base).

    Returns (stripped_name, di_match_base) where di_match_base is the base
    from "X di Y" pattern if present.
    """
    import re

    base = region_name

    # Strip French vineyard specifics
    base = re.sub(r"\s+1er\s+Cru\s+'[^']*'$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+Grand\s+Cru\s+'[^']*'$", "", base, flags=re.IGNORECASE)
    base = re.sub(r'\s+1er\s+Cru\s+"[^"]*"$', "", base, flags=re.IGNORECASE)
    base = re.sub(r'\s+Grand\s+Cru\s+"[^"]*"$', "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+1er\s+Cru$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+Grand\s+Cru$", "", base, flags=re.IGNORECASE)

    # Strip Italian suffixes
    base = re.sub(r"\s+Classico\s+Superiore$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+Classico$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+Superiore$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+Riserva$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+Ripasso\s+Classico$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+Ripasso$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+Chiaretto$", "", base, flags=re.IGNORECASE)

    # "di X" pattern for Italian
    di_match = re.match(r"^(.+?)\s+di\s+\w+$", base, re.IGNORECASE)
    di_base = di_match.group(1) if di_match else None

    return base, di_base


def main():
    parser = argparse.ArgumentParser(description="Expand region_name_mappings")
    parser.add_argument("--dry-run", action="store_true", help="Preview without inserting")
    args = parser.parse_args()

    dry_run = args.dry_run
    sb = get_supabase()

    print("Loading reference data...")

    regions = fetch_all("regions", "id,name,slug,country_id,is_catch_all")
    appellations = fetch_all("appellations", "id,name,region_id,country_id,designation_type")
    countries = fetch_all("countries", "id,name")
    existing_mappings = fetch_all("region_name_mappings", "region_name,country")

    country_map: dict[str, str] = {}  # name->id and id->name
    for c in countries:
        country_map[c["name"]] = c["id"]
        country_map[c["id"]] = c["name"]

    # Region lookups: "name|country_id" -> id
    region_by_name: dict[str, str] = {}
    region_by_norm: dict[str, str] = {}
    catch_all_by_country_id: dict[str, str] = {}
    for r in regions:
        region_by_name[f"{r['name']}|{r['country_id']}"] = r["id"]
        region_by_norm[f"{normalize(r['name'])}|{r['country_id']}"] = r["id"]
        if r.get("is_catch_all"):
            catch_all_by_country_id[r["country_id"]] = r["id"]

    # Appellation lookups: "name|country_id" -> {id, region_id}
    appell_by_name: dict[str, dict] = {}
    appell_by_norm: dict[str, dict] = {}
    for a in appellations:
        appell_by_name[f"{a['name']}|{a['country_id']}"] = {"id": a["id"], "region_id": a["region_id"]}
        appell_by_norm[f"{normalize(a['name'])}|{a['country_id']}"] = {"id": a["id"], "region_id": a["region_id"]}

    existing_set = {f"{m['region_name']}|{m['country']}" for m in existing_mappings}

    print(f"  {len(regions)} regions, {len(appellations)} appellations, {len(existing_mappings)} existing mappings")

    # Get unmapped region_names from wine_candidates
    all_candidates = fetch_all("wine_candidates", "region_name,country")

    unmapped_counts: dict[str, int] = {}
    for wc in all_candidates:
        if not wc.get("region_name"):
            continue
        key = f"{wc['region_name']}|{wc['country']}"
        if key in existing_set:
            continue
        unmapped_counts[key] = unmapped_counts.get(key, 0) + 1

    total_unmapped_wines = sum(unmapped_counts.values())
    print(f"  {len(unmapped_counts)} unmapped region_name|country combos ({total_unmapped_wines} wines)\n")

    # Build new mappings
    new_mappings: list[dict] = []
    matched_by_appellation = 0
    matched_by_appellation_norm = 0
    matched_by_region = 0
    matched_by_sub_appellation = 0
    matched_by_manual = 0
    unmatched = 0

    sorted_unmapped = sorted(unmapped_counts.items(), key=lambda x: -x[1])

    for key, wine_count in sorted_unmapped:
        sep_idx = key.rfind("|")
        region_name = key[:sep_idx]
        country = key[sep_idx + 1:]
        country_id = country_map.get(country)
        if not country_id:
            unmatched += 1
            continue

        region_id = None
        appellation_id = None
        match_type = None

        # Strategy 1: Exact appellation match
        appell_exact = appell_by_name.get(f"{region_name}|{country_id}")
        if appell_exact:
            region_id = appell_exact["region_id"]
            appellation_id = appell_exact["id"]
            match_type = "appellation_exact"
            matched_by_appellation += 1

        # Strategy 2: Normalized appellation match
        if not match_type:
            appell_norm = appell_by_norm.get(f"{normalize(region_name)}|{country_id}")
            if appell_norm:
                region_id = appell_norm["region_id"]
                appellation_id = appell_norm["id"]
                match_type = "appellation_norm"
                matched_by_appellation_norm += 1

        # Strategy 3: Sub-appellation patterns
        if not match_type:
            base_name, di_base = strip_sub_appellation(region_name)
            if base_name != region_name:
                stripped = (
                    appell_by_name.get(f"{base_name}|{country_id}")
                    or appell_by_norm.get(f"{normalize(base_name)}|{country_id}")
                )
                if stripped:
                    region_id = stripped["region_id"]
                    appellation_id = stripped["id"]
                    match_type = "sub_appellation"
                    matched_by_sub_appellation += 1

            if not match_type and di_base:
                di_stripped = (
                    appell_by_name.get(f"{di_base}|{country_id}")
                    or appell_by_norm.get(f"{normalize(di_base)}|{country_id}")
                )
                if di_stripped:
                    region_id = di_stripped["region_id"]
                    appellation_id = di_stripped["id"]
                    match_type = "sub_appellation"
                    matched_by_sub_appellation += 1

        # Strategy 4: Exact region match
        if not match_type:
            reg_exact = region_by_name.get(f"{region_name}|{country_id}")
            if reg_exact:
                region_id = reg_exact
                match_type = "region_exact"
                matched_by_region += 1

        # Strategy 4b: Normalized region match
        if not match_type:
            reg_norm = region_by_norm.get(f"{normalize(region_name)}|{country_id}")
            if reg_norm:
                region_id = reg_norm
                match_type = "region_norm"
                matched_by_region += 1

        # Strategy 5: Manual mapping
        if not match_type:
            manual = MANUAL_REGION_ALIASES.get(f"{region_name}|{country}")
            if manual:
                region_id = (
                    region_by_name.get(f"{manual['region']}|{country_id}")
                    or region_by_norm.get(f"{normalize(manual['region'])}|{country_id}")
                )
                if manual.get("appellation"):
                    app = (
                        appell_by_name.get(f"{manual['appellation']}|{country_id}")
                        or appell_by_norm.get(f"{normalize(manual['appellation'])}|{country_id}")
                    )
                    if app:
                        appellation_id = app["id"]
                if region_id:
                    match_type = "manual"
                    matched_by_manual += 1

        if match_type:
            new_mappings.append({
                "region_name": region_name,
                "country": country,
                "region_id": region_id,
                "appellation_id": appellation_id,
                "match_type": match_type,
            })
        else:
            if wine_count >= 20:
                print(f"  UNMATCHED ({wine_count}w): \"{region_name}\" [{country}]")
            unmatched += 1

    print(f"\n-- Match Results --")
    print(f"  Appellation exact:    {matched_by_appellation}")
    print(f"  Appellation norm:     {matched_by_appellation_norm}")
    print(f"  Sub-appellation:      {matched_by_sub_appellation}")
    print(f"  Region exact/norm:    {matched_by_region}")
    print(f"  Manual mapping:       {matched_by_manual}")
    print(f"  Total matched:        {len(new_mappings)}")
    print(f"  Unmatched:            {unmatched}")

    wines_matched = sum(
        unmapped_counts.get(f"{m['region_name']}|{m['country']}", 0) for m in new_mappings
    )
    print(f"  Wines covered by new mappings: {wines_matched}")

    if dry_run:
        print(f"\nDRY RUN -- no database changes made.")
        for mtype in ["appellation_exact", "appellation_norm", "sub_appellation",
                       "region_exact", "region_norm", "manual"]:
            samples = [m for m in new_mappings if m["match_type"] == mtype][:5]
            if samples:
                print(f"\n  {mtype} samples:")
                for m in samples:
                    wc = unmapped_counts.get(f"{m['region_name']}|{m['country']}", 0)
                    reg_name = next((r["name"] for r in regions if r["id"] == m["region_id"]), "?")
                    app_name = "-"
                    if m["appellation_id"]:
                        app_name = next((a["name"] for a in appellations if a["id"] == m["appellation_id"]), "?")
                    print(f"    \"{m['region_name']}\" [{m['country']}] -> region: {reg_name}, appellation: {app_name} ({wc}w)")
        return

    # Map internal match types to DB-allowed values
    for m in new_mappings:
        m["match_type"] = DB_MATCH_TYPE.get(m["match_type"], "alias")

    # Insert new mappings
    print(f"\nInserting {len(new_mappings)} new region_name_mappings...")
    insert_errors = 0
    for i in range(0, len(new_mappings), 500):
        batch = new_mappings[i:i + 500]
        try:
            sb.table("region_name_mappings").insert(batch).execute()
        except Exception as e:
            print(f"  Batch error at {i}: {e}")
            insert_errors += 1
    print(f"  Done inserting {len(new_mappings)} mappings ({insert_errors} errors).")
    print(f"\nMappings inserted! Use MCP SQL to update wines with new region_id / appellation_id.")


if __name__ == "__main__":
    main()
