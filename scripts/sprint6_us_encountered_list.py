"""Sprint 6 — Opus-curated list of ~800 producers American drinkers encounter.

Categorized across tiers to ensure broad coverage:
  A — Mass market / grocery
  B — Premium American (CA, OR, WA)
  C — Collector / cult American
  D — Popular imports (Bordeaux, Burgundy, Tuscany, Piedmont, Rioja, Port,
      Champagne, Germany, Austria, Australia, NZ, Chile, Argentina, South Africa)
  E — Wine-trade visible / sommelier-shelf (grower Champagne, natural wine,
      second wines, serious Rhône / Loire / Alsace, German Riesling, Greek)
  USER — user's personal collection + Sprint 4 demo benchmark set

Plus a "union" step: dedup across tiers, resolve against DB producers via
fuzzy-match, and enumerate Core auto-SKIP pairs not yet web-validated.
"""
import sys
import json
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn

# -------- USER'S COLLECTION + DEMO SET (explicit priority) --------
USER_AND_DEMO = [
    # User's own collection
    "Stag's Leap Wine Cellars", "Fort Ross Vineyard", "Ridge Vineyards",
    "Lopez de Heredia", "CIRQ",
    # Sprint 4 demo producers
    "Domaine Tempier", "E. Guigal", "Trimbach", "Domaine Huet",
    "Domaine de la Romanee-Conti", "Krug", "Giacomo Conterno",
    "Chateau Margaux", "Chateau Latour",
]

# -------- TIER A: MASS MARKET / GROCERY --------
TIER_A = [
    # Ubiquitous US grocery brands
    "Barefoot", "Cupcake", "19 Crimes", "Josh Cellars", "Yellow Tail",
    "Meiomi", "Apothic", "Sutter Home", "Clos du Bois", "Beringer",
    "Kendall-Jackson", "Mondavi", "Robert Mondavi", "Woodbridge",
    "Black Box", "Carlo Rossi", "Franzia", "Andre", "Korbel",
    "Menage a Trois", "La Crema", "Rodney Strong", "Chateau Ste. Michelle",
    "Columbia Crest", "Bogle", "Cline", "Layer Cake", "Bonterra",
    "Simi", "Toasted Head", "Smoking Loon", "Rex Goliath", "Red Bicyclette",
    "Oyster Bay", "Kim Crawford", "Matua", "Starborough", "Nobilo",
    "Villa Maria", "Brancott", "Whitehaven",
    "Catena", "Don Miguel Gascon", "Alamos", "Trapiche",
    "Santa Rita 120", "Casillero del Diablo", "Cono Sur", "Concha y Toro",
    "Frontera", "Montes",
    "Ruffino", "Cavit", "Riunite", "Bolla", "Da Vinci",
    "Santa Margherita", "Zonin", "Folonari",
    "Campo Viejo", "Marques de Caceres", "Faustino", "Bodegas LAN",
    "Freixenet", "Cordorniu",
    "Jacobs Creek", "Wolf Blass", "Lindemans", "Rosemount",
    "Beaulieu Vineyard", "BV", "Bonny Doon", "Domaine Carneros",
    "Gloria Ferrer", "Mumm Napa", "Roederer Estate", "Schramsberg",
    "Dark Horse", "Gnarly Head", "Ravenswood", "Hess Collection",
    "Cavaliere d'Oro", "La Marca", "Mezzacorona",
    "Stella Rosa", "Dr. Loosen Dr. L", "Fess Parker", "Firestone",
    "La Vieille Ferme", "Georges Duboeuf",
    "Wente", "J. Lohr", "Louis M. Martini", "Estancia",
    "Charles Shaw", "Two Buck Chuck",
    "Gallo Family Vineyards", "Ecco Domani",
    "Red Diamond", "Blackstone", "Mark West", "Bread & Butter",
    "Decoy", "Educated Guess", "Frei Brothers",
]

# -------- TIER B: PREMIUM AMERICAN --------
TIER_B = [
    # Napa cult / iconic
    "Caymus Vineyards", "Silver Oak", "Stag's Leap Wine Cellars",
    "Stag's Leap", "Opus One", "Duckhorn Vineyards", "Duckhorn",
    "Paraduxx", "Rombauer", "Cakebread Cellars", "Grgich Hills",
    "Joseph Phelps", "Insignia", "Shafer Vineyards", "Silverado Vineyards",
    "Frog's Leap", "Spottswoode", "Heitz Cellar", "Chateau Montelena",
    "Stags' Leap Winery", "Mayacamas", "Clos du Val", "Groth",
    "Robert Mondavi Winery", "Pine Ridge", "Franciscan", "Sterling Vineyards",
    "Beringer Private Reserve", "BV Georges de Latour",
    "Pahlmeyer", "Quintessa", "Dalla Valle", "Far Niente",
    "Nickel & Nickel", "Turley", "Ehlers", "Corison",
    "Forman", "Hourglass", "Continuum", "Ovid",
    "Araujo", "Bond", "Realm", "Schrader", "Morlet",
    "Maybach", "Colgin", "Sloan", "Arietta",
    # +100 additional premium American
    # Napa additional (30)
    "Frank Family Vineyards", "Trefethen Family Vineyards", "Cliff Lede",
    "Provenance Vineyards", "Rudd Estate", "Darioush", "Smith-Madrone",
    "Miner Family Winery", "Long Meadow Ranch", "Plumpjack Winery",
    "Cade Estate Winery", "Luna Vineyards", "Cain Vineyard",
    "Robert Foley Vineyards", "Chappellet", "David Arthur Vineyards",
    "Viader", "Freemark Abbey", "Chimney Rock Winery",
    "Clos Pegase", "Swanson Vineyards", "Spring Mountain Vineyard",
    "Philip Togni", "V. Sattui", "Stony Hill", "Revana",
    "Fairchild Estate", "Seavey Vineyard", "Ghost Block",
    "Whitehall Lane",
    # Sonoma additional (20)
    "Patz & Hall", "Marimar Estate", "Iron Horse Vineyards",
    "Dry Creek Vineyard", "Preston Farm & Winery",
    "Quivira Vineyards", "A. Rafanelli", "Seghesio Family Vineyards",
    "Martinelli Winery", "Hanzell Vineyards", "De Loach Vineyards",
    "Siduri Wines", "Scherrer Winery", "Red Car Wine",
    "Sonoma-Cutrer", "Davis Bynum", "Unti Vineyards",
    "Benovia Winery", "Gundlach Bundschu", "Imagery Estate",
    # Oregon additional (10)
    "Shea Wine Cellars", "Resonance Vineyard", "Chehalem",
    "Panther Creek Cellars", "WillaKenzie Estate", "Scott Paul Wines",
    "Alexana Winery", "Sokol Blosser", "Evening Land",
    "Maysara Winery",
    # Washington additional (10)
    "Upchurch Vineyard", "Browne Family Vineyards", "Trust Cellars",
    "Mark Ryan Winery", "Tamarack Cellars", "Figgins Family",
    "Guardian Cellars", "Rasa Vineyards", "Powers Winery",
    "Maryhill Winery",
    # Central / Santa Barbara / Paso (20)
    "Calera Wine Company", "Mount Eden Vineyards", "Rhys Vineyards",
    "Varner Wine", "Halter Ranch", "L'Aventure Winery",
    "Stolpman Vineyards", "Foxen Vineyard", "Zaca Mesa",
    "Fiddlehead Cellars", "Presqu'ile Winery", "Tyler Winery",
    "Paul Lato Wines", "Laetitia Vineyard", "Chamisal Vineyards",
    "Tolosa Winery", "Justin Vineyards", "Eberle Winery",
    "Brewer-Clifton", "Bedrock Wine Co.",
    # Virginia / NY / other states (10)
    "Barboursville Vineyards", "Linden Vineyards", "Early Mountain Vineyards",
    "Ankida Ridge", "Wolffer Estate", "Channing Daughters",
    "Paumanok Vineyards", "Bedell Cellars", "Lenz Winery",
    "Shelburne Vineyard",
    # Sonoma premium (original)
    "Williams Selyem", "Kistler", "Marcassin", "Aubert",
    "Kosta Browne", "Dehlinger", "Rochioli", "Merry Edwards",
    "Jordan Vineyard & Winery", "Jordan", "Chateau St. Jean",
    "Ferrari-Carano", "Simi Winery", "Gary Farrell",
    "MacRostie", "Ramey", "Peter Michael", "Flowers",
    "Hirsch Vineyards", "Littorai", "DuMOL", "Paul Hobbs",
    "Failla", "Arnot-Roberts",
    # Oregon (original)
    "Domaine Serene", "Archery Summit", "Beaux Freres", "Ponzi",
    "King Estate", "Bergstrom", "Cristom", "Eyrie Vineyards",
    "Soter", "Adelsheim", "Ken Wright", "Elk Cove",
    "Drouhin Oregon", "Domaine Drouhin", "Lange", "Argyle",
    "Rex Hill", "Anne Amie", "Penner-Ash", "Owen Roe",
    # Washington (original)
    "Quilceda Creek", "Leonetti", "Andrew Will", "Cayuse",
    "Woodward Canyon", "L'Ecole No 41", "Delille Cellars",
    "Charles Smith", "K Vintners", "Gramercy Cellars",
    "Seven Hills", "Betz Family", "Reynvaan",
    "Pepper Bridge", "Hedges",
    # Central Coast (original)
    "Au Bon Climat", "Qupe", "Tablas Creek", "Alban Vineyards",
    "Saxum", "Sea Smoke", "Melville", "Sanford", "Talley",
    "Ridge Lytton Springs", "Ridge Monte Bello",
]

# -------- TIER C: COLLECTOR / CULT AMERICAN --------
TIER_C = [
    "Screaming Eagle", "Harlan Estate", "Scarecrow", "Sine Qua Non",
    "Bryant Family", "Dominus Estate", "Dominus", "Colgin",
    "Abreu", "Hundred Acre", "Kapcsandy", "Kistler Vineyards",
    "Marcassin Vineyard", "Aubert Vineyards", "Futo", "Tusk",
    "Blankiet", "Shafer Hillside Select", "Fisher Vineyards",
    "Jonata", "Kongsgaard", "Rudd", "Bryant",
    "Rivers-Marie", "Schrader Cellars", "Memento Mori",
    "Verite", "Vineyard 29", "Diamond Creek",
]

# -------- TIER D: POPULAR IMPORTS --------
TIER_D_FRENCH_BDX = [
    # Medoc Classed Growths (1855) - top tier
    "Chateau Lafite Rothschild", "Chateau Latour", "Chateau Margaux",
    "Chateau Mouton Rothschild", "Chateau Haut-Brion",
    "Chateau Pichon Longueville Baron", "Chateau Pichon Lalande",
    "Chateau Cos d'Estournel", "Chateau Ducru-Beaucaillou", "Chateau Leoville Las Cases",
    "Chateau Leoville Barton", "Chateau Leoville Poyferre", "Chateau Montrose",
    "Chateau Lynch-Bages", "Chateau Pontet-Canet", "Chateau Palmer",
    "Chateau Beychevelle", "Chateau Calon-Segur",
    # Pomerol
    "Petrus", "Chateau Lafleur", "Le Pin", "Vieux Chateau Certan",
    "Chateau Trotanoy", "Chateau L'Evangile", "Chateau La Conseillante",
    "Chateau Clinet",
    # Saint-Emilion Premier
    "Chateau Cheval Blanc", "Chateau Ausone", "Chateau Angelus",
    "Chateau Pavie", "Chateau Figeac", "Chateau Canon",
    "Chateau Troplong Mondot", "Chateau Valandraud",
    # Sauternes
    "Chateau d'Yquem", "Chateau Climens", "Chateau Suduiraut",
    "Chateau Rieussec", "Chateau Coutet",
    # Graves/Pessac
    "Chateau La Mission Haut-Brion", "Chateau Pape Clement", "Domaine de Chevalier",
    "Chateau Haut-Bailly", "Chateau Smith Haut Lafitte",
]

TIER_D_FRENCH_BURG = [
    # Cote d'Or estates - top tier
    "Domaine de la Romanee-Conti", "Domaine Leroy", "Domaine Leflaive",
    "Domaine Georges Roumier", "Domaine Armand Rousseau", "Domaine Dujac",
    "Domaine Ponsot", "Domaine Mugnier", "Domaine Comte de Vogue",
    "Domaine Coche-Dury", "Domaine Lafon", "Domaine Raveneau",
    "Domaine Rene et Vincent Dauvissat",
    "Domaine Bonneau du Martray", "Domaine d'Auvenay",
    "Domaine Henri Gouges", "Domaine Mortet", "Domaine Fourrier",
    "Domaine Meo-Camuzet", "Domaine Jean Grivot", "Domaine Sylvain Cathiard",
    "Domaine Bruno Clair",
    "Domaine Hubert Lignier", "Domaine Perrot-Minot",
    "Domaine Michel Lafarge", "Domaine Marquis d'Angerville",
    "Domaine de Montille",
    "Domaine Jacques Prieur", "Domaine Joseph Drouhin",
    "Domaine Roulot", "Domaine Comte Armand",
    "Domaine des Comtes Lafon",
    "Domaine Faiveley", "Domaine Bouchard Pere et Fils",
    "Domaine Louis Jadot", "Domaine Louis Latour",
    "Domaine Anne Gros", "Domaine Claude Dugat",
    # Chablis
    "Domaine William Fevre", "Domaine Laroche",
    # Beaujolais cru (kept for sommelier recognition)
    "Domaine Jean Foillard", "Domaine Marcel Lapierre",
]

TIER_D_CHAMPAGNE = [
    # Grandes marques
    "Krug", "Dom Perignon", "Louis Roederer", "Roederer Cristal",
    "Bollinger", "Veuve Clicquot", "Moet & Chandon", "Ruinart",
    "Taittinger", "Pol Roger", "Perrier-Jouet",
    "Charles Heidsieck", "Piper-Heidsieck",
    "Laurent-Perrier", "Deutz", "Lanson",
    "Salon", "Delamotte", "Philipponnat",
    "Billecart-Salmon", "Gosset",
    # Grower Champagne (sommelier visible)
    "Pierre Peters", "Jacques Selosse", "Agrapart", "Chartogne-Taillet",
    "Egly-Ouriet", "Larmandier-Bernier", "Ulysse Collin",
    "Cedric Bouchard", "Pierre Gimonnet",
]

TIER_D_RHONE_LOIRE = [
    # Northern Rhone
    "M. Chapoutier", "E. Guigal", "Jean-Louis Chave", "Paul Jaboulet Aine",
    "Domaine Jamet", "Domaine Rostaing", "Domaine Clusel-Roch",
    "Domaine Pierre Gaillard",
    "Delas Freres", "Yves Cuilleron", "Georges Vernay",
    "Domaine Ogier",
    "Domaine Graillot", "Domaine Gonon",
    "Domaine Saint-Cosme",
    # Southern Rhone - Chateauneuf-du-Pape
    "Chateau de Beaucastel", "Chateau Rayas", "Domaine du Pegau",
    "Clos des Papes", "Domaine du Vieux Telegraphe",
    "Domaine de la Janasse", "Domaine de la Mordoree",
    "Domaine de la Vieille Julienne", "Domaine Pierre Usseglio",
    "Chateau La Nerthe", "Chateau Mont-Redon",
    # Loire
    "Domaine Huet", "Francois Chidaine",
    "Henri Bourgeois", "Pascal Jolivet",
    "Domaine Vacheron",
    "Nicolas Joly", "Coulee de Serrant",
    "Charles Joguet", "Bernard Baudry",
    "Clos Rougeard", "Jacky Blot",
    # Alsace
    "Trimbach", "Hugel", "Zind-Humbrecht", "Domaine Weinbach",
    "Domaine Schlumberger", "Domaine Ostertag",
    "Marcel Deiss", "Albert Boxler", "Josmeyer",
    # Provence
    "Domaine Tempier", "Chateau de Pibarnon", "Domaine Ott",
    "Chateau Simone", "Chateau d'Esclans", "Whispering Angel",
]

TIER_D_ITALY = [
    # Piedmont - Barolo/Barbaresco
    "Giacomo Conterno", "Bruno Giacosa", "Bartolo Mascarello",
    "Giuseppe Mascarello", "Vietti", "Gaja", "Roberto Voerzio",
    "Elio Altare", "Paolo Scavino",
    "Luciano Sandrone", "Poderi Aldo Conterno",
    "G.D. Vajra", "Ceretto",
    "Elio Grasso", "Massolino",
    "Produttori del Barbaresco",
    "Giuseppe Rinaldi", "Francesco Rinaldi",
    "Pio Cesare", "Prunotto",
    # Tuscany
    "Marchesi Antinori", "Antinori", "Tenuta San Guido",
    "Sassicaia", "Ornellaia", "Masseto",
    "Fontodi", "Isole e Olena", "Felsina", "Castello di Ama",
    "Biondi-Santi", "Poggio di Sotto",
    "Il Poggione", "Casanova di Neri",
    "Soldera",
    "Poliziano", "Avignonesi", "Le Macchiole",
    "Castello di Fonterutoli",
    "Frescobaldi", "Banfi",
    "Argiano", "Col d'Orcia",
    # Veneto
    "Quintarelli", "Dal Forno Romano",
    "Allegrini", "Masi", "Zenato",
    "Bertani",
    # Friuli / elsewhere
    "Jermann", "Livio Felluga",
    # Sicily / south
    "Planeta", "Donnafugata", "COS", "Valentini",
    "Tasca d'Almerita",
]

TIER_D_SPAIN = [
    # Rioja
    "Lopez de Heredia", "La Rioja Alta", "CVNE", "Bodegas Muga",
    "Marques de Riscal", "Marques de Murrieta",
    "Remelluri", "Contino", "Artadi",
    # Ribera del Duero
    "Vega Sicilia", "Dominio de Pingus", "Pingus",
    "Emilio Moro", "Aalto", "Bodegas Alion",
    # Priorat
    "Clos Mogador", "Alvaro Palacios", "L'Ermita",
    "Clos Erasmus",
    # Rias Baixas
    "Martin Codax",
    # Sherry
    "Gonzalez Byass", "Tio Pepe", "Lustau", "Valdespino",
    "Hidalgo La Gitana", "Equipo Navazos",
    # Cava
    "Raventos i Blanc", "Gramona",
]

TIER_D_PORT_MADEIRA = [
    "Taylor Fladgate", "Taylor's", "Fonseca", "Croft", "Dow's", "Graham's",
    "Warre's", "Sandeman", "Niepoort",
    "Quinta do Noval", "Quinta do Crasto",
    "Symington Family",
    "Blandy's", "Henriques & Henriques",
    "Wine & Soul",
]

TIER_D_GERMANY_AUSTRIA = [
    # Germany Riesling
    "Dr. Loosen", "J.J. Prum", "Egon Muller", "Donnhoff",
    "Fritz Haag", "Willi Schaefer",
    "Selbach-Oster", "Markus Molitor",
    "Van Volxem", "Keller", "Philipp Wittmann",
    "Muller-Catoir", "Dr. Burklin-Wolf",
    "Maximin Grunhaus", "Schloss Johannisberg",
    "Robert Weil", "Schloss Vollrads",
    "Peter Jakob Kuhn",
    "St. Urbans-Hof",
    # Austria
    "F.X. Pichler", "Emmerich Knoll", "Nikolaihof", "Prager",
    "Hirtzberger", "Alzinger",
    "Brundlmayer", "Kracher",
    "Weingut Schloss Gobelsburg",
]

TIER_D_AUSSIE_NZ = [
    # Australia
    "Penfolds", "Henschke", "Torbreck", "Yalumba", "Tyrrell's",
    "Rockford", "Two Hands",
    "Clarendon Hills",
    "Mollydooker", "d'Arenberg", "Charles Melton",
    "Peter Lehmann", "St. Hallett", "Seppeltsfield",
    "Leeuwin Estate", "Cullen", "Moss Wood", "Vasse Felix",
    "Cape Mentelle",
    "Mount Mary", "Yarra Yering", "Giaconda",
    "Grosset", "Clonakilla",
    # NZ
    "Cloudy Bay", "Kim Crawford", "Felton Road",
    "Ata Rangi", "Rippon",
    "Craggy Range", "Te Mata", "Dog Point",
    "Greywacke", "Pegasus Bay",
    "Neudorf",
]

TIER_D_SAM_AFR = [
    # Argentina
    "Catena Zapata", "Bodega Catena Zapata", "Achaval-Ferrer",
    "Cheval des Andes", "Susana Balbo",
    "El Enemigo", "Zuccardi",
    # Chile
    "Errazuriz", "Concha y Toro", "Montes Alpha",
    "Almaviva", "Clos Apalta",
    "Lapostolle", "Santa Rita",
    "De Martino", "Matetic",
    # South Africa
    "Kanonkop", "Boekenhoutskloof", "Mullineux", "Sadie Family",
    "Meerlust", "Klein Constantia", "Vergelegen",
    "Hamilton Russell", "Ken Forrester",
    "Rust en Vrede",
    "Graham Beck",
]

# -------- TIER E: WINE-TRADE VISIBLE / SOMMELIER SHELF --------
TIER_E = [
    # Second wines (common sommelier references)
    "Pavillon Rouge du Chateau Margaux", "Les Forts de Latour",
    "Carruades de Lafite", "Le Petit Mouton",
    "Alter Ego de Palmer",
    # Greek
    "Domaine Sigalas", "Gaia Estate", "Alpha Estate",
    # Central/Eastern Europe
    "Radikon", "Gravner",
    "Chateau Musar",
    # Hungarian Tokaji
    "Royal Tokaji", "Oremus",
    # Canada
    "Mission Hill", "Inniskillin",
    # English sparkling
    "Nyetimber", "Gusbourne",
    # Natural wine / biodynamic darlings
    "Overnoy", "Pierre Overnoy",
    # Additional US families not yet listed in B
    "Matthiasson", "Dirty and Rowdy",
    "Scholium Project",
    "Jean-Pierre Moueix",
]

ALL_CATEGORIES = {
    "USER_AND_DEMO": USER_AND_DEMO,
    "A_mass_market": TIER_A,
    "B_premium_american": TIER_B,
    "C_collector_cult": TIER_C,
    "D_bordeaux": TIER_D_FRENCH_BDX,
    "D_burgundy": TIER_D_FRENCH_BURG,
    "D_champagne": TIER_D_CHAMPAGNE,
    "D_rhone_loire": TIER_D_RHONE_LOIRE,
    "D_italy": TIER_D_ITALY,
    "D_spain": TIER_D_SPAIN,
    "D_port_madeira": TIER_D_PORT_MADEIRA,
    "D_germany_austria": TIER_D_GERMANY_AUSTRIA,
    "D_aussie_nz": TIER_D_AUSSIE_NZ,
    "D_sam_afr": TIER_D_SAM_AFR,
    "E_trade_visible": TIER_E,
}


def main():
    # Dedup across tiers
    seen = set()
    all_names = []
    by_category = {}
    for cat, names in ALL_CATEGORIES.items():
        unique_in_cat = []
        for n in names:
            ln = n.lower().strip()
            if ln in seen:
                continue
            seen.add(ln)
            unique_in_cat.append(n)
            all_names.append(n)
        by_category[cat] = unique_in_cat

    print(f"Total unique producer names after dedup: {len(all_names)}")
    print()
    for cat, names in by_category.items():
        print(f"  {cat:<25} {len(names):>4}")

    # DB lookup via trigram match against producers.name
    conn = get_conn()
    cur = conn.cursor()

    # Use pg_trgm similarity + name normalization
    cur.execute("DROP TABLE IF EXISTS sprint6_us_producers_tmp")
    cur.execute("""
        CREATE TEMP TABLE sprint6_us_producers_tmp (
          input_name TEXT,
          category TEXT,
          producer_id UUID,
          matched_name TEXT,
          similarity NUMERIC
        )
    """)

    # Prefix/suffix noise to strip for match attempts
    PREFIXES = ["domaine", "chateau", "ch\u00e2teau", "bodega", "bodegas",
                "weingut", "tenuta", "casa", "cantina", "azienda agricola",
                "clos", "maison", "dom.", "ch."]

    def norm(s: str) -> str:
        import unicodedata
        s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
        return s.lower().strip()

    def prefix_variants(name: str) -> list[str]:
        """Return candidate lookup strings: original, accent-stripped, prefix-stripped."""
        out = [name]
        n = norm(name)
        if n != name.lower():
            out.append(n)
        for p in PREFIXES:
            if n.startswith(p + " "):
                stripped = n[len(p):].strip()
                out.append(stripped)
                break
        return out

    matched = 0
    unmatched = []
    for cat, names in by_category.items():
        for n in names:
            best = None
            for variant in prefix_variants(n):
                cur.execute("""
                    SELECT p.id, p.name,
                           GREATEST(
                             similarity(p.name, %s),
                             similarity(lower(unaccent(p.name)), lower(unaccent(%s)))
                           ) AS sim
                    FROM producers p
                    WHERE p.deleted_at IS NULL
                      AND (
                        p.name ILIKE %s
                        OR lower(unaccent(p.name)) ILIKE lower(unaccent(%s))
                        OR p.name %% %s
                        OR similarity(lower(unaccent(p.name)), lower(unaccent(%s))) > 0.55
                      )
                    ORDER BY GREATEST(
                        similarity(p.name, %s),
                        similarity(lower(unaccent(p.name)), lower(unaccent(%s)))
                    ) DESC
                    LIMIT 1
                """, (variant, variant, f"%{variant}%", f"%{variant}%", variant, variant, variant, variant))
                row = cur.fetchone()
                if row and row[2] and row[2] > 0.55:
                    if not best or row[2] > best[2]:
                        best = row
                    if row[2] >= 0.85:
                        break  # strong match; don't keep searching
            if best:
                cur.execute("""
                    INSERT INTO sprint6_us_producers_tmp VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (n, cat, best[0], best[1], best[2]))
                matched += 1
            else:
                unmatched.append(n)

    print()
    print(f"Matched to DB: {matched} / {len(all_names)} ({100*matched/len(all_names):.1f}%)")
    print(f"Unmatched: {len(unmatched)}")

    if unmatched:
        print("\nFirst 30 unmatched names (for sanity check):")
        for n in unmatched[:30]:
            print(f"  - {n}")

    # Now count Core auto-SKIP pairs involving the matched producers, NOT already web-validated
    cur.execute("""
        SELECT COUNT(DISTINCT b.id)
        FROM producer_dedup_pairs b
        JOIN producer_dedup_routing_stage1 s1 ON s1.pair_id = b.id
        LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = b.id
        LEFT JOIN producer_dedup_pairs w
          ON w.method_name = 'l2_haiku_rich_web'
          AND w.producer_id_a = b.producer_id_a AND w.producer_id_b = b.producer_id_b
        WHERE b.method_name = 'blocking'
          AND COALESCE(s2.stage2_action, s1.stage1_action) IN ('stage1_auto_skip', 'stage2_auto_skip')
          AND w.id IS NULL
          AND (b.producer_id_a IN (SELECT producer_id FROM sprint6_us_producers_tmp)
               OR b.producer_id_b IN (SELECT producer_id FROM sprint6_us_producers_tmp))
    """)
    n_pairs = cur.fetchone()[0]
    print()
    print(f"=== Targeted re-audit scope ===")
    print(f"Auto-SKIP pairs involving a matched US-encountered producer: {n_pairs:,}")
    print(f"Cost at $0.006/pair: ${n_pairs*0.006:.2f}")

    # Also show how many US-encountered producer IDs ended up in our sprint6_core_producers set
    cur.execute("""
        SELECT COUNT(*) FROM sprint6_us_producers_tmp
        WHERE producer_id IN (SELECT producer_id FROM sprint6_core_producers)
    """)
    in_core = cur.fetchone()[0]
    print(f"Matched producers already in sprint6_core_producers: {in_core} / {matched}")

    # Save matched ids to file for next step
    cur.execute("SELECT DISTINCT producer_id FROM sprint6_us_producers_tmp")
    pids = [str(r[0]) for r in cur.fetchall()]
    outp = Path(__file__).resolve().parents[1] / "data" / "sprints" / "dedup" / "us_encountered_producer_ids.json"
    outp.write_text(json.dumps(pids))
    print(f"\nSaved {len(pids)} producer IDs to {outp.name}")

    # Save pair manifest for the re-audit run
    cur.execute("""
        SELECT DISTINCT b.id
        FROM producer_dedup_pairs b
        JOIN producer_dedup_routing_stage1 s1 ON s1.pair_id = b.id
        LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = b.id
        LEFT JOIN producer_dedup_pairs w
          ON w.method_name = 'l2_haiku_rich_web'
          AND w.producer_id_a = b.producer_id_a AND w.producer_id_b = b.producer_id_b
        WHERE b.method_name = 'blocking'
          AND COALESCE(s2.stage2_action, s1.stage1_action) IN ('stage1_auto_skip', 'stage2_auto_skip')
          AND w.id IS NULL
          AND (b.producer_id_a IN (SELECT producer_id FROM sprint6_us_producers_tmp)
               OR b.producer_id_b IN (SELECT producer_id FROM sprint6_us_producers_tmp))
        ORDER BY b.id
    """)
    pair_ids = [r[0] for r in cur.fetchall()]
    outp_pairs = Path(__file__).resolve().parents[1] / "data" / "sprints" / "dedup" / "step7b_us_reaudit_manifest.json"
    outp_pairs.write_text(json.dumps(pair_ids))
    print(f"Saved {len(pair_ids)} pair IDs to {outp_pairs.name}")

    conn.commit()
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
