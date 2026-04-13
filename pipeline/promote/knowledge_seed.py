#!/usr/bin/env python3
"""
Knowledge Seed: Generate and promote notable wines from Claude training data.

Multi-stage pipeline that treats Claude's training knowledge as a data source,
with proper staging, dedup, TTB linking, Haiku validation, and conservative promotion.

Stages:
  1. generate   — Haiku generates wine lists by category → JSON + staging table
  2. dedup      — Match against canonical DB, mark exists/new
  3. ttb-match  — Match new wines against source_ttb_colas
  4. validate   — Haiku independently validates gap wines for accuracy
  5. promote    — Create verified wines in canonical tables
  6. report     — Summary of pipeline state

Usage:
    python -m pipeline.promote.knowledge_seed generate [--budget 10.0] [--category bordeaux_classified]
    python -m pipeline.promote.knowledge_seed dedup
    python -m pipeline.promote.knowledge_seed ttb-match
    python -m pipeline.promote.knowledge_seed validate [--budget 5.0]
    python -m pipeline.promote.knowledge_seed promote [--dry-run]
    python -m pipeline.promote.knowledge_seed report
"""

import argparse
import json
import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import HAIKU_MODEL, SONNET_MODEL
from pipeline.lib.normalize import normalize, normalize_producer, normalize_wine_name, slugify

# ── Constants ────────────────────────────────────────────────
MAX_TOKENS = 16384
INPUT_COST_PER_M = 0.80
OUTPUT_COST_PER_M = 4.00

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "knowledge_seed"

GENERATE_SYSTEM = """You are a wine database expert. Generate a precise, structured list of wines for the given category.

For each wine, return a JSON object with these exact fields:
- wine_name: The wine's name as it appears on the label. For estate wines where the wine IS the producer name (e.g., "Opus One"), use the producer name. For wines with distinct bottling names, use the bottling name (e.g., "La Tâche", "Hillside Select", "Private Selection Cabernet Sauvignon").
- producer_name: The official producer/winery/house name (e.g., "Domaine de la Romanée-Conti", "Shafer Vineyards", "Moët & Chandon")
- country: Full country name (e.g., "France", "United States", "Italy")
- region: Wine region (e.g., "Bordeaux", "California", "Piedmont")
- appellation: Specific appellation/denomination (e.g., "Pauillac", "Oakville", "Barolo"). Leave empty string if no specific appellation.
- grapes: Primary grape varieties, comma-separated (e.g., "Cabernet Sauvignon, Merlot, Cabernet Franc")
- color: Exactly "red", "white", or "rosé"
- wine_type: Exactly "table", "sparkling", "fortified", or "dessert"
- why_notable: One sentence explaining why this wine is important or well-known

Rules:
- ONLY include wines you are HIGHLY confident actually exist
- Be specific about appellations when you know them
- Producer name should be the official full name
- For brand wines with multiple varietals (e.g., "Josh Cellars Cabernet Sauvignon"), list each varietal as a separate wine entry
- Do NOT include fictional or uncertain wines
- Do NOT duplicate wines within the same list

Return ONLY a valid JSON array. No other text, no markdown fences."""

VALIDATE_SYSTEM = """You are a wine fact-checker. For each wine listed below, independently verify whether this wine exists and whether the attributed details are correct.

For each wine, return a JSON object with:
- idx: the index number from the input
- exists: true/false — does this wine actually exist?
- producer_correct: true/false — is the attributed producer correct?
- grapes_correct: true/false/null — are the grapes correct? null if you're unsure
- region_correct: true/false — is the region/appellation correct?
- color_correct: true/false — is the color correct?
- confidence: 0.0-1.0 — your overall confidence in this wine entry
- notes: any corrections or concerns (empty string if all correct)

Important: Think independently. If you're not sure about a wine, set confidence < 0.8.
Return ONLY a valid JSON array. No other text."""

# ── Wine Categories ──────────────────────────────────────────
CATEGORIES = [
    # === FRANCE — BORDEAUX ===
    ("bordeaux_classified",
     "List the 61 Bordeaux classified growths from the 1855 Classification (Médoc + Sauternes). "
     "Include all 5 First Growths, 14 Second Growths, 14 Third Growths, 10 Fourth Growths, "
     "18 Fifth Growths, plus Château d'Yquem (Premier Cru Supérieur). "
     "Also add the 4 main Right Bank icons: Pétrus, Le Pin, Cheval Blanc, Ausone.",
     65),

    ("bordeaux_notable",
     "List 30 notable Bordeaux wines BEYOND the 1855 classified growths and Right Bank icons above. "
     "Include: top Saint-Émilion Grand Cru Classé (Angélus, Pavie, Figeac, Canon, Troplong Mondot, "
     "Beauséjour Bécot), top Pomerol (La Fleur-Pétrus, L'Evangile, Trotanoy, Clinet, Gazin), "
     "top Cru Bourgeois (Phélan Ségur, Sociando-Mallet, Poujeaux, Chasse-Spleen), "
     "popular value (Mouton Cadet), white Bordeaux (Domaine de Chevalier Blanc, Pavillon Blanc du Château Margaux).",
     30),

    # === FRANCE — BURGUNDY ===
    ("burgundy_red",
     "List 40 top red Burgundy wines Americans encounter. Include the signature Grand Cru and "
     "Premier Cru bottlings from: Domaine de la Romanée-Conti (Romanée-Conti, La Tâche, Richebourg, "
     "Romanée-St-Vivant, Grands Echézeaux, Echézeaux), Domaine Leroy, Domaine Dujac, "
     "Domaine Georges/Christophe Roumier, Méo-Camuzet, Domaine Faiveley, Louis Jadot, "
     "Joseph Drouhin, Bouchard Père & Fils, Domaine de la Vougeraie, Comte Georges de Vogüé, "
     "Domaine Ponsot, Domaine Armand Rousseau, Domaine des Lambrays.",
     40),

    ("burgundy_white",
     "List 30 top white Burgundy wines. Include Grand Cru Chablis producers (Raveneau, "
     "William Fèvre, Vincent Dauvissat), Meursault (Coche-Dury, Roulot, Comtes Lafon), "
     "Puligny-Montrachet (Domaine Leflaive, Etienne Sauzet), Chassagne-Montrachet, "
     "Corton-Charlemagne (Bonneau du Martray, Coche-Dury), Louis Latour, Olivier Leflaive.",
     30),

    # === FRANCE — CHAMPAGNE ===
    ("champagne",
     "List 50 Champagnes Americans encounter. Include major houses with their key cuvées: "
     "Moët & Chandon (Brut Impérial, Rosé), Dom Pérignon, Veuve Clicquot (Yellow Label, La Grande Dame), "
     "Krug (Grande Cuvée, Vintage), Ruinart (Blanc de Blancs), Taittinger (Comtes de Champagne), "
     "Pol Roger (Brut, Cuvée Sir Winston Churchill), Bollinger (Special Cuvée, La Grande Année), "
     "Louis Roederer (Brut Premier, Cristal), Perrier-Jouët (Grand Brut, Belle Epoque), "
     "Laurent-Perrier (Brut, Grand Siècle, Cuvée Rosé), Piper-Heidsieck, G.H. Mumm, "
     "Nicolas Feuillatte, Charles Heidsieck, Billecart-Salmon (Brut, Rosé), Deutz, "
     "Grower Champagnes: Egly-Ouriet, Jacques Selosse, Pierre Gimonnet, Agrapart, Laherte Frères, Pierre Péters.",
     50),

    # === FRANCE — RHÔNE ===
    ("rhone",
     "List 30 top Rhône Valley wines. Northern: Hermitage (M. Chapoutier, Paul Jaboulet Aîné "
     "La Chapelle, Jean-Louis Chave), Côte-Rôtie (E. Guigal La La La trilogy, René Rostaing, "
     "Stéphane Ogier, Jamet), Cornas (Auguste Clape), Condrieu (Yves Cuilleron, Georges Vernay), "
     "Saint-Joseph (Jean-Louis Chave Sélection). Southern: Châteauneuf-du-Pape (Château de Beaucastel, "
     "Château Rayas, Vieux Télégraphe, Clos des Papes, Domaine du Vieux Donjon), Gigondas, Vacqueyras.",
     30),

    # === FRANCE — LOIRE, ALSACE, OTHER ===
    ("france_other",
     "List 30 top wines from Loire Valley, Alsace, Provence, and other French regions. "
     "Loire: Sancerre (Domaine Dagueneau, Domaine Vacheron, Pascal Jolivet, Lucien Crochet), "
     "Pouilly-Fumé (Didier Dagueneau Silex), Vouvray (Domaine Huet), Chinon (Charles Joguet), "
     "Savennières (Nicolas Joly Coulée de Serrant), Muscadet (Domaine de l'Ecu). "
     "Alsace: Trimbach (Clos Sainte Hune Riesling, Frédéric Emile), Domaine Zind-Humbrecht, "
     "Hugel (Jubilee), Marcel Deiss. "
     "Provence: Whispering Angel (Château d'Esclans), Miraval, Domaines Ott. "
     "Jura: Domaine Tissot, Château-Chalon. Languedoc: Mas de Daumas Gassac, Gérard Bertrand.",
     30),

    # === ITALY ===
    ("italy_piedmont",
     "List 40 top Piedmont wines. Barolo producers with signature crus: Giacomo Conterno "
     "(Monfortino, Cascina Francia), Bruno Giacosa (Falletto, Le Rocche), Bartolo Mascarello, "
     "Vietti (Ravera, Castiglione), Aldo Conterno (Granbussia), Gaja (Sperss), Pio Cesare, "
     "Marchesi di Barolo, Paolo Scavino (Bric dël Fiasc), G.D. Vajra, Massolino, Oddero, "
     "Ceretto, Luciano Sandrone, Roberto Voerzio, Elio Altare, Elvio Cogno. "
     "Barbaresco: Gaja (Barbaresco, Sorì San Lorenzo, Sorì Tildìn), Produttori del Barbaresco, "
     "Bruno Giacosa Santo Stefano. Barbera: Braida, Coppo.",
     40),

    ("italy_tuscany",
     "List 40 top Tuscan wines. Super Tuscans: Sassicaia (Tenuta San Guido), Tignanello "
     "(Antinori), Ornellaia, Solaia (Antinori), Masseto, Guado al Tasso (Antinori), "
     "Luce della Vite. Brunello di Montalcino: Biondi-Santi, Casanova di Neri, "
     "Ciacci Piccolomini, Il Poggione, Banfi, Altesino, Argiano, Poggio di Sotto, "
     "Fuligni, Canalicchio di Sopra. Chianti Classico: Fontodi, Isole e Olena, "
     "Fèlsina (Fontalloro), Castello di Ama, Badia a Coltibuono, Castello dei Rampolla, "
     "Querciabella. Vino Nobile: Avignonesi, Boscarelli. Bolgheri: Le Macchiole (Messorio), "
     "Tenuta dell'Ornellaia.",
     40),

    ("italy_other",
     "List 40 wines from other Italian regions. Veneto: Amarone (Bertani, Masi Costasera, "
     "Allegrini, Dal Forno Romano, Tommasi, Zenato), Valpolicella Ripasso, Prosecco brands "
     "(La Marca, Mionetto, Zonin, Ruffino). Sicily: Planeta, Donnafugata (Ben Ryé), "
     "Cusumano, Etna producers (Benanti, Passopisciaro/Franchetti, Graci). "
     "Campania: Feudi di San Gregorio, Mastroberardino. Abruzzo: Masciarelli, Valentini. "
     "Alto Adige: Cantina Terlano, Elena Walch. Friuli: Jermann, Livio Felluga, Gravner. "
     "Lombardy: Ca' del Bosco (Franciacorta), Bellavista.",
     40),

    # === SPAIN ===
    ("spain",
     "List 40 top Spanish wines. Rioja: CVNE/Imperial, La Rioja Alta (Gran Reserva 904/890), "
     "Muga, Marqués de Murrieta, Marqués de Riscal, López de Heredia (Viña Tondonia), "
     "Artadi, Roda, Faustino, Bodegas LAN, Beronia, Ramón Bilbao. "
     "Ribera del Duero: Vega Sicilia (Único), Pesquera, Pingus, Emilio Moro, Protos. "
     "Priorat: Álvaro Palacios (L'Ermita, Finca Dofí, Les Terrasses), Clos Mogador, Clos Erasmus. "
     "Rías Baixas: Pazo de Señoráns, Do Ferreiro, Martín Códax (Albariño). "
     "Sherry: Tio Pepe (González Byass), Lustau, Hidalgo La Gitana. "
     "Cava: Codorníu, Freixenet, Raventós i Blanc. Rueda: Marqués de Riscal.",
     40),

    # === PORTUGAL ===
    ("portugal",
     "List 25 top Portuguese wines. Vintage Port: Taylor's, Graham's, Fonseca, Dow's, "
     "Warre's, Cockburn's, Sandeman, Croft, Niepoort. Also Tawny 10yr/20yr/30yr from "
     "Taylor's and Graham's. Douro table wines: Quinta do Noval, Quinta do Crasto, "
     "Niepoort Redoma, Quinta do Vallado. Vinho Verde: Aveleda, Broadbent. "
     "Dão: Quinta dos Roques. Alentejo: Esporão, Herdade do Mouchão. "
     "Madeira: Blandy's (5yr, 10yr), Henriques & Henriques.",
     25),

    # === GERMANY & AUSTRIA ===
    ("germany",
     "List 20 top German wines. Mosel: Joh. Jos. Prüm (Wehlener Sonnenuhr), Egon Müller "
     "(Scharzhofberger), Fritz Haag, Dr. Loosen (Erdener Prälat, Ürziger Würzgarten), "
     "Markus Molitor, Selbach-Oster, Willi Schaefer, Zilliken. Rheingau: Robert Weil "
     "(Kiedricher Gräfenberg), Schloss Johannisberg. Pfalz: Müller-Catoir, Bürklin-Wolf, "
     "Von Winning. Nahe: Dönnhoff (Hermannshöhle), Emrich-Schönleber. "
     "Include specific Riesling bottlings (Kabinett, Spätlese, Auslese, GG).",
     20),

    ("austria",
     "List 15 top Austrian wines. Wachau: F.X. Pichler, Knoll, Hirtzberger, Domäne Wachau, "
     "Nikolaihof. Kamptal: Bründlmayer (Heiligenstein), Schloss Gobelsburg. "
     "Burgenland reds: Moric (Blaufränkisch), Heinrich. Sweet: Alois Kracher. "
     "Grüner Veltliner specialists.",
     15),

    # === USA — CALIFORNIA ===
    ("napa_cabernet",
     "List 50 Napa Valley Cabernet Sauvignon and Bordeaux-blend wines. Cult: Screaming Eagle, "
     "Harlan Estate, Scarecrow, Dalla Valle Maya, Colgin (Tychson Hill, IX Estate), Bond "
     "(Vecina, St. Eden, Quella), Hundred Acre. Icons: Opus One, Insignia (Joseph Phelps), "
     "Silver Oak Napa, Caymus (Special Selection), Stag's Leap Wine Cellars (S.L.V., Fay, "
     "CASK 23), Robert Mondavi (Reserve, To Kalon), Heitz (Martha's Vineyard), Dominus, "
     "Dunn (Howell Mountain), Shafer (Hillside Select, One Point Five), Beringer (Private Reserve). "
     "Widely available: Jordan, Cakebread, Duckhorn, Groth, Hall, DAOU, Frank Family, "
     "Frog's Leap, Grgich Hills, Chateau Montelena, Corison, Spottswoode, Pine Ridge, "
     "Flora Springs, Far Niente, Nickel & Nickel, Trefethen, Inglenook, Spring Mountain, "
     "Pride Mountain, Plumpjack, Chimney Rock, Clos du Val, Turnbull.",
     50),

    ("napa_other",
     "List 20 Napa Valley wines that are NOT Cabernet-dominant. Chardonnay: Rombauer, "
     "Trefethen, Kongsgaard, HDV (de Villaine). Sauvignon Blanc: Cakebread, Honig, "
     "Merry Edwards (technically Sonoma but iconic), Groth. Pinot Noir: Carneros producers "
     "(Etude, Saintsbury, Bouchaine). Merlot: Duckhorn Three Palms, Markham. "
     "Sparkling: Schramsberg (Blanc de Blancs, Brut Rosé), Domaine Carneros, Mumm Napa.",
     20),

    ("sonoma",
     "List 30 Sonoma County wines. Russian River Pinot Noir: Williams Selyem, Kistler, "
     "Littorai, Gary Farrell, Rochioli, Merry Edwards, Hartford Court, Dehlinger. "
     "Dry Creek Zinfandel: Ridge (Lytton Springs, Geyserville), Seghesio. "
     "Alexander Valley Cabernet: Jordan, Silver Oak Alexander Valley. "
     "Sonoma Coast: Hirsch, Flowers, Fort Ross, Marcassin. "
     "Chardonnay: Kistler, Ramey, Sonoma-Cutrer, Chateau St. Jean, Simi. "
     "Other: Benziger, Hanzell, Ravenswood, Kenwood, Laurel Glen.",
     30),

    ("california_other",
     "List 25 California wines outside Napa and Sonoma. Paso Robles: Tablas Creek, Justin, "
     "DAOU (if not already listed), L'Aventure, Saxum. Santa Barbara: Au Bon Climat, "
     "Sanford (Pinot), Hitching Post, Qupé, Bien Nacido. Santa Cruz Mountains: Ridge "
     "Monte Bello. Lodi: Michael David (Seven Deadly Zins), LangeTwins. "
     "Monterey/Santa Lucia Highlands: Hahn, Pisoni. Edna Valley: Tolosa. "
     "Central Coast blends: Orin Swift. Sta. Rita Hills: Brewer-Clifton, Melville. "
     "Anderson Valley: Roederer Estate, Navarro. Amador: Renwood.",
     25),

    # === USA — OTHER ===
    ("oregon",
     "List 20 Oregon wines. Willamette Valley Pinot Noir: Domaine Drouhin Oregon, "
     "Domaine Serene (Evenstad Reserve), Eyrie Vineyards, Ponzi, Cristom, Adelsheim, "
     "King Estate, Rex Hill, Bethel Heights, Beaux Frères, WillaKenzie, Sokol Blosser, "
     "Archery Summit, Ken Wright, Bergström, Evening Land, Shea Wine Cellars. "
     "Other: Elk Cove, A to Z Wineworks, Willamette Valley Vineyards.",
     20),

    ("washington",
     "List 20 Washington State wines. Quilceda Creek, Leonetti Cellar, Woodward Canyon, "
     "Andrew Will, DeLille Cellars, Chateau Ste. Michelle (single-vineyard and standard), "
     "Columbia Crest (Grand Estates, Reserve), K Vintners/Charles Smith (Wines of Substance), "
     "L'Ecole No. 41, Gramercy Cellars, Cayuse, Northstar, Long Shadows, Col Solare, "
     "Hedges Family Estate, Pepper Bridge, Dunham Cellars, Betz Family Winery, Mark Ryan.",
     20),

    ("us_other",
     "List 15 wines from other US states. New York: Wölffer Estate (Long Island), "
     "Dr. Konstantin Frank (Finger Lakes Riesling), Hermann J. Wiemer, Red Newt Cellars. "
     "Virginia: Barboursville, RdV Vineyards, Early Mountain, King Family. "
     "Texas: Becker Vineyards, William Chris. Michigan: Chateau Grand Traverse, "
     "Left Foot Charley. Other states if notable.",
     15),

    # === SOUTHERN HEMISPHERE ===
    ("australia",
     "List 25 Australian wines Americans encounter. Barossa Shiraz: Penfolds (Grange, "
     "Bin 389, Bin 28, Bin 128), Henschke (Hill of Grace, Mount Edelstone), Torbreck "
     "(RunRig), Two Hands, Peter Lehmann, d'Arenberg, Yalumba (The Signature). "
     "McLaren Vale: Mollydooker. Coonawarra: Wynns (John Riddoch). Hunter Valley: "
     "Tyrrell's (Vat 1). Margaret River: Vasse Felix, Leeuwin Estate (Art Series), Cullen. "
     "Popular: Jacob's Creek, Lindeman's, Yellow Tail, 19 Crimes.",
     25),

    ("new_zealand",
     "List 20 New Zealand wines. Marlborough Sauvignon Blanc: Cloudy Bay, Kim Crawford, "
     "Oyster Bay, Craggy Range, Villa Maria, Whitehaven, Brancott Estate, Wither Hills, "
     "Nautilus, Dog Point. Central Otago Pinot: Felton Road, Mt Difficulty, Amisfield, "
     "Rippon, Burn Cottage. Hawke's Bay: Craggy Range Te Muna Road, Te Mata (Coleraine). "
     "Martinborough: Ata Rangi, Dry River.",
     20),

    ("argentina",
     "List 20 Argentine wines. Mendoza Malbec: Catena Zapata (Nicolás Catena Zapata, "
     "Catena Alta), Achaval Ferrer, Bodega Noemia, Viña Cobos, Terrazas de los Andes, "
     "Zuccardi (José Zuccardi), Altos Las Hormigas, Kaiken, Pascual Toso, Trapiche "
     "(Iscay, single-vineyard), Luigi Bosca, Norton. Torrontés: Colomé. Patagonia: "
     "Bodega Chacra. Popular: Alamos, Clos de los Siete, Susana Balbo.",
     20),

    ("chile",
     "List 20 Chilean wines. Premium: Almaviva, Seña, Concha y Toro Don Melchor, "
     "Montes Alpha/Folly, Errázuriz (Don Maximiano), Viña Santa Rita (Casa Real), "
     "Casa Lapostolle (Clos Apalta), Cono Sur (20 Barrels). Carmenère: Carmen, De Martino. "
     "Popular: Casillero del Diablo (various), Frontera, 120 by Santa Rita, Undurraga.",
     20),

    ("south_africa",
     "List 15 South African wines. Stellenbosch: Kanonkop (Pinotage), Rust en Vrede, "
     "Meerlust (Rubicon), Thelema, Vergelegen. Swartland: Mullineux, Sadie Family "
     "(Columella, Palladius), Badenhorst. Constantia: Klein Constantia (Vin de Constance). "
     "Elgin: Paul Cluver. Chenin Blanc: Ken Forrester, Raats. Popular: KWV, Boschendal.",
     15),

    # === POPULAR / COMMERCIAL ===
    ("us_popular_red",
     "List 40 most-sold red wines in American grocery stores and restaurants. These are "
     "high-volume commercial wines. Include: Meiomi Pinot Noir, Josh Cellars Cabernet "
     "Sauvignon, Apothic Red, Bread & Butter Pinot Noir, Decoy by Duckhorn Cabernet, "
     "Dark Horse Cabernet, Bogle Phantom/Old Vine Zinfandel/Cabernet, Menage a Trois "
     "Red, Robert Mondavi Private Selection Cabernet, Sutter Home, Barefoot Cabernet/"
     "Merlot, Woodbridge by Robert Mondavi Cabernet, Mark West Pinot Noir, Louis M. "
     "Martini Sonoma Cabernet, 14 Hands Hot to Trot/Cabernet, J. Lohr Seven Oaks "
     "Cabernet, Layer Cake Shiraz/Cabernet, Bota Box Cabernet/Red Blend, Gnarly Head "
     "Old Vine Zinfandel, Black Box Cabernet, Apothic Dark/Crush/Inferno, Rodney Strong "
     "Cabernet, Fetzer, Ravenswood Vintners Blend Zinfandel, Blackstone Merlot, "
     "Noble Vines Cabernet, Coppola Diamond Collection, Liberty Creek.",
     40),

    ("us_popular_white",
     "List 30 most popular white, rosé, and sparkling wines sold in US grocery stores. "
     "Include: Barefoot Moscato/Pinot Grigio/Chardonnay, Woodbridge Chardonnay, "
     "Kendall-Jackson Vintner's Reserve Chardonnay, Josh Cellars Chardonnay/Sauvignon Blanc, "
     "Cupcake Chardonnay/Sauvignon Blanc, Chateau Ste. Michelle Riesling/Chardonnay, "
     "Nobilo Sauvignon Blanc, Prophecy Pinot Grigio, Ecco Domani Pinot Grigio, "
     "Starborough Sauvignon Blanc, Butter Chardonnay (JaM Cellars), La Marca Prosecco, "
     "Meiomi Chardonnay, Sonoma-Cutrer Chardonnay, Rombauer Chardonnay, "
     "Coppola Rosé/Sofia, Whispering Angel (already in France section — skip if so), "
     "Sutter Home White Zinfandel, Bota Box Pinot Grigio/Chardonnay.",
     30),

    ("import_popular",
     "List 40 most popular imported wines Americans buy at grocery stores. Include: "
     "Yellow Tail (Shiraz, Chardonnay, Cabernet), Santa Margherita Pinot Grigio, "
     "Cavit Pinot Grigio, Ruffino (Chianti, Prosecco, Pinot Grigio, Lumina), "
     "Oyster Bay Sauvignon Blanc, Concha y Toro Casillero del Diablo (Cabernet, "
     "Carmenère), Masi Campofiorin, Antinori Santa Cristina, Campo Viejo (Rioja "
     "Tempranillo, Garnacha), Marqués de Cáceres, Dr. Loosen (Blue Slate Riesling), "
     "Santa Rita 120, Catena Malbec, Mezzacorona Pinot Grigio, Zonin Prosecco, "
     "Bolla (Valpolicella, Soave), Matua Sauvignon Blanc, Brancott Estate, "
     "Trivento Malbec, Layer Cake, 19 Crimes (various), Bread & Butter.",
     40),

    # === SPECIALTY ===
    ("dessert_fortified",
     "List 25 notable dessert and fortified wines. Sauternes: Château Climens, "
     "Château Rieussec, Château Suduiraut, Château Coutet (d'Yquem covered above). "
     "Tokaji: Royal Tokaji, Disznókő, Oremus. Vin Santo: Avignonesi. "
     "Port: Taylor's Vintage/LBV/10yr/20yr/30yr, Graham's Six Grapes/10yr/20yr, "
     "Fonseca Bin 27, Dow's, Warre's Warrior/Otima, Sandeman (various). "
     "Sherry: Tio Pepe, Lustau, Hidalgo La Gitana, Barbadillo. "
     "Madeira: Blandy's 5yr/10yr. Moscato d'Asti: Vietti, Saracco. "
     "Ice wine: Inniskillin Vidal.",
     25),

    ("sparkling_non_champagne",
     "List 20 sparkling wines NOT from Champagne. Franciacorta: Ca' del Bosco, Bellavista. "
     "Crémant d'Alsace: Lucien Albrecht. Crémant de Bourgogne: Louis Bouillot. "
     "Cava: Codorníu, Freixenet (if not in Spain section). English: Nyetimber, Chapel Down. "
     "US: Schramsberg, Gruet, Roederer Estate, Domaine Carneros, Mumm Napa (if not above). "
     "Prosecco: La Marca, Mionetto (if not above). Italian: Ferrari (Trento). "
     "Sekt: Raumland. Australia: Jansz.",
     20),

    # === REST OF WORLD ===
    ("rest_of_world",
     "List 20 notable wines from countries not covered above. Greece: Sigalas (Santorini "
     "Assyrtiko), Alpha Estate, Gaia Wines. Lebanon: Château Musar. Georgia: Pheasant's "
     "Tears (amber wine). Israel: Yarden (Golan Heights Winery). Canada: Mission Hill, "
     "Inniskillin, Norman Hardie. Croatia: Bibich, Stina. Hungary: Royal Tokaji (if not "
     "above). Slovenia: Movia. Japan: Koshu from Grace Wine. England: Nyetimber (if not above). "
     "Other notable producers Americans might discover.",
     20),
]


# ── Utility Functions ────────────────────────────────────────

def compute_cost(input_tokens, output_tokens):
    return (input_tokens * INPUT_COST_PER_M + output_tokens * OUTPUT_COST_PER_M) / 1_000_000


def parse_json_array(text):
    """Extract JSON array from response text, handling markdown fences."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

    start = text.find("[")
    end = text.rfind("]") + 1
    if start < 0 or end <= start:
        return []

    try:
        return json.loads(text[start:end])
    except json.JSONDecodeError as e:
        print(f"  WARNING: JSON parse error: {e}")
        return []


# ── Stage 1: Generate ────────────────────────────────────────

def stage_generate(args):
    """Generate wine lists by category via Haiku, save to JSON + staging table."""
    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    budget = args.budget
    cumulative_cost = 0.0
    total_wines = 0
    filter_cat = args.category

    categories = CATEGORIES
    if filter_cat:
        categories = [(k, p, n) for k, p, n in CATEGORIES if k == filter_cat]
        if not categories:
            print(f"Unknown category: {filter_cat}")
            print(f"Available: {', '.join(k for k, _, _ in CATEGORIES)}")
            return

    try:
        for cat_key, cat_prompt, target_count in categories:
            json_path = DATA_DIR / f"{cat_key}.json"

            # Resume: skip if JSON already exists and has wines in staging
            if json_path.exists() and not args.force:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM source_claude_knowledge WHERE category = %s",
                        (cat_key,)
                    )
                    existing = cur.fetchone()[0]
                if existing > 0:
                    print(f"  SKIP {cat_key} — already generated ({existing} wines in staging)")
                    continue

            if cumulative_cost >= budget:
                print(f"\n  BUDGET EXCEEDED (${cumulative_cost:.4f} >= ${budget:.2f}). Stopping.")
                break

            print(f"\n{'-' * 60}")
            print(f"  Generating: {cat_key} (target: {target_count})")
            print(f"{'-' * 60}")

            user_prompt = (
                f"Generate exactly {target_count} wines for this category:\n\n"
                f"{cat_prompt}\n\n"
                f"Return a JSON array of {target_count} wine objects."
            )

            try:
                response = client.messages.create(
                    model=HAIKU_MODEL,
                    max_tokens=MAX_TOKENS,
                    system=GENERATE_SYSTEM,
                    messages=[{"role": "user", "content": user_prompt}],
                )
            except Exception as e:
                print(f"  API ERROR: {e}")
                time.sleep(2)
                continue

            inp_tok = response.usage.input_tokens
            out_tok = response.usage.output_tokens
            batch_cost = compute_cost(inp_tok, out_tok)
            cumulative_cost += batch_cost

            wines = parse_json_array(response.content[0].text)
            if not wines:
                print(f"  WARNING: No wines parsed for {cat_key}")
                continue

            # Validate structure
            valid_wines = []
            for w in wines:
                if not isinstance(w, dict):
                    continue
                if not w.get("wine_name") or not w.get("producer_name") or not w.get("country"):
                    continue
                valid_wines.append({
                    "wine_name": w["wine_name"].strip(),
                    "producer_name": w["producer_name"].strip(),
                    "country": w["country"].strip(),
                    "region": (w.get("region") or "").strip(),
                    "appellation": (w.get("appellation") or "").strip(),
                    "grapes": (w.get("grapes") or "").strip(),
                    "color": (w.get("color") or "").strip().lower(),
                    "wine_type": (w.get("wine_type") or "table").strip().lower(),
                    "why_notable": (w.get("why_notable") or "").strip(),
                    "category": cat_key,
                })

            # Save JSON
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(valid_wines, f, indent=2, ensure_ascii=False)

            # Insert into staging table
            with conn.cursor() as cur:
                # Clear any previous entries for this category
                cur.execute("DELETE FROM source_claude_knowledge WHERE category = %s", (cat_key,))
                for w in valid_wines:
                    cur.execute("""
                        INSERT INTO source_claude_knowledge
                            (wine_name, producer_name, country, region, appellation,
                             grapes, color, wine_type, category, why_notable)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        w["wine_name"], w["producer_name"], w["country"],
                        w["region"], w["appellation"], w["grapes"],
                        w["color"], w["wine_type"], w["category"], w["why_notable"]
                    ))
            conn.commit()

            total_wines += len(valid_wines)
            print(f"  OK {len(valid_wines)}/{target_count} wines | "
                  f"tokens: {inp_tok}+{out_tok} | cost: ${batch_cost:.4f} | "
                  f"total: ${cumulative_cost:.4f}")

            # Show first few
            for w in valid_wines[:3]:
                print(f"    • {w['producer_name']} — {w['wine_name']} ({w['color']}, {w['country']})")
            if len(valid_wines) > 3:
                print(f"    ... and {len(valid_wines) - 3} more")

            time.sleep(0.5)  # rate limit courtesy

        # Summary
        print(f"\n{'=' * 60}")
        print(f"  GENERATE COMPLETE")
        print(f"  Total wines generated: {total_wines}")
        print(f"  Total cost: ${cumulative_cost:.4f}")
        print(f"  JSON files: {DATA_DIR}")
        print(f"{'=' * 60}")

    finally:
        conn.close()


# ── Stage 2: Dedup ───────────────────────────────────────────

def stage_dedup(args):
    """Match staged wines against canonical producers and wines."""
    conn = get_conn()

    try:
        cur = conn.cursor()

        # Load staged wines that haven't been deduped yet
        cur.execute("""
            SELECT id, wine_name, producer_name, country, region, appellation
            FROM source_claude_knowledge
            WHERE match_status = 'pending'
            ORDER BY category, producer_name, wine_name
        """)
        staged = cur.fetchall()
        print(f"Staged wines to dedup: {len(staged)}")
        if not staged:
            return

        # Load all canonical producers into memory (same pattern as batch_matcher)
        print("Loading canonical producers...")
        cur.execute("""
            SELECT id, name, name_normalized, country_id
            FROM producers WHERE deleted_at IS NULL
        """)
        producers_raw = cur.fetchall()
        producer_by_norm = {}
        producer_by_stripped = defaultdict(list)
        for pid, name, name_norm, country_id in producers_raw:
            norm = name_norm or normalize(name)
            producer_by_norm[norm] = {"id": pid, "name": name, "country_id": country_id}
            stripped = normalize_producer(name)
            if stripped and stripped != norm:
                producer_by_stripped[stripped].append(
                    {"id": pid, "name": name, "country_id": country_id}
                )
        print(f"  {len(producers_raw)} producers loaded")

        # Load producer aliases
        cur.execute("SELECT producer_id, name, name_normalized FROM producer_aliases")
        producer_by_alias = {}
        for row in cur.fetchall():
            anorm = row[2] or normalize(row[1])
            producer_by_alias[anorm] = row[0]
        print(f"  {len(producer_by_alias)} aliases loaded")

        # Load countries
        cur.execute("SELECT id, name, iso_code FROM countries")
        countries = {}
        for cid, name, iso in cur.fetchall():
            countries[name.lower()] = cid
            if iso:
                countries[iso.lower()] = cid
        for alias, target in [("usa", "united states"), ("us", "united states"),
                              ("uk", "united kingdom"), ("england", "united kingdom")]:
            if target in countries:
                countries[alias] = countries[target]

        # Match each staged wine
        stats = defaultdict(int)
        batch_updates = []

        for row_id, wine_name, producer_name, country, region, appellation in staged:
            country_id = countries.get(country.lower().strip()) if country else None

            # Producer matching (3-tier, same as batch_matcher)
            prod_norm = normalize(producer_name)
            prod_match = None
            prod_conf = 0

            # Tier 1: exact normalized
            p = producer_by_norm.get(prod_norm)
            if p:
                prod_match = p
                prod_conf = 1.0
            else:
                # Tier 2: alias
                pid = producer_by_alias.get(prod_norm)
                if pid:
                    prod_match = {"id": pid}
                    prod_conf = 0.9
                else:
                    # Tier 3: stripped
                    stripped = normalize_producer(producer_name)
                    if stripped and stripped != prod_norm:
                        candidates = producer_by_stripped.get(stripped, [])
                        if not candidates:
                            p2 = producer_by_norm.get(stripped)
                            if p2:
                                candidates = [p2]
                        if candidates:
                            if country_id:
                                same = [c for c in candidates if c.get("country_id") == country_id]
                                prod_match = same[0] if same else candidates[0]
                            else:
                                prod_match = candidates[0]
                            prod_conf = 0.85 if country_id and prod_match.get("country_id") == country_id else 0.8

            if not prod_match:
                batch_updates.append((row_id, None, None, "new", 0, "no producer match"))
                stats["new_no_producer"] += 1
                continue

            producer_id = prod_match["id"]

            # Wine matching: load this producer's wines
            cur.execute("""
                SELECT id, name, name_normalized
                FROM wines
                WHERE producer_id = %s AND deleted_at IS NULL
            """, (producer_id,))
            producer_wines = cur.fetchall()

            wine_norm = normalize(wine_name)
            wine_norm_stripped = normalize_wine_name(wine_name)
            wine_match = None
            wine_conf = 0

            for wid, wname, wnorm in producer_wines:
                wnorm = wnorm or normalize(wname)
                # Exact match
                if wnorm == wine_norm or wnorm == wine_norm_stripped:
                    wine_match = wid
                    wine_conf = 1.0
                    break
                # Normalized wine name match
                w_stripped = normalize_wine_name(wname)
                if w_stripped == wine_norm_stripped and wine_norm_stripped:
                    wine_match = wid
                    wine_conf = 0.95
                    break
                # Substring containment (wine name inside canon or vice versa)
                if len(wine_norm_stripped) >= 5 and len(wnorm) >= 5:
                    if wine_norm_stripped in wnorm or wnorm in wine_norm_stripped:
                        if not wine_match or len(wnorm) > len(normalize(producer_wines[0][1])):
                            wine_match = wid
                            wine_conf = 0.75

            if wine_match:
                batch_updates.append((row_id, producer_id, wine_match, "exists", wine_conf, None))
                stats["exists"] += 1
            else:
                batch_updates.append((row_id, producer_id, None, "new", prod_conf,
                                      f"producer matched ({prod_conf:.2f}), wine not found"))
                stats["new_with_producer"] += 1

        # Write updates
        for row_id, prod_id, wine_id, status, conf, notes in batch_updates:
            cur.execute("""
                UPDATE source_claude_knowledge
                SET canonical_producer_id = %s,
                    canonical_wine_id = %s,
                    match_status = %s,
                    match_confidence = %s,
                    match_notes = %s,
                    processed_at = NOW()
                WHERE id = %s
            """, (prod_id, wine_id, status, conf, notes, row_id))
        conn.commit()

        # Summary
        print(f"\n{'=' * 60}")
        print(f"  DEDUP COMPLETE")
        print(f"  Already exists in canon:  {stats['exists']}")
        print(f"  New (producer matched):   {stats['new_with_producer']}")
        print(f"  New (no producer match):  {stats['new_no_producer']}")
        print(f"  Total processed:          {len(staged)}")
        print(f"{'=' * 60}")

    finally:
        conn.close()


# ── Stage 3: TTB Match ───────────────────────────────────────

def stage_ttb_match(args):
    """Match new wines against source_ttb_colas by brand_name + fanciful_name."""
    conn = get_conn()

    try:
        cur = conn.cursor()

        # Load new wines (gaps) — only US wines for TTB matching
        cur.execute("""
            SELECT id, wine_name, producer_name, country, appellation
            FROM source_claude_knowledge
            WHERE match_status = 'new'
              AND LOWER(country) = 'united states'
              AND ttb_id IS NULL
        """)
        us_gaps = cur.fetchall()
        print(f"US gap wines to TTB-match: {len(us_gaps)}")
        if not us_gaps:
            print("No US gap wines to match.")
            return

        matched = 0
        for row_id, wine_name, producer_name, country, appellation in us_gaps:
            # Search TTB by brand_name (producer)
            prod_norm = normalize(producer_name).replace(" ", "%")
            cur.execute("""
                SELECT ttb_id, brand_name, fanciful_name, origin_desc, class_type
                FROM source_ttb_colas
                WHERE LOWER(brand_name) LIKE %s
                  AND fanciful_name IS NOT NULL
                  AND fanciful_name != ''
                LIMIT 200
            """, (f"%{prod_norm}%",))
            ttb_rows = cur.fetchall()

            if not ttb_rows:
                # Try exact normalized match
                cur.execute("""
                    SELECT ttb_id, brand_name, fanciful_name, origin_desc, class_type
                    FROM source_ttb_colas
                    WHERE LOWER(REPLACE(brand_name, ' ', '')) = %s
                      AND fanciful_name IS NOT NULL
                    LIMIT 200
                """, (normalize(producer_name).replace(" ", ""),))
                ttb_rows = cur.fetchall()

            if not ttb_rows:
                continue

            # Fuzzy match wine name against fanciful_name
            wine_norm = normalize(wine_name)
            best_match = None
            best_score = 0

            for ttb_id, brand, fanciful, origin, class_type in ttb_rows:
                fn = normalize(fanciful or "")
                if not fn:
                    continue

                # Exact match
                if fn == wine_norm:
                    best_match = ttb_id
                    best_score = 1.0
                    break

                # Containment
                if wine_norm and len(wine_norm) >= 4:
                    if wine_norm in fn or fn in wine_norm:
                        score = min(len(wine_norm), len(fn)) / max(len(wine_norm), len(fn))
                        if score > best_score:
                            best_match = ttb_id
                            best_score = score

            if best_match and best_score >= 0.5:
                cur.execute("""
                    UPDATE source_claude_knowledge
                    SET ttb_id = %s, ttb_match_confidence = %s
                    WHERE id = %s
                """, (best_match, best_score, row_id))
                matched += 1

        conn.commit()

        print(f"\n{'=' * 60}")
        print(f"  TTB MATCH COMPLETE")
        print(f"  US gap wines:  {len(us_gaps)}")
        print(f"  TTB matched:   {matched}")
        print(f"{'=' * 60}")

    finally:
        conn.close()


# ── Stage 4: Validate ────────────────────────────────────────

def stage_validate(args):
    """Haiku/Sonnet cross-validates gap wines independently for accuracy."""
    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()
    conn.autocommit = True
    with conn.cursor() as _c:
        _c.execute("SET statement_timeout = 0")

    model = getattr(args, "model", "haiku")
    recheck = getattr(args, "recheck", False)
    if model == "sonnet":
        model_id = SONNET_MODEL
        input_cpm = 3.00
        output_cpm = 15.00
        print(f"Using SONNET for validation (higher quality, ~5x cost)")
    else:
        model_id = HAIKU_MODEL
        input_cpm = INPUT_COST_PER_M
        output_cpm = OUTPUT_COST_PER_M

    budget = args.budget
    cumulative_cost = 0.0
    batch_size = 20 if model == "sonnet" else 25

    try:
        cur = conn.cursor()

        # Load wines needing validation
        if recheck:
            # Re-validate already-confirmed wines (second quality gate)
            cur.execute("""
                SELECT id, wine_name, producer_name, country, region, appellation,
                       grapes, color, wine_type, ttb_id
                FROM source_claude_knowledge
                WHERE match_status = 'new'
                  AND validation_status = 'confirmed'
                ORDER BY category, producer_name
            """)
        else:
            cur.execute("""
                SELECT id, wine_name, producer_name, country, region, appellation,
                       grapes, color, wine_type, ttb_id
                FROM source_claude_knowledge
                WHERE match_status = 'new'
                  AND validation_status = 'pending'
                ORDER BY category, producer_name
            """)
        to_validate = cur.fetchall()
        print(f"Wines to validate: {len(to_validate)}")
        if not to_validate:
            return

        # TTB-matched wines get auto-validated (TTB match IS verification for US wines)
        auto_validated = 0
        remaining = []
        for row in to_validate:
            ttb_id = row[9]
            if ttb_id:
                cur.execute("""
                    UPDATE source_claude_knowledge
                    SET validation_status = 'confirmed',
                        validation_notes = 'TTB COLA match (auto-validated)'
                    WHERE id = %s
                """, (row[0],))
                auto_validated += 1
            else:
                remaining.append(row)
        conn.commit()
        print(f"  Auto-validated via TTB: {auto_validated}")
        print(f"  Remaining for Haiku validation: {len(remaining)}")

        if not remaining:
            return

        # Batch validate
        batches = [remaining[i:i + batch_size] for i in range(0, len(remaining), batch_size)]
        total_confirmed = 0
        total_uncertain = 0
        total_mismatch = 0

        for batch_idx, batch in enumerate(batches):
            if cumulative_cost >= budget:
                print(f"\n  BUDGET EXCEEDED. Stopping validation.")
                break

            # Build validation prompt — ask model to independently verify each wine
            lines = ["Verify each of these wines independently:\n"]
            for i, row in enumerate(batch, 1):
                _, wine_name, producer, country, region, appellation, grapes, color, wtype, _ = row
                lines.append(
                    f"{i}. \"{wine_name}\" by {producer}\n"
                    f"   Claimed: {country}, {region}, {appellation or 'N/A'}\n"
                    f"   Grapes: {grapes or 'N/A'}, Color: {color}, Type: {wtype}"
                )

            prompt = "\n".join(lines)

            try:
                response = client.messages.create(
                    model=model_id,
                    max_tokens=MAX_TOKENS,
                    system=VALIDATE_SYSTEM,
                    messages=[{"role": "user", "content": prompt}],
                )
            except Exception as e:
                print(f"  Batch {batch_idx + 1}: API ERROR: {e}")
                time.sleep(2)
                continue

            inp_tok = response.usage.input_tokens
            out_tok = response.usage.output_tokens
            batch_cost = (inp_tok * input_cpm + out_tok * output_cpm) / 1_000_000
            cumulative_cost += batch_cost

            results = parse_json_array(response.content[0].text)

            for item in results:
                if not isinstance(item, dict):
                    continue
                idx = item.get("idx")
                if not isinstance(idx, int) or idx < 1 or idx > len(batch):
                    continue

                row_id = batch[idx - 1][0]
                exists = item.get("exists", False)
                confidence = item.get("confidence", 0)
                notes_parts = []
                if item.get("notes"):
                    notes_parts.append(item["notes"])

                # Determine validation status
                if exists and confidence >= 0.85:
                    # Check for field mismatches
                    mismatches = []
                    if item.get("producer_correct") is False:
                        mismatches.append("producer")
                    if item.get("grapes_correct") is False:
                        mismatches.append("grapes")
                    if item.get("region_correct") is False:
                        mismatches.append("region")
                    if item.get("color_correct") is False:
                        mismatches.append("color")

                    if mismatches:
                        status = "mismatch"
                        notes_parts.insert(0, f"Mismatches: {', '.join(mismatches)}")
                        total_mismatch += 1
                    else:
                        status = "confirmed"
                        total_confirmed += 1
                else:
                    status = "uncertain"
                    total_uncertain += 1

                cur.execute("""
                    UPDATE source_claude_knowledge
                    SET validation_status = %s,
                        validation_notes = %s
                    WHERE id = %s
                """, (status, "; ".join(notes_parts) if notes_parts else None, row_id))

            conn.commit()

            print(f"  Batch {batch_idx + 1}/{len(batches)}: "
                  f"cost ${batch_cost:.4f} | total ${cumulative_cost:.4f}")

            time.sleep(0.3)

        print(f"\n{'=' * 60}")
        print(f"  VALIDATE COMPLETE")
        print(f"  Auto-validated (TTB):  {auto_validated}")
        print(f"  Haiku confirmed:       {total_confirmed}")
        print(f"  Haiku mismatch:        {total_mismatch}")
        print(f"  Haiku uncertain:       {total_uncertain}")
        print(f"  Total cost:            ${cumulative_cost:.4f}")
        print(f"{'=' * 60}")

    finally:
        conn.close()


# ── Stage 5: Promote ─────────────────────────────────────────

def stage_promote(args):
    """Create verified wines in canonical tables."""
    conn = get_conn()
    conn.autocommit = True
    dry_run = args.dry_run

    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = 0")

        # Load validated new wines
        cur.execute("""
            SELECT id, wine_name, producer_name, country, region, appellation,
                   grapes, color, wine_type, canonical_producer_id, ttb_id
            FROM source_claude_knowledge
            WHERE match_status = 'new'
              AND validation_status = 'confirmed'
              AND promoted_at IS NULL
            ORDER BY producer_name, wine_name
        """)
        to_promote = cur.fetchall()
        print(f"Validated wines to promote: {len(to_promote)}")
        if not to_promote:
            return

        if dry_run:
            print("DRY RUN — no writes\n")

        # Load reference lookups
        cur.execute("SELECT id, name FROM countries")
        countries = {name.lower(): cid for cid, name in cur.fetchall()}
        countries.update({"usa": countries.get("united states"), "us": countries.get("united states"),
                          "uk": countries.get("united kingdom")})

        cur.execute("SELECT id, name FROM regions")
        regions_raw = cur.fetchall()
        region_by_norm = {normalize(r[1]): r[0] for r in regions_raw}

        cur.execute("SELECT id, name FROM appellations")
        app_raw = cur.fetchall()
        app_by_norm = {normalize(a[1]): a[0] for a in app_raw}

        # Load existing producers for dedup
        cur.execute("SELECT id, name_normalized FROM producers WHERE deleted_at IS NULL")
        existing_producers = {row[1]: row[0] for row in cur.fetchall()}

        producers_created = 0
        wines_created = 0
        wines_skipped = 0
        errors = 0

        for (row_id, wine_name, producer_name, country, region, appellation,
             grapes, color, wine_type, existing_prod_id, ttb_id) in to_promote:

            country_id = countries.get(country.lower().strip()) if country else None
            region_id = region_by_norm.get(normalize(region)) if region else None
            appellation_id = app_by_norm.get(normalize(appellation)) if appellation else None

            # Producer: use existing match or create
            producer_id = existing_prod_id
            if not producer_id:
                prod_norm = normalize(producer_name)
                if prod_norm in existing_producers:
                    producer_id = existing_producers[prod_norm]
                elif not dry_run:
                    try:
                        slug = slugify(producer_name)[:180] + f"-ks-{uuid.uuid4().hex[:6]}"
                        cur.execute("""
                            INSERT INTO producers (name, name_normalized, slug, country_id)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                        """, (producer_name, prod_norm, slug, country_id))
                        result = cur.fetchone()
                        if result:
                            producer_id = result[0]
                            existing_producers[prod_norm] = producer_id
                            producers_created += 1
                    except Exception as e:
                        if "23505" in str(e):
                            # Slug conflict — lookup
                            cur.execute(
                                "SELECT id FROM producers WHERE name_normalized = %s AND deleted_at IS NULL",
                                (prod_norm,)
                            )
                            r = cur.fetchone()
                            if r:
                                producer_id = r[0]
                                existing_producers[prod_norm] = producer_id
                        else:
                            errors += 1
                            print(f"  ERROR creating producer '{producer_name}': {e}")
                            continue
                else:
                    producers_created += 1  # dry-run count

            if not producer_id and not dry_run:
                wines_skipped += 1
                continue

            # Wine: create
            wine_norm = normalize(wine_name)
            slug = slugify(wine_name)[:180] + f"-ks-{uuid.uuid4().hex[:6]}"

            # Check existing wine under this producer
            if producer_id and not dry_run:
                cur.execute("""
                    SELECT id FROM wines
                    WHERE producer_id = %s AND name_normalized = %s AND deleted_at IS NULL
                """, (producer_id, wine_norm))
                existing_wine = cur.fetchone()
                if existing_wine:
                    # Already exists — update staging and skip
                    cur.execute("""
                        UPDATE source_claude_knowledge
                        SET canonical_wine_id = %s, match_status = 'exists', promoted_at = NOW()
                        WHERE id = %s
                    """, (existing_wine[0], row_id))
                    wines_skipped += 1
                    continue

            # Normalize color and wine_type (DB uses 'rose' without accent)
            color_val = color.lower().strip() if color else None
            if color_val == "rosé":
                color_val = "rose"
            if color_val not in ("red", "white", "rose"):
                color_val = None
            wtype = wine_type if wine_type in ("table", "sparkling", "fortified", "dessert") else "table"

            if not dry_run:
                try:
                    cur.execute("""
                        INSERT INTO wines
                            (name, name_normalized, slug, producer_id, country_id,
                             region_id, appellation_id, color, wine_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (wine_name, wine_norm, slug, producer_id, country_id,
                          region_id, appellation_id, color_val, wtype))
                    result = cur.fetchone()
                    if result:
                        wine_id = result[0]
                        wines_created += 1

                        # Update staging
                        cur.execute("""
                            UPDATE source_claude_knowledge
                            SET canonical_wine_id = %s, match_status = 'promoted', promoted_at = NOW()
                            WHERE id = %s
                        """, (wine_id, row_id))

                        # Add TTB external_id if matched
                        if ttb_id:
                            cur.execute("""
                                INSERT INTO external_ids (entity_type, entity_id, system, external_id)
                                VALUES ('wine', %s, 'cola', %s)
                                ON CONFLICT DO NOTHING
                            """, (wine_id, ttb_id))

                        # Add grape links if we have grape data
                        if grapes:
                            _link_grapes(cur, wine_id, grapes)

                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  ERROR creating wine '{wine_name}': {e}")
                    continue
            else:
                wines_created += 1

        print(f"\n{'=' * 60}")
        print(f"  PROMOTE COMPLETE {'(DRY RUN)' if dry_run else ''}")
        print(f"  Producers created:  {producers_created}")
        print(f"  Wines created:      {wines_created}")
        print(f"  Wines skipped:      {wines_skipped}")
        print(f"  Errors:             {errors}")
        print(f"{'=' * 60}")

    finally:
        conn.close()


def _link_grapes(cur, wine_id, grapes_str):
    """Link wine to grapes from comma-separated string."""
    grape_names = [g.strip() for g in grapes_str.split(",") if g.strip()]
    for grape_name in grape_names:
        gnorm = normalize(grape_name)
        # Look up grape by name or synonym
        cur.execute("""
            SELECT id FROM grapes
            WHERE name_normalized = %s OR LOWER(name) = %s
            LIMIT 1
        """, (gnorm, grape_name.lower()))
        result = cur.fetchone()
        if not result:
            # Try synonyms
            cur.execute("""
                SELECT g.id FROM grapes g
                JOIN grape_synonyms gs ON gs.grape_id = g.id
                WHERE LOWER(gs.synonym) = %s
                LIMIT 1
            """, (grape_name.lower(),))
            result = cur.fetchone()
        if result:
            try:
                cur.execute("""
                    INSERT INTO wine_grapes (wine_id, grape_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (wine_id, result[0]))
            except Exception:
                pass  # skip duplicates


# ── Stage 6: Report ──────────────────────────────────────────

def stage_report(args):
    """Show summary of pipeline state."""
    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM source_claude_knowledge")
        total = cur.fetchone()[0]

        cur.execute("""
            SELECT match_status, COUNT(*)
            FROM source_claude_knowledge
            GROUP BY match_status
            ORDER BY match_status
        """)
        status_counts = cur.fetchall()

        cur.execute("""
            SELECT validation_status, COUNT(*)
            FROM source_claude_knowledge
            WHERE match_status = 'new'
            GROUP BY validation_status
        """)
        val_counts = cur.fetchall()

        cur.execute("""
            SELECT category, COUNT(*),
                   SUM(CASE WHEN match_status = 'exists' THEN 1 ELSE 0 END) as existing,
                   SUM(CASE WHEN match_status = 'new' THEN 1 ELSE 0 END) as new,
                   SUM(CASE WHEN match_status = 'promoted' THEN 1 ELSE 0 END) as promoted
            FROM source_claude_knowledge
            GROUP BY category
            ORDER BY category
        """)
        cat_counts = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) FROM source_claude_knowledge
            WHERE match_status = 'new' AND ttb_id IS NOT NULL
        """)
        ttb_matched = cur.fetchone()[0]

        print(f"\n{'=' * 60}")
        print(f"  KNOWLEDGE SEED PIPELINE REPORT")
        print(f"{'=' * 60}")
        print(f"\n  Total staged wines: {total}")
        print(f"\n  Match status:")
        for status, count in status_counts:
            print(f"    {status:20s} {count:5d}")

        print(f"\n  Validation status (new wines only):")
        for status, count in val_counts:
            print(f"    {status:20s} {count:5d}")

        print(f"\n  TTB-matched gap wines: {ttb_matched}")

        print(f"\n  Per category:")
        print(f"    {'Category':<30s} {'Total':>6s} {'Exists':>7s} {'New':>5s} {'Promoted':>9s}")
        print(f"    {'-' * 57}")
        for cat, total_c, existing, new, promoted in cat_counts:
            print(f"    {cat:<30s} {total_c:>6d} {existing:>7d} {new:>5d} {promoted:>9d}")

        print(f"\n{'=' * 60}")

    finally:
        conn.close()


# ── CLI ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Knowledge Seed: generate and promote notable wines from training data"
    )
    subparsers = parser.add_subparsers(dest="stage", help="Pipeline stage to run")

    # generate
    gen = subparsers.add_parser("generate", help="Generate wine lists by category")
    gen.add_argument("--budget", type=float, default=10.0, help="Max spend in USD")
    gen.add_argument("--category", type=str, default=None, help="Run single category")
    gen.add_argument("--force", action="store_true", help="Regenerate even if JSON exists")

    # dedup
    subparsers.add_parser("dedup", help="Match staged wines against canonical DB")

    # ttb-match
    subparsers.add_parser("ttb-match", help="Match gap wines against TTB COLA")

    # validate
    val = subparsers.add_parser("validate", help="Haiku/Sonnet cross-validation")
    val.add_argument("--budget", type=float, default=5.0, help="Max spend in USD")
    val.add_argument("--model", choices=["haiku", "sonnet"], default="haiku",
                     help="Model to use (sonnet is higher quality, ~5x cost)")
    val.add_argument("--recheck", action="store_true",
                     help="Re-validate already-confirmed wines (second quality gate)")

    # promote
    pro = subparsers.add_parser("promote", help="Create verified wines in canon")
    pro.add_argument("--dry-run", action="store_true", help="Preview without writing")

    # report
    subparsers.add_parser("report", help="Pipeline status report")

    args = parser.parse_args()

    if not args.stage:
        parser.print_help()
        return

    stages = {
        "generate": stage_generate,
        "dedup": stage_dedup,
        "ttb-match": stage_ttb_match,
        "validate": stage_validate,
        "promote": stage_promote,
        "report": stage_report,
    }

    t0 = time.time()
    stages[args.stage](args)
    elapsed = time.time() - t0
    print(f"\nCompleted in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
