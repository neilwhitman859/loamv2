"""Insert appellation_rules for Batch C appellations."""
import json
import sys
sys.path.insert(0, 'C:/Users/neilw/Documents/GitHub/loamv2')
from pipeline.lib.db import get_conn

conn = get_conn()
cur = conn.cursor()

today = '2026-04-07'

MONT_CAUME_ID = '656ba7de-793c-4fa0-b874-530d57a8cd94'
AMYNTAIO_ID   = '57f6d70b-90b5-4bb4-9736-ecee738af5a4'
CERES_ID      = '50111107-0ce8-4bd8-a251-c680b33a9010'
RIEBEEK_ID    = '3a823cad-1c80-44dc-b9c7-0710f08067ac'
CDEBV_ID      = '37faaf4d-5d96-4e14-bcf0-ebd543f61121'

INSERT_SQL = """
    INSERT INTO appellation_rules
        (appellation_id, rules, source, source_url, source_organization,
         source_document_title, source_accessed_date, source_text_excerpt)
    VALUES (%s, %s::jsonb, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (appellation_id) DO NOTHING
"""

# ============================================================
# 1. MONT CAUME
# ============================================================
rules_mont_caume = {
    "colors": ["red", "rose", "white"],
    "wine_type": "still",
    "grape_rules": {
        "red": {
            "mode": "multi_variety",
            "principal_varieties": [
                "Grenache Noir (Garnacha Tinta)", "Cinsaut", "Mourvedre (Monastrell)", "Syrah"
            ],
            "notes": "IGP permissive blend — no single-variety minimum mandated at appellation level"
        },
        "rose": {
            "mode": "multi_variety",
            "principal_varieties": [
                "Grenache Noir (Garnacha Tinta)", "Cinsaut", "Mourvedre (Monastrell)", "Syrah"
            ],
            "notes": "IGP permissive blend rules"
        },
        "white": {
            "mode": "multi_variety",
            "principal_varieties": [
                "Clairette Blanche", "Vermentino (Rolle)", "Ugni Blanc (Trebbiano Toscano)"
            ],
            "notes": "IGP permissive blend rules"
        }
    },
    "max_yield_hl_ha": {"red": 50, "rose": 50, "white": 55},
    "min_alcohol_pct": {"red": 11.0, "rose": 11.0, "white": 10.5},
    "geographic_area": "Approximately 26 communes in arrondissement of Toulon, Var (83), Provence — hinterland of Bandol",
    "notes": "IGP sub-zone of IGP Var. More restrictive than base IGP Var but permissive relative to neighboring AOC Bandol."
}

cur.execute(INSERT_SQL, (
    MONT_CAUME_ID,
    json.dumps(rules_mont_caume),
    "INAO / European Commission",
    "https://www.inao.gouv.fr/nos-actions/proteger-et-valoriser/les-signes-officiels-de-la-qualite-et-de-l-origine/vins",
    "INAO (Institut National de l'Origine et de la Qualite)",
    "Cahier des charges IGP Mont Caume",
    today,
    "IGP Mont Caume: rouges/roses (Grenache N., Cinsault, Mourvedre, Syrah); blancs (Clairette B., Vermentino/Rolle, Ugni Blanc). "
    "Rendement max: 50 hl/ha (R/Ro), 55 hl/ha (B). TAV min: 11% (R/Ro), 10,5% (B). "
    "Zone geographique: ~26 communes du Var, arrondissement de Toulon."
))
print(f"Mont Caume rule inserted: {cur.rowcount}")

# ============================================================
# 2. AMYNTAIO
# ============================================================
rules_amyntaio = {
    "colors": ["red", "rose"],
    "wine_type": "still_and_sparkling",
    "sparkling_permitted": True,
    "grape_rules": {
        "red": {
            "mode": "single_variety_dominant",
            "principal_variety": "Xinomavro (Xynomavro)",
            "min_pct": 80,
            "notes": "Xinomavro >=80% required for PDO red; complementary varieties permitted up to 20%"
        },
        "rose": {
            "mode": "single_variety_dominant",
            "principal_variety": "Xinomavro (Xynomavro)",
            "min_pct": 80,
            "notes": "Xinomavro >=80%; produces distinctive dry rose"
        },
        "sparkling": {
            "mode": "single_variety_dominant",
            "principal_variety": "Xinomavro (Xynomavro)",
            "min_pct": 80,
            "notes": "Amyntaio is the only Greek PDO producing sparkling wine from Xinomavro (POP Amynteo)"
        }
    },
    "altitude_min_m": 650,
    "geographic_area": "Western Macedonia, northern Greece — basin around lake Petron at altitude 650m+",
    "region": "Macedonia",
    "notes": "Unique PDO in Greece for producing red, rose, AND sparkling wines from Xinomavro. High altitude produces wines with more elegance and acidity than Naoussa. EU PDO registered."
}

cur.execute(INSERT_SQL, (
    AMYNTAIO_ID,
    json.dumps(rules_amyntaio),
    "EU PDO Register / Wines of Greece",
    "https://www.winesofgreece.org/variety/xinomavro/",
    "Greek Ministry of Rural Development and Food / EU PDO Register",
    "Amyntaio PDO Specification (EU Protected Designation of Origin)",
    today,
    "Xinomavro is solely responsible for or participates in PDO Amynteo reds. "
    "Amyntaio produces red, rose, and sparkling wines (POP Amynteo) from Xinomavro >=80%. "
    "Altitude 650m+, western Macedonia. Only Greek PDO making both red/rose and sparkling from Xinomavro."
))
print(f"Amyntaio rule inserted: {cur.rowcount}")

# ============================================================
# 3. CERES
# ============================================================
rules_ceres = {
    "colors": ["red", "white", "rose"],
    "wine_type": "still_and_sparkling",
    "system": "Wine of Origin (WO)",
    "tier": "ward",
    "district": "Breede River Valley",
    "geographic_area": "Ceres valley in the Breede River Valley district, Western Cape. "
                       "High-altitude valley (300-500m) bounded by Skurweberg and Hex River Mountains.",
    "grape_rules": {
        "general": {
            "mode": "permissive",
            "notes": "South African WO is geographic certification — no mandatory grape variety requirements. "
                     "Any cultivar approved for SA wine production may be used. "
                     "Single-cultivar wine requires >=75% of that variety."
        }
    },
    "typical_varieties": ["Cabernet Sauvignon", "Chenin Blanc", "Pinotage", "Sauvignon Blanc", "Chardonnay"],
    "notes": "Ceres is a ward (most specific WO geographic unit) within Breede River Valley district. "
             "Cold climate favors aromatic white varieties. "
             "The ward is distinct from the broader Ceres Plateau district designation."
}

cur.execute(INSERT_SQL, (
    CERES_ID,
    json.dumps(rules_ceres),
    "SAWIS / WOSA",
    "https://www.wosa.co.za/The-Industry/Wines-Of-Origin/Introduction/",
    "SAWIS (South African Wine Industry Information and Systems) / WOSA (Wines of South Africa)",
    "Wine of Origin Scheme — Ward: Ceres (Breede River Valley District)",
    today,
    "Ceres is a Wine of Origin ward within the Breede River Valley district. "
    "WO is a geographic certification system with no mandatory grape variety requirements. "
    "Cold mountain valley climate. Typical varieties: Cabernet Sauvignon, Chenin Blanc, Pinotage, Sauvignon Blanc, Chardonnay."
))
print(f"Ceres rule inserted: {cur.rowcount}")

# ============================================================
# 4. RIEBEEKSRIVIER
# ============================================================
rules_riebeeksrivier = {
    "colors": ["red", "white", "rose"],
    "wine_type": "still_and_sparkling",
    "system": "Wine of Origin (WO)",
    "tier": "ward",
    "district": "Swartland",
    "geographic_area": "Ward in the Swartland district, Western Cape, centered on the Riebeeksrivier valley "
                       "around Riebeek-Kasteel and Riebeek-West. Shale and granite soils. "
                       "Mediterranean climate with warm dry summers.",
    "grape_rules": {
        "general": {
            "mode": "permissive",
            "notes": "South African WO is geographic certification — no mandatory grape variety requirements. "
                     "Any cultivar approved for SA wine production may be used. "
                     "Single-cultivar wine requires >=75% of that variety."
        }
    },
    "typical_varieties": ["Cabernet Sauvignon", "Chenin Blanc", "Pinotage", "Grenache", "Syrah", "Mourvedre"],
    "notes": "Riebeeksrivier is a ward within Swartland district. "
             "The Swartland is known for old-vine Chenin Blanc and Rhone varieties (Grenache, Syrah, Mourvedre). "
             "Ward surrounds the twin towns of Riebeek-Kasteel and Riebeek-West."
}

cur.execute(INSERT_SQL, (
    RIEBEEK_ID,
    json.dumps(rules_riebeeksrivier),
    "SAWIS / WOSA",
    "https://www.wosa.co.za/The-Industry/Wines-Of-Origin/Introduction/",
    "SAWIS (South African Wine Industry Information and Systems) / WOSA (Wines of South Africa)",
    "Wine of Origin Scheme — Ward: Riebeeksrivier (Swartland District)",
    today,
    "Riebeeksrivier is a Wine of Origin ward within the Swartland district. "
    "WO is a geographic certification system. Swartland known for old-vine Chenin Blanc and Rhone varieties. "
    "Typical varieties: Cabernet Sauvignon, Chenin Blanc, Pinotage, Grenache, Syrah, Mourvedre."
))
print(f"Riebeeksrivier rule inserted: {cur.rowcount}")

# ============================================================
# 5. COTE DE BEAUNE-VILLAGES
# ============================================================
rules_cdebv = {
    "colors": ["red"],
    "wine_type": "still",
    "grape_rules": {
        "red": {
            "mode": "single_variety",
            "grape": "Pinot Noir N",
            "min_pct": 100,
            "notes": "Pinot Noir is the sole authorized variety for Cote de Beaune-Villages AOC"
        }
    },
    "max_yield_hl_ha": {"base": 45},
    "min_alcohol_pct": {"base": 10.5},
    "eligible_communes": [
        "Pernand-Vergelesses", "Ladoix-Serrigny", "Chorey-les-Beaune", "Savigny-les-Beaune",
        "Auxey-Duresses", "Monthelie", "Saint-Romain", "Meursault", "Blagny",
        "Puligny-Montrachet", "Chassagne-Montrachet", "Saint-Aubin", "Santenay", "Les Maranges"
    ],
    "excluded_communes": ["Beaune", "Aloxe-Corton", "Pommard", "Volnay"],
    "notes": "Red-only catch-all AOC for Cote de Beaune communes that choose to declassify red wine to "
             "this regional appellation rather than use their own village name. "
             "Eligible communes can produce white wine under their own village AOC. "
             "No Premier Cru or Grand Cru sites qualify for this appellation."
}

cur.execute(INSERT_SQL, (
    CDEBV_ID,
    json.dumps(rules_cdebv),
    "INAO (Institut National de l'Origine et de la Qualite)",
    "https://www.inao.gouv.fr/Les-signes-officiels-de-la-qualite-et-de-l-origine-SIQO/Vins",
    "INAO (Institut National de l'Origine et de la Qualite)",
    "Cahier des charges AOC Cote de Beaune-Villages",
    today,
    "AOC Cote de Beaune-Villages: vin rouge uniquement, cepage Pinot Noir N. 100%. "
    "14 communes eligibles: Pernand-Vergelesses, Ladoix, Chorey, Savigny, Auxey, Monthelie, "
    "Saint-Romain, Meursault, Blagny, Puligny-Montrachet, Chassagne-Montrachet, Saint-Aubin, "
    "Santenay, Les Maranges. Beaune, Aloxe-Corton, Pommard et Volnay exclus. "
    "Rendement max ~45 hl/ha. TAV min 10,5%."
))
print(f"Cote de Beaune-Villages rule inserted: {cur.rowcount}")

conn.commit()
print("All committed.")
cur.close()
conn.close()
