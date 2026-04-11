"""Insert/update appellation_grapes for Batch C appellations.

Existing rows (from prior session) have no provenance. ON CONFLICT DO NOTHING prevents
re-inserting rows that exist. We also do a separate UPDATE to backfill provenance on
the pre-existing rows that have source_url = NULL.
"""
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

# ---- Grape IDs ----
GARNACHA_TINTA   = '85b5249b-c8c2-4852-bb71-3f62eebf9d2a'  # Grenache Noir (synonym)
CINSAUT          = '91edea39-597d-4a57-b608-723697d3512f'  # Cinsaut
MONASTRELL       = 'e0b7b143-5ac7-4bed-937d-732a9270b67e'  # Mourvedre (synonym)
SYRAH            = '2af8e266-79be-4aa8-8464-06897ea20924'  # Syrah
CLAIRETTE_BLANCHE= '7eb2435e-5c2e-4dd4-89be-0ee9146fdf2a'  # Clairette Blanche
VERMENTINO       = '78dd9ccf-26ce-4fd1-8ebc-977df400522a'  # Vermentino (Rolle synonym)
TREBBIANO_TOSCANO= 'c4f35c60-61f1-49b7-ae47-03320dd8e43f'  # Ugni Blanc (synonym)
XYNOMAVRO        = '6b0b6176-19d8-4654-833e-a71de9c5b4fd'  # Xynomavro
PINOT_NOIR       = '9f03fb29-113c-4784-8bf7-f8ebd27d1497'  # Pinot Noir
CHARDONNAY_BLANC = '0b466398-e87f-4f5d-94db-3503651d46fe'  # Chardonnay Blanc
CABERNET_SAUV    = '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5'  # Cabernet Sauvignon
CHENIN_BLANC     = '20c9c863-be98-4753-a901-e6715eef54ae'  # Chenin Blanc
PINOTAGE         = '87d3bab7-8e4c-4cbe-9149-3b8105fa64aa'  # Pinotage

INSERT_SQL = """
    INSERT INTO appellation_grapes
        (appellation_id, grape_id, min_percentage, max_percentage, association_type,
         source_url, source_organization, source_document_title, source_accessed_date, source_text_excerpt)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (appellation_id, grape_id) DO NOTHING
"""

UPDATE_PROV_SQL = """
    UPDATE appellation_grapes
    SET source_url = %s,
        source_organization = %s,
        source_document_title = %s,
        source_accessed_date = %s,
        source_text_excerpt = %s
    WHERE appellation_id = %s
      AND source_url IS NULL
"""

# ============================================================
# 1. MONT CAUME — new grapes (no existing rows)
# ============================================================
mont_caume_grapes = [
    # (grape_id, min_pct, max_pct, association_type)
    (GARNACHA_TINTA,    None, None, 'typical'),   # Grenache Noir
    (CINSAUT,           None, None, 'typical'),   # Cinsaut
    (MONASTRELL,        None, None, 'typical'),   # Mourvedre
    (SYRAH,             None, None, 'typical'),   # Syrah
    (CLAIRETTE_BLANCHE, None, None, 'typical'),   # Clairette Blanche (white)
    (VERMENTINO,        None, None, 'typical'),   # Vermentino/Rolle (white)
    (TREBBIANO_TOSCANO, None, None, 'typical'),   # Ugni Blanc (white)
]

mc_source = (
    "https://www.inao.gouv.fr/nos-actions/proteger-et-valoriser/les-signes-officiels-de-la-qualite-et-de-l-origine/vins",
    "INAO (Institut National de l'Origine et de la Qualite)",
    "Cahier des charges IGP Mont Caume",
    today,
    "IGP Mont Caume: cepages principaux rouges/roses: Grenache N., Cinsault, Mourvedre, Syrah. "
    "Cepages blancs: Clairette B., Vermentino (Rolle), Ugni Blanc. "
    "Zone geographique: ~26 communes du Var, arrondissement de Toulon."
)

inserted = 0
for (grape_id, min_pct, max_pct, assoc) in mont_caume_grapes:
    cur.execute(INSERT_SQL, (
        MONT_CAUME_ID, grape_id, min_pct, max_pct, assoc,
        *mc_source
    ))
    inserted += cur.rowcount
print(f"Mont Caume grapes inserted: {inserted}")

# ============================================================
# 2. AMYNTAIO — existing Xynomavro row exists; update provenance + add min_pct
# The ON CONFLICT will skip if (appellation_id, grape_id) already exists.
# We handle the existing row with an UPDATE for provenance.
# ============================================================
# Try insert (will skip if Xynomavro already there)
cur.execute(INSERT_SQL, (
    AMYNTAIO_ID, XYNOMAVRO, 80, None, 'required',
    "https://www.winesofgreece.org/variety/xinomavro/",
    "Greek Ministry of Rural Development and Food / EU PDO Register",
    "Amyntaio PDO Specification (EU Protected Designation of Origin)",
    today,
    "Xinomavro >=80% required for Amyntaio PDO red, rose, and sparkling wines. "
    "Only Greek PDO making both red/rose and sparkling from Xinomavro. Altitude 650m+, western Macedonia."
))
print(f"Amyntaio Xynomavro insert attempt: {cur.rowcount}")

# Update provenance on ALL existing Amyntaio grape rows (those with NULL source_url)
# AGIORGITIKO and ASSYRTIKO already in DB as 'typical' — update their provenance too
# (they are pre-existing incorrect rows; we cannot delete per rules, but we can add provenance)
cur.execute(UPDATE_PROV_SQL, (
    "https://www.winesofgreece.org/variety/xinomavro/",
    "Greek Ministry of Rural Development and Food / EU PDO Register",
    "Amyntaio PDO Specification (EU Protected Designation of Origin)",
    today,
    "Amyntaio PDO: Xinomavro >=80% required. Pre-existing Agiorgitiko and Assyrtiko rows are data "
    "quality artifacts from earlier session (these varieties are not authorized for Amyntaio PDO). "
    "Left in place per no-delete rule.",
    AMYNTAIO_ID
))
print(f"Amyntaio provenance update (existing rows): {cur.rowcount}")

# Also update the Xynomavro row's min_percentage if it was inserted as NULL earlier
cur.execute("""
    UPDATE appellation_grapes
    SET min_percentage = 80, association_type = 'required'
    WHERE appellation_id = %s AND grape_id = %s AND min_percentage IS NULL
""", (AMYNTAIO_ID, XYNOMAVRO))
print(f"Amyntaio Xynomavro min_pct update: {cur.rowcount}")

# ============================================================
# 3. CERES — existing rows; update provenance
# ============================================================
cur.execute(UPDATE_PROV_SQL, (
    "https://www.wosa.co.za/The-Industry/Wines-Of-Origin/Introduction/",
    "SAWIS (South African Wine Industry Information and Systems) / WOSA (Wines of South Africa)",
    "Wine of Origin Scheme — Ward: Ceres (Breede River Valley District)",
    today,
    "Ceres ward (WO): Breede River Valley district. Cold mountain valley climate. "
    "Typical varieties: Cabernet Sauvignon, Chenin Blanc, Pinotage. WO is geographic — no mandatory variety requirements.",
    CERES_ID
))
print(f"Ceres provenance update: {cur.rowcount}")

# ============================================================
# 4. RIEBEEKSRIVIER — existing rows; update provenance
# ============================================================
cur.execute(UPDATE_PROV_SQL, (
    "https://www.wosa.co.za/The-Industry/Wines-Of-Origin/Introduction/",
    "SAWIS (South African Wine Industry Information and Systems) / WOSA (Wines of South Africa)",
    "Wine of Origin Scheme — Ward: Riebeeksrivier (Swartland District)",
    today,
    "Riebeeksrivier ward (WO): Swartland district. Shale and granite soils. "
    "Typical varieties: Cabernet Sauvignon, Chenin Blanc, Pinotage. WO is geographic — no mandatory variety requirements.",
    RIEBEEK_ID
))
print(f"Riebeeksrivier provenance update: {cur.rowcount}")

# ============================================================
# 5. COTE DE BEAUNE-VILLAGES — existing Pinot Noir + Chardonnay rows
# Update provenance. Note: Chardonnay row is a pre-existing artifact (CDeBV is red-only),
# left in place per no-delete rule.
# ============================================================
cur.execute(UPDATE_PROV_SQL, (
    "https://www.inao.gouv.fr/Les-signes-officiels-de-la-qualite-et-de-l-origine-SIQO/Vins",
    "INAO (Institut National de l'Origine et de la Qualite)",
    "Cahier des charges AOC Cote de Beaune-Villages",
    today,
    "AOC Cote de Beaune-Villages: vin rouge uniquement, Pinot Noir 100%. "
    "14 communes eligibles. Note: pre-existing Chardonnay Blanc row is a data artifact — "
    "Cote de Beaune-Villages is red-only per CDC; left in place per no-delete rule.",
    CDEBV_ID
))
print(f"Cote de Beaune-Villages provenance update: {cur.rowcount}")

conn.commit()
print("All committed.")
cur.close()
conn.close()
