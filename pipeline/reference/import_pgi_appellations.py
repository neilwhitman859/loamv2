"""
Import ~454 PGI/IGP/IGT wine appellations from eAmbrosia API data
plus ~5 base-tier table wine designations (Vin de France, Vino d'Italia, etc.)

Source: eAmbrosia EU Geographical Indications Register (official EU data)
Data file: data/eambrosia_pgi_wines.json

Usage:
    python -m pipeline.reference.import_pgi_appellations --analyze
    python -m pipeline.reference.import_pgi_appellations --dry-run
    python -m pipeline.reference.import_pgi_appellations --import
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert
from pipeline.lib.normalize import normalize, slugify

# ── Country code -> designation type mapping ─────────────────────
DESIGNATION_TYPES = {
    "IT": "IGT", "FR": "IGP", "ES": "VdlT", "PT": "VR",
    "DE": "Landwein", "AT": "Landwein",
    "GR": "PGI", "RO": "PGI", "HU": "PGI", "BG": "PGI",
    "CZ": "PGI", "SI": "PGI", "CY": "PGI", "NL": "PGI",
    "DK": "PGI", "BE": "PGI", "GB": "PGI", "MT": "PGI",
    "SK": "PGI", "CN": "PGI", "US": "PGI", "HR": "PGI",
}

# ── Greek transliteration map ────────────────────────────────────
GREEK_TRANSLITERATIONS = {
    "\u0386\u03b2\u03b4\u03b7\u03c1\u03b1": "Avdira",
    "\u0386\u03b3\u03b9\u03bf \u038c\u03c1\u03bf\u03c2": "Agio Oros",
    "\u0391\u03b3\u03bf\u03c1\u03ac": "Agora",
    "\u0391\u03b9\u03b3\u03b1\u03af\u03bf \u03a0\u03ad\u03bb\u03b1\u03b3\u03bf\u03c2": "Aegean Sea",
    "\u0391\u03bd\u03ac\u03b2\u03c5\u03c3\u03c3\u03bf\u03c2": "Anavyssos",
    "\u0391\u03c1\u03b3\u03bf\u03bb\u03af\u03b4\u03b1": "Argolida",
    "\u0391\u03c1\u03ba\u03b1\u03b4\u03af\u03b1": "Arkadia",
    "\u0391\u03c4\u03c4\u03b9\u03ba\u03ae": "Attiki",
    "\u0391\u03c7\u03b1\u0390\u03b1": "Achaia",
    "\u0393\u03c1\u03b5\u03b2\u03b5\u03bd\u03ac": "Grevena",
    "\u0394\u03c1\u03ac\u03bc\u03b1": "Drama",
    "\u0394\u03c9\u03b4\u03b5\u03ba\u03ac\u03bd\u03b7\u03c3\u03bf\u03c2": "Dodecanese",
    "\u0388\u03b2\u03c1\u03bf\u03c2": "Evros",
    "\u0395\u03cd\u03b2\u03bf\u03b9\u03b1": "Evia",
    "\u0396\u03ac\u03ba\u03c5\u03bd\u03b8\u03bf\u03c2": "Zakynthos",
    "\u0397\u03bb\u03b5\u03af\u03b1": "Ilia",
    "\u0397\u03bc\u03b1\u03b8\u03af\u03b1": "Imathia",
    "\u0397\u03c1\u03ac\u03ba\u03bb\u03b5\u03b9\u03bf": "Heraklion",
    "\u0398\u03ac\u03c3\u03bf\u03c2": "Thasos",
    "\u0398\u03b5\u03c3\u03c3\u03b1\u03bb\u03bf\u03bd\u03af\u03ba\u03b7": "Thessaloniki",
    "\u0399\u03c9\u03ac\u03bd\u03bd\u03b9\u03bd\u03b1": "Ioannina",
    "\u039a\u03b1\u03b2\u03ac\u03bb\u03b1": "Kavala",
    "\u039a\u03b1\u03c1\u03b4\u03af\u03c4\u03c3\u03b1": "Karditsa",
    "\u039a\u03b1\u03c3\u03c4\u03bf\u03c1\u03b9\u03ac": "Kastoria",
    "\u039a\u03ad\u03c1\u03ba\u03c5\u03c1\u03b1": "Corfu",
    "\u039a\u03bf\u03b6\u03ac\u03bd\u03b7": "Kozani",
    "\u039a\u03bf\u03c1\u03b9\u03bd\u03b8\u03af\u03b1": "Korinthia",
    "\u039a\u03c1\u03ae\u03c4\u03b7": "Crete",
    "\u039a\u03c5\u03ba\u03bb\u03ac\u03b4\u03b5\u03c2": "Cyclades",
    "\u039b\u03b1\u03ba\u03c9\u03bd\u03af\u03b1": "Lakonia",
    "\u039b\u03b1\u03c3\u03af\u03b8\u03b9": "Lasithi",
    "\u039b\u03ad\u03c3\u03b2\u03bf\u03c2": "Lesvos",
    "\u039b\u03b5\u03c5\u03ba\u03ac\u03b4\u03b1": "Lefkada",
    "\u039c\u03b1\u03b3\u03bd\u03b7\u03c3\u03af\u03b1": "Magnisia",
    "\u039c\u03b1\u03ba\u03b5\u03b4\u03bf\u03bd\u03af\u03b1": "Macedonia",
    "\u039c\u03b5\u03c3\u03c3\u03b7\u03bd\u03af\u03b1": "Messinia",
    "\u03a0\u03ad\u03bb\u03bb\u03b1": "Pella",
    "\u03a0\u03b5\u03bb\u03bf\u03c0\u03cc\u03bd\u03bd\u03b7\u03c3\u03bf\u03c2": "Peloponnese",
    "\u03a0\u03b9\u03b5\u03c1\u03af\u03b1": "Pieria",
    "\u03a1\u03ad\u03b8\u03c5\u03bc\u03bd\u03bf": "Rethymno",
    "\u03a3\u03ad\u03c1\u03c1\u03b5\u03c2": "Serres",
    "\u03a3\u03c4\u03b5\u03c1\u03b5\u03ac \u0395\u03bb\u03bb\u03ac\u03b4\u03b1": "Central Greece",
    "\u03a4\u03c1\u03b9\u03c6\u03c5\u03bb\u03af\u03b1": "Trifilia",
    "\u03a6\u03bb\u03ce\u03c1\u03b9\u03bd\u03b1": "Florina",
    "\u03a7\u03b1\u03bb\u03ba\u03b9\u03b4\u03b9\u03ba\u03ae": "Halkidiki",
    "\u03a7\u03b1\u03bd\u03b9\u03ac": "Chania",
    "\u03a7\u03af\u03bf\u03c2": "Chios",
    "\u0389\u03c0\u03b5\u03b9\u03c1\u03bf\u03c2": "Epirus",
    "\u0398\u03b5\u03c3\u03c3\u03b1\u03bb\u03af\u03b1": "Thessalia",
    "\u0398\u03c1\u03ac\u03ba\u03b7": "Thrace",
}

# ── Chinese transliteration map ──────────────────────────────────
CHINESE_TRANSLITERATIONS = {
    "\u8d3a\u5170\u5c71\u4e1c\u9e93\u8461\u8404\u9152": "Helan Mountain East",
    "\u6853\u4ec1\u51b0\u9152": "Huanren Ice Wine",
    "\u70df\u53f0\u8461\u8404\u9152": "Yantai Wine",
    "\u6c99\u57ce\u8461\u8404\u9152": "Shacheng Wine",
}

# ── Italian name corrections ─────────────────────────────────────
ITALIAN_NAME_ALIASES = {
    "Toscano": ["Toscana", "Toscana IGT", "IGT Toscana", "IGT Toscano"],
    "Terre Siciliane": ["Sicilia IGT", "IGT Sicilia", "IGT Terre Siciliane"],
    "Trevenezie": ["delle Venezie", "Tre Venezie", "IGT Trevenezie"],
}

# ── Base-tier (table wine) designations ──────────────────────────
BASE_TIER = [
    {"name": "Vin de France", "country": "FR", "designation_type": "VdF"},
    {"name": "Vino d'Italia", "country": "IT", "designation_type": "VdI"},
    {"name": "Vino de Espana", "country": "ES", "designation_type": "VdE"},
    {"name": "Vinho de Portugal", "country": "PT", "designation_type": "VdP"},
    {"name": "Deutscher Wein", "country": "DE", "designation_type": "VdT"},
]

# ── Region mapping helpers ───────────────────────────────────────
ITALIAN_REGION_MAP = {
    "Tuscany": ["Toscano", "Colli della Toscana centrale", "Costa Toscana",
                "Alta Valle della Greve", "Costa Etrusco Romana", "Montecastelli"],
    "Sicily": ["Terre Siciliane", "Avola", "Camarro", "Fontanarossa di Cerda",
               "Salemi", "Salina", "Valle Belice"],
    "Veneto": ["Veneto", "Veneto Orientale", "Marca Trevigiana",
               "Colli Trevigiani", "Conselvano", "Verona"],
    "Piedmont": [],
    "Lombardy": ["Collina del Milanese", "Benaco Bresciano", "Montenetto di Brescia",
                 "Ronchi di Brescia", "Provincia di Mantova", "Provincia di Pavia",
                 "Quistello", "Sabbioneta", "Sebino", "Alto Mincio", "Bergamasca",
                 "Terre Lariane", "Valcamonica"],
    "Puglia": ["Puglia", "Daunia", "Murgia", "Salento", "Tarantino", "Valle d'Itria"],
    "Campania": ["Campania", "Catalanesca del Monte Somma", "Colli di Salerno",
                 "Dugenta", "Epomeo", "Paestum", "Pompeiano", "Roccamonfina",
                 "Terre del Volturno"],
    "Emilia-Romagna": ["dell'Emilia", "Bianco del Sillaro", "Castelfranco Emilia",
                       "Fortana del Taro", "Forli", "Ravenna", "Rubicone",
                       "Terre di Veleja", "Val Tidone"],
    "Calabria": ["Calabria", "Arghilla", "Costa Viola", "Lipuda", "Locride",
                 "Palizzi", "Pellaro", "Scilla", "Val di Neto", "Valdamato"],
    "Sardinia": ["Barbagia", "Colli del Limbara", "Isola dei Nuraghi", "Marmilla",
                 "Nurra", "Ogliastra", "Parteolla", "Planargia",
                 "Provincia di Nuoro", "Romangia", "Sibiola", "Tharros",
                 "Trexenta", "Valle del Tirso", "Valli di Porto Pino"],
    "Lazio": ["Lazio", "Anagni", "Civitella d'Agliano", "Colli Cimini",
              "Costa Etrusco Romana", "Frusinate", "Rotae"],
    "Marche": ["Marche"],
    "Umbria": ["Umbria", "Allerona", "Bettona", "Cannara", "Narni", "Spello"],
    "Abruzzo": ["Colli Aprutini", "Colli del Sangro", "Colline Frentane",
                "Colline Pescaresi", "Colline Teatine", "del Vastese",
                "Terre Abruzzesi", "Terre Aquilane", "Terre di Chieti"],
    "Friuli Venezia Giulia": ["Venezia Giulia", "Alto Livenza"],
    "Liguria": ["Colline del Genovesato", "Colline Savonesi",
                "Liguria di Levante", "Terrazze dell'Imperiese"],
    "Trentino-Alto Adige": ["Mitterberg", "Vallagarina", "Vigneti delle Dolomiti"],
    "Basilicata": ["Basilicata"],
    "Molise": ["Osco", "Rotae"],
}

FRENCH_REGION_MAP = {
    "Languedoc-Roussillon": ["Pays d'Oc", "Cite de Carcassonne",
                             "Coteaux d'Enserune", "Coteaux de Beziers",
                             "Coteaux de Narbonne", "Coteaux de Peyriac",
                             "Cotes Catalanes", "Cote Vermeille",
                             "Cotes de Thau", "Cotes de Thongue",
                             "Haute Vallee de l'Aude", "Haute Vallee de l'Orb",
                             "Le Pays Cathare", "Pays d'Herault",
                             "Saint-Guilhem-le-Desert", "Vallee du Paradis",
                             "Vallee du Torgan", "Vicomte d'Aumelas",
                             "Cevennes", "Aude", "Gard"],
    "Provence": ["Alpilles", "Maures", "Mont Caume", "Var",
                 "Pays des Bouches-du-Rhone", "Mediterranee"],
    "Rhone Valley": ["Ardeche", "Collines Rhodaniennes", "Comtes Rhodaniens",
                     "Coteaux des Baronnies", "Drome", "Vaucluse"],
    "South West France": ["Agenais", "Ariege", "Aveyron", "Comte Tolosan",
                          "Cotes de Gascogne", "Cotes du Lot", "Cotes du Tarn",
                          "Gers", "Lavilledieu", "Perigord", "Pays de Brive"],
    "Loire Valley": ["Val de Loire", "Coteaux du Cher et de l'Arnon", "Urfe"],
    "Bordeaux": ["Atlantique"],
    "Corsica": ["Ile de Beaute"],
    "Burgundy": ["Coteaux de l'Auxois", "Saone-et-Loire",
                 "Coteaux de Tannay", "Yonne", "Cotes de la Charite"],
    "Jura": ["Franche-Comte"],
    "Savoie": ["Vin des Allobroges", "Coteaux de l'Ain", "Isere"],
    "Alsace": [],
    "Champagne": ["Haute-Marne", "Coteaux de Coiffy", "Sainte-Marie-la-Blanche"],
}

PT_MAP = {
    "Minho": "Minho", "Transmontano": "Tras-os-Montes", "Duriense": "Douro",
    "Lisboa": "Lisboa", "Tejo": "Tejo", "Alentejano": "Alentejo",
    "Algarve": "Algarve", "Acores": "Azores", "Peninsula de Setubal": "Setubal",
    "Terras Madeirenses": "Madeira",
}


def fetch_all(sb, table: str, columns: str = "*") -> list[dict]:
    rows = []
    offset = 0
    while True:
        result = sb.table(table).select(columns).range(offset, offset + 999).execute()
        rows.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000
    return rows


def main():
    parser = argparse.ArgumentParser(description="Import PGI/IGP/IGT appellations")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--analyze", action="store_true", help="Show what would be imported")
    group.add_argument("--dry-run", action="store_true", help="Full run without DB writes")
    group.add_argument("--import", dest="do_import", action="store_true", help="Actually import")
    args = parser.parse_args()

    mode = "import" if args.do_import else ("dry-run" if args.dry_run else "analyze")
    print(f"Mode: {mode}\n")

    sb = get_supabase()

    # Load reference data
    print("Loading reference data...")
    countries = fetch_all(sb, "countries", "id,name,iso_code")
    regions = fetch_all(sb, "regions", "id,name,country_id,parent_id,is_catch_all")
    existing_apps = fetch_all(sb, "appellations", "id,name,country_id,region_id,designation_type")

    country_by_code = {c["iso_code"]: c for c in countries if c.get("iso_code")}
    country_by_id = {c["id"]: c for c in countries}

    existing_by_norm = {}
    for a in existing_apps:
        key = normalize(a["name"]) + "|" + a["country_id"]
        existing_by_norm[key] = a

    # Load eAmbrosia PGI data
    data_file = Path(__file__).resolve().parents[2] / "data" / "eambrosia_pgi_wines.json"
    pgi_data = json.loads(data_file.read_text(encoding="utf-8"))
    print(f"eAmbrosia PGI entries: {len(pgi_data)}")

    to_insert = []
    skipped = []
    aliases = []

    for entry in pgi_data:
        country = country_by_code.get(entry.get("country"))
        if not country:
            skipped.append({"name": entry["name"], "reason": f"Unknown country code: {entry.get('country')}"})
            continue

        display_name = entry["name"]

        # Transliterate Greek/Chinese
        if entry["country"] == "GR" and entry["name"] in GREEK_TRANSLITERATIONS:
            display_name = GREEK_TRANSLITERATIONS[entry["name"]]
            aliases.append({"original": entry["name"], "display": display_name, "type": "greek_original"})
        if entry["country"] == "CN" and entry["name"] in CHINESE_TRANSLITERATIONS:
            display_name = CHINESE_TRANSLITERATIONS[entry["name"]]
            aliases.append({"original": entry["name"], "display": display_name, "type": "chinese_original"})

        exist_key = normalize(display_name) + "|" + country["id"]
        if exist_key in existing_by_norm:
            skipped.append({"name": display_name, "reason": "Already exists in appellations"})
            continue

        designation_type = DESIGNATION_TYPES.get(entry["country"], "PGI")

        # Resolve region
        region_id = None
        region_maps = ITALIAN_REGION_MAP if entry["country"] == "IT" else (
            FRENCH_REGION_MAP if entry["country"] == "FR" else None)

        if region_maps:
            for region_name, igt_names in region_maps.items():
                if any(normalize(n) == normalize(entry["name"]) for n in igt_names):
                    region = next((r for r in regions
                                   if normalize(r["name"]) == normalize(region_name)
                                   and r["country_id"] == country["id"]), None)
                    if region:
                        region_id = region["id"]
                        break

        # Direct name match
        if not region_id:
            direct = next((r for r in regions
                           if r["country_id"] == country["id"]
                           and normalize(r["name"]) == normalize(display_name)), None)
            if direct:
                region_id = direct["id"]

        # Portugal VRs
        if not region_id and entry["country"] == "PT" and entry["name"] in PT_MAP:
            pt_region = next((r for r in regions
                              if normalize(r["name"]) == normalize(PT_MAP[entry["name"]])
                              and r["country_id"] == country["id"]), None)
            if pt_region:
                region_id = pt_region["id"]

        # Catch-all fallback
        if not region_id:
            catch_all = next((r for r in regions
                              if r["country_id"] == country["id"]
                              and r["is_catch_all"] and not r.get("parent_id")), None)
            if catch_all:
                region_id = catch_all["id"]

        hemisphere = "south" if entry["country"] in ("AR", "CL", "ZA", "AU", "NZ") else "north"

        to_insert.append({
            "name": display_name,
            "country_id": country["id"],
            "region_id": region_id,
            "designation_type": designation_type,
            "hemisphere": hemisphere,
            "eambrosia_id": entry.get("gi_identifier"),
            "eambrosia_file": entry.get("file_number"),
            "original_name": entry["name"] if entry["name"] != display_name else None,
        })

        # Generate aliases
        entry_aliases = []
        dt = designation_type
        entry_aliases.append(f"{display_name} {dt}")
        entry_aliases.append(f"{dt} {display_name}")
        if entry["name"] in ITALIAN_NAME_ALIASES:
            entry_aliases.extend(ITALIAN_NAME_ALIASES[entry["name"]])
        aliases.extend({"display": display_name, "alias": a, "type": "generated"}
                       for a in entry_aliases)

    # Add base-tier designations
    for bt in BASE_TIER:
        country = country_by_code.get(bt["country"])
        if not country:
            continue
        exist_key = normalize(bt["name"]) + "|" + country["id"]
        if exist_key in existing_by_norm:
            skipped.append({"name": bt["name"], "reason": "Already exists"})
            continue
        to_insert.append({
            "name": bt["name"],
            "country_id": country["id"],
            "region_id": None,
            "designation_type": bt["designation_type"],
            "hemisphere": "north",
            "eambrosia_id": None,
            "eambrosia_file": None,
            "original_name": None,
        })

    # Analysis output
    print(f"\n=== ANALYSIS ===")
    print(f"To insert: {len(to_insert)}")
    print(f"Skipped: {len(skipped)}")
    print(f"Aliases to create: {len(aliases)}")

    by_country: dict[str, int] = {}
    for r in to_insert:
        c_name = country_by_id.get(r["country_id"], {}).get("name", "Unknown")
        by_country[c_name] = by_country.get(c_name, 0) + 1
    print("\nBy country:")
    for k, v in sorted(by_country.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    with_region = sum(1 for r in to_insert if r["region_id"])
    print(f"\nRegion resolved: {with_region}/{len(to_insert)} ({with_region / max(len(to_insert), 1) * 100:.1f}%)")

    unresolved = [r for r in to_insert if not r["region_id"]]
    if unresolved:
        print(f"\nUnresolved regions ({len(unresolved)}):")
        for u in unresolved[:30]:
            iso = next((c["iso_code"] for c in countries if c["id"] == u["country_id"]), "??")
            print(f"  [{iso}] {u['name']}")
        if len(unresolved) > 30:
            print(f"  ... and {len(unresolved) - 30} more")

    if mode == "analyze":
        print("\nRun with --dry-run or --import to proceed.")
        return

    if mode == "dry-run":
        print("\n--- DRY RUN -- no DB writes ---")
        print("Sample records:")
        for r in to_insert[:5]:
            print(json.dumps(r, indent=2, default=str))
        return

    # Insert appellations
    print("\nInserting appellations...")
    inserted = 0
    inserted_ids: dict[str, str] = {}
    BATCH = 50

    for i in range(0, len(to_insert), BATCH):
        batch = []
        for r in to_insert[i:i + BATCH]:
            slug = slugify(r["name"])
            if not slug or len(slug) < 2:
                slug = slugify(r.get("original_name") or "") or (
                    r.get("eambrosia_id", "").lower() or f"pgi-{i}")
            batch.append({
                "name": r["name"],
                "slug": slug,
                "country_id": r["country_id"],
                "region_id": r["region_id"],
                "designation_type": r["designation_type"],
                "hemisphere": r["hemisphere"],
            })

        try:
            result = sb.table("appellations").insert(batch).select("id,name").execute()
            if result.data:
                inserted += len(result.data)
                for d in result.data:
                    inserted_ids[d["name"]] = d["id"]
        except Exception as e:
            print(f"Batch error at {i}: {e}")
            for row in batch:
                try:
                    r = sb.table("appellations").insert(row).select("id,name").execute()
                    if r.data:
                        inserted += 1
                        inserted_ids[r.data[0]["name"]] = r.data[0]["id"]
                except Exception as e2:
                    print(f"  Row error ({row['name']}): {e2}")

    print(f"Inserted {inserted} appellations.")

    # Insert aliases
    print("\nInserting aliases...")
    alias_count = 0
    alias_rows = [
        {
            "appellation_id": inserted_ids[a["display"]],
            "alias": a["alias"],
            "alias_normalized": normalize(a["alias"]),
        }
        for a in aliases
        if a.get("alias") and a["display"] in inserted_ids
    ]
    alias_count = batch_insert("appellation_aliases", alias_rows, batch_size=50)
    print(f"Inserted {alias_count} aliases.")

    # Final count
    count_result = sb.table("appellations").select("id", count="exact").execute()
    print(f"\nTotal appellations in DB: {count_result.count}")


if __name__ == "__main__":
    main()
