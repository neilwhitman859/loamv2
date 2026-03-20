"""
Seed label_designation_aliases with common abbreviations and alternate spellings.

Sources:
- EU wine regulations (Commission Delegated Regulation 2019/33)
- WSET Level 3 terminology
- Common retailer/LWIN abbreviations

Usage:
    python -m pipeline.reference.seed_label_designation_aliases [--dry-run]
"""

import argparse
import sys
from pathlib import Path

# Allow running as module from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize


# Format: (alias, canonical_designation_name, alias_type, language_code)
ALIASES = [
    # German Pradikats -- abbreviations
    ("TBA", "Trockenbeerenauslese", "abbreviation", "de"),
    ("BA", "Beerenauslese", "abbreviation", "de"),
    ("Spatlese", "Spätlese", "alternate_spelling", "de"),
    ("Spaetlese", "Spätlese", "alternate_spelling", "de"),
    ("Kab", "Kabinett", "abbreviation", "de"),
    ("Kab.", "Kabinett", "abbreviation", "de"),

    # French late harvest
    ("VT", "Vendanges Tardives", "abbreviation", "fr"),
    ("SGN", "Sélection de Grains Nobles", "abbreviation", "fr"),
    ("Selection de Grains Nobles", "Sélection de Grains Nobles", "alternate_spelling", "fr"),

    # Spanish aging tiers
    ("GR", "Gran Reserva", "abbreviation", "es"),
    ("Crza", "Crianza", "abbreviation", "es"),
    ("Res", "Reserva", "abbreviation", "es"),
    ("Res.", "Reserva", "abbreviation", "es"),
    ("Grande Reserva", "Gran Reserva", "alternate_spelling", None),
    ("Grande Réserve", "Gran Reserva", "translation", "fr"),

    # Italian aging tiers
    ("Ris", "Riserva", "abbreviation", "it"),
    ("Ris.", "Riserva", "abbreviation", "it"),
    ("Sup", "Superiore", "abbreviation", "it"),
    ("Sup.", "Superiore", "abbreviation", "it"),

    # Sparkling sweetness -- synonyms and translations
    ("Brut Zero", "Brut Nature", "synonym", None),
    ("Dosage Zero", "Brut Nature", "synonym", None),
    ("Dosage Zéro", "Brut Nature", "synonym", "fr"),
    ("Non Dosé", "Brut Nature", "synonym", "fr"),
    ("Non Dose", "Brut Nature", "alternate_spelling", None),
    ("Pas Dosé", "Brut Nature", "synonym", "fr"),
    ("Pas Dose", "Brut Nature", "alternate_spelling", None),
    ("Zero Dosage", "Brut Nature", "synonym", None),
    ("BN", "Brut Nature", "abbreviation", None),
    ("EB", "Extra Brut", "abbreviation", None),
    ("Extra Sec", "Extra Dry", "translation", "fr"),
    ("Extra Seco", "Extra Dry", "translation", "es"),
    ("Extra Trocken", "Extra Dry", "translation", "de"),
    ("Sec", "Dry", "translation", "fr"),
    ("Seco", "Dry", "translation", "es"),
    ("Trocken", "Dry", "translation", "de"),
    ("Demi-Sec", "Demi-Sec", "alternate_name", "fr"),
    ("Halbtrocken", "Demi-Sec", "translation", "de"),
    ("Semi-Seco", "Demi-Sec", "translation", "es"),
    ("Dolce", "Doux", "translation", "it"),
    ("Dulce", "Doux", "translation", "es"),

    # Production methods
    ("Méthode Traditionnelle", "Traditional Method", "translation", "fr"),
    ("Methode Traditionnelle", "Traditional Method", "alternate_spelling", "fr"),
    ("Metodo Classico", "Traditional Method", "translation", "it"),
    ("Méthode Champenoise", "Traditional Method", "synonym", "fr"),
    ("Methode Champenoise", "Traditional Method", "alternate_spelling", "fr"),
    ("Metodo Charmat", "Charmat Method", "translation", "it"),
    ("Méthode Charmat", "Charmat Method", "translation", "fr"),
    ("Tank Method", "Charmat Method", "synonym", "en"),
    ("Método Ancestral", "Pétillant Naturel", "translation", "es"),
    ("Metodo Ancestrale", "Pétillant Naturel", "translation", "it"),
    ("Méthode Ancestrale", "Pétillant Naturel", "translation", "fr"),
    ("Pet-Nat", "Pétillant Naturel", "abbreviation", None),
    ("Pet Nat", "Pétillant Naturel", "abbreviation", None),
    ("Petillant Naturel", "Pétillant Naturel", "alternate_spelling", None),

    # Estate bottling
    ("Mis en Bouteille au Château", "Château Bottled", "translation", "fr"),
    ("Mis en Bouteille au Domaine", "Estate Bottled", "translation", "fr"),
    ("Erzeugerabfüllung", "Estate Bottled", "translation", "de"),
    ("Gutsabfüllung", "Estate Bottled", "translation", "de"),
    ("Imbottigliato all'Origine", "Estate Bottled", "translation", "it"),

    # Vineyard designations
    ("Vieilles Vignes", "Old Vines", "translation", "fr"),
    ("VV", "Old Vines", "abbreviation", None),
    ("Viñas Viejas", "Old Vines", "translation", "es"),
    ("Vecchie Viti", "Old Vines", "translation", "it"),
    ("Alte Reben", "Old Vines", "translation", "de"),

    # Ice wine
    ("Icewine", "Ice Wine", "alternate_spelling", None),
    ("Vin de Glace", "Ice Wine", "translation", "fr"),

    # Port-specific
    ("Colheita", "Colheita", "alternate_name", "pt"),
    ("LBV", "Late Bottled Vintage", "abbreviation", None),
    ("L.B.V.", "Late Bottled Vintage", "abbreviation", None),
]


def main():
    parser = argparse.ArgumentParser(description="Seed label_designation_aliases table")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    args = parser.parse_args()

    sb = get_supabase()

    # Load label designations
    result = sb.table("label_designations").select("id,name").execute()
    desigs = result.data

    by_name: dict[str, dict] = {}
    by_norm: dict[str, dict] = {}
    for d in desigs:
        by_name[d["name"].lower()] = d
        by_norm[normalize(d["name"])] = d

    inserted = 0
    skipped = 0
    not_found = 0

    for alias_text, canonical, alias_type, lang in ALIASES:
        desig = by_name.get(canonical.lower()) or by_norm.get(normalize(canonical))
        if not desig:
            print(f"  Warning: Designation not found: \"{canonical}\" (alias: \"{alias_text}\")")
            not_found += 1
            continue

        alias_norm = normalize(alias_text)
        if alias_norm == normalize(desig["name"]):
            skipped += 1
            continue

        if args.dry_run:
            print(f"  [DRY RUN] \"{alias_text}\" -> {desig['name']} ({alias_type})")
            inserted += 1
            continue

        try:
            sb.table("label_designation_aliases").upsert(
                {
                    "label_designation_id": desig["id"],
                    "alias": alias_text,
                    "alias_normalized": alias_norm,
                    "alias_type": alias_type,
                    "language_code": lang,
                    "source": "eu-reg-2019-33-wset-l3",
                },
                on_conflict="alias_normalized",
            ).execute()
            inserted += 1
        except Exception as e:
            print(f"  Warning: Error for \"{alias_text}\": {e}")

    print(f"\nDone. Inserted: {inserted}, Skipped: {skipped}, Not found: {not_found}")


if __name__ == "__main__":
    main()
