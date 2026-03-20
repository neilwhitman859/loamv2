#!/usr/bin/env python3
"""
Promote classification data from wine metadata to entity_classifications table.
Handles:
  - metadata.classification (77 wines -- mostly Jadot/Antinori)
  - metadata.vdp_level (28 wines -- Donnhoff)

Usage:
    python -m pipeline.promote.classifications [--dry-run]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all
from pipeline.lib.normalize import normalize


VDP_MAP = {
    "gutswein": ("VDP Classification", "Gutswein"),
    "ortswein": ("VDP Classification", "Ortswein"),
    "erste lage": ("VDP Classification", "Erste Lage"),
    "grosse lage": ("VDP Classification", "Grosse Lage"),
    "grosses gewachs": ("VDP Classification", "Grosse Lage"),
    "grosses gewächs": ("VDP Classification", "Grosse Lage"),
    "gg": ("VDP Classification", "Grosse Lage"),
    "vdp.gutswein": ("VDP Classification", "Gutswein"),
    "vdp.ortswein": ("VDP Classification", "Ortswein"),
    "vdp.erste lage": ("VDP Classification", "Erste Lage"),
    "vdp.grosse lage": ("VDP Classification", "Grosse Lage"),
}

BURG_MAP = {
    "grand cru": ("Burgundy Vineyard Classification", "Grand Cru"),
    "premier cru": ("Burgundy Vineyard Classification", "Premier Cru"),
    "1er cru": ("Burgundy Vineyard Classification", "Premier Cru"),
}

SKIP_CLASSIFICATIONS = {
    "bolgheri superiore doc", "chianti classico docg", "brunello di montalcino docg",
    "igt toscana", "docg", "doc", "premier cru supérieur (1855)",
}


def main():
    parser = argparse.ArgumentParser(description="Promote classifications from metadata")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    dry = args.dry_run

    print(f"Promoting classifications from metadata...{' (DRY RUN)' if dry else ''}\n")

    # Load classification levels
    cl_levels = fetch_all("classification_levels", "id,classification_id,level_name,level_rank")
    cl_systems = fetch_all("classifications", "id,name,country_id")
    system_map = {s["id"]: s for s in cl_systems}

    # Build lookup: "system_name|level_name" -> level entry
    level_lookup: dict[str, dict] = {}
    for cl in cl_levels:
        sys_entry = system_map.get(cl["classification_id"])
        if not sys_entry:
            continue
        entry = {
            "level_id": cl["id"],
            "system_name": sys_entry["name"],
            "level_name": cl["level_name"],
        }
        key = f"{sys_entry['name'].lower()}|{cl['level_name'].lower()}"
        level_lookup[key] = entry

    # Load wines with metadata
    wines = fetch_all("wines", "id,name,metadata")

    # Check which wines already have classifications
    existing_classifs = fetch_all("entity_classifications", "entity_id,entity_type,classification_level_id")
    classified = {ec["entity_id"] for ec in existing_classifs if ec["entity_type"] == "wine"}

    promoted = 0
    skipped = 0
    not_found = 0

    for w in wines:
        meta = w.get("metadata")
        if not meta:
            continue
        if w["id"] in classified:
            if meta.get("vdp_level") or meta.get("classification"):
                skipped += 1
            continue

        # VDP levels
        if meta.get("vdp_level"):
            vdp_key = meta["vdp_level"].lower().strip()
            mapping = VDP_MAP.get(vdp_key)
            if mapping:
                key = f"{mapping[0].lower()}|{mapping[1].lower()}"
                level = level_lookup.get(key)
                if level:
                    if dry:
                        print(f"  [DRY] {w['name']} -> {level['system_name']} / {level['level_name']}")
                    else:
                        try:
                            sb.table("entity_classifications").insert({
                                "classification_level_id": level["level_id"],
                                "entity_type": "wine",
                                "entity_id": w["id"],
                            }).execute()
                        except Exception as e:
                            if "duplicate" not in str(e):
                                print(f"  Warning: {w['name']}: {e}")

                        # Clean metadata
                        new_meta = {k: v for k, v in meta.items() if k != "vdp_level"}
                        sb.table("wines").update({
                            "metadata": new_meta if new_meta else None,
                        }).eq("id", w["id"]).execute()

                    promoted += 1
                    continue

            print(f"  Warning: VDP level not mapped: \"{meta['vdp_level']}\" for {w['name']}")
            not_found += 1

        # Burgundy classifications
        if meta.get("classification"):
            classif = meta["classification"].lower().strip()
            burg_mapping = BURG_MAP.get(classif)
            if burg_mapping:
                key = f"{burg_mapping[0].lower()}|{burg_mapping[1].lower()}"
                level = level_lookup.get(key)
                if level:
                    if dry:
                        print(f"  [DRY] {w['name']} -> {level['system_name']} / {level['level_name']}")
                    else:
                        try:
                            sb.table("entity_classifications").insert({
                                "classification_level_id": level["level_id"],
                                "entity_type": "wine",
                                "entity_id": w["id"],
                            }).execute()
                        except Exception as e:
                            if "duplicate" not in str(e):
                                print(f"  Warning: {w['name']}: {e}")

                        new_meta = {k: v for k, v in meta.items() if k != "classification"}
                        sb.table("wines").update({
                            "metadata": new_meta if new_meta else None,
                        }).eq("id", w["id"]).execute()

                    promoted += 1
                    continue

            # Not a simple classification -- log for manual review
            if classif not in SKIP_CLASSIFICATIONS:
                print(f"  Info: classification not auto-mapped: \"{meta['classification']}\" for {w['name']}")
            not_found += 1

    print(f"\nDone. Promoted: {promoted}, Skipped (already classified): {skipped}, Not mapped: {not_found}")


if __name__ == "__main__":
    main()
