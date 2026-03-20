"""
Imports 36 Argentine IGs/DOCs with Nominatim centroids.

Usage: python -m pipeline.geo.import_argentina_ig [--dry-run]
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import nominatim_search, geo_slugify, RATE_LIMIT_S

GEOCODE_QUERIES = {
    "Lujan de Cuyo": ["Lujan de Cuyo, Mendoza, Argentina"],
    "Maipu": ["Maipu, Mendoza, Argentina"],
    "San Rafael": ["San Rafael, Mendoza, Argentina"],
    "Tupungato": ["Tupungato, Mendoza, Argentina"],
    "San Carlos": ["San Carlos, Mendoza, Argentina"],
    "Tunuyan": ["Tunuyan, Mendoza, Argentina"],
    "Rivadavia": ["Rivadavia, Mendoza, Argentina"],
    "Junin": ["Junin, Mendoza, Argentina"],
    "Lavalle": ["Lavalle, Mendoza, Argentina"],
    "Las Heras": ["Las Heras, Mendoza, Argentina"],
    "Santa Rosa": ["Santa Rosa, Mendoza, Argentina"],
    "La Paz": ["La Paz, Mendoza, Argentina"],
    "General Alvear": ["General Alvear, Mendoza, Argentina"],
    "Malargue": ["Malargue, Mendoza, Argentina"],
    "Godoy Cruz": ["Godoy Cruz, Mendoza, Argentina"],
    "Guaymallen": ["Guaymallen, Mendoza, Argentina"],
    "Cafayate": ["Cafayate, Salta, Argentina"],
    "Molinos": ["Molinos, Salta, Argentina"],
    "San Antonio de los Cobres": ["San Antonio de los Cobres, Salta, Argentina"],
    "Cachi": ["Cachi, Salta, Argentina"],
    "Chilecito": ["Chilecito, La Rioja, Argentina"],
    "Famatina": ["Famatina, La Rioja, Argentina"],
    "San Blas de los Sauces": ["San Blas de los Sauces, La Rioja, Argentina"],
    "Calingasta": ["Calingasta, San Juan, Argentina"],
    "Iglesia": ["Iglesia, San Juan, Argentina"],
    "Jachal": ["Jachal, San Juan, Argentina"],
    "Pedernal": ["Pedernal, San Juan, Argentina"],
    "Pocito": ["Pocito, San Juan, Argentina"],
    "Rawson": ["Rawson, San Juan, Argentina"],
    "Sarmiento": ["Sarmiento, San Juan, Argentina"],
    "Tulum": ["Valle de Tulum, San Juan, Argentina"],
    "Ullum": ["Ullum, San Juan, Argentina"],
    "Zonda": ["Zonda, San Juan, Argentina"],
    "Neuquen": ["Neuquen, Argentina", "Neuquen city, Argentina"],
    "Rio Negro": ["General Roca, Rio Negro, Argentina"],
    "Chapadmalal": ["Chapadmalal, Buenos Aires, Argentina"],
}


def main():
    parser = argparse.ArgumentParser(description="Import Argentine IGs from INV")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    client = httpx.Client(timeout=30)
    print("=== Argentine IG Import ===")

    ar = sb.table("countries").select("id").eq("iso_code", "AR").single().execute().data
    ar_id = ar["id"]

    regions = sb.table("regions").select("id, name, is_catch_all").eq("country_id", ar_id).execute().data
    catch_all = next((r for r in regions if r["is_catch_all"]), None)

    existing = sb.table("appellations").select("id, name").eq("country_id", ar_id).execute().data
    app_by_name = {a["name"].lower(): a for a in existing}

    stats = {"created": 0, "skipped": 0, "errors": 0}

    for name, queries in GEOCODE_QUERIES.items():
        if name.lower() in app_by_name:
            stats["skipped"] += 1
            continue

        print(f"  {name}...", end="", flush=True)
        lat, lng = None, None

        for query in queries:
            time.sleep(RATE_LIMIT_S)
            try:
                results = nominatim_search(client, query, polygon=False)
                if results:
                    lat = float(results[0]["lat"])
                    lng = float(results[0]["lon"])
                    break
            except Exception as e:
                print(f" error: {e}", end="")

        if args.dry_run:
            print(f" [dry-run] ({lat:.4f}, {lng:.4f})" if lat else " no result")
            stats["created"] += 1
            continue

        new_app = {
            "name": name, "slug": geo_slugify(name),
            "country_id": ar_id, "region_id": catch_all["id"] if catch_all else None,
            "designation_type": "IG", "hemisphere": "south",
            "latitude": round(lat, 5) if lat else None,
            "longitude": round(lng, 5) if lng else None,
        }
        try:
            sb.table("appellations").insert(new_app).execute()
            print(f" ok ({lat:.4f}, {lng:.4f})" if lat else " ok (no coords)")
            stats["created"] += 1
        except Exception as e:
            print(f" FAIL {e}")
            stats["errors"] += 1

    client.close()
    print(f"\n=== Complete: {stats['created']} created, {stats['skipped']} skipped, {stats['errors']} errors ===")


if __name__ == "__main__":
    main()
