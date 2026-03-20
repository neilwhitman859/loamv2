"""
Imports RLP Einzellagen boundaries from ldproxy OGC API. Caches data to JSON.
Uses 6-strategy name matching for German vineyard appellations.

Usage: python -m pipeline.geo.import_rlp_einzellagen [--dry-run]
"""

import sys
import re
import json
import time
import unicodedata
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import simplify_precision, fetch_all_paginated

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LDPROXY_BASE = "https://demo.ldproxy.net/vineyards/collections/vineyards/items"
CACHE_FILE = PROJECT_ROOT / "data" / "geo" / "rlp_vineyards_cache.json"


def normalize_de(s: str) -> str:
    """Normalize German text for matching."""
    t = unicodedata.normalize("NFD", s.lower())
    t = re.sub(r"[\u0300-\u036f]", "", t)
    t = t.replace("\u00df", "ss").replace("\u00e4", "ae").replace("\u00f6", "oe").replace("\u00fc", "ue")
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def main():
    parser = argparse.ArgumentParser(description="Import RLP Einzellagen from ldproxy OGC API")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    print("=== RLP Einzellagen Import ===")

    # Load or fetch vineyard data
    if CACHE_FILE.exists():
        print(f"Loading from cache: {CACHE_FILE}")
        vineyards = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    else:
        print("Fetching from ldproxy API...")
        client = httpx.Client(timeout=60)
        vineyards = []
        offset = 0
        limit = 500

        while True:
            resp = client.get(LDPROXY_BASE, params={
                "f": "json", "limit": str(limit), "offset": str(offset),
            })
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", [])
            vineyards.extend(features)
            print(f"  Fetched {len(vineyards)} vineyards...")

            if len(features) < limit:
                break
            offset += limit
            time.sleep(0.5)

        client.close()
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(json.dumps(vineyards, ensure_ascii=False), encoding="utf-8")
        print(f"  Cached {len(vineyards)} vineyards to {CACHE_FILE}")

    print(f"Total vineyards: {len(vineyards)}")

    # Get Germany country ID
    de = sb.table("countries").select("id").eq("iso_code", "DE").single().execute().data
    de_id = de["id"]

    # Load existing German appellations
    existing_apps = fetch_all_paginated(sb, "appellations", "id, name, slug, country_id",
                                         {"country_id": de_id})
    app_by_name = {a["name"].lower(): a for a in existing_apps}
    app_by_norm = {normalize_de(a["name"]): a for a in existing_apps}
    app_by_slug = {a["slug"]: a for a in existing_apps}

    stats = {"matched": 0, "boundaries": 0, "unmatched": 0, "errors": 0}
    unmatched_names = []

    for v in vineyards:
        props = v.get("properties", {})
        name = props.get("name") or props.get("einzellage") or props.get("lage")
        if not name:
            continue

        geometry = v.get("geometry")
        if not geometry or geometry.get("type") not in ("Polygon", "MultiPolygon"):
            continue

        # 6-strategy matching
        app = None

        # 1. Direct name match
        app = app_by_name.get(name.lower())

        # 2. Normalized match
        if not app:
            app = app_by_norm.get(normalize_de(name))

        # 3. With Anbaugebiet prefix stripped
        if not app:
            clean = re.sub(r"^[A-Z][a-z]+er\s+", "", name)
            app = app_by_name.get(clean.lower()) or app_by_norm.get(normalize_de(clean))

        # 4. With common suffixes stripped
        if not app:
            for suffix in ["berg", "lay", "kupp", "graben"]:
                if name.lower().endswith(suffix):
                    base = name[:-len(suffix)].strip()
                    app = app_by_name.get(base.lower())
                    if app:
                        break

        # 5. Slug match
        if not app:
            from pipeline.lib.normalize import slugify
            app = app_by_slug.get(slugify(name))

        # 6. Partial match (name contains appellation name)
        if not app:
            for a_name, a in app_by_name.items():
                if len(a_name) > 4 and a_name in name.lower():
                    app = a
                    break

        if not app:
            stats["unmatched"] += 1
            if len(unmatched_names) < 50:
                unmatched_names.append(name)
            continue

        stats["matched"] += 1

        # Import boundary
        simplified = simplify_precision(geometry)

        if args.dry_run:
            stats["boundaries"] += 1
            continue

        try:
            sb.rpc("upsert_appellation_boundary", {
                "p_appellation_id": app["id"],
                "p_geojson": json.dumps(simplified),
                "p_source_id": f"ldproxy-rlp/{props.get('id', '')}",
                "p_confidence": "official",
            }).execute()
            stats["boundaries"] += 1
        except Exception as e:
            print(f"  [ERROR] {name}: {e}")
            stats["errors"] += 1

    print(f"\n=== Complete ===")
    print(f"  Matched: {stats['matched']}")
    print(f"  Boundaries: {stats['boundaries']}")
    print(f"  Unmatched: {stats['unmatched']}")
    print(f"  Errors: {stats['errors']}")
    if unmatched_names:
        print(f"\n  Sample unmatched ({len(unmatched_names)}):")
        for n in unmatched_names[:20]:
            print(f"    - {n}")


if __name__ == "__main__":
    main()
