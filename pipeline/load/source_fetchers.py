#!/usr/bin/env python3
"""
Load fetched source data into staging tables.

Sources: specs, berliner, texsom, wallys, enofile, flatiron, bc_liquor

Uses direct REST API calls (httpx) instead of supabase-py client, which
hangs on realtime websocket initialization in v2.28.

Usage:
    python -m pipeline.load.source_fetchers --source all
    python -m pipeline.load.source_fetchers --source specs,berliner
    python -m pipeline.load.source_fetchers --source texsom --truncate
"""

import argparse
import json
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

DATA_DIR = Path("data/imports")

# Load env
_env_path = Path(__file__).resolve().parents[2] / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=True)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE") or os.environ.get("SUPABASE_ANON_KEY", "")

_client = httpx.Client(
    base_url=f"{SUPABASE_URL}/rest/v1",
    headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    },
    timeout=30.0,
)


def _rest_upsert(table: str, rows: list[dict], on_conflict: str = "", batch_size: int = 500) -> int:
    """Upsert rows via PostgREST (merge-duplicates). Returns count inserted."""
    conflict_param = f"?on_conflict={on_conflict}" if on_conflict else ""
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        r = _client.post(
            f"/{table}{conflict_param}",
            json=batch,
            headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )
        if r.status_code in (200, 201):
            data = r.json()
            inserted += len(data) if isinstance(data, list) else 1
        else:
            print(f"  ERROR batch {i}-{i+len(batch)}: {r.status_code} {r.text[:300]}")
    return inserted


def _rest_insert(table: str, rows: list[dict], batch_size: int = 500) -> int:
    """Insert rows via PostgREST. Returns count inserted."""
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        r = _client.post(
            f"/{table}",
            json=batch,
            headers={"Prefer": "return=minimal"},
        )
        if r.status_code in (200, 201):
            inserted += len(batch)
        else:
            print(f"  ERROR batch {i}-{i+len(batch)}: {r.status_code} {r.text[:300]}")
            # Fall back to one-by-one
            for row in batch:
                r2 = _client.post(f"/{table}", json=row, headers={"Prefer": "return=minimal"})
                if r2.status_code in (200, 201):
                    inserted += 1
                else:
                    name = row.get("title") or row.get("name") or row.get("wine_name") or "unknown"
                    print(f"    Row error ({name}): {r2.status_code} {r2.text[:200]}")
    return inserted


def _rest_count(table: str) -> int:
    """Get row count for a table."""
    r = _client.get(f"/{table}?select=id", headers={"Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"})
    ct = r.headers.get("content-range", "")
    # content-range looks like "0-0/12345" or "*/0"
    if "/" in ct:
        return int(ct.split("/")[1])
    return 0


def _rest_truncate(table: str):
    """Delete all rows from a table."""
    r = _client.delete(f"/{table}?id=neq.00000000-0000-0000-0000-000000000000", headers={"Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        print(f"  Truncate error: {r.status_code} {r.text[:200]}")


def _read_json(filename: str, key: str = "wines") -> list[dict]:
    """Read a JSON data file and return the wines/products list."""
    path = DATA_DIR / filename
    if not path.exists():
        print(f"  File not found: {path}")
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        data = json.loads(path.read_text(encoding="latin-1"))

    if isinstance(data, list):
        return data
    return data.get(key, [])


def _extract_tag(tags: list[str], prefix: str) -> str | None:
    """Extract a tag value by prefix (e.g., 'Country: France' -> 'France')."""
    if not tags:
        return None
    for tag in tags:
        if tag.lower().startswith(prefix.lower()):
            val = tag[len(prefix):].strip().strip(":")
            if val:
                return val
    return None


def _extract_tags_multi(tags: list[str], prefix: str) -> list[str]:
    """Extract all tag values matching a prefix."""
    results = []
    if not tags:
        return results
    for tag in tags:
        if tag.lower().startswith(prefix.lower()):
            val = tag[len(prefix):].strip().strip(":")
            if val:
                results.append(val)
    return results


def load_specs() -> int:
    """Load Spec's Wine data (21.9K wines, UPC via SKU)."""
    wines = _read_json("specs_wines.json")
    if not wines:
        return 0

    rows = []
    for w in wines:
        rows.append({
            "specs_id": w["specs_id"],
            "name": w.get("name"),
            "slug": w.get("slug"),
            "upc": w.get("upc"),
            "sku": w.get("sku"),
            "price": float(w["price"]) if w.get("price") else None,
            "permalink": w.get("permalink"),
            "wine_category": w.get("wine_category"),
            "wine_origin": w.get("wine_origin"),
            "wine_size": w.get("wine_size"),
            "rating": w.get("rating"),
            "review_count": w.get("review_count"),
            "image_url": w.get("image_url"),
            "in_stock": w.get("in_stock"),
        })

    print(f"  Loading {len(rows)} Spec's wines...")
    return _rest_upsert("source_specs", rows, on_conflict="specs_id")


def load_berliner() -> int:
    """Load Berliner Wine Trophy data (73.9K wines)."""
    wines = _read_json("berliner_wine_trophy.json")
    if not wines:
        return 0

    rows = []
    seen_urls = set()
    for w in wines:
        url = w.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        rows.append({
            "wine_name": w.get("wine_name"),
            "url": url,
            "trophy_code": w.get("trophy_code"),
            "slug": w.get("slug"),
            "producer": w.get("producer"),
            "origin": w.get("origin"),
            "country": w.get("country"),
            "region": w.get("region"),
            "grapes": w.get("grapes"),
            "award_raw": w.get("award_raw"),
            "medal": w.get("medal"),
            "competition": w.get("competition"),
            "competition_year": w.get("competition_year"),
            "photo_url": w.get("photo_url"),
        })

    print(f"  Loading {len(rows)} Berliner wines...")
    return _rest_upsert("source_berliner", rows, on_conflict="url")


def load_texsom() -> int:
    """Load TEXSOM competition data (46.9K wines)."""
    wines = _read_json("texsom_wines.json")
    if not wines:
        return 0

    rows = []
    for w in wines:
        extra = w.get("extra_fields")
        rows.append({
            "year": w.get("year"),
            "producer": w.get("producer"),
            "wine_name": w.get("wine_name"),
            "appellation": w.get("appellation"),
            "country": w.get("country"),
            "vintage": w.get("vintage"),
            "award": w.get("award"),
            "extra_fields": extra if extra else None,
        })

    print(f"  Loading {len(rows)} TEXSOM wines...")
    return _rest_insert("source_texsom", rows)


def load_wallys() -> int:
    """Load Wally's Wine data (19.4K wines)."""
    wines = _read_json("wallys_wines.json")
    if not wines:
        return 0

    rows = []
    for w in wines:
        pairings = w.get("pairings")
        tags = w.get("tags")
        rows.append({
            "shopify_id": w["shopify_id"],
            "title": w.get("title"),
            "vendor": w.get("vendor"),
            "product_type": w.get("product_type"),
            "handle": w.get("handle"),
            "country": w.get("country"),
            "region": w.get("region"),
            "grapes": w.get("grapes"),
            "vintage": w.get("vintage"),
            "producer": w.get("producer"),
            "sweetness": w.get("sweetness"),
            "body": w.get("body"),
            "pairings": pairings if pairings else None,
            "size": w.get("size"),
            "price": w.get("price"),
            "sku": w.get("sku"),
            "tags": tags if tags else None,
            "shopify_created_at": w.get("created_at"),
        })

    print(f"  Loading {len(rows)} Wally's wines...")
    return _rest_upsert("source_wallys", rows, on_conflict="shopify_id")


def load_enofile() -> int:
    """Load EnofileOnline competition data (9.2K wines)."""
    wines = _read_json("enofileonline_wines.json")
    if not wines:
        return 0

    rows = []
    for w in wines:
        brand = w.get("brand", "")
        if brand:
            brand = brand.strip()
        rows.append({
            "enofile_id": w["enofile_id"],
            "year": w.get("year"),
            "competition": w.get("competition"),
            "brand": brand or None,
            "varietal": w.get("varietal"),
            "vintage": w.get("vintage"),
            "appellation": w.get("appellation"),
            "designation": w.get("designation"),
            "addl_designation": w.get("addl_designation"),
            "price": float(w["price"]) if w.get("price") else None,
            "award": w.get("award"),
            "website": w.get("website"),
        })

    print(f"  Loading {len(rows)} EnofileOnline wines...")
    return _rest_upsert("source_enofile", rows, on_conflict="enofile_id")


def load_flatiron() -> int:
    """Load Flatiron Wines data (4.1K wines, raw Shopify format with tag extraction)."""
    products = _read_json("flatiron_wines.json", key="products")
    if not products:
        products = _read_json("flatiron_wines.json", key="wines")
    if not products:
        path = DATA_DIR / "flatiron_wines.json"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except UnicodeDecodeError:
            data = json.loads(path.read_text(encoding="latin-1"))
        if isinstance(data, list):
            products = data

    if not products:
        return 0

    rows = []
    for p in products:
        tags = p.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",")]

        country = _extract_tag(tags, "Country:")
        region = _extract_tag(tags, "Region:")
        grapes = _extract_tags_multi(tags, "Grape Variety:")
        vintage = _extract_tag(tags, "Vintage:")
        producer_tag = _extract_tag(tags, "Producer:")
        body = _extract_tag(tags, "Body:")
        sweetness = _extract_tag(tags, "Sweetness:")

        variants = p.get("variants", [])
        price = variants[0].get("price") if variants else None
        sku = variants[0].get("sku") if variants else None

        shopify_id = p.get("shopify_id") or p.get("id")

        rows.append({
            "shopify_id": shopify_id,
            "title": p.get("title"),
            "vendor": p.get("vendor"),
            "product_type": p.get("product_type"),
            "handle": p.get("handle"),
            "country": country,
            "region": region,
            "grapes": grapes if grapes else None,
            "vintage": vintage,
            "producer": producer_tag,
            "body": body,
            "sweetness": sweetness,
            "price": price,
            "sku": sku,
            "tags": tags if tags else None,
            "shopify_created_at": p.get("created_at"),
        })

    print(f"  Loading {len(rows)} Flatiron wines...")
    return _rest_upsert("source_flatiron", rows, on_conflict="shopify_id")


def load_bc_liquor() -> int:
    """Load BC Liquor Stores data (3.3K wines, UPC)."""
    wines = _read_json("bc_liquor_wines.json")
    if not wines:
        return 0

    rows = []
    seen_skus = set()
    for w in wines:
        sku = w["sku"]
        if sku in seen_skus:
            continue
        seen_skus.add(sku)
        rows.append({
            "sku": sku,
            "name": w.get("name"),
            "upc": w.get("upc"),
            "country": w.get("country"),
            "country_code": w.get("country_code"),
            "region": w.get("region"),
            "sub_region": w.get("sub_region"),
            "grape_type": w.get("grape_type"),
            "abv": float(w["abv"]) if w.get("abv") else None,
            "sweetness": w.get("sweetness"),
            "price": w.get("price"),
            "regular_price": w.get("regular_price"),
            "color": w.get("color"),
            "product_type": w.get("product_type"),
            "description": w.get("description"),
            "volume_ml": float(w["volume_ml"]) if w.get("volume_ml") else None,
            "organic": w.get("organic"),
            "kosher": w.get("kosher"),
            "vqa": w.get("vqa"),
            "rating": float(w["rating"]) if w.get("rating") else None,
            "votes": w.get("votes"),
            "category": w.get("category"),
            "sub_category": w.get("sub_category"),
            "image_url": w.get("image_url"),
        })

    print(f"  Loading {len(rows)} BC Liquor wines...")
    return _rest_upsert("source_bc_liquor", rows, on_conflict="sku")


LOADERS = {
    "specs": load_specs,
    "berliner": load_berliner,
    "texsom": load_texsom,
    "wallys": load_wallys,
    "enofile": load_enofile,
    "flatiron": load_flatiron,
    "bc_liquor": load_bc_liquor,
}


def main():
    parser = argparse.ArgumentParser(description="Load fetched source data into staging tables")
    parser.add_argument("--source", required=True, help="Source(s) to load: all, or comma-separated list")
    parser.add_argument("--truncate", action="store_true", help="Truncate table before loading")
    args = parser.parse_args()

    if args.source == "all":
        sources = list(LOADERS.keys())
    else:
        sources = [s.strip() for s in args.source.split(",")]

    for source in sources:
        if source not in LOADERS:
            print(f"Unknown source: {source}. Available: {', '.join(LOADERS.keys())}")
            continue

        table = f"source_{source}"
        print(f"\n=== Loading {source} into {table} ===")

        if args.truncate:
            print(f"  Truncating {table}...")
            _rest_truncate(table)

        loader = LOADERS[source]
        count = loader()
        print(f"  Done: {count} rows loaded into {table}")

    # Summary
    print("\n=== Summary ===")
    for source in sources:
        if source in LOADERS:
            table = f"source_{source}"
            count = _rest_count(table)
            print(f"  {table}: {count} rows")


if __name__ == "__main__":
    main()
