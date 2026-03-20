"""
Smart multi-slice Vivino fetcher. Partitions by wine_type x country
to get non-overlapping result sets, maximizing unique wine coverage.

Usage:
    python -m pipeline.vivino.fetch_mega --probe-only
    python -m pipeline.vivino.fetch_mega
    python -m pipeline.vivino.fetch_mega --resume
    python -m pipeline.vivino.fetch_mega --export out.json
    python -m pipeline.vivino.fetch_mega --max-unique 5000
"""

import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BASE_URL = "https://www.vivino.com/api/explore/explore"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)

MANIFEST_FILE = "vivino_mega_manifest.json"
DATA_FILE = "vivino_mega_data.jsonl"
SEEN_FILE = "vivino_mega_seen.json"

WINE_TYPES = [
    {"id": "1", "label": "Red"},
    {"id": "2", "label": "White"},
    {"id": "3", "label": "Sparkling"},
    {"id": "4", "label": "Rose"},
    {"id": "7", "label": "Dessert"},
    {"id": "24", "label": "Fortified"},
]

L1_COUNTRIES = ["fr", "it", "us", "es", "pt"]
L2_COUNTRIES = ["de", "au", "cl", "ar", "za", "at", "nz", "br"]

COUNTRY_LABELS = {
    "fr": "France", "it": "Italy", "us": "USA", "es": "Spain", "pt": "Portugal",
    "de": "Germany", "au": "Australia", "cl": "Chile", "ar": "Argentina",
    "za": "South Africa", "at": "Austria", "nz": "New Zealand", "br": "Brazil",
    "_other": "Other",
}


def extract_listing(match: dict) -> dict:
    v = match.get("vintage") or {}
    wine = v.get("wine") or {}
    winery = wine.get("winery") or {}
    region = wine.get("region") or {}
    country = region.get("country") or {}
    stats = v.get("statistics") or {}
    price = match.get("price")

    price_per_bottle = None
    merchant_name = None
    source_url = None
    bottle_qty = 1
    if price:
        bottle_qty = price.get("bottle_quantity") or 1
        amt = price.get("amount")
        if amt is not None:
            price_per_bottle = round(amt / bottle_qty, 2)
        merchant_name = price.get("merchant_name")
        source_url = price.get("url")

    import re
    vintage_year = None
    year = v.get("year")
    if year and year > 1900:
        vintage_year = year
    else:
        m = re.search(r"-(\d{4})$", v.get("seo_name") or "")
        if m:
            vintage_year = int(m.group(1))

    return {
        "vivino_wine_id": wine.get("id"),
        "vivino_vintage_id": v.get("id"),
        "winery_name": winery.get("name"),
        "wine_name": wine.get("name"),
        "vintage_year": vintage_year,
        "region_name": region.get("name"),
        "country_name": country.get("name"),
        "country_code": country.get("code"),
        "wine_type_id": wine.get("type_id"),
        "rating_average": stats.get("ratings_average"),
        "rating_count": stats.get("ratings_count") or 0,
        "price_usd": price_per_bottle,
        "price_raw": price["amount"] if price else None,
        "bottle_quantity": bottle_qty,
        "merchant_name": merchant_name,
        "source_url": source_url,
        "is_natural": wine.get("is_natural") or False,
    }


def generate_slices() -> list[dict]:
    slices = []
    type_order = ["3", "4", "7", "24", "1", "2"]
    type_map = {t["id"]: t for t in WINE_TYPES}

    for type_id in type_order:
        wt = type_map[type_id]
        for cc in L1_COUNTRIES:
            slices.append({
                "id": f"{wt['label'].lower()}|{cc}",
                "label": f"{wt['label']} x {COUNTRY_LABELS[cc]}",
                "tier": "L1",
                "params": {"wine_type_ids[]": type_id, "country_codes[]": cc},
            })
        for cc in L2_COUNTRIES:
            slices.append({
                "id": f"{wt['label'].lower()}|{cc}",
                "label": f"{wt['label']} x {COUNTRY_LABELS[cc]}",
                "tier": "L2",
                "params": {"wine_type_ids[]": type_id, "country_codes[]": cc},
            })
        slices.append({
            "id": f"{wt['label'].lower()}|other",
            "label": f"{wt['label']} x Other countries",
            "tier": "L3",
            "params": {"wine_type_ids[]": type_id},
        })
    return slices


def load_manifest():
    p = Path(MANIFEST_FILE)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def create_manifest(slices, base_delay, max_pages):
    return {
        "version": 1,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "config": {"delay_ms": base_delay, "max_pages_per_slice": max_pages},
        "global_stats": {
            "total_listings_fetched": 0,
            "total_unique_written": 0,
            "total_duplicates_skipped": 0,
            "total_pages_fetched": 0,
            "total_errors": 0,
        },
        "slices": [{
            **s,
            "status": "pending",
            "records_matched": None,
            "pages_fetched": 0,
            "listings_fetched": 0,
            "unique_written": 0,
            "last_page": 0,
            "started_at": None,
            "completed_at": None,
        } for s in slices],
    }


def save_manifest(manifest):
    Path(MANIFEST_FILE).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def load_seen() -> set:
    p = Path(SEEN_FILE)
    if p.exists():
        return set(json.loads(p.read_text(encoding="utf-8")))
    return set()


def save_seen(seen: set):
    Path(SEEN_FILE).write_text(json.dumps(list(seen)), encoding="utf-8")


def fetch_page(client: httpx.Client, params: dict, page: int) -> dict:
    qs = {
        "country_code": "US",
        "currency_code": "USD",
        "min_rating": "1",
        "order_by": "ratings_count",
        "order": "desc",
        "price_range_min": "0",
        "price_range_max": "500",
        **params,
        "page": str(page),
    }
    resp = client.get(BASE_URL, params=qs)
    if resp.status_code == 429:
        raise httpx.HTTPStatusError("Rate limited", request=resp.request, response=resp)
    resp.raise_for_status()
    return resp.json()["explore_vintage"]


def export_to_json(output_path: str):
    print(f"Exporting {DATA_FILE} -> {output_path}...")
    count = 0
    with open(output_path, "w", encoding="utf-8") as out:
        out.write("[\n")
        first = True
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if not first:
                    out.write(",\n")
                out.write("  " + line)
                first = False
                count += 1
        out.write("\n]\n")
    print(f"Exported {count:,} listings to {output_path}")


def run_probe(manifest, client: httpx.Client, base_delay: float):
    print("=== PROBE MODE -- Fetching page 1 of each slice ===\n")
    total_records = 0

    for sl in manifest["slices"]:
        try:
            result = fetch_page(client, sl["params"], 1)
            matched = result.get("records_matched") or 0
            sl["records_matched"] = matched
            total_records += matched
            sample = len(result.get("matches") or [])
            print(f"  {sl['label']:<32} {matched:>8} records  ({sample} per page)  [{sl['tier']}]")
            time.sleep(0.5)
        except Exception as err:
            print(f"  {sl['label']:<32} ERROR: {err}")
            time.sleep(1)

    unique_est = round(total_records * 0.35)
    pages_est = (total_records + 24) // 25
    hours_est = pages_est * base_delay / 3600

    print(f"\n=== PROBE SUMMARY ===")
    print(f"  Total records_matched (sum):  {total_records:,}")
    print(f"  Estimated unique wines:       ~{unique_est:,} (35% dedup ratio)")
    print(f"  Estimated pages:              {pages_est:,}")
    print(f"  Estimated fetch time:         ~{hours_est:.1f} hours at {base_delay*1000:.0f}ms delay")

    big = [s for s in manifest["slices"] if (s.get("records_matched") or 0) > 125000]
    if big:
        print(f"\n  WARNING: {len(big)} slices have >125K records (may need price sub-slicing):")
        for s in big:
            print(f"    {s['label']}: {s['records_matched']:,}")

    save_manifest(manifest)
    print(f"\nManifest saved to {MANIFEST_FILE}")


def run_fetch(manifest, seen: set, client: httpx.Client, base_delay: float,
              max_pages: int, max_unique: float, resume: bool):
    start_time = time.time()
    current_delay = base_delay
    gs = manifest["global_stats"]

    print("=== VIVINO MEGA FETCH ===\n")
    print(f"  Slices: {len(manifest['slices'])}")
    print(f"  Delay: {base_delay*1000:.0f}ms")
    print(f"  Max pages/slice: {max_pages}")
    if resume:
        print("  Resuming from manifest...")
    print(f"  Seen IDs loaded: {len(seen):,}\n")

    for si, sl in enumerate(manifest["slices"]):
        if sl["status"] in ("completed", "paused"):
            continue
        if gs["total_unique_written"] >= max_unique:
            print(f"\n  Reached max-unique limit ({max_unique}). Stopping.")
            break

        sl["status"] = "in_progress"
        if not sl.get("started_at"):
            sl["started_at"] = datetime.now(timezone.utc).isoformat()
        start_page = (sl.get("last_page") or 0) + 1 if resume else 1

        print(f"\n-- Slice {si+1}/{len(manifest['slices'])}: {sl['label']} [{sl['tier']}] --")
        if start_page > 1:
            print(f"  Resuming from page {start_page}")

        consecutive_errors = 0
        consecutive_zero_new = 0
        zero_new_limit = 15
        current_delay = base_delay

        for page in range(start_page, max_pages + 1):
            try:
                result = fetch_page(client, sl["params"], page)

                if page == start_page and not sl.get("records_matched"):
                    sl["records_matched"] = result.get("records_matched") or 0
                    max_p = min((sl["records_matched"] + 24) // 25, max_pages)
                    print(f"  records_matched: {sl['records_matched']:,} (~{max_p} pages)")

                matches = result.get("matches") or []
                if not matches:
                    print(f"  Page {page}: No more results.")
                    break

                slice_new = 0
                for match in matches:
                    listing = extract_listing(match)
                    vid = listing["vivino_vintage_id"]
                    sl["listings_fetched"] += 1
                    gs["total_listings_fetched"] += 1

                    if vid and vid in seen:
                        gs["total_duplicates_skipped"] += 1
                        continue

                    if vid:
                        seen.add(vid)
                    with open(DATA_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(listing) + "\n")
                    slice_new += 1
                    sl["unique_written"] += 1
                    gs["total_unique_written"] += 1

                sl["pages_fetched"] += 1
                sl["last_page"] = page
                gs["total_pages_fetched"] += 1
                consecutive_errors = 0
                current_delay = base_delay

                if slice_new == 0:
                    consecutive_zero_new += 1
                    if consecutive_zero_new >= zero_new_limit:
                        print(f"\n  {zero_new_limit} consecutive pages with 0 new -- skipping rest of slice.")
                        break
                else:
                    consecutive_zero_new = 0

                elapsed = (time.time() - start_time) / 60
                print(
                    f"\r  Page {page} -- {slice_new} new, {sl['unique_written']} slice total, "
                    f"{gs['total_unique_written']:,} global unique [{elapsed:.1f}m]",
                    end="", flush=True,
                )

                if page % 50 == 0:
                    save_manifest(manifest)
                    save_seen(seen)

                if gs["total_unique_written"] >= max_unique:
                    break

                time.sleep(current_delay)

            except Exception as err:
                consecutive_errors += 1
                gs["total_errors"] += 1
                err_str = str(err)

                if "429" in err_str:
                    current_delay = min(current_delay * 2, 30)
                    print(f"\n  Rate limited at page {page}. Backing off to {current_delay*1000:.0f}ms...")
                else:
                    current_delay = min(current_delay * 1.5, 15)
                    print(f"\n  Error page {page}: {err}. Delay now {current_delay*1000:.0f}ms")

                if consecutive_errors >= 5:
                    print("  5 consecutive errors -- pausing slice.")
                    sl["status"] = "paused"
                    save_manifest(manifest)
                    save_seen(seen)
                    break

                time.sleep(current_delay)

        if sl["status"] != "paused":
            sl["status"] = "completed"
            sl["completed_at"] = datetime.now(timezone.utc).isoformat()

        save_manifest(manifest)
        save_seen(seen)
        print(f"\n  Done {sl['label']}: {sl['unique_written']:,} unique")

    total_min = (time.time() - start_time) / 60
    print(f"\n\n=== FETCH COMPLETE ===")
    print(f"  Time:               {total_min:.1f} minutes")
    print(f"  Pages fetched:      {gs['total_pages_fetched']:,}")
    print(f"  Listings fetched:   {gs['total_listings_fetched']:,}")
    print(f"  Unique written:     {gs['total_unique_written']:,}")
    print(f"  Duplicates skipped: {gs['total_duplicates_skipped']:,}")
    print(f"  Errors:             {gs['total_errors']}")

    completed = sum(1 for s in manifest["slices"] if s["status"] == "completed")
    paused = sum(1 for s in manifest["slices"] if s["status"] == "paused")
    print(f"  Slices completed:   {completed}/{len(manifest['slices'])}")
    if paused:
        print(f"  Slices paused:      {paused} (run --resume to retry)")

    print(f"\nData saved to {DATA_FILE}")
    print(f"Manifest saved to {MANIFEST_FILE}")

    # Quick unique wine count
    wine_ids = set()
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                l = json.loads(line)
                wid = l.get("vivino_wine_id")
                if wid:
                    wine_ids.add(wid)
            except Exception:
                pass
    print(f"  Unique wines (by vivino_wine_id): {len(wine_ids):,}")


def main():
    parser = argparse.ArgumentParser(description="Multi-slice Vivino fetcher")
    parser.add_argument("--probe-only", action="store_true", help="Probe mode: 1 page per slice")
    parser.add_argument("--resume", action="store_true", help="Resume interrupted run")
    parser.add_argument("--export", default=None, help="Export JSONL to JSON array")
    parser.add_argument("--max-unique", type=int, default=0, help="Stop after N unique listings")
    parser.add_argument("--delay-ms", type=int, default=1500, help="Delay between requests in ms")
    parser.add_argument("--max-pages", type=int, default=5000, help="Max pages per slice")
    args = parser.parse_args()

    base_delay = args.delay_ms / 1000.0
    max_unique = args.max_unique if args.max_unique > 0 else float("inf")

    if args.export:
        export_to_json(args.export)
        return

    slice_defs = generate_slices()

    if args.resume and Path(MANIFEST_FILE).exists():
        manifest = load_manifest()
        seen = load_seen()
        print(f"Resumed manifest with {len(manifest['slices'])} slices.")
    else:
        manifest = create_manifest(slice_defs, args.delay_ms, args.max_pages)
        seen = set()
        if not args.resume:
            Path(DATA_FILE).write_text("", encoding="utf-8")

    client = httpx.Client(
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=30,
    )

    try:
        if args.probe_only:
            run_probe(manifest, client, base_delay)
        else:
            run_fetch(manifest, seen, client, base_delay, args.max_pages, max_unique, args.resume)
    finally:
        client.close()


if __name__ == "__main__":
    main()
