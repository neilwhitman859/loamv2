#!/usr/bin/env python3
"""
PRO Platform Multi-State Wine Brand Fetcher.

Fetches wine brand registrations from the PRO (Product Registration Online)
platform by Sovos/ShipCompliant, active in 11 US states.

Each record includes: TTB COLA number, ABV, vintage, appellation, brand name,
label description, distributor(s), unit size, container type, approval dates.

API: POST {state}.productregistrationonline.com/Search/ActiveBrandSearch
Body: { draw: N, start: offset, length: pageSize }
Response: { Items: [...], TotalItems: N, MaxResults: 200 }

Usage:
    python -m pipeline.fetch.pro_states
    python -m pipeline.fetch.pro_states --state AR,CO,KY
    python -m pipeline.fetch.pro_states --state CO --analyze
    python -m pipeline.fetch.pro_states --resume
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DATA_DIR = Path("data/imports")
CHECKPOINT_DIR = DATA_DIR / "pro_checkpoints"

PRO_STATES = {
    "AR": {"name": "Arkansas"},
    "CO": {"name": "Colorado"},
    "KY": {"name": "Kentucky"},
    "LA": {"name": "Louisiana"},
    "MN": {"name": "Minnesota"},
    "NM": {"name": "New Mexico"},
    "NY": {"name": "New York"},
    "OH": {"name": "Ohio"},
    "OK": {"name": "Oklahoma"},
    "SC": {"name": "South Carolina"},
    "SD": {"name": "South Dakota"},
}

PAGE_SIZE = 200  # API max
DELAY_S = 1.2
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def pct(n: int, t: int) -> str:
    return f"{n / t * 100:.1f}%" if t else "0%"


def fetch_page(client: httpx.Client, api_url: str, start: int, draw: int) -> dict:
    resp = client.post(api_url, json={"draw": draw, "start": start, "length": PAGE_SIZE})
    resp.raise_for_status()
    return resp.json()


def save_checkpoint(file: Path, brands: list, next_start: int):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    file.write_text(json.dumps({
        "brands": brands,
        "nextStart": next_start,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }))


def slim_brand(b: dict) -> dict:
    distributors = b.get("Distributors") or []
    return {
        "cola_number": b.get("ColaNumber") or None,
        "brand": b.get("BrandDescription") or None,
        "label": b.get("LabelDescription") or None,
        "licensee": b.get("LicenseeName") or None,
        "abv": b.get("ABV") or None,
        "vintage": b.get("Vintage") or None,
        "appellation": b.get("Appellation") or None,
        "unit_size": b.get("UnitSize") or None,
        "unit_of_measure": b.get("UnitOfMeasure") or None,
        "container_type": b.get("ContainerType") or None,
        "approval_date": b.get("ApprovalDate") or None,
        "expiration_date": b.get("ExpirationDateString") or None,
        "approval_number": b.get("ApprovalNumber") or None,
        "distributors": [d.get("Name") or d for d in distributors if d] if distributors else [],
        "origin": b.get("OriginName") or None,
        "item_number": b.get("ItemNumber") or None,
    }


def fetch_state(client: httpx.Client, state_code: str, *, analyze_only: bool = False, resume: bool = False) -> dict | None:
    config = PRO_STATES.get(state_code)
    if not config:
        print(f"  Unknown state: {state_code}")
        return None

    api_url = f"https://{state_code.lower()}.productregistrationonline.com/Search/ActiveBrandSearch"
    output_file = DATA_DIR / f"pro_{state_code.lower()}_wines.json"
    checkpoint_file = CHECKPOINT_DIR / f"{state_code.lower()}.json"

    print(f"\n=== {config['name']} ({state_code}) ===")

    # Get total count
    try:
        data = fetch_page(client, api_url, 0, 1)
        total_items = data.get("TotalItems")
        print(f"  Total brands: {total_items:,}" if total_items else "  Total brands: unknown")

        if not total_items:
            print("  No data -- skipping")
            return None
        if analyze_only:
            return {"state": state_code, "name": config["name"], "total": total_items}
    except Exception as err:
        print(f"  Error: {err} -- skipping")
        return None

    # Resume or start fresh
    all_brands: list[dict] = []
    start_offset = 0
    if resume and checkpoint_file.exists():
        cp = json.loads(checkpoint_file.read_text())
        all_brands = cp.get("brands", [])
        start_offset = cp.get("nextStart", 0)
        print(f"  Resuming from offset {start_offset} ({len(all_brands)} cached)")

    # Paginate
    total_pages = (total_items + PAGE_SIZE - 1) // PAGE_SIZE
    start_page = start_offset // PAGE_SIZE + 1
    print(f"  Pages: {start_page}-{total_pages} ({PAGE_SIZE}/page)")

    draw = start_page
    consecutive_empty = 0
    errors = 0

    for offset in range(start_offset, total_items, PAGE_SIZE):
        draw += 1
        try:
            data = fetch_page(client, api_url, offset, draw)
            items = data.get("Items") or []

            if len(items) == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    print("  3 consecutive empty pages -- stopping")
                    break
                continue
            consecutive_empty = 0

            all_brands.extend(slim_brand(b) for b in items)

            page = offset // PAGE_SIZE + 1
            if page % 20 == 0 or page >= total_pages:
                print(f"  Page {page}/{total_pages} -- {len(all_brands):,} brands")
                save_checkpoint(checkpoint_file, all_brands, offset + PAGE_SIZE)

            time.sleep(DELAY_S)

        except Exception as err:
            errors += 1
            print(f"  Offset {offset}: {err}")
            if errors >= 5:
                print("  Too many errors -- saving checkpoint and moving on")
                save_checkpoint(checkpoint_file, all_brands, offset)
                break
            time.sleep(DELAY_S * 5)

    # Deduplicate
    seen: set[str] = set()
    deduped: list[dict] = []
    for b in all_brands:
        key = b.get("cola_number") or b.get("approval_number") or json.dumps(b, sort_keys=True)
        if key not in seen:
            seen.add(key)
            deduped.append(b)

    # Stats
    has_cola = sum(1 for b in deduped if b.get("cola_number"))
    has_abv = sum(1 for b in deduped if b.get("abv"))
    has_vintage = sum(1 for b in deduped if b.get("vintage"))
    has_appellation = sum(1 for b in deduped if b.get("appellation"))

    stats = {
        "total_from_api": total_items,
        "fetched": len(deduped),
        "has_cola": has_cola,
        "has_abv": has_abv,
        "has_vintage": has_vintage,
        "has_appellation": has_appellation,
    }

    print(f"  Fetched: {len(deduped):,} (deduped from {len(all_brands):,})")
    print(f"  COLA: {has_cola:,} ({pct(has_cola, len(deduped))})")
    print(f"  ABV: {has_abv:,} ({pct(has_abv, len(deduped))})")
    print(f"  Vintage: {has_vintage:,} ({pct(has_vintage, len(deduped))})")
    print(f"  Appellation: {has_appellation:,} ({pct(has_appellation, len(deduped))})")

    # Save
    output = {
        "metadata": {
            "source": f"PRO Platform -- {config['name']} ({state_code})",
            "url": api_url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "stats": stats,
        },
        "brands": deduped,
    }

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    file_size_mb = output_file.stat().st_size / 1024 / 1024
    print(f"  Saved: {output_file} ({file_size_mb:.1f} MB)")

    if checkpoint_file.exists():
        checkpoint_file.unlink()

    return {"state": state_code, "name": config["name"], **stats}


def main():
    parser = argparse.ArgumentParser(description="PRO Platform multi-state fetcher")
    parser.add_argument("--state", type=str, default=None, help="Comma-separated state codes")
    parser.add_argument("--analyze", action="store_true", help="Count only, don't fetch")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    args = parser.parse_args()

    target_states = (
        [s.strip().upper() for s in args.state.split(",")]
        if args.state
        else list(PRO_STATES.keys())
    )

    print("=== PRO Platform Multi-State Wine Brand Fetcher ===")
    print(f"States: {', '.join(target_states)}")
    print(f"Mode: {'ANALYZE' if args.analyze else 'FULL FETCH'}")

    results: list[dict] = []
    grand_total = 0

    with httpx.Client(timeout=30.0, headers=HEADERS) as client:
        for sc in target_states:
            r = fetch_state(client, sc, analyze_only=args.analyze, resume=args.resume)
            if r:
                results.append(r)
                grand_total += r.get("fetched") or r.get("total") or 0
            time.sleep(2.0)

    print("\n=== GRAND SUMMARY ===")
    print(f"States: {len(results)}/{len(target_states)}")
    print(f"Total wines: {grand_total:,}\n")
    for r in results:
        count = r.get("fetched") or r.get("total") or 0
        print(f"  {r['state']} {r['name']}: {count:,}")

    summary_file = DATA_DIR / "pro_states_summary.json"
    summary_file.write_text(json.dumps({
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "mode": "analyze" if args.analyze else "full",
        "states": results,
        "grand_total": grand_total,
    }, indent=2))


if __name__ == "__main__":
    main()
