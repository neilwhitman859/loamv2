#!/usr/bin/env python3
"""
TTB Label Barcode Scanner

Scans downloaded TTB COLA label images for barcodes (UPC/EAN/QR).
Extracts UPC/EAN product codes and writes results to DB + JSON.

Usage:
    python -m pipeline.analyze.barcode_scanner                         # scan all
    python -m pipeline.analyze.barcode_scanner --limit 100             # test with 100
    python -m pipeline.analyze.barcode_scanner --output results.json   # custom output
    python -m pipeline.analyze.barcode_scanner --update-db             # write barcodes to source_ttb_colas
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import zxingcpp
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ─── Constants ───────────────────────────────────────────────────────────────

DEFAULT_IMAGE_DIR = Path.home() / "Desktop" / "Loam Cowork" / "data" / "images" / "ttb_labels"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[2] / "data" / "imports" / "ttb_barcode_results.json"

# Barcode types we care about for product identification
UPC_TYPES = {"EAN13", "EAN8", "UPCA", "UPCE"}
QR_TYPES = {"QRCode", "DataMatrix"}
# Code39 on TTB labels is just the TTB ID itself — useful for validation but not product UPC


# ─── Scanner ─────────────────────────────────────────────────────────────────

def scan_image(img_path: str) -> list[dict]:
    """Scan a single image for barcodes. Returns list of detected codes."""
    try:
        img = Image.open(img_path)
        results = zxingcpp.read_barcodes(img)

        codes = []
        for r in results:
            fmt = r.format.name
            text = r.text.strip()

            if not text:
                continue

            code_type = "unknown"
            if fmt in UPC_TYPES:
                code_type = "upc"
            elif fmt in QR_TYPES:
                code_type = "qr"
            elif fmt == "Code39":
                code_type = "code39"
            elif fmt == "Code128":
                code_type = "code128"

            codes.append({
                "format": fmt,
                "type": code_type,
                "value": text,
            })

        return codes

    except Exception as e:
        return [{"format": "error", "type": "error", "value": str(e)[:200]}]


def find_all_images(image_dir: Path) -> list[tuple[str, str]]:
    """Find all label images, return (ttb_id, image_path) tuples."""
    results = []
    for prefix_dir in sorted(image_dir.iterdir()):
        if not prefix_dir.is_dir():
            continue
        for ttb_dir in sorted(prefix_dir.iterdir()):
            if not ttb_dir.is_dir():
                continue
            ttb_id = ttb_dir.name
            for img_file in sorted(ttb_dir.glob("*.jpg")):
                results.append((ttb_id, str(img_file)))
    return results


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scan TTB label images for barcodes")
    parser.add_argument("--image-dir", type=str, default=str(DEFAULT_IMAGE_DIR))
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--limit", type=int, help="Max images to scan")
    parser.add_argument("--update-db", action="store_true", help="Write UPC barcodes to source_ttb_colas")
    args = parser.parse_args()

    image_dir = Path(args.image_dir)
    output_path = Path(args.output)

    print("=== TTB Label Barcode Scanner ===")
    print(f"  Image dir: {image_dir}")
    print(f"  Output: {output_path}")
    print()

    # Find all images
    print("Finding images...")
    images = find_all_images(image_dir)
    print(f"  Found {len(images):,} images")

    if args.limit:
        images = images[:args.limit]
        print(f"  Limited to {len(images):,}")

    # Scan
    print("\nScanning for barcodes...")
    start_time = time.time()

    # Results grouped by TTB ID
    by_ttb: dict[str, dict] = {}
    total_scanned = 0
    total_upc = 0
    total_qr = 0
    total_code39 = 0
    total_no_barcode = 0
    errors = 0

    for i, (ttb_id, img_path) in enumerate(images):
        codes = scan_image(img_path)
        total_scanned += 1

        if ttb_id not in by_ttb:
            by_ttb[ttb_id] = {
                "ttb_id": ttb_id,
                "upcs": [],
                "qr_codes": [],
                "code39": [],
                "other": [],
                "image_count": 0,
            }

        rec = by_ttb[ttb_id]
        rec["image_count"] += 1

        has_upc = False
        for code in codes:
            if code["type"] == "error":
                errors += 1
            elif code["type"] == "upc":
                if code["value"] not in rec["upcs"]:
                    rec["upcs"].append(code["value"])
                has_upc = True
            elif code["type"] == "qr":
                if code["value"] not in rec["qr_codes"]:
                    rec["qr_codes"].append(code["value"])
            elif code["type"] == "code39":
                if code["value"] not in rec["code39"]:
                    rec["code39"].append(code["value"])
            else:
                rec["other"].append(code)

        if has_upc:
            total_upc += 1
        if any(c["type"] == "qr" for c in codes):
            total_qr += 1
        if any(c["type"] == "code39" for c in codes):
            total_code39 += 1
        if not codes or all(c["type"] == "error" for c in codes):
            total_no_barcode += 1

        if (i + 1) % 500 == 0 or i + 1 == len(images):
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (len(images) - i - 1) / rate if rate > 0 else 0
            print(
                f"  [{i+1:,}/{len(images):,}] "
                f"UPC: {total_upc:,}  QR: {total_qr:,}  "
                f"{rate:.0f} img/sec  ~{remaining:.0f}s left",
                end="\r",
            )

    elapsed = time.time() - start_time

    # Compute stats
    ttb_with_upc = sum(1 for r in by_ttb.values() if r["upcs"])
    ttb_with_qr = sum(1 for r in by_ttb.values() if r["qr_codes"])
    unique_upcs = set()
    for r in by_ttb.values():
        unique_upcs.update(r["upcs"])

    print(f"\n\n=== RESULTS ===")
    print(f"  Images scanned: {total_scanned:,}")
    print(f"  Unique TTB IDs: {len(by_ttb):,}")
    print(f"  Time: {elapsed:.1f}s ({total_scanned / elapsed:.0f} img/sec)")
    print()
    print(f"  TTB labels with UPC/EAN: {ttb_with_upc:,} ({ttb_with_upc / len(by_ttb) * 100:.1f}%)")
    print(f"  TTB labels with QR code: {ttb_with_qr:,} ({ttb_with_qr / len(by_ttb) * 100:.1f}%)")
    print(f"  Unique UPC/EAN codes: {len(unique_upcs):,}")
    print(f"  Images with no barcode: {total_no_barcode:,}")
    print(f"  Errors: {errors:,}")

    # UPC length distribution
    upc_lengths: dict[int, int] = {}
    for upc in unique_upcs:
        upc_lengths[len(upc)] = upc_lengths.get(len(upc), 0) + 1
    if upc_lengths:
        print(f"\n  UPC length distribution:")
        for length in sorted(upc_lengths.keys()):
            print(f"    {length} digits: {upc_lengths[length]:,}")

    # Save results
    output_data = {
        "metadata": {
            "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "image_dir": str(image_dir),
            "images_scanned": total_scanned,
            "unique_ttb_ids": len(by_ttb),
            "ttb_with_upc": ttb_with_upc,
            "ttb_with_qr": ttb_with_qr,
            "unique_upcs": len(unique_upcs),
        },
        "results": sorted(by_ttb.values(), key=lambda r: r["ttb_id"]),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"\n  Saved to: {output_path}")

    # Optionally update DB
    if args.update_db and ttb_with_upc > 0:
        print("\nUpdating source_ttb_colas with detected barcodes...")
        update_db(by_ttb)


def update_db(by_ttb: dict[str, dict]):
    """Write detected UPC barcodes back to source_ttb_colas and external_ids."""
    from pipeline.lib.db import get_supabase
    sb = get_supabase()

    # Only records with UPCs
    upc_records = [r for r in by_ttb.values() if r["upcs"]]
    print(f"  Writing {len(upc_records):,} UPC records...")

    # We don't have a barcode column on source_ttb_colas yet, so store in metadata
    # or create a separate results table. For now, just report — the JSON output
    # is the bridge file that the merge engine will use.
    #
    # TODO: Add barcode column to source_ttb_colas, or use external_ids table
    # during canonical promotion.

    batch_size = 100
    updated = 0
    for i in range(0, len(upc_records), batch_size):
        batch = upc_records[i:i + batch_size]
        for rec in batch:
            try:
                # Store first UPC as the primary barcode
                sb.table("source_ttb_colas").update({
                    "barcode": rec["upcs"][0],
                }).eq("ttb_id", rec["ttb_id"]).execute()
                updated += 1
            except Exception as e:
                # barcode column may not exist yet
                if "barcode" in str(e):
                    print(f"  Column 'barcode' not found on source_ttb_colas. Skipping DB update.")
                    print(f"  Results saved to JSON — use that for merge pipeline.")
                    return
                pass

        print(f"  {min(i + batch_size, len(upc_records)):,}/{len(upc_records):,}", end="\r")

    print(f"\n  Updated {updated:,} records in source_ttb_colas")


if __name__ == "__main__":
    main()
