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
    python -m pipeline.analyze.barcode_scanner --workers 12            # custom worker count
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ─── Constants ───────────────────────────────────────────────────────────────

DEFAULT_IMAGE_DIR = Path.home() / "Desktop" / "Label Images" / "labels"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[2] / "data" / "imports" / "ttb_barcode_results.json"

# Barcode types we care about for product identification
UPC_TYPES = {"EAN13", "EAN8", "UPCA", "UPCE"}
QR_TYPES = {"QRCode", "DataMatrix"}


# ─── Scanner ─────────────────────────────────────────────────────────────────

def scan_image(img_path: str) -> list[dict]:
    """Scan a single image for barcodes. Returns list of detected codes."""
    import zxingcpp
    from PIL import Image

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


def scan_batch(batch: list[tuple[str, str]]) -> list[tuple[str, str, list[dict]]]:
    """Scan a batch of images. Worker function for multiprocessing."""
    results = []
    for ttb_id, img_path in batch:
        codes = scan_image(img_path)
        results.append((ttb_id, img_path, codes))
    return results


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
            for img_file in sorted(ttb_dir.glob("*")):
                if img_file.suffix.lower() in (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff"):
                    results.append((ttb_id, str(img_file)))
    return results


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scan TTB label images for barcodes")
    parser.add_argument("--image-dir", type=str, default=str(DEFAULT_IMAGE_DIR))
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--limit", type=int, help="Max images to scan")
    parser.add_argument("--workers", type=int, default=max(1, os.cpu_count() - 2),
                        help="Number of parallel workers (default: CPU count - 2)")
    parser.add_argument("--update-db", action="store_true", help="Write UPC barcodes to source_ttb_colas")
    args = parser.parse_args()

    image_dir = Path(args.image_dir)
    output_path = Path(args.output)

    print("=== TTB Label Barcode Scanner ===")
    print(f"  Image dir: {image_dir}")
    print(f"  Output: {output_path}")
    print(f"  Workers: {args.workers}")
    print()

    # Find all images
    print("Finding images...")
    images = find_all_images(image_dir)
    print(f"  Found {len(images):,} images")

    if args.limit:
        images = images[:args.limit]
        print(f"  Limited to {len(images):,}")

    if not images:
        print("No images found.")
        return

    # Split into batches for workers (256 images per batch)
    batch_size = 256
    batches = []
    for i in range(0, len(images), batch_size):
        batches.append(images[i:i + batch_size])

    print(f"  {len(batches):,} batches of ~{batch_size} images")

    # Scan with multiprocessing
    print(f"\nScanning for barcodes with {args.workers} workers...")
    start_time = time.time()

    by_ttb: dict[str, dict] = {}
    total_scanned = 0
    total_upc = 0
    total_qr = 0
    total_code39 = 0
    total_no_barcode = 0
    errors = 0
    batches_done = 0

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(scan_batch, b): b for b in batches}

        for future in as_completed(futures):
            results = future.result()
            batches_done += 1

            for ttb_id, img_path, codes in results:
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

            elapsed = time.time() - start_time
            rate = total_scanned / elapsed if elapsed > 0 else 0
            remaining = (len(images) - total_scanned) / rate if rate > 0 else 0
            remaining_h = remaining / 3600
            print(
                f"  [{total_scanned:,}/{len(images):,}] "
                f"UPC: {total_upc:,}  QR: {total_qr:,}  Err: {errors:,}  "
                f"{rate:.0f} img/s  ~{remaining_h:.1f}h left   ",
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
    print(f"  Workers: {args.workers}")
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

    batch_size = 100
    updated = 0
    for i in range(0, len(upc_records), batch_size):
        batch = upc_records[i:i + batch_size]
        for rec in batch:
            try:
                sb.table("source_ttb_colas").update({
                    "barcode": rec["upcs"][0],
                }).eq("ttb_id", rec["ttb_id"]).execute()
                updated += 1
            except Exception as e:
                if "barcode" in str(e):
                    print(f"  Column 'barcode' not found on source_ttb_colas. Skipping DB update.")
                    print(f"  Results saved to JSON — use that for merge pipeline.")
                    return

        print(f"  {min(i + batch_size, len(upc_records)):,}/{len(upc_records):,}", end="\r")

    print(f"\n  Updated {updated:,} records in source_ttb_colas")


if __name__ == "__main__":
    main()
