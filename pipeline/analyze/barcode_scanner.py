#!/usr/bin/env python3
"""
TTB Label Barcode Scanner

Scans downloaded TTB COLA label images for barcodes (UPC/EAN/QR).
Extracts UPC/EAN product codes and writes results to JSON.

Processes year-by-year to minimize memory usage and support resume.
Saves incremental results after each year so crashes don't lose progress.

Usage:
    python -m pipeline.analyze.barcode_scanner --image-dir "D:\\TTB Label Images\\labels"
    python -m pipeline.analyze.barcode_scanner --image-dir "D:\\TTB Label Images\\labels" --workers 12
    python -m pipeline.analyze.barcode_scanner --image-dir "D:\\TTB Label Images\\labels" --years 2024,2025,2026
    python -m pipeline.analyze.barcode_scanner --image-dir "D:\\TTB Label Images\\labels" --limit-per-year 100  # test
"""

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

# Force UTF-8 on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ─── Constants ───────────────────────────────────────────────────────────────

DEFAULT_IMAGE_DIR = Path(r"D:\TTB Label Images\labels")
DEFAULT_OUTPUT = Path(__file__).resolve().parents[2] / "data" / "imports" / "ttb_barcode_results.json"

UPC_TYPES = {"EAN13", "EAN8", "UPCA", "UPCE"}
QR_TYPES = {"QRCode", "DataMatrix"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"}

BATCH_SIZE = 200  # images per worker batch


# ─── Scanner (runs in worker process) ────────────────────────────────────────

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

            codes.append({"format": fmt, "type": code_type, "value": text})

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


# ─── Image Discovery (streams year-by-year) ──────────────────────────────────

def list_years(image_dir: Path) -> list[str]:
    """List year subdirectories in the image dir."""
    if not image_dir.exists():
        return []
    return sorted([d.name for d in image_dir.iterdir() if d.is_dir() and d.name.isdigit()])


def find_images_in_year(image_dir: Path, year: str) -> list[tuple[str, str]]:
    """Find all images in one year dir. Returns (ttb_id, path) tuples."""
    year_dir = image_dir / year
    if not year_dir.exists():
        return []

    results = []
    for cola_dir in year_dir.iterdir():
        try:
            if not cola_dir.is_dir():
                continue
            ttb_id = cola_dir.name
            for img_file in cola_dir.iterdir():
                if img_file.suffix.lower() in IMAGE_EXTS:
                    results.append((ttb_id, str(img_file)))
        except OSError:
            # Skip directories that fail to read (USB hiccups)
            continue
    return results


# ─── Result Aggregation ──────────────────────────────────────────────────────

def init_ttb_record(ttb_id: str) -> dict:
    return {
        "ttb_id": ttb_id,
        "upcs": [],
        "qr_codes": [],
        "code39": [],
        "other": [],
        "image_count": 0,
    }


def merge_codes_into_record(rec: dict, codes: list[dict]) -> tuple[bool, bool, bool, int]:
    """Merge scanned codes into a TTB record. Returns (has_upc, has_qr, has_code39, errors)."""
    rec["image_count"] += 1
    has_upc = has_qr = has_code39 = False
    errors = 0

    for code in codes:
        ctype = code["type"]
        value = code["value"]

        if ctype == "error":
            errors += 1
        elif ctype == "upc":
            if value not in rec["upcs"]:
                rec["upcs"].append(value)
            has_upc = True
        elif ctype == "qr":
            if value not in rec["qr_codes"]:
                rec["qr_codes"].append(value)
            has_qr = True
        elif ctype == "code39":
            if value not in rec["code39"]:
                rec["code39"].append(value)
            has_code39 = True
        else:
            rec["other"].append(code)

    return has_upc, has_qr, has_code39, errors


# ─── Incremental Save + Resume ───────────────────────────────────────────────

def load_existing_results(output_path: Path) -> dict:
    """Load existing results for resume. Returns {} if no file."""
    if not output_path.exists():
        return {}
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        by_ttb = {r["ttb_id"]: r for r in data.get("results", [])}
        return by_ttb
    except Exception as e:
        print(f"  WARN: failed to load existing results: {e}")
        return {}


def save_results(output_path: Path, by_ttb: dict, image_dir: Path, years_done: list[str]):
    """Save results to JSON. Atomic write via temp file."""
    results = sorted(by_ttb.values(), key=lambda r: r["ttb_id"])
    ttb_with_upc = sum(1 for r in results if r["upcs"])
    ttb_with_qr = sum(1 for r in results if r["qr_codes"])
    unique_upcs = set()
    for r in results:
        unique_upcs.update(r["upcs"])

    output_data = {
        "metadata": {
            "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "image_dir": str(image_dir),
            "years_completed": years_done,
            "unique_ttb_ids": len(results),
            "ttb_with_upc": ttb_with_upc,
            "ttb_with_qr": ttb_with_qr,
            "unique_upcs": len(unique_upcs),
        },
        "results": results,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic write: temp file + rename
    tmp_path = output_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False)
    tmp_path.replace(output_path)


# ─── Logging ─────────────────────────────────────────────────────────────────

def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("barcode_scanner")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)
    return logger


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scan TTB label images for barcodes")
    parser.add_argument("--image-dir", type=str, default=str(DEFAULT_IMAGE_DIR))
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 2),
                        help="Number of parallel workers (default: CPU count - 2)")
    parser.add_argument("--years", type=str, default=None,
                        help="Comma-separated years to scan (e.g. 2024,2025,2026)")
    parser.add_argument("--limit-per-year", type=int, default=None,
                        help="Max images per year (for testing)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Start fresh, ignore existing output file")
    args = parser.parse_args()

    image_dir = Path(args.image_dir)
    output_path = Path(args.output)
    log_path = output_path.parent / "barcode_scan.log"

    logger = setup_logging(log_path)
    logger.info("=" * 60)
    logger.info("TTB Label Barcode Scanner started")
    logger.info(f"  Image dir: {image_dir}")
    logger.info(f"  Output: {output_path}")
    logger.info(f"  Log: {log_path}")
    logger.info(f"  Workers: {args.workers}")

    print("=== TTB Label Barcode Scanner ===")
    print(f"  Image dir: {image_dir}")
    print(f"  Output: {output_path}")
    print(f"  Log: {log_path}")
    print(f"  Workers: {args.workers}")
    print()

    if not image_dir.exists():
        print(f"ERROR: image dir does not exist: {image_dir}")
        logger.error(f"Image dir does not exist: {image_dir}")
        return

    # Determine which years to process
    all_years = list_years(image_dir)
    print(f"  Years available: {all_years}")

    if args.years:
        target_years = [y.strip() for y in args.years.split(",")]
    else:
        target_years = all_years

    # Load existing results for resume
    by_ttb = {} if args.no_resume else load_existing_results(output_path)
    existing_ttbs = set(by_ttb.keys())
    print(f"  Resume: {len(existing_ttbs):,} COLAs already processed")
    logger.info(f"Resume: {len(existing_ttbs):,} COLAs already processed")

    years_done = []
    overall_start = time.time()
    total_scanned = 0
    total_upc = 0
    total_qr = 0
    total_errors = 0

    try:
        for year in target_years:
            year_start = time.time()
            print(f"\n--- Year {year} ---")
            logger.info(f"Starting year {year}")

            print(f"  Listing images...")
            images = find_images_in_year(image_dir, year)
            print(f"  Found {len(images):,} images in {len(set(ttb for ttb, _ in images)):,} COLAs")
            logger.info(f"Year {year}: {len(images):,} images")

            if args.limit_per_year:
                images = images[:args.limit_per_year]
                print(f"  Limited to {len(images):,}")

            # Filter out already-processed COLAs (resume)
            images = [(ttb, path) for ttb, path in images if ttb not in existing_ttbs]
            print(f"  To scan: {len(images):,} (after resume filter)")

            if not images:
                years_done.append(year)
                continue

            # Build batches
            batches = [images[i:i + BATCH_SIZE] for i in range(0, len(images), BATCH_SIZE)]

            year_scanned = 0
            year_upc = 0

            with ProcessPoolExecutor(max_workers=args.workers) as pool:
                futures = {pool.submit(scan_batch, b): i for i, b in enumerate(batches)}

                for future in as_completed(futures):
                    try:
                        results = future.result()
                    except Exception as e:
                        logger.error(f"Batch failed: {e}")
                        continue

                    for ttb_id, img_path, codes in results:
                        if ttb_id not in by_ttb:
                            by_ttb[ttb_id] = init_ttb_record(ttb_id)

                        has_upc, has_qr, has_code39, errs = merge_codes_into_record(by_ttb[ttb_id], codes)
                        year_scanned += 1
                        total_scanned += 1
                        total_errors += errs

                        if has_upc and len(by_ttb[ttb_id]["upcs"]) == 1 and by_ttb[ttb_id]["image_count"] <= 1:
                            # Count first time a UPC appears for this TTB
                            year_upc += 1
                            total_upc += 1

                    elapsed = time.time() - year_start
                    rate = year_scanned / elapsed if elapsed > 0 else 0
                    remaining = (len(images) - year_scanned) / rate if rate > 0 else 0
                    print(
                        f"  [{year}] {year_scanned:,}/{len(images):,}  "
                        f"UPC: {year_upc:,}  Err: {total_errors:,}  "
                        f"{rate:.0f} img/s  ~{remaining/60:.0f}min left    ",
                        end="\r",
                    )

            year_elapsed = time.time() - year_start
            print(f"\n  Year {year} done: {year_scanned:,} images in {year_elapsed/60:.1f} min")
            logger.info(
                f"Year {year} complete: scanned={year_scanned:,} new_upcs={year_upc:,} "
                f"time={year_elapsed/60:.1f}min rate={year_scanned/max(1,year_elapsed):.0f} img/s"
            )

            years_done.append(year)

            # Save incrementally after each year
            save_results(output_path, by_ttb, image_dir, years_done)
            print(f"  Saved incremental results ({len(by_ttb):,} TTB records)")
            logger.info(f"Saved: {len(by_ttb):,} TTB records, {sum(1 for r in by_ttb.values() if r['upcs']):,} with UPCs")

    except KeyboardInterrupt:
        print("\n\nInterrupted! Saving current progress...")
        logger.warning("Interrupted by user")
        save_results(output_path, by_ttb, image_dir, years_done)
        print(f"Saved {len(by_ttb):,} records to {output_path}")
        return

    # Final stats
    total_elapsed = time.time() - overall_start
    ttb_with_upc = sum(1 for r in by_ttb.values() if r["upcs"])
    ttb_with_qr = sum(1 for r in by_ttb.values() if r["qr_codes"])
    unique_upcs = set()
    for r in by_ttb.values():
        unique_upcs.update(r["upcs"])

    print(f"\n\n=== RESULTS ===")
    print(f"  Total TTB COLAs: {len(by_ttb):,}")
    print(f"  Images scanned this run: {total_scanned:,}")
    print(f"  Time this run: {total_elapsed/60:.1f} min")
    if total_elapsed > 0:
        print(f"  Rate: {total_scanned/total_elapsed:.0f} img/sec")
    print()
    print(f"  COLAs with UPC/EAN: {ttb_with_upc:,} ({ttb_with_upc / max(1,len(by_ttb)) * 100:.1f}%)")
    print(f"  COLAs with QR: {ttb_with_qr:,} ({ttb_with_qr / max(1,len(by_ttb)) * 100:.1f}%)")
    print(f"  Unique UPCs: {len(unique_upcs):,}")
    print(f"  Total errors: {total_errors:,}")

    upc_lengths: dict[int, int] = {}
    for upc in unique_upcs:
        upc_lengths[len(upc)] = upc_lengths.get(len(upc), 0) + 1
    if upc_lengths:
        print(f"\n  UPC length distribution:")
        for length in sorted(upc_lengths.keys()):
            print(f"    {length} digits: {upc_lengths[length]:,}")

    logger.info(
        f"Run complete: scanned={total_scanned:,} ttbs={len(by_ttb):,} "
        f"with_upc={ttb_with_upc:,} unique_upcs={len(unique_upcs):,} "
        f"time={total_elapsed/60:.1f}min"
    )
    print(f"\n  Output: {output_path}")
    print(f"  Log: {log_path}")


if __name__ == "__main__":
    # Required for Windows multiprocessing
    from multiprocessing import freeze_support
    freeze_support()
    main()
