#!/usr/bin/env python3
"""
TTB COLA Label Image Downloader

Downloads label images from URLs already stored in source_ttb_colas.label_image_urls.
Images are saved locally organized by TTB ID for barcode detection.

Usage:
    python -m pipeline.fetch.ttb_image_downloader                    # download all with URLs
    python -m pipeline.fetch.ttb_image_downloader --year-min 2020    # 2020+ only
    python -m pipeline.fetch.ttb_image_downloader --year-min 2020 --year-max 2026
    python -m pipeline.fetch.ttb_image_downloader --limit 100        # test with 100
    python -m pipeline.fetch.ttb_image_downloader --resume           # skip already downloaded
"""

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

import ssl

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

# ─── Constants ───────────────────────────────────────────────────────────────

DEFAULT_OUTPUT = Path.home() / "Desktop" / "Loam Cowork" / "data" / "images" / "ttb_labels"
CONCURRENT_DOWNLOADS = 20
RATE_LIMIT_PER_SEC = 10  # polite to TTB
REQUEST_TIMEOUT = 30
BATCH_SIZE = 1000  # DB fetch batch size


# ─── DB fetch ────────────────────────────────────────────────────────────────

def fetch_records(year_min: int | None, year_max: int | None, limit: int | None) -> list[dict]:
    """Fetch TTB IDs and image URLs from staging table via RPC SQL."""
    sb = get_supabase()

    where_clauses = [
        "label_image_urls IS NOT NULL",
        "array_length(label_image_urls, 1) > 0",
    ]
    if year_min:
        where_clauses.append(f"completed_date >= '{year_min}-01-01'")
    if year_max:
        where_clauses.append(f"completed_date <= '{year_max}-12-31'")

    where_sql = " AND ".join(where_clauses)
    limit_sql = f"LIMIT {limit}" if limit else ""

    sql = f"""
        SELECT ttb_id, label_image_urls
        FROM source_ttb_colas
        WHERE {where_sql}
        ORDER BY completed_date DESC
        {limit_sql}
    """

    # Query via postgrest year-by-year with quarterly chunks to avoid timeout
    return _fetch_year_by_year(sb, year_min or 1955, year_max or 2026, limit)


def _fetch_year_by_year(sb, year_min: int, year_max: int, limit: int | None) -> list[dict]:
    """Fallback: fetch records year by year with small date ranges to avoid timeout."""
    all_records = []

    for year in range(year_min, year_max + 1):
        # Query quarter by quarter to keep result sets small
        for q_start, q_end in [
            (f"{year}-01-01", f"{year}-03-31"),
            (f"{year}-04-01", f"{year}-06-30"),
            (f"{year}-07-01", f"{year}-09-30"),
            (f"{year}-10-01", f"{year}-12-31"),
        ]:
            offset = 0
            while True:
                try:
                    result = sb.table("source_ttb_colas") \
                        .select("ttb_id, label_image_urls") \
                        .gte("completed_date", q_start) \
                        .lte("completed_date", q_end) \
                        .not_.is_("label_image_urls", "null") \
                        .range(offset, offset + 499) \
                        .execute()
                except Exception as e:
                    print(f"  Error {year} Q{q_start[5:7]}: {e}")
                    break

                if not result.data:
                    break

                batch = [r for r in result.data
                         if r.get("label_image_urls") and len(r["label_image_urls"]) > 0]
                all_records.extend(batch)

                if len(result.data) < 500:
                    break
                offset += 500

        if all_records:
            print(f"  {year}: {len(all_records):,} total so far")

        if limit and len(all_records) >= limit:
            all_records = all_records[:limit]
            break

    return all_records


# ─── Image download ──────────────────────────────────────────────────────────

def get_image_path(output_dir: Path, ttb_id: str, url: str, index: int) -> Path:
    """Determine output path for an image."""
    # Organize by TTB ID prefix (first 5 chars) for filesystem sanity
    prefix = ttb_id[:5]
    ttb_dir = output_dir / prefix / ttb_id

    # Determine extension from URL or default to .jpg
    if ".JPG" in url or ".jpg" in url:
        ext = ".jpg"
    elif ".PNG" in url or ".png" in url:
        ext = ".png"
    elif ".GIF" in url or ".gif" in url:
        ext = ".gif"
    elif "publicViewImage" in url:
        ext = ".jpg"  # TTB's composite images are JPEGs
    else:
        ext = ".jpg"

    filename = f"label_{index}{ext}"
    return ttb_dir / filename


async def download_image(
    session: aiohttp.ClientSession,
    url: str,
    output_path: Path,
    semaphore: asyncio.Semaphore,
    rate_limiter: asyncio.Semaphore,
) -> tuple[bool, str]:
    """Download a single image. Returns (success, error_message)."""
    async with semaphore:
        # Rate limiting
        async with rate_limiter:
            await asyncio.sleep(1.0 / RATE_LIMIT_PER_SEC)

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"

                content = await resp.read()

                # Skip tiny responses (error pages, empty images)
                if len(content) < 500:
                    return False, f"Too small ({len(content)} bytes)"

                # Create directory and write
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(content)
                return True, ""

        except asyncio.TimeoutError:
            return False, "Timeout"
        except aiohttp.ClientError as e:
            return False, str(e)[:100]
        except Exception as e:
            return False, str(e)[:100]


async def download_all(records: list[dict], output_dir: Path, resume: bool,
                       concurrent: int = CONCURRENT_DOWNLOADS,
                       rate_limit: int = RATE_LIMIT_PER_SEC):
    """Download all images with concurrency control."""
    # Build download tasks
    tasks = []
    skipped_existing = 0

    for rec in records:
        ttb_id = rec["ttb_id"]
        urls = rec.get("label_image_urls") or []

        for i, url in enumerate(urls):
            output_path = get_image_path(output_dir, ttb_id, url, i)

            # Resume support: skip if file exists and is non-empty
            if resume and output_path.exists() and output_path.stat().st_size > 500:
                skipped_existing += 1
                continue

            tasks.append((ttb_id, url, output_path))

    print(f"\n  Total images to download: {len(tasks):,}")
    if skipped_existing:
        print(f"  Skipped (already downloaded): {skipped_existing:,}")

    if not tasks:
        print("  Nothing to download!")
        return

    # Estimate time
    est_seconds = len(tasks) / rate_limit
    est_minutes = est_seconds / 60
    print(f"  Estimated time: {est_minutes:.0f} minutes at {rate_limit}/sec")
    print()

    # Download with progress
    semaphore = asyncio.Semaphore(concurrent)
    rate_limiter = asyncio.Semaphore(rate_limit)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "image/*,*/*",
    }

    # TTB uses a government CA that Python doesn't trust by default
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=concurrent)

    success_count = 0
    error_count = 0
    total_bytes = 0
    start_time = time.time()

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        # Process in batches for progress reporting
        batch_size = 200
        for batch_start in range(0, len(tasks), batch_size):
            batch = tasks[batch_start:batch_start + batch_size]

            coros = [
                download_image(session, url, path, semaphore, rate_limiter)
                for (_, url, path) in batch
            ]
            results = await asyncio.gather(*coros)

            for (ttb_id, url, path), (ok, err) in zip(batch, results):
                if ok:
                    success_count += 1
                    total_bytes += path.stat().st_size
                else:
                    error_count += 1

            elapsed = time.time() - start_time
            done = batch_start + len(batch)
            rate = done / elapsed if elapsed > 0 else 0
            remaining = (len(tasks) - done) / rate if rate > 0 else 0

            print(
                f"  [{done:,}/{len(tasks):,}] "
                f"OK {success_count:,}  ERR {error_count:,}  "
                f"{rate:.1f}/sec  "
                f"~{remaining / 60:.0f}m remaining  "
                f"{total_bytes / 1024 / 1024:.0f} MB",
                end="\r",
            )

    elapsed = time.time() - start_time
    print(f"\n\n=== COMPLETE ===")
    print(f"  Downloaded: {success_count:,}")
    print(f"  Errors: {error_count:,}")
    print(f"  Total size: {total_bytes / 1024 / 1024:.1f} MB")
    print(f"  Time: {elapsed / 60:.1f} minutes")
    print(f"  Rate: {success_count / elapsed:.1f} images/sec")
    print(f"  Output: {output_dir}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Download TTB COLA label images")
    parser.add_argument("--year-min", type=int, help="Minimum completed_date year")
    parser.add_argument("--year-max", type=int, help="Maximum completed_date year")
    parser.add_argument("--limit", type=int, help="Max records to process")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--resume", action="store_true", help="Skip already-downloaded images")
    parser.add_argument("--concurrent", type=int, default=CONCURRENT_DOWNLOADS)
    parser.add_argument("--rate", type=int, default=RATE_LIMIT_PER_SEC)
    args = parser.parse_args()

    concurrent = args.concurrent
    rate = args.rate

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=== TTB COLA Label Image Downloader ===")
    print(f"  Output: {output_dir}")
    print(f"  Year range: {args.year_min or 'all'} - {args.year_max or 'all'}")
    print(f"  Concurrency: {concurrent}, Rate: {rate}/sec")
    print()

    print("Fetching records from DB...")
    records = fetch_records(args.year_min, args.year_max, args.limit)
    print(f"  Found {len(records):,} records with image URLs")

    total_urls = sum(len(r.get("label_image_urls") or []) for r in records)
    print(f"  Total image URLs: {total_urls:,}")

    asyncio.run(download_all(records, output_dir, args.resume,
                             concurrent=concurrent, rate_limit=rate))


if __name__ == "__main__":
    main()
