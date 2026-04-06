"""
NASA POWER Historical Weather bulk fetcher for Loam.

Uses NASA's POWER API (free, no auth, no rate limits) to fetch 1981-2025
daily data in a single request per location. Resolution ~50km (MERRA-2).

Higher-resolution Open-Meteo data overwrites this when fetched via the drip
pipeline (ON CONFLICT upsert on appellation_id+year).

Variables: T2M_MAX, T2M_MIN, T2M, PRECTOTCORR, ALLSKY_SFC_SW_DWN, EVPTRNS, RH2M
Missing vs Open-Meteo: no sunshine_duration (sunshine_hours stored as NULL)
ET0: EVPTRNS (MJ/m2/day) converted to mm/day via /2.45

Usage:
    python -m pipeline.fetch.nasa_power_weather                     # All appellations
    python -m pipeline.fetch.nasa_power_weather --test              # First 10
    python -m pipeline.fetch.nasa_power_weather --limit 50
    python -m pipeline.fetch.nasa_power_weather --id <uuid>
    python -m pipeline.fetch.nasa_power_weather --no-resume
    python -m pipeline.fetch.nasa_power_weather --delay 1.5         # Seconds between calls
"""

import argparse
import time
from datetime import date, datetime

import requests

from pipeline.lib.db import get_conn
from pipeline.fetch.open_meteo_weather import (
    process_appellation, save_yearly, save_monthly,
    get_appellations, get_fetched_ids,
    END_YEAR,
)

# ── Constants ────────────────────────────────────────────────────────────────

NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

POWER_PARAMS = ",".join([
    "T2M_MAX", "T2M_MIN", "T2M",
    "PRECTOTCORR",
    "ALLSKY_SFC_SW_DWN",
    "EVPTRNS",
    "RH2M",
])

POWER_START = 19810101   # NASA POWER meteorological data begins 1981
POWER_END = 20251231

FILL_VALUE = -999.0      # NASA POWER's missing data sentinel


# ── API ──────────────────────────────────────────────────────────────────────

def fetch_power_daily(lat: float, lon: float, retries: int = 3) -> dict | None:
    """Fetch daily weather from NASA POWER for 1981-2025 in one call."""
    params = {
        "parameters": POWER_PARAMS,
        "community": "AG",
        "longitude": round(lon, 4),
        "latitude": round(lat, 4),
        "start": POWER_START,
        "end": POWER_END,
        "format": "JSON",
    }
    for attempt in range(retries):
        try:
            resp = requests.get(NASA_POWER_URL, params=params, timeout=180)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"429({wait}s) ", end="", flush=True)
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = 15 * (attempt + 1)
                print(f"{resp.status_code}({wait}s) ", end="", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
            else:
                print(f"API error after {retries} attempts: {e}")
                return None
    return None


def parse_power_daily(data: dict) -> list[dict]:
    """Parse NASA POWER JSON into daily dicts matching Open-Meteo pipeline format.

    Maps NASA POWER fields to the same dict keys used by compute_yearly/compute_monthly:
      tmax, tmin, tmean, precip, sun_sec (None), rad, et0 (converted), rh
    """
    params = data["properties"]["parameter"]

    tmax_d = params["T2M_MAX"]
    tmin_d = params["T2M_MIN"]
    tmean_d = params["T2M"]
    precip_d = params["PRECTOTCORR"]
    rad_d = params["ALLSKY_SFC_SW_DWN"]
    evp_d = params["EVPTRNS"]
    rh_d = params["RH2M"]

    def clean(val):
        return None if val is None or val == FILL_VALUE else val

    days = []
    for date_str in sorted(tmax_d.keys()):
        y, m, d_num = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
        try:
            dt = date(y, m, d_num)
        except ValueError:
            continue  # skip leap-second artifacts or bad dates

        tx = clean(tmax_d.get(date_str))
        tn = clean(tmin_d.get(date_str))
        tm = clean(tmean_d.get(date_str))
        pr = clean(precip_d.get(date_str))
        rd = clean(rad_d.get(date_str))
        ev = clean(evp_d.get(date_str))
        hu = clean(rh_d.get(date_str))

        # Convert EVPTRNS energy flux (MJ/m2/day) to ET0 (mm/day)
        et0 = round(ev / 2.45, 2) if ev is not None and ev > 0 else None

        days.append({
            "date": dt,
            "tmax": tx,
            "tmin": tn,
            "tmean": tm,
            "precip": pr,
            "sun_sec": None,   # NASA POWER lacks sunshine duration
            "rad": rd,
            "et0": et0,
            "rh": hu,
        })

    return days


# ── Coordinate cache ─────────────────────────────────────────────────────────

def coord_key(lat: float, lon: float) -> str:
    """Round to 1dp (~11km) for cache key — within POWER's ~50km grid."""
    return f"{round(lat, 1)},{round(lon, 1)}"


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="NASA POWER bulk weather fetch")
    parser.add_argument("--test", action="store_true", help="Process 10 test appellations")
    parser.add_argument("--limit", type=int, help="Max appellations to process")
    parser.add_argument("--id", type=str, help="Single appellation UUID")
    parser.add_argument("--no-resume", action="store_true", help="Re-fetch even if data exists")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Seconds between API calls (default 1)")
    args = parser.parse_args()

    conn = get_conn()

    limit = 10 if args.test else args.limit
    apps = get_appellations(conn, appellation_id=args.id, limit=limit)
    print(f"Found {len(apps)} appellations with coordinates + season metadata")

    # Resume: skip already-fetched
    if not args.no_resume and not args.id:
        fetched = get_fetched_ids(conn)
        before = len(apps)
        apps = [a for a in apps if str(a["id"]) not in fetched]
        skipped = before - len(apps)
        if skipped:
            print(f"Skipping {skipped} already fetched (resume mode)")

    # Count unique coordinates at 1dp
    unique_coords = len({coord_key(float(a["latitude"]), float(a["longitude"])) for a in apps})
    print(f"Processing {len(apps)} appellations ({unique_coords} unique 1dp coords) x 1981-{END_YEAR}")
    print()

    total_y = 0
    total_m = 0
    errors = 0
    api_calls = 0
    cache_hits = 0
    t0 = time.time()

    daily_cache: dict[str, list[dict]] = {}

    for i, app in enumerate(apps):
        name = app["name"]
        lat, lon = float(app["latitude"]), float(app["longitude"])
        ck = coord_key(lat, lon)
        safe_name = name.encode("ascii", errors="replace").decode("ascii")
        print(f"[{i+1}/{len(apps)}] {safe_name} ({lat:.2f}, {lon:.2f}) ", end="", flush=True)

        if ck in daily_cache:
            all_days = daily_cache[ck]
            cache_hits += 1
            print("(cached) ", end="", flush=True)
        else:
            print("... ", end="", flush=True)
            data = fetch_power_daily(lat, lon)
            api_calls += 1
            if data is None:
                print("API ERROR")
                errors += 1
                continue
            try:
                all_days = parse_power_daily(data)
            except (KeyError, TypeError) as e:
                print(f"PARSE ERROR: {e}")
                errors += 1
                continue
            daily_cache[ck] = all_days
            if i < len(apps) - 1:
                time.sleep(args.delay)

        yearly, monthly = process_appellation(app, all_days)

        # Patch fields NASA POWER can't provide
        for row in yearly:
            row["sunshine_hours"] = None
            row["data_source"] = "nasa-power"

        # Patch monthly sunshine_hours (would be 0.0 from sum of None → 0)
        for row in monthly:
            row["sunshine_hours"] = None

        if yearly:
            save_yearly(conn, yearly)
        if monthly:
            save_monthly(conn, monthly)

        total_y += len(yearly)
        total_m += len(monthly)
        print(f"{len(yearly)} years, {len(monthly)} months")

    elapsed = time.time() - t0
    print()
    print(f"Done in {elapsed:.0f}s.")
    print(f"  {total_y:,} yearly rows, {total_m:,} monthly rows")
    print(f"  {api_calls} API calls, {cache_hits} cache hits, {errors} errors")

    if api_calls > 0:
        avg = elapsed / api_calls
        remaining = unique_coords - api_calls - cache_hits
        if remaining > 0:
            print(f"  Avg {avg:.1f}s/call — est {remaining * avg / 60:.0f} min remaining")

    conn.close()


if __name__ == "__main__":
    main()
