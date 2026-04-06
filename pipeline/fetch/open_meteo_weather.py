"""
Open-Meteo Historical Weather fetcher for Loam.

Fetches daily weather data from Open-Meteo's Historical API and computes:
- Yearly growing-season metrics → appellation_weather_years
- Monthly rollups → appellation_weather_months

All metrics derived from the same daily API response. One call per appellation
covers 1980-2025 (46 years of daily data).

Growing season: computed from 10°C 5-day rolling mean threshold.
Harvest metrics: convention-based from appellation's harvest_start/end_month.

Usage:
    python -m pipeline.fetch.open_meteo_weather                     # All appellations
    python -m pipeline.fetch.open_meteo_weather --test              # First 10 only
    python -m pipeline.fetch.open_meteo_weather --limit 50          # First 50
    python -m pipeline.fetch.open_meteo_weather --id <uuid>         # Single appellation
    python -m pipeline.fetch.open_meteo_weather --no-resume         # Re-fetch all
"""

import argparse
import sys
import time
from datetime import date, datetime, timedelta

import requests
import psycopg2
import psycopg2.extras

from pipeline.lib.db import get_conn

# ── Constants ────────────────────────────────────────────────────────────────

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

DAILY_VARS = ",".join([
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "sunshine_duration",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
    "relative_humidity_2m_mean",
])

START_YEAR = 1980
END_YEAR = 2025

GS_TEMP_THRESHOLD = 10.0    # °C — Vitis vinifera biological zero
ROLLING_WINDOW = 5           # days for rolling mean
CONSECUTIVE_DAYS = 5         # days above threshold to confirm season start
HEAT_SPIKE_C = 35.0          # °C — vine shutdown threshold
FROST_C = 0.0                # °C
RAIN_DAY_MM = 1.0            # mm — threshold for "rain day"
HUMID_PCT = 85.0             # % RH — threshold for "humid day"


# ── API ──────────────────────────────────────────────────────────────────────

def fetch_daily(lat: float, lon: float, retries: int = 5) -> dict | None:
    """Fetch daily weather from Open-Meteo for full date range (1980-2025)."""
    params = {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        "start_date": f"{START_YEAR}-01-01",
        "end_date": f"{END_YEAR}-12-31",
        "daily": DAILY_VARS,
        "timezone": "GMT",
    }
    for attempt in range(retries):
        try:
            resp = requests.get(OPEN_METEO_URL, params=params, timeout=120)
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"rate limited, waiting {wait}s ... ", end="", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                print(f"API error after {retries} attempts: {e}")
                return None
    return None


def parse_daily(data: dict) -> list[dict]:
    """Parse Open-Meteo JSON into list of daily dicts."""
    d = data["daily"]
    dates = d["time"]
    return [
        {
            "date": date.fromisoformat(dates[i]),
            "tmax": d["temperature_2m_max"][i],
            "tmin": d["temperature_2m_min"][i],
            "tmean": d["temperature_2m_mean"][i],
            "precip": d["precipitation_sum"][i],
            "sun_sec": d["sunshine_duration"][i],
            "rad": d["shortwave_radiation_sum"][i],
            "et0": d["et0_fao_evapotranspiration"][i],
            "rh": d["relative_humidity_2m_mean"][i],
        }
        for i in range(len(dates))
    ]


# ── Growing season detection ────────────────────────────────────────────────

def rolling_mean(values: list, window: int = ROLLING_WINDOW) -> list:
    """Centered rolling mean, skipping None values."""
    n = len(values)
    half = window // 2
    result = []
    for i in range(n):
        start = max(0, i - half)
        end = min(n, i + half + 1)
        vals = [v for v in values[start:end] if v is not None]
        result.append(sum(vals) / len(vals) if vals else None)
    return result


def compute_growing_season(days: list[dict], hemisphere: str,
                           gs_start_month: int, gs_end_month: int,
                           year: int) -> tuple[date | None, date | None]:
    """Find growing season start/end using 10°C rolling mean threshold.

    Start: first 5-consecutive-day run where rolling mean >= 10°C.
    End: last date in the search window where rolling mean >= 10°C.
    """
    if not days:
        return None, None

    rm = rolling_mean([d["tmean"] for d in days])
    dates = [d["date"] for d in days]

    # Search window: 2 months before expected start through 2 months after expected end
    if hemisphere == "south" and gs_start_month > gs_end_month:
        s_start = date(year - 1, max(1, gs_start_month - 2), 1)
        s_end = date(year, min(12, gs_end_month + 2), 28)
    else:
        s_start = date(year, max(1, gs_start_month - 2), 1)
        s_end = date(year, min(12, gs_end_month + 2), 28)

    # Find start: first CONSECUTIVE_DAYS run above threshold
    gs_start = None
    run = 0
    for i, d in enumerate(dates):
        if d < s_start or d > s_end:
            run = 0
            continue
        if rm[i] is not None and rm[i] >= GS_TEMP_THRESHOLD:
            run += 1
            if run >= CONSECUTIVE_DAYS and gs_start is None:
                gs_start = dates[i - CONSECUTIVE_DAYS + 1]
        else:
            run = 0

    if gs_start is None:
        return None, None

    # Find end: last date where rolling mean still >= threshold
    gs_end = gs_start
    for i, d in enumerate(dates):
        if d < gs_start or d > s_end:
            continue
        if rm[i] is not None and rm[i] >= GS_TEMP_THRESHOLD:
            gs_end = d

    return gs_start, gs_end


# ── Huglin coefficient ───────────────────────────────────────────────────────

def huglin_k(lat: float) -> float:
    """Day-length coefficient K for the Huglin heliothermal index."""
    a = abs(lat)
    if a <= 40: return 1.00
    if a <= 42: return 1.02
    if a <= 44: return 1.03
    if a <= 46: return 1.04
    if a <= 48: return 1.05
    return 1.06


# ── Yearly metrics ───────────────────────────────────────────────────────────

def compute_yearly(days: list[dict], gs_start: date, gs_end: date,
                   lat: float, hemisphere: str, year: int,
                   gs_start_month: int, h_start: int, h_end: int) -> dict | None:
    """Compute all yearly weather metrics for one appellation-year."""

    gs = [d for d in days if gs_start <= d["date"] <= gs_end]
    if not gs:
        return None

    def safe_avg(vals):
        clean = [v for v in vals if v is not None]
        return sum(clean) / len(clean) if clean else None

    # ── Temperature / Ripening ──

    gdd = sum(
        max(0, (d["tmax"] + d["tmin"]) / 2 - 10)
        for d in gs if d["tmax"] is not None and d["tmin"] is not None
    )

    # Huglin Index (fixed period: Apr 1 – Sep 30 NH, Oct 1 – Mar 31 SH)
    k = huglin_k(lat)
    if hemisphere == "south":
        hug_days = [d for d in days
                    if date(year - 1, 10, 1) <= d["date"] <= date(year, 3, 31)]
    else:
        hug_days = [d for d in days
                    if date(year, 4, 1) <= d["date"] <= date(year, 9, 30)]
    huglin = sum(
        max(0, ((d["tmean"] or 0) + (d["tmax"] or 0)) / 2 - 10) * k
        for d in hug_days if d["tmean"] is not None and d["tmax"] is not None
    )

    avg_gs_temp = safe_avg([d["tmean"] for d in gs])
    all_tmax = [d["tmax"] for d in days if d["tmax"] is not None]
    max_temp = max(all_tmax) if all_tmax else None
    heat_spikes = sum(1 for d in gs if d["tmax"] is not None and d["tmax"] > HEAT_SPIKE_C)

    diurnals = [d["tmax"] - d["tmin"] for d in gs
                if d["tmax"] is not None and d["tmin"] is not None]
    avg_diurnal = sum(diurnals) / len(diurnals) if diurnals else None

    # ── Water ──

    gs_rain = sum(d["precip"] or 0 for d in gs)

    # Dormant rainfall: off-season before this growing season
    if hemisphere == "south" and gs_start_month > 6:
        dorm = [d for d in days
                if date(year - 1, 5, 1) <= d["date"] <= date(year - 1, 9, 30)]
    else:
        dorm_s = date(year - 1, 11, 1) if year > START_YEAR else date(year, 1, 1)
        dorm = [d for d in days if dorm_s <= d["date"] <= date(year, 3, 31)]
    dormant_rain = sum(d["precip"] or 0 for d in dorm)

    rain_days = sum(1 for d in gs if (d["precip"] or 0) > RAIN_DAY_MM)
    et0 = sum(d["et0"] or 0 for d in gs)
    water_balance = gs_rain - et0

    # ── Frost ──

    cutoff = gs_start - timedelta(days=90)
    pre_gs = [d for d in days if cutoff <= d["date"] < gs_start]
    spring_frosts = sum(1 for d in pre_gs
                        if d["tmin"] is not None and d["tmin"] < FROST_C)

    last_frost = None
    for d in sorted(pre_gs, key=lambda x: x["date"], reverse=True):
        if d["tmin"] is not None and d["tmin"] < FROST_C:
            last_frost = d["date"]
            break

    # ── Sunlight ──

    sunshine = sum((d["sun_sec"] or 0) / 3600 for d in gs)
    radiation = sum(d["rad"] or 0 for d in gs)

    # ── Humidity ──

    avg_rh = safe_avg([d["rh"] for d in gs])
    humid_days = sum(1 for d in gs if d["rh"] is not None and d["rh"] > HUMID_PCT)

    # ── Harvest metrics (convention-based) ──

    harv = [d for d in days
            if d["date"].year == year and h_start <= d["date"].month <= h_end]
    harv_rain = sum(d["precip"] or 0 for d in harv) if harv else None
    harv_temp = safe_avg([d["tmean"] for d in harv]) if harv else None

    # Cool night index: avg Tmin in last harvest month
    cni_days = [d for d in harv if d["date"].month == h_end]
    cni = safe_avg([d["tmin"] for d in cni_days]) if cni_days else None

    def rnd(v, dp=1):
        return round(v, dp) if v is not None else None

    return {
        "growing_season_start": gs_start,
        "growing_season_end": gs_end,
        "gdd": rnd(gdd),
        "huglin_index": rnd(huglin),
        "avg_growing_season_temp_c": rnd(avg_gs_temp, 2),
        "max_temp_c": rnd(max_temp),
        "heat_spike_days": heat_spikes,
        "avg_diurnal_range_c": rnd(avg_diurnal, 2),
        "growing_season_rainfall_mm": rnd(gs_rain),
        "dormant_rainfall_mm": rnd(dormant_rain),
        "rain_days": rain_days,
        "et0_mm": rnd(et0),
        "water_balance_mm": rnd(water_balance),
        "spring_frost_days": spring_frosts,
        "last_frost_date": last_frost,
        "sunshine_hours": rnd(sunshine),
        "solar_radiation_mj_m2": rnd(radiation),
        "avg_relative_humidity_pct": rnd(avg_rh),
        "humid_days": humid_days,
        "harvest_rainfall_mm": rnd(harv_rain),
        "harvest_avg_temp_c": rnd(harv_temp, 2),
        "cool_night_index_c": rnd(cni, 2),
        "harvest_method": "latitude_convention",
    }


# ── Monthly rollups ──────────────────────────────────────────────────────────

def compute_monthly(days: list[dict], year: int) -> list[dict]:
    """Compute monthly weather rollups for one calendar year (12 rows)."""
    months = []
    for m in range(1, 13):
        md = [d for d in days if d["date"].year == year and d["date"].month == m]
        if not md:
            months.append({"month": m})
            continue

        def sa(vals):
            clean = [v for v in vals if v is not None]
            return round(sum(clean) / len(clean), 2) if clean else None

        months.append({
            "month": m,
            "avg_temp_c": sa([d["tmean"] for d in md]),
            "min_temp_c": sa([d["tmin"] for d in md]),
            "max_temp_c": sa([d["tmax"] for d in md]),
            "rainfall_mm": round(sum(d["precip"] or 0 for d in md), 1),
            "rain_days": sum(1 for d in md if (d["precip"] or 0) > RAIN_DAY_MM),
            "sunshine_hours": round(sum((d["sun_sec"] or 0) / 3600 for d in md), 1),
            "et0_mm": round(sum(d["et0"] or 0 for d in md), 1),
            "avg_humidity_pct": sa([d["rh"] for d in md]),
        })
    return months


# ── Process one appellation ──────────────────────────────────────────────────

def process_appellation(app: dict, all_days: list[dict]) -> tuple[list[dict], list[dict]]:
    """Process all vintage years for one appellation.

    Returns (yearly_rows, monthly_rows).
    """
    app_id = str(app["id"])
    hemisphere = app["hemisphere"]
    gs_sm = app["growing_season_start_month"]
    gs_em = app["growing_season_end_month"]
    h_start = app["harvest_start_month"]
    h_end = app["harvest_end_month"]
    lat = float(app["latitude"])
    now = datetime.now(tz=None)  # naive UTC-ish timestamp for fetched_at

    # SH needs previous year's data for growing season start
    first_year = START_YEAR + 1 if (hemisphere == "south" and gs_sm > gs_em) else START_YEAR

    yearly_rows = []
    monthly_rows = []

    for yr in range(first_year, END_YEAR + 1):
        # Slice daily data for this vintage year (include dormant/pre-season)
        if hemisphere == "south" and gs_sm > gs_em:
            sl_start = date(yr - 1, 5, 1)
            sl_end = date(yr, 6, 30)
        else:
            sl_start = date(yr - 1, 11, 1) if yr > START_YEAR else date(yr, 1, 1)
            sl_end = date(yr, 12, 31)

        year_days = [d for d in all_days if sl_start <= d["date"] <= sl_end]
        if len(year_days) < 100:
            continue

        # Growing season
        gs_start, gs_end = compute_growing_season(
            year_days, hemisphere, gs_sm, gs_em, yr
        )
        if gs_start is None:
            continue

        # Yearly metrics
        metrics = compute_yearly(
            year_days, gs_start, gs_end, lat, hemisphere, yr, gs_sm, h_start, h_end
        )
        if metrics is None:
            continue

        metrics["appellation_id"] = app_id
        metrics["year"] = yr
        metrics["data_source"] = "open-meteo"
        metrics["fetched_at"] = now
        yearly_rows.append(metrics)

        # Monthly rollups (calendar year)
        for md in compute_monthly(all_days, yr):
            md["appellation_id"] = app_id
            md["year"] = yr
            monthly_rows.append(md)

    return yearly_rows, monthly_rows


# ── Database ─────────────────────────────────────────────────────────────────

YEARLY_COLS = [
    "appellation_id", "year", "growing_season_start", "growing_season_end",
    "gdd", "huglin_index", "avg_growing_season_temp_c", "max_temp_c",
    "heat_spike_days", "avg_diurnal_range_c", "growing_season_rainfall_mm",
    "dormant_rainfall_mm", "rain_days", "et0_mm", "water_balance_mm",
    "spring_frost_days", "last_frost_date", "sunshine_hours", "solar_radiation_mj_m2",
    "avg_relative_humidity_pct", "humid_days", "harvest_rainfall_mm",
    "harvest_avg_temp_c", "cool_night_index_c", "harvest_method",
    "data_source", "fetched_at",
]

MONTHLY_COLS = [
    "appellation_id", "year", "month", "avg_temp_c", "min_temp_c",
    "max_temp_c", "rainfall_mm", "rain_days", "sunshine_hours",
    "et0_mm", "avg_humidity_pct",
]


def save_yearly(conn, rows: list[dict]):
    """Upsert yearly weather rows via ON CONFLICT."""
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(YEARLY_COLS))
    update_cols = [c for c in YEARLY_COLS if c not in ("appellation_id", "year")]
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = f"""INSERT INTO appellation_weather_years ({", ".join(YEARLY_COLS)})
              VALUES ({placeholders})
              ON CONFLICT (appellation_id, year) DO UPDATE SET {update_str}"""
    values = [tuple(r.get(c) for c in YEARLY_COLS) for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, values, page_size=100)
    conn.commit()


def save_monthly(conn, rows: list[dict]):
    """Upsert monthly weather rows via ON CONFLICT."""
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(MONTHLY_COLS))
    update_cols = [c for c in MONTHLY_COLS if c not in ("appellation_id", "year", "month")]
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = f"""INSERT INTO appellation_weather_months ({", ".join(MONTHLY_COLS)})
              VALUES ({placeholders})
              ON CONFLICT (appellation_id, year, month) DO UPDATE SET {update_str}"""
    values = [tuple(r.get(c) for c in MONTHLY_COLS) for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, values, page_size=500)
    conn.commit()


# ── Query helpers ────────────────────────────────────────────────────────────

def get_appellations(conn, appellation_id: str = None, limit: int = None) -> list[dict]:
    """Get appellations ready for weather fetch."""
    sql = """
        SELECT id, name, latitude, longitude, hemisphere,
               growing_season_start_month, growing_season_end_month,
               harvest_start_month, harvest_end_month
        FROM appellations
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND growing_season_start_month IS NOT NULL
          AND harvest_start_month IS NOT NULL
    """
    params = []
    if appellation_id:
        sql += " AND id = %s"
        params.append(appellation_id)
    sql += " ORDER BY name"
    if limit:
        sql += f" LIMIT {int(limit)}"

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def get_fetched_ids(conn) -> set:
    """Get appellation IDs that already have weather data."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT appellation_id FROM appellation_weather_years")
        return {str(r[0]) for r in cur.fetchall()}


# ── Coordinate cache ─────────────────────────────────────────────────────────

def coord_key(lat: float, lon: float) -> str:
    """Round to 2dp for cache key — within Open-Meteo's ~9km grid resolution."""
    return f"{round(lat, 2)},{round(lon, 2)}"


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch Open-Meteo weather for appellations")
    parser.add_argument("--test", action="store_true", help="Process 10 test appellations")
    parser.add_argument("--limit", type=int, help="Max appellations to process")
    parser.add_argument("--id", type=str, help="Single appellation UUID")
    parser.add_argument("--no-resume", action="store_true", help="Re-fetch even if data exists")
    parser.add_argument("--delay", type=float, default=5.0, help="Seconds between API calls (default 5)")
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

    print(f"Processing {len(apps)} appellations × {START_YEAR}-{END_YEAR}")
    print()

    total_y = 0
    total_m = 0
    errors = 0
    api_calls = 0
    cache_hits = 0
    t0 = time.time()

    # Cache: coord_key → parsed daily rows
    daily_cache: dict[str, list[dict]] = {}

    for i, app in enumerate(apps):
        name = app["name"]
        lat, lon = float(app["latitude"]), float(app["longitude"])
        ck = coord_key(lat, lon)
        print(f"[{i+1}/{len(apps)}] {name} ({lat:.2f}, {lon:.2f}) ", end="", flush=True)

        # Check coordinate cache
        if ck in daily_cache:
            all_days = daily_cache[ck]
            cache_hits += 1
            print(f"(cached) ", end="", flush=True)
        else:
            print(f"... ", end="", flush=True)
            data = fetch_daily(lat, lon)
            api_calls += 1
            if data is None:
                print("API ERROR")
                errors += 1
                # After error, wait extra before next attempt
                time.sleep(30)
                continue
            all_days = parse_daily(data)
            daily_cache[ck] = all_days
            # Rate limit only after actual API calls
            if i < len(apps) - 1:
                time.sleep(args.delay)

        yearly, monthly = process_appellation(app, all_days)

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
    conn.close()


if __name__ == "__main__":
    main()
