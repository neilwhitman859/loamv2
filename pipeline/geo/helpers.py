"""
Shared helpers for geo pipeline scripts.

Common functions used across multiple geo scripts:
- Coordinate operations (centroid, haversine, simplification)
- Nominatim API interaction
- GeoJSON manipulation
- Transliteration
"""

import math
import re
import time
import json
import unicodedata

import httpx


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
RATE_LIMIT_S = 1.1
USER_AGENT = "Loam/1.0 (wine-intelligence-platform)"


def compute_centroid(geojson: dict) -> dict | None:
    """Compute centroid by averaging all coordinates in a GeoJSON geometry."""
    total_lat = 0.0
    total_lng = 0.0
    count = 0

    def extract(coords):
        nonlocal total_lat, total_lng, count
        if isinstance(coords[0], (int, float)):
            total_lng += coords[0]
            total_lat += coords[1]
            count += 1
        else:
            for c in coords:
                extract(c)

    if geojson.get("type") == "GeometryCollection":
        for g in geojson.get("geometries", []):
            extract(g["coordinates"])
    else:
        extract(geojson["coordinates"])

    return {"lat": total_lat / count, "lng": total_lng / count} if count > 0 else None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute Haversine distance in km between two lat/lon points."""
    R = 6371
    d_lat = (lat2 - lat1) * math.pi / 180
    d_lon = (lon2 - lon1) * math.pi / 180
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(lat1 * math.pi / 180) * math.cos(lat2 * math.pi / 180) *
         math.sin(d_lon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def simplify_precision(geojson: dict) -> dict:
    """Round coordinates to 5 decimal places (~1m accuracy)."""
    def round_coords(coords):
        if isinstance(coords[0], (int, float)):
            return [round(coords[0], 5), round(coords[1], 5)]
        return [round_coords(c) for c in coords]
    return {"type": geojson["type"], "coordinates": round_coords(geojson["coordinates"])}


def simplify_line(points: list, tolerance: float) -> list:
    """Douglas-Peucker line simplification."""
    if len(points) <= 2:
        return points
    max_dist = 0
    max_idx = 0
    ax, ay = points[0][0], points[0][1]
    bx, by = points[-1][0], points[-1][1]
    dx, dy = bx - ax, by - ay
    len_sq = dx * dx + dy * dy

    for i in range(1, len(points) - 1):
        if len_sq == 0:
            dist = math.sqrt((points[i][0] - ax) ** 2 + (points[i][1] - ay) ** 2)
        else:
            t = max(0, min(1, ((points[i][0] - ax) * dx + (points[i][1] - ay) * dy) / len_sq))
            dist = math.sqrt((points[i][0] - (ax + t * dx)) ** 2 + (points[i][1] - (ay + t * dy)) ** 2)
        if dist > max_dist:
            max_dist = dist
            max_idx = i

    if max_dist > tolerance:
        left = simplify_line(points[:max_idx + 1], tolerance)
        right = simplify_line(points[max_idx:], tolerance)
        return left[:-1] + right
    return [points[0], points[-1]]


def simplify_geometry(geojson: dict, tolerance: float) -> dict:
    """Apply Douglas-Peucker simplification to a GeoJSON geometry."""
    def simplify_ring(ring):
        simplified = simplify_line(ring, tolerance)
        return simplified if len(simplified) >= 4 else ring

    def simplify_coords(coords, depth):
        if depth == 0:
            return simplify_ring(coords)
        return [simplify_coords(c, depth - 1) for c in coords]

    depth = 2 if geojson["type"] == "MultiPolygon" else 1 if geojson["type"] == "Polygon" else 0
    return {"type": geojson["type"], "coordinates": simplify_coords(geojson["coordinates"], depth)}


def progressive_simplify(geojson: dict, max_size_kb: int = 250) -> dict:
    """Progressively simplify a GeoJSON geometry until it's under max_size_kb."""
    tolerances = [0.001, 0.002, 0.005, 0.01]
    simplified = simplify_precision(geojson)
    for tol in tolerances:
        size_kb = len(json.dumps(simplified)) / 1024
        if size_kb <= max_size_kb:
            return simplified
        simplified = simplify_geometry(simplified, tol)
        simplified = simplify_precision(simplified)
    return simplified


def nominatim_search(client: httpx.Client, query: str,
                     polygon: bool = True, retries: int = 3) -> list[dict]:
    """Search Nominatim with rate limiting and retry on 429."""
    params = {"q": query, "format": "jsonv2" if polygon else "json", "limit": "1"}
    if polygon:
        params["polygon_geojson"] = "1"

    for attempt in range(retries):
        resp = client.get(NOMINATIM_URL, params=params,
                          headers={"User-Agent": USER_AGENT})
        if resp.status_code == 429:
            wait = min(30, 5 * (attempt + 1))
            print(f"    Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception("Rate limited after retries")


def has_polygon(result: dict) -> bool:
    """Check if a Nominatim result has a polygon geometry."""
    geojson = result.get("geojson")
    return geojson is not None and geojson.get("type") in ("Polygon", "MultiPolygon")


def geo_slugify(s: str) -> str:
    """Slugify for geo scripts (strips accents + apostrophes)."""
    t = unicodedata.normalize("NFD", s.lower())
    t = re.sub(r"[\u0300-\u036f]", "", t)
    t = re.sub(r"['\u2019]", "", t)
    t = re.sub(r"[^a-z0-9]+", "-", t)
    return t.strip("-")


def fetch_all_paginated(sb, table: str, select: str, filters: dict | None = None,
                        batch_size: int = 1000) -> list[dict]:
    """Fetch all rows from a Supabase table with pagination."""
    all_rows = []
    offset = 0
    while True:
        query = sb.table(table).select(select).range(offset, offset + batch_size - 1)
        if filters:
            for k, v in filters.items():
                query = query.eq(k, v)
        result = query.execute()
        all_rows.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += batch_size
    return all_rows


# --- Greek transliteration ---
GREEK_TO_LATIN = {
    "\u0391": "A", "\u0392": "V", "\u0393": "G", "\u0394": "D", "\u0395": "E",
    "\u0396": "Z", "\u0397": "I", "\u0398": "Th", "\u0399": "I", "\u039a": "K",
    "\u039b": "L", "\u039c": "M", "\u039d": "N", "\u039e": "X", "\u039f": "O",
    "\u03a0": "P", "\u03a1": "R", "\u03a3": "S", "\u03a4": "T", "\u03a5": "Y",
    "\u03a6": "F", "\u03a7": "Ch", "\u03a8": "Ps", "\u03a9": "O",
    "\u03b1": "a", "\u03b2": "v", "\u03b3": "g", "\u03b4": "d", "\u03b5": "e",
    "\u03b6": "z", "\u03b7": "i", "\u03b8": "th", "\u03b9": "i", "\u03ba": "k",
    "\u03bb": "l", "\u03bc": "m", "\u03bd": "n", "\u03be": "x", "\u03bf": "o",
    "\u03c0": "p", "\u03c1": "r", "\u03c3": "s", "\u03c2": "s", "\u03c4": "t",
    "\u03c5": "y", "\u03c6": "f", "\u03c7": "ch", "\u03c8": "ps", "\u03c9": "o",
}

# --- Cyrillic transliteration ---
CYRILLIC_TO_LATIN = {
    "\u0410": "A", "\u0411": "B", "\u0412": "V", "\u0413": "G", "\u0414": "D",
    "\u0415": "E", "\u0416": "Zh", "\u0417": "Z", "\u0418": "I", "\u0419": "Y",
    "\u041a": "K", "\u041b": "L", "\u041c": "M", "\u041d": "N", "\u041e": "O",
    "\u041f": "P", "\u0420": "R", "\u0421": "S", "\u0422": "T", "\u0423": "U",
    "\u0424": "F", "\u0425": "Kh", "\u0426": "Ts", "\u0427": "Ch", "\u0428": "Sh",
    "\u0429": "Sht", "\u042a": "a", "\u042c": "", "\u042e": "Yu", "\u042f": "Ya",
    "\u0430": "a", "\u0431": "b", "\u0432": "v", "\u0433": "g", "\u0434": "d",
    "\u0435": "e", "\u0436": "zh", "\u0437": "z", "\u0438": "i", "\u0439": "y",
    "\u043a": "k", "\u043b": "l", "\u043c": "m", "\u043d": "n", "\u043e": "o",
    "\u043f": "p", "\u0440": "r", "\u0441": "s", "\u0442": "t", "\u0443": "u",
    "\u0444": "f", "\u0445": "kh", "\u0446": "ts", "\u0447": "ch", "\u0448": "sh",
    "\u0449": "sht", "\u044a": "a", "\u044c": "", "\u044e": "yu", "\u044f": "ya",
}


def transliterate_greek(s: str) -> str:
    return "".join(GREEK_TO_LATIN.get(c, c) for c in s)


def transliterate_cyrillic(s: str) -> str:
    return "".join(CYRILLIC_TO_LATIN.get(c, c) for c in s)


def transliterate_all(s: str) -> str:
    return transliterate_cyrillic(transliterate_greek(s))


def esri_rings_to_geojson(rings: list) -> dict:
    """Convert Esri JSON rings to GeoJSON polygon. Uses signed area for ring direction."""
    def signed_area(ring):
        total = 0
        for i in range(len(ring)):
            j = (i + 1) % len(ring)
            total += ring[i][0] * ring[j][1]
            total -= ring[j][0] * ring[i][1]
        return total / 2

    if not rings:
        return {"type": "Polygon", "coordinates": []}

    polygons = []
    current_polygon = None

    for ring in rings:
        area = signed_area(ring)
        if area < 0:
            # Outer ring (clockwise in Esri = counterclockwise in GeoJSON)
            if current_polygon is not None:
                polygons.append(current_polygon)
            current_polygon = [ring]
        else:
            # Inner ring (hole)
            if current_polygon is not None:
                current_polygon.append(ring)

    if current_polygon is not None:
        polygons.append(current_polygon)

    if len(polygons) == 1:
        return {"type": "Polygon", "coordinates": polygons[0]}
    return {"type": "MultiPolygon", "coordinates": polygons}
