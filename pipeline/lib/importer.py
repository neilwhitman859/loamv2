"""
Shared producer import library for the Loam pipeline.

Takes a standardized JSON file and inserts producer, wines, vintages,
scores, grape compositions, and label designations into Supabase.

Usage:
    python -m pipeline.lib.importer data/imports/moone-tsai.json [--dry-run] [--replace] [--validate]

This is the Python equivalent of lib/import.mjs. Most reference data resolution
is delegated to pipeline.lib.resolve.ReferenceResolver.
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert, batch_upsert
from pipeline.lib.normalize import normalize, slugify
from pipeline.lib.resolve import ReferenceResolver


# ── Helpers ──────────────────────────────────────────────────

MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def parse_date(val: str | None) -> str | None:
    """Parse informal dates into ISO date strings."""
    if not val:
        return None
    s = str(val).strip()
    # Already ISO-ish: 2024-08-19
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # "August 19, 2024" or "August 19 2024"
    m = re.match(r"^(\w+)\s+(\d{1,2}),?\s+(\d{4})$", s)
    if m:
        mm = MONTHS.get(m.group(1).lower())
        if mm:
            return f"{m.group(3)}-{mm}-{m.group(2).zfill(2)}"
    # "August 2024" → "2024-08-01"
    m2 = re.match(r"^(\w+)\s+(\d{4})$", s)
    if m2:
        mm = MONTHS.get(m2.group(1).lower())
        if mm:
            return f"{m2.group(2)}-{mm}-01"
    return None


def clean_metadata(meta: dict | None, keys_to_strip: list[str]) -> dict | None:
    """Remove specified keys from a metadata dict, returning None if empty."""
    if not meta or not isinstance(meta, dict):
        return meta or None
    cleaned = {k: v for k, v in meta.items() if k not in keys_to_strip}
    return cleaned if cleaned else None


def parse_altitude(val) -> dict:
    """Parse altitude value into {low, high} dict."""
    if not val:
        return {"low": None, "high": None}
    s = re.sub(r"[^\d\-.,]", " ", str(val)).strip()
    # Range: "330-600" or "330 - 600"
    m = re.search(r"(\d+)\s*[-–]\s*(\d+)", s)
    if m:
        return {"low": int(m.group(1)), "high": int(m.group(2))}
    # Single value: "450"
    m2 = re.search(r"(\d+)", s)
    if m2:
        n = int(m2.group(1))
        return {"low": n, "high": n}
    return {"low": None, "high": None}


def fetch_all_sync(table: str, columns: str = "*", filters: dict | None = None,
                   batch_size: int = 1000) -> list[dict]:
    """Fetch all rows from a Supabase table (sync), paginating past 1000-row limit."""
    sb = get_supabase()
    all_rows = []
    offset = 0
    while True:
        query = sb.table(table).select(columns).range(offset, offset + batch_size - 1)
        if filters:
            for k, v in filters.items():
                query = query.eq(k, v)
        result = query.execute()
        all_rows.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += batch_size
    return all_rows


class ReferenceData:
    """
    Extended reference data loader with additional tables beyond ReferenceResolver.

    Loads varietal categories, label designations, bottle formats, winemakers,
    and other data needed by the producer importer.
    """

    def __init__(self):
        self.resolver = ReferenceResolver(verbose=True)
        self.varietal_categories: dict[str, str] = {}  # name_lower | slug -> id
        self.label_designations: dict[str, str] = {}   # "name|country_id" -> id
        self.bottle_formats: dict[str, dict] = {}      # name_lower | volume_ml_str -> row
        self.winemakers: dict[str, dict] = {}           # slug | name_lower -> row
        self.farming_certs: dict[str, str] = {}         # name_lower -> id

    def load(self):
        """Load all reference data."""
        self.resolver.init_sync()

        print("Loading extended reference data...")

        # Varietal categories
        vcs = fetch_all_sync("varietal_categories", "id,name,slug")
        for v in vcs:
            self.varietal_categories[v["name"].lower()] = v["id"]
            self.varietal_categories[v["slug"]] = v["id"]
        print(f"  Varietal categories: {len(vcs)}")

        # Label designations
        lds = fetch_all_sync("label_designations", "id,canonical_name,local_name,category,country_id")
        for ld in lds:
            cid = ld.get("country_id") or "null"
            self.label_designations[f"{ld['canonical_name'].lower()}|{cid}"] = ld["id"]
            norm_canonical = normalize(ld["canonical_name"])
            if norm_canonical != ld["canonical_name"].lower():
                self.label_designations[f"{norm_canonical}|{cid}"] = ld["id"]
            if ld.get("local_name"):
                self.label_designations[f"{ld['local_name'].lower()}|{cid}"] = ld["id"]
                norm_local = normalize(ld["local_name"])
                if norm_local != ld["local_name"].lower():
                    self.label_designations[f"{norm_local}|{cid}"] = ld["id"]
        print(f"  Label designations: {len(lds)}")

        # Bottle formats
        formats = fetch_all_sync("bottle_formats", "id,name,volume_ml")
        for f in formats:
            self.bottle_formats[f["name"].lower()] = f
            self.bottle_formats[str(f["volume_ml"])] = f
        print(f"  Bottle formats: {len(formats)}")

        # Winemakers
        wms = fetch_all_sync("winemakers", "id,slug,name")
        for w in wms:
            self.winemakers[w["slug"]] = w
            self.winemakers[w["name"].lower()] = w
        print(f"  Winemakers: {len(wms)}")

        # Farming certifications
        fcs = fetch_all_sync("farming_certifications", "id,name")
        for fc in fcs:
            self.farming_certs[fc["name"].lower()] = fc["id"]
        print(f"  Farming certifications: {len(fcs)}")

        print("Extended reference data loaded.\n")

    # ── Delegation to resolver ───────────────────────────────────

    def resolve_country(self, name):
        return self.resolver.resolve_country(name)

    def resolve_region(self, name, country_id=None):
        return self.resolver.resolve_region(name, country_id)

    def resolve_appellation(self, name, country_id=None):
        return self.resolver.resolve_appellation(name, country_id)

    def resolve_grape(self, name):
        return self.resolver.resolve_grape(name)

    def resolve_publication(self, name):
        return self.resolver.resolve_publication(name)

    def resolve_classification(self, system, level):
        return self.resolver.resolve_classification(system, level)

    # ── Extended resolution ──────────────────────────────────────

    def resolve_varietal_category(self, name: str | None) -> str | None:
        if not name:
            return None
        lower = name.lower().strip()
        return (self.varietal_categories.get(lower) or
                self.varietal_categories.get(slugify(name)))

    def resolve_label_designation(self, name: str | None, country_id: str | None = None) -> str | None:
        if not name:
            return None
        lower = name.lower().strip()
        norm = normalize(lower)
        # Country-specific first
        if country_id:
            lid = (self.label_designations.get(f"{lower}|{country_id}") or
                   self.label_designations.get(f"{norm}|{country_id}"))
            if lid:
                return lid
        # Universal (null country)
        universal = (self.label_designations.get(f"{lower}|null") or
                     self.label_designations.get(f"{norm}|null"))
        if universal:
            return universal
        # Fallback: any country
        for key, lid in self.label_designations.items():
            if key.startswith(f"{lower}|") or key.startswith(f"{norm}|"):
                return lid
        return None

    def resolve_bottle_format(self, name_or_ml) -> dict | None:
        if not name_or_ml:
            return None
        key = str(name_or_ml).lower().strip()
        return self.bottle_formats.get(key)

    def resolve_farming_cert(self, name: str | None) -> str | None:
        if not name:
            return None
        return self.farming_certs.get(name.lower().strip())
