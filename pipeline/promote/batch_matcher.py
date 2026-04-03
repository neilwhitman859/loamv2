"""
Reusable batch matcher for linking staging tables to canonical wines/producers.

In-memory producer matching with suffix stripping for speed.
Per-producer wine loading with normalized name matching.
Handles sources with and without explicit producer columns.

Usage:
    python -m pipeline.promote.batch_matcher --source flatiron,lcbo,systembolaget,bc_liquor [--dry-run]
    python -m pipeline.promote.batch_matcher --source flatiron --dry-run
"""

import argparse
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, normalize_producer, normalize_wine_name, parse_vintage


# ── Swedish → English Translation Maps (Systembolaget) ──────────

SWEDISH_COUNTRIES = {
    "frankrike": "france", "italien": "italy", "spanien": "spain",
    "usa": "united states", "sydafrika": "south africa", "portugal": "portugal",
    "tyskland": "germany", "chile": "chile", "sverige": "sweden",
    "australien": "australia", "argentina": "argentina", "österrike": "austria",
    "nya zeeland": "new zealand", "grekland": "greece", "ungern": "hungary",
    "libanon": "lebanon", "schweiz": "switzerland", "japan": "japan",
    "storbritannien": "united kingdom", "georgien": "georgia",
    "rumänien": "romania", "bulgarien": "bulgaria", "kroatien": "croatia",
    "slovenien": "slovenia", "turkiet": "turkey", "marocko": "morocco",
    "mexiko": "mexico", "kanada": "canada", "israel": "israel",
    "kina": "china", "brasilien": "brazil", "serbien": "serbia",
    "makedonien": "north macedonia", "luxemburg": "luxembourg",
    "moldavien": "moldova", "tjeckien": "czech republic",
    "indien": "india", "peru": "peru", "uruguay": "uruguay",
    "internationellt märke": None,  # multi-country brand, skip
}

SWEDISH_REGIONS = {
    "bourgogne": "burgundy", "rhonedalen": "rhône valley",
    "rhônedalen": "rhône valley", "toskana": "toscana",
    "kalifornien": "california", "loiredalen": "loire valley",
    "katalonien": "catalunya", "sicilien": "sicilia",
    "sardinien": "sardegna", "lombardiet": "lombardia",
    "kastilien-león": "castilla y león", "kastilien-la mancha": "castilla-la mancha",
    "sydvästra frankrike": "southwest france",
    "frankrike sydväst": "southwest france",
    "venetien": "veneto", "kampanien": "campania",
    "andalusien": "andalucía", "galicien": "galicia",
    "niederösterreich": "niederösterreich", "burgenland": "burgenland",
    "steiermark": "steiermark", "skåne län": "skåne",
}


class BatchMatcher:
    """
    In-memory producer + wine matcher for staging table promotion.

    Loads all canonical producers into memory for O(1) matching.
    Wine matching is done per-producer (load wines for that producer, match in-memory).
    """

    def __init__(self, verbose=True, dry_run=False):
        self.sb = get_supabase()
        self.verbose = verbose
        self.dry_run = dry_run

        # In-memory indexes (populated by init())
        self.producer_by_norm = {}       # name_normalized -> {id, name, country_id}
        self.producer_by_alias = {}      # alias_normalized -> producer_id
        self.producer_by_stripped = {}    # normalize_producer(name) -> [{id, name, country_id}]
        self.countries = {}              # name_lower | iso_lower -> id
        self.country_names = {}          # id -> name

        # Stats
        self.stats = defaultdict(int)

    def init(self):
        """Load all canonical producers and countries into memory."""
        t0 = time.time()
        if self.verbose:
            print("BatchMatcher: loading producers and countries...")

        # Load countries
        offset = 0
        countries_raw = []
        while True:
            result = self.sb.table("countries").select("id,name,iso_code").range(offset, offset + 999).execute()
            countries_raw.extend(result.data)
            if len(result.data) < 1000:
                break
            offset += 1000
        for c in countries_raw:
            self.countries[c["name"].lower()] = c["id"]
            self.country_names[c["id"]] = c["name"]
            if c.get("iso_code"):
                self.countries[c["iso_code"].lower()] = c["id"]
        # Common aliases
        us = self.countries.get("united states")
        if us:
            for alias in ["usa", "us", "u.s.a.", "u.s.", "america"]:
                self.countries[alias] = us
        uk = self.countries.get("united kingdom")
        if uk:
            for alias in ["uk", "england", "great britain"]:
                self.countries[alias] = uk
        if self.verbose:
            print(f"  Countries: {len(countries_raw)}")

        # Load all producers
        offset = 0
        producers_raw = []
        while True:
            result = (self.sb.table("producers")
                      .select("id,name,name_normalized,country_id")
                      .is_("deleted_at", "null")
                      .range(offset, offset + 999)
                      .execute())
            producers_raw.extend(result.data)
            if len(result.data) < 1000:
                break
            offset += 1000

        for p in producers_raw:
            norm = p["name_normalized"] or normalize(p["name"])
            self.producer_by_norm[norm] = p

            # Also index by suffix-stripped form
            stripped = normalize_producer(p["name"])
            if stripped and stripped != norm:
                if stripped not in self.producer_by_stripped:
                    self.producer_by_stripped[stripped] = []
                self.producer_by_stripped[stripped].append(p)
        if self.verbose:
            print(f"  Producers: {len(producers_raw)} (norm index: {len(self.producer_by_norm)}, stripped index: {len(self.producer_by_stripped)})")

        # Load producer aliases
        offset = 0
        alias_count = 0
        while True:
            result = (self.sb.table("producer_aliases")
                      .select("producer_id,name,name_normalized")
                      .range(offset, offset + 999)
                      .execute())
            for a in result.data:
                anorm = a.get("name_normalized") or normalize(a["name"])
                self.producer_by_alias[anorm] = a["producer_id"]
                alias_count += 1
            if len(result.data) < 1000:
                break
            offset += 1000
        if self.verbose:
            print(f"  Producer aliases: {alias_count}")

        elapsed = time.time() - t0
        if self.verbose:
            print(f"BatchMatcher: ready ({elapsed:.1f}s)\n")

    # ── Country Resolution ───────────────────────────────────────

    def resolve_country(self, name):
        """Resolve country name to UUID. Handles Swedish names."""
        if not name:
            return None
        lower = name.lower().strip()
        # Swedish translation
        translated = SWEDISH_COUNTRIES.get(lower)
        if translated is not None:
            return self.countries.get(translated)
        elif translated is None and lower in SWEDISH_COUNTRIES:
            return None  # "internationellt märke" etc.
        return self.countries.get(lower) or self.countries.get(normalize(lower))

    # ── Producer Matching ────────────────────────────────────────

    def match_producer(self, name, country_id=None):
        """
        3-tier in-memory producer matching:
          Tier 1: exact normalized name
          Tier 2: alias normalized name
          Tier 3: suffix-stripped name (Domaine/Château/Winery etc.)

        Returns: {id, name, country_id, confidence, tier} or None
        """
        if not name:
            return None
        norm = normalize(name)

        # Tier 1: exact normalized match
        p = self.producer_by_norm.get(norm)
        if p:
            if country_id and p.get("country_id") != country_id:
                # Check if there's a same-country match among all producers with this norm
                # (rare edge case — most normalized names are unique)
                pass
            return {"id": p["id"], "name": p["name"], "country_id": p.get("country_id"),
                    "confidence": 1.0, "tier": 1}

        # Tier 2: alias match
        pid = self.producer_by_alias.get(norm)
        if pid:
            return {"id": pid, "name": name, "country_id": None,
                    "confidence": 0.9, "tier": 2}

        # Tier 3: suffix-stripped match
        stripped = normalize_producer(name)
        if stripped and stripped != norm:
            candidates = self.producer_by_stripped.get(stripped, [])
            if candidates:
                # Prefer same-country match
                if country_id:
                    same = [c for c in candidates if c.get("country_id") == country_id]
                    if same:
                        return {"id": same[0]["id"], "name": same[0]["name"],
                                "country_id": same[0].get("country_id"),
                                "confidence": 0.85, "tier": 3}
                return {"id": candidates[0]["id"], "name": candidates[0]["name"],
                        "country_id": candidates[0].get("country_id"),
                        "confidence": 0.8, "tier": 3}

        return None

    def match_producer_from_title(self, title, country_id=None):
        """
        Extract producer from a wine title by trying progressively longer
        word prefixes against the producer index. Used for sources without
        a separate producer column (LCBO, BC Liquor).

        Returns: (producer_match, remaining_wine_name) or (None, title)
        """
        if not title:
            return None, title

        # Clean the title
        clean = re.sub(r"\s+", " ", title).strip()
        words = clean.split()

        best_match = None
        best_length = 0

        # Try prefixes of 1..min(8, len) words
        for n in range(1, min(9, len(words) + 1)):
            prefix = " ".join(words[:n])
            match = self.match_producer(prefix, country_id)
            if match:
                best_match = match
                best_length = n

        if best_match:
            remaining = " ".join(words[best_length:]).strip()
            return best_match, remaining

        return None, clean

    # ── Wine Matching ────────────────────────────────────────────

    def load_producer_wines(self, producer_id):
        """Load all wines for a producer into a dict keyed by name_normalized."""
        wines = {}
        offset = 0
        while True:
            result = (self.sb.table("wines")
                      .select("id,name,name_normalized,slug")
                      .eq("producer_id", producer_id)
                      .is_("deleted_at", "null")
                      .range(offset, offset + 999)
                      .execute())
            for w in result.data:
                norm = w.get("name_normalized") or normalize(w["name"])
                wines[norm] = w
            if len(result.data) < 1000:
                break
            offset += 1000
        return wines

    def match_wine(self, producer_wines, wine_name):
        """
        Match a wine name against a producer's loaded wine catalog.

        Tier 1: exact normalized name match
        Tier 2: normalized wine name (strip vintage/size)
        Tier 3: substring containment (wine catalog name in source name or vice versa)

        Returns: {id, name, confidence, tier} or None
        """
        if not wine_name or not producer_wines:
            return None

        norm = normalize(wine_name)
        norm_clean = normalize_wine_name(wine_name)

        # Tier 1: exact match on name_normalized
        w = producer_wines.get(norm)
        if w:
            return {"id": w["id"], "name": w["name"], "confidence": 1.0, "tier": 1}

        # Tier 2: match after stripping vintage/size
        w2 = producer_wines.get(norm_clean)
        if w2:
            return {"id": w2["id"], "name": w2["name"], "confidence": 0.95, "tier": 2}

        # Tier 3: containment — check if any canonical wine name is contained
        # in the source name or vice versa (for long descriptive titles)
        if len(norm_clean) >= 5:
            for canon_norm, canon_wine in producer_wines.items():
                if not canon_norm or len(canon_norm) < 4:
                    continue
                if canon_norm in norm_clean or norm_clean in canon_norm:
                    return {"id": canon_wine["id"], "name": canon_wine["name"],
                            "confidence": 0.75, "tier": 3}

        return None

    # ── Source Adapters ──────────────────────────────────────────

    def _load_staging(self, table, columns, batch_size=1000):
        """Load all unmatched rows from a staging table."""
        rows = []
        offset = 0
        while True:
            result = (self.sb.table(table)
                      .select(columns)
                      .is_("canonical_wine_id", "null")
                      .range(offset, offset + batch_size - 1)
                      .execute())
            rows.extend(result.data)
            if len(result.data) < batch_size:
                break
            offset += batch_size
        return rows

    def _write_matches(self, table, updates):
        """Write producer/wine matches back to staging table in batches via SQL."""
        if self.dry_run or not updates:
            return

        now = datetime.now(timezone.utc).isoformat()
        written = 0
        errors = 0

        # Group by (producer_id, wine_id) pattern for batch efficiency
        # But simplest reliable approach: per-row updates in chunks
        for i, row in enumerate(updates):
            try:
                update_data = {
                    "canonical_producer_id": row["producer_id"],
                    "processed_at": now,
                }
                if row.get("wine_id"):
                    update_data["canonical_wine_id"] = row["wine_id"]
                self.sb.table(table).update(update_data).eq("id", row["id"]).execute()
                written += 1
            except Exception as e:
                errors += 1
                if errors <= 5 and self.verbose:
                    print(f"  Write error ({row['id']}): {e}")

            if (i + 1) % 500 == 0 and self.verbose:
                print(f"  ... wrote {written}/{i+1} updates")

        if self.verbose:
            print(f"  Written: {written}, errors: {errors}")

    # ── Flatiron Adapter ─────────────────────────────────────────

    def run_flatiron(self):
        """Match Flatiron wines. Has producer column, structured tags."""
        print("=" * 60)
        print("FLATIRON (4,130 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_flatiron",
            "id,title,vendor,producer,country,region,vintage,price,grapes"
        )
        print(f"  Unmatched rows: {len(rows)}")
        if not rows:
            return

        # Group by producer
        by_producer = defaultdict(list)
        for r in rows:
            pname = r.get("producer") or ""
            by_producer[pname].append(r)
        print(f"  Distinct producers: {len(by_producer)}")

        updates = []
        matched_wines = 0
        matched_producers = 0

        for pname, wines in by_producer.items():
            country_id = self.resolve_country(wines[0].get("country"))
            pmatch = self.match_producer(pname, country_id)

            if not pmatch:
                self.stats["flatiron_producer_miss"] += len(wines)
                continue

            matched_producers += 1
            producer_wines = self.load_producer_wines(pmatch["id"])

            for w in wines:
                # Extract wine name from title: "Producer, Wine Name, Vintage"
                wine_name = w.get("title", "")
                # Strip producer prefix if present
                if pname and wine_name.lower().startswith(pname.lower()):
                    wine_name = wine_name[len(pname):].lstrip(" ,")
                # Strip trailing vintage
                wine_name = re.sub(r",?\s*(NV|nv|\d{4})(\s*\[.*?\])?\s*$", "", wine_name).strip()
                # Strip trailing comma
                wine_name = wine_name.rstrip(",").strip()

                wmatch = self.match_wine(producer_wines, wine_name)
                update = {"id": w["id"], "producer_id": pmatch["id"]}
                if wmatch:
                    update["wine_id"] = wmatch["id"]
                    update["confidence"] = wmatch["confidence"]
                    matched_wines += 1
                updates.append(update)

            if matched_producers % 100 == 0 and self.verbose:
                print(f"  ... {matched_producers} producers processed, {matched_wines} wines matched")

        print(f"  Producers matched: {matched_producers}/{len(by_producer)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_flatiron", updates)
        self.stats["flatiron_producer_match"] = matched_producers
        self.stats["flatiron_wine_match"] = matched_wines
        self.stats["flatiron_total"] = len(rows)

    # ── LCBO Adapter ─────────────────────────────────────────────

    def run_lcbo(self):
        """Match LCBO wines. No producer column — extract from name."""
        print("\n" + "=" * 60)
        print("LCBO (7,030 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_lcbo",
            "id,name,upc,country,region,abv,price_cad_cents,category"
        )
        print(f"  Unmatched rows: {len(rows)}")
        if not rows:
            return

        updates = []
        matched_wines = 0
        matched_producers = 0
        producer_cache = {}  # cache per-producer wine loads

        for i, r in enumerate(rows):
            country_id = self.resolve_country(r.get("country"))
            name = r.get("name", "")

            pmatch, wine_name = self.match_producer_from_title(name, country_id)

            if not pmatch:
                self.stats["lcbo_producer_miss"] += 1
                continue

            matched_producers += 1
            pid = pmatch["id"]

            # Cache producer wines
            if pid not in producer_cache:
                producer_cache[pid] = self.load_producer_wines(pid)

            wmatch = self.match_wine(producer_cache[pid], wine_name)
            update = {"id": r["id"], "producer_id": pid}
            if wmatch:
                update["wine_id"] = wmatch["id"]
                matched_wines += 1
            updates.append(update)

            if (i + 1) % 500 == 0 and self.verbose:
                print(f"  ... {i+1}/{len(rows)} processed, {matched_wines} wines matched")

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_lcbo", updates)
        self.stats["lcbo_producer_match"] = matched_producers
        self.stats["lcbo_wine_match"] = matched_wines
        self.stats["lcbo_total"] = len(rows)

    # ── Systembolaget Adapter ────────────────────────────────────

    def run_systembolaget(self):
        """Match Systembolaget wines. Has producer column. Swedish names."""
        print("\n" + "=" * 60)
        print("SYSTEMBOLAGET (12,646 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_systembolaget",
            "id,name_bold,name_thin,producer,country,origin_level1,origin_level2,vintage,abv,price_sek,grapes,color"
        )
        print(f"  Unmatched rows: {len(rows)}")
        if not rows:
            return

        # Group by producer
        by_producer = defaultdict(list)
        for r in rows:
            pname = (r.get("producer") or r.get("name_bold") or "").strip()
            by_producer[pname].append(r)
        print(f"  Distinct producers: {len(by_producer)}")

        updates = []
        matched_wines = 0
        matched_producers = 0

        for pname, wines in by_producer.items():
            country_name = wines[0].get("country", "")
            country_id = self.resolve_country(country_name)
            pmatch = self.match_producer(pname, country_id)

            if not pmatch:
                self.stats["syst_producer_miss"] += len(wines)
                continue

            matched_producers += 1
            producer_wines = self.load_producer_wines(pmatch["id"])

            for w in wines:
                # Wine name is name_thin, falling back to name_bold minus producer
                wine_name = w.get("name_thin") or ""
                if not wine_name:
                    bold = w.get("name_bold", "")
                    if pname and bold.lower().startswith(pname.lower()):
                        wine_name = bold[len(pname):].strip()
                    else:
                        wine_name = bold

                wmatch = self.match_wine(producer_wines, wine_name)
                update = {"id": w["id"], "producer_id": pmatch["id"]}
                if wmatch:
                    update["wine_id"] = wmatch["id"]
                    matched_wines += 1
                updates.append(update)

            if matched_producers % 100 == 0 and self.verbose:
                print(f"  ... {matched_producers} producers processed, {matched_wines} wines matched")

        print(f"  Producers matched: {matched_producers}/{len(by_producer)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_systembolaget", updates)
        self.stats["syst_producer_match"] = matched_producers
        self.stats["syst_wine_match"] = matched_wines
        self.stats["syst_total"] = len(rows)

    # ── BC Liquor Adapter ────────────────────────────────────────

    def run_bc_liquor(self):
        """Match BC Liquor wines. Name format: 'APPELLATION/WINE - PRODUCER VINTAGE'."""
        print("\n" + "=" * 60)
        print("BC LIQUOR (3,200 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_bc_liquor",
            "id,name,upc,country,region,sub_region,grape_type,abv,price,regular_price"
        )
        print(f"  Unmatched rows: {len(rows)}")
        if not rows:
            return

        updates = []
        matched_wines = 0
        matched_producers = 0
        producer_cache = {}

        for i, r in enumerate(rows):
            country_id = self.resolve_country(r.get("country"))
            name = r.get("name", "")

            # BC Liquor format: "WINE_DESC - PRODUCER [VINTAGE]"
            # The dash separates wine/appellation from producer
            producer_name = None
            wine_name = name

            if " - " in name:
                parts = name.rsplit(" - ", 1)
                wine_part = parts[0].strip()
                producer_part = parts[1].strip()
                # Strip vintage from producer part
                producer_part = re.sub(r"\s+\d{4}\s*$", "", producer_part).strip()
                producer_name = producer_part
                wine_name = wine_part
            else:
                # Try prefix matching as fallback
                pmatch, wine_name = self.match_producer_from_title(name, country_id)
                if pmatch:
                    pid = pmatch["id"]
                    if pid not in producer_cache:
                        producer_cache[pid] = self.load_producer_wines(pid)
                    wmatch = self.match_wine(producer_cache[pid], wine_name)
                    update = {"id": r["id"], "producer_id": pid}
                    if wmatch:
                        update["wine_id"] = wmatch["id"]
                        matched_wines += 1
                    matched_producers += 1
                    updates.append(update)
                    continue
                else:
                    self.stats["bc_producer_miss"] += 1
                    continue

            if producer_name:
                pmatch = self.match_producer(producer_name, country_id)
                if not pmatch:
                    self.stats["bc_producer_miss"] += 1
                    continue

                matched_producers += 1
                pid = pmatch["id"]
                if pid not in producer_cache:
                    producer_cache[pid] = self.load_producer_wines(pid)

                wmatch = self.match_wine(producer_cache[pid], wine_name)
                update = {"id": r["id"], "producer_id": pid}
                if wmatch:
                    update["wine_id"] = wmatch["id"]
                    matched_wines += 1
                updates.append(update)

            if (i + 1) % 500 == 0 and self.verbose:
                print(f"  ... {i+1}/{len(rows)} processed, {matched_wines} wines matched")

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_bc_liquor", updates)
        self.stats["bc_producer_match"] = matched_producers
        self.stats["bc_wine_match"] = matched_wines
        self.stats["bc_total"] = len(rows)

    # ── Summary ──────────────────────────────────────────────────

    def print_summary(self):
        print("\n" + "=" * 60)
        print("BATCH MATCHER SUMMARY")
        print("=" * 60)
        for source in ["flatiron", "lcbo", "syst", "bc"]:
            total = self.stats.get(f"{source}_total", 0)
            if total == 0:
                continue
            prod = self.stats.get(f"{source}_producer_match", 0)
            wine = self.stats.get(f"{source}_wine_match", 0)
            miss = self.stats.get(f"{source}_producer_miss", 0)
            print(f"  {source:15s}: {wine:5d}/{total:5d} wines matched "
                  f"({wine/total*100:.1f}%), {prod} producers, {miss} producer misses")
        if self.dry_run:
            print("\n  ** DRY RUN — no writes performed **")


def main():
    parser = argparse.ArgumentParser(description="Batch match staging tables to canonical")
    parser.add_argument("--source", default="flatiron,lcbo,systembolaget,bc_liquor",
                        help="Comma-separated sources to match")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--quiet", action="store_true", help="Less output")
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(",")]
    matcher = BatchMatcher(verbose=not args.quiet, dry_run=args.dry_run)
    matcher.init()

    source_map = {
        "flatiron": matcher.run_flatiron,
        "lcbo": matcher.run_lcbo,
        "systembolaget": matcher.run_systembolaget,
        "bc_liquor": matcher.run_bc_liquor,
    }

    for src in sources:
        fn = source_map.get(src)
        if fn:
            fn()
        else:
            print(f"Unknown source: {src}")

    matcher.print_summary()


if __name__ == "__main__":
    main()
