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

from pipeline.lib.db import get_conn
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
        self.conn = get_conn()
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

        cur = self.conn.cursor()

        # Load countries
        cur.execute("SELECT id, name, iso_code FROM countries")
        countries_raw = cur.fetchall()
        for row in countries_raw:
            cid, name, iso_code = row
            self.countries[name.lower()] = cid
            self.country_names[cid] = name
            if iso_code:
                self.countries[iso_code.lower()] = cid
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
        cur.execute(
            "SELECT id, name, name_normalized, country_id "
            "FROM producers WHERE deleted_at IS NULL"
        )
        producers_raw = cur.fetchall()
        for row in producers_raw:
            pid, name, name_normalized, country_id = row
            p = {"id": pid, "name": name, "name_normalized": name_normalized, "country_id": country_id}
            norm = name_normalized or normalize(name)
            self.producer_by_norm[norm] = p

            # Also index by suffix-stripped form
            stripped = normalize_producer(name)
            if stripped and stripped != norm:
                if stripped not in self.producer_by_stripped:
                    self.producer_by_stripped[stripped] = []
                self.producer_by_stripped[stripped].append(p)
        if self.verbose:
            print(f"  Producers: {len(producers_raw)} (norm index: {len(self.producer_by_norm)}, stripped index: {len(self.producer_by_stripped)})")

        # Load producer aliases
        cur.execute("SELECT producer_id, name, name_normalized FROM producer_aliases")
        aliases_raw = cur.fetchall()
        for row in aliases_raw:
            producer_id, name, name_normalized = row
            anorm = name_normalized or normalize(name)
            self.producer_by_alias[anorm] = producer_id
        if self.verbose:
            print(f"  Producer aliases: {len(aliases_raw)}")

        cur.close()

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
            # Also check producer_by_norm directly — producer_by_stripped only indexes
            # producers where normalize_producer(name) != name_normalized, so "Failla"
            # (stripped = norm = "failla") is missing from stripped index but is in norm index.
            if not candidates:
                p = self.producer_by_norm.get(stripped)
                if p:
                    candidates = [p]
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
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id, name, name_normalized, slug FROM wines "
            "WHERE producer_id = %s AND deleted_at IS NULL",
            (producer_id,)
        )
        for row in cur.fetchall():
            wid, name, name_normalized, slug = row
            w = {"id": wid, "name": name, "name_normalized": name_normalized, "slug": slug}
            norm = name_normalized or normalize(name)
            wines[norm] = w
        cur.close()
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
        # Validate table/column names against injection (they come from hardcoded strings
        # in our adapter methods, but be safe)
        assert re.match(r'^[a-z_]+$', table), f"Invalid table name: {table}"
        col_list = [c.strip() for c in columns.split(",")]
        for c in col_list:
            assert re.match(r'^[a-z_0-9]+$', c), f"Invalid column name: {c}"

        cur = self.conn.cursor()
        cur.execute(
            f"SELECT {columns} FROM {table} WHERE canonical_wine_id IS NULL"
        )
        col_names = [desc[0] for desc in cur.description]
        rows = [dict(zip(col_names, row)) for row in cur.fetchall()]
        cur.close()
        return rows

    def _write_matches(self, table, updates):
        """Write producer/wine matches back to staging table in batches via SQL."""
        if self.dry_run or not updates:
            return

        from psycopg2.extras import execute_values

        assert re.match(r'^[a-z_]+$', table), f"Invalid table name: {table}"

        now = datetime.now(timezone.utc)
        cur = self.conn.cursor()
        written = 0
        errors = 0
        batch_size = 500

        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            # Build tuples: (id, canonical_producer_id, canonical_wine_id, processed_at)
            values = []
            for row in batch:
                values.append((
                    row["id"],
                    row["producer_id"],
                    row.get("wine_id"),
                    now,
                ))
            try:
                execute_values(
                    cur,
                    f"UPDATE {table} AS t SET "
                    f"canonical_producer_id = v.producer_id, "
                    f"canonical_wine_id = v.wine_id, "
                    f"processed_at = v.processed_at "
                    f"FROM (VALUES %s) AS v(id, producer_id, wine_id, processed_at) "
                    f"WHERE t.id = v.id",
                    values,
                    template="(%s::uuid, %s::uuid, %s::uuid, %s::timestamptz)"
                )
                self.conn.commit()
                written += len(batch)
            except Exception as e:
                self.conn.rollback()
                errors += len(batch)
                if errors <= 5 * batch_size and self.verbose:
                    print(f"  Batch write error at offset {i}: {e}")

            if self.verbose and (i + batch_size) % 1000 < batch_size:
                print(f"  ... wrote {written}/{min(i + batch_size, len(updates))} updates")

        cur.close()
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

    # ── Wally's Adapter ────────────────────────────────────────

    def run_wallys(self):
        """Match Wally's wines. Title format: '2023 Producer WineName, Region 750mL'."""
        print("\n" + "=" * 60)
        print("WALLY'S (19,446 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_wallys",
            "id,title,vendor,producer,country,region,vintage,price"
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
            # Use producer tag if available, otherwise extract from title
            producer_name = r.get("producer")
            title = r.get("title", "")

            if producer_name:
                pmatch = self.match_producer(producer_name, country_id)
                wine_name = title
                if pmatch:
                    # Strip producer from title for wine name
                    if producer_name.lower() in title.lower():
                        idx = title.lower().find(producer_name.lower())
                        wine_name = (title[:idx] + title[idx + len(producer_name):]).strip(" ,")
                    wine_name = re.sub(r"^\d{4}\s+", "", wine_name)  # strip vintage
                    wine_name = re.sub(r"\s+\d+(\.\d+)?m[lL]$", "", wine_name)  # strip volume
                    wine_name = wine_name.strip(" ,")
            else:
                # Extract from title: strip vintage year and volume first
                clean = re.sub(r"^\d{4}\s+", "", title)
                clean = re.sub(r"\s+\d+(\.\d+)?m[lL]$", "", clean).strip()
                pmatch, wine_name = self.match_producer_from_title(clean, country_id)

            if not pmatch:
                self.stats["wallys_producer_miss"] += 1
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

            if (i + 1) % 1000 == 0 and self.verbose:
                print(f"  ... {i+1}/{len(rows)} processed, {matched_wines} wines matched")

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_wallys", updates)
        self.stats["wallys_producer_match"] = matched_producers
        self.stats["wallys_wine_match"] = matched_wines
        self.stats["wallys_total"] = len(rows)

    # ── Spec's Adapter ─────────────────────────────────────────

    def run_specs(self):
        """Match Spec's wines. No producer column — extract from name."""
        print("\n" + "=" * 60)
        print("SPEC'S (21,913 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_specs",
            "id,name,upc,sku,price,wine_origin"
        )
        print(f"  Unmatched rows: {len(rows)}")
        if not rows:
            return

        updates = []
        matched_wines = 0
        matched_producers = 0
        producer_cache = {}

        for i, r in enumerate(rows):
            country_id = self.resolve_country(r.get("wine_origin"))
            name = r.get("name", "")

            pmatch, wine_name = self.match_producer_from_title(name, country_id)

            if not pmatch:
                self.stats["specs_producer_miss"] += 1
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

            if (i + 1) % 1000 == 0 and self.verbose:
                print(f"  ... {i+1}/{len(rows)} processed, {matched_wines} wines matched")

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_specs", updates)
        self.stats["specs_producer_match"] = matched_producers
        self.stats["specs_wine_match"] = matched_wines
        self.stats["specs_total"] = len(rows)

    # ── Enofile Adapter ────────────────────────────────────────

    def run_enofile(self):
        """Match Enofile wines. `brand` is producer, has appellation + vintage."""
        print("\n" + "=" * 60)
        print("ENOFILE (9,166 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_enofile",
            "id,brand,varietal,vintage,appellation,designation,addl_designation"
        )
        print(f"  Unmatched rows: {len(rows)}")
        if not rows:
            return

        updates = []
        matched_wines = 0
        matched_producers = 0
        producer_cache = {}

        for i, r in enumerate(rows):
            brand = r.get("brand")
            if not brand:
                self.stats["enofile_producer_miss"] += 1
                continue

            pmatch = self.match_producer(brand, None)
            if not pmatch:
                self.stats["enofile_producer_miss"] += 1
                continue

            matched_producers += 1
            pid = pmatch["id"]

            if pid not in producer_cache:
                producer_cache[pid] = self.load_producer_wines(pid)

            # Build wine name: varietal + designation + addl_designation
            parts = []
            for key in ("varietal", "designation", "addl_designation"):
                v = r.get(key)
                if v and v.strip():
                    parts.append(v.strip())
            wine_name = " ".join(parts) if parts else (r.get("appellation") or "")

            wmatch = self.match_wine(producer_cache[pid], wine_name)
            update = {"id": r["id"], "producer_id": pid}
            if wmatch:
                update["wine_id"] = wmatch["id"]
                matched_wines += 1
            updates.append(update)

            if (i + 1) % 1000 == 0 and self.verbose:
                print(f"  ... {i+1}/{len(rows)} processed, {matched_wines} wines matched")

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_enofile", updates)
        self.stats["enofile_producer_match"] = matched_producers
        self.stats["enofile_wine_match"] = matched_wines
        self.stats["enofile_total"] = len(rows)

    # ── Domestique Adapter ─────────────────────────────────────

    def run_domestique(self):
        """Match Domestique wines. Producer set, title has 'WineName YYYY'."""
        print("\n" + "=" * 60)
        print("DOMESTIQUE (247 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_domestique",
            "id,title,producer,wine_name,country,vintage"
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
            producer_name = r.get("producer")
            title = r.get("title") or ""
            wine_name = r.get("wine_name") or title

            if producer_name:
                pmatch = self.match_producer(producer_name, country_id)
            else:
                pmatch, wine_name = self.match_producer_from_title(title, country_id)

            if not pmatch:
                self.stats["domestique_producer_miss"] += 1
                continue

            matched_producers += 1
            pid = pmatch["id"]

            # Clean wine name
            wine_name = re.sub(r"\s+\d{4}$", "", wine_name).strip()

            if pid not in producer_cache:
                producer_cache[pid] = self.load_producer_wines(pid)

            wmatch = self.match_wine(producer_cache[pid], wine_name)
            update = {"id": r["id"], "producer_id": pid}
            if wmatch:
                update["wine_id"] = wmatch["id"]
                matched_wines += 1
            updates.append(update)

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_domestique", updates)
        self.stats["domestique_producer_match"] = matched_producers
        self.stats["domestique_wine_match"] = matched_wines
        self.stats["domestique_total"] = len(rows)

    # ── Last Bottle Adapter ────────────────────────────────────

    def run_last_bottle(self):
        """Match Last Bottle wines. Title format: 'Producer Varietal Region YYYY'."""
        print("\n" + "=" * 60)
        print("LAST BOTTLE (160 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_last_bottle",
            "id,title,producer,wine_name,country,vintage"
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
            producer_name = r.get("producer")
            title = r.get("title") or ""

            if producer_name:
                pmatch = self.match_producer(producer_name, country_id)
                wine_name = r.get("wine_name") or title
            else:
                # Strip trailing vintage year from title
                clean = re.sub(r"\s+\d{4}$", "", title).strip()
                pmatch, wine_name = self.match_producer_from_title(clean, country_id)

            if not pmatch:
                self.stats["last_bottle_producer_miss"] += 1
                continue

            matched_producers += 1
            pid = pmatch["id"]

            if pid not in producer_cache:
                producer_cache[pid] = self.load_producer_wines(pid)

            wmatch = self.match_wine(producer_cache[pid], wine_name or "")
            update = {"id": r["id"], "producer_id": pid}
            if wmatch:
                update["wine_id"] = wmatch["id"]
                matched_wines += 1
            updates.append(update)

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_last_bottle", updates)
        self.stats["last_bottle_producer_match"] = matched_producers
        self.stats["last_bottle_wine_match"] = matched_wines
        self.stats["last_bottle_total"] = len(rows)

    # ── PA PLCB Adapter ────────────────────────────────────────

    def run_pa(self):
        """Match PA PLCB wines. `brand_name` is producer, `item_description` has wine name."""
        print("\n" + "=" * 60)
        print("PA PLCB (5,905 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_pa",
            "id,brand_name,item_description,country,region,vintage"
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
            brand = r.get("brand_name")
            desc = r.get("item_description") or ""

            if brand:
                pmatch = self.match_producer(brand, country_id)
                wine_name = desc
                # Strip brand from description if present
                if pmatch and brand.lower() in desc.lower():
                    idx = desc.lower().find(brand.lower())
                    wine_name = (desc[:idx] + desc[idx + len(brand):]).strip(" ,")
            else:
                pmatch, wine_name = self.match_producer_from_title(desc, country_id)

            if not pmatch:
                self.stats["pa_producer_miss"] += 1
                continue

            matched_producers += 1
            pid = pmatch["id"]

            # Strip vintage year and volume info
            wine_name = re.sub(r"\s+\d{4}\s*", " ", wine_name)
            wine_name = re.sub(r"\s+\d+(\.\d+)?\s*(ml|l)\b", "", wine_name, flags=re.IGNORECASE)
            wine_name = wine_name.strip()

            if pid not in producer_cache:
                producer_cache[pid] = self.load_producer_wines(pid)

            wmatch = self.match_wine(producer_cache[pid], wine_name)
            update = {"id": r["id"], "producer_id": pid}
            if wmatch:
                update["wine_id"] = wmatch["id"]
                matched_wines += 1
            updates.append(update)

            if (i + 1) % 1000 == 0 and self.verbose:
                print(f"  ... {i+1}/{len(rows)} processed, {matched_wines} wines matched")

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_pa", updates)
        self.stats["pa_producer_match"] = matched_producers
        self.stats["pa_wine_match"] = matched_wines
        self.stats["pa_total"] = len(rows)

    # ── Best Wine Store Adapter ────────────────────────────────

    def run_best_wine_store(self):
        """Match Best Wine Store. Producer set, title has wine name + region."""
        print("\n" + "=" * 60)
        print("BEST WINE STORE (1,658 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_best_wine_store",
            "id,title,producer,wine_name,country,vintage"
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
            producer_name = r.get("producer")
            title = r.get("title") or ""

            if producer_name:
                pmatch = self.match_producer(producer_name, country_id)
                wine_name = title
                if pmatch and producer_name.lower() in title.lower():
                    idx = title.lower().find(producer_name.lower())
                    wine_name = (title[:idx] + title[idx + len(producer_name):]).strip(" ,")
            else:
                pmatch, wine_name = self.match_producer_from_title(title, country_id)

            if not pmatch:
                self.stats["best_wine_store_producer_miss"] += 1
                continue

            matched_producers += 1
            pid = pmatch["id"]

            # Strip volume info and vintage
            wine_name = re.sub(r"\s+\d+(\.\d+)?\s*(ml|l)\b", "", wine_name, flags=re.IGNORECASE)
            wine_name = re.sub(r"\s+\d{4}$", "", wine_name).strip()

            if pid not in producer_cache:
                producer_cache[pid] = self.load_producer_wines(pid)

            wmatch = self.match_wine(producer_cache[pid], wine_name)
            update = {"id": r["id"], "producer_id": pid}
            if wmatch:
                update["wine_id"] = wmatch["id"]
                matched_wines += 1
            updates.append(update)

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_best_wine_store", updates)
        self.stats["best_wine_store_producer_match"] = matched_producers
        self.stats["best_wine_store_wine_match"] = matched_wines
        self.stats["best_wine_store_total"] = len(rows)

    # ── Berliner Wine Trophy Adapter ───────────────────────────

    def run_berliner(self):
        """Match Berliner Wine Trophy. Has producer, wine_name, country, grapes, medal."""
        print("\n" + "=" * 60)
        print("BERLINER WINE TROPHY (73,896 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_berliner",
            "id,wine_name,producer,country,region"
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
            producer_name = r.get("producer")
            wine_name = r.get("wine_name") or ""

            if producer_name:
                pmatch = self.match_producer(producer_name, country_id)
            else:
                pmatch, wine_name = self.match_producer_from_title(wine_name, country_id)

            if not pmatch:
                self.stats["berliner_producer_miss"] += 1
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

            if (i + 1) % 5000 == 0 and self.verbose:
                print(f"  ... {i+1}/{len(rows)} processed, {matched_wines} wines matched")

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_berliner", updates)
        self.stats["berliner_producer_match"] = matched_producers
        self.stats["berliner_wine_match"] = matched_wines
        self.stats["berliner_total"] = len(rows)

    # ── TEXSOM Adapter ─────────────────────────────────────────

    def run_texsom(self):
        """Match TEXSOM. Has producer, wine_name, appellation, country, vintage."""
        print("\n" + "=" * 60)
        print("TEXSOM (46,896 wines)")
        print("=" * 60)

        rows = self._load_staging(
            "source_texsom",
            "id,producer,wine_name,appellation,country,vintage"
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
            producer_name = r.get("producer")
            wine_name = r.get("wine_name") or ""

            if producer_name:
                pmatch = self.match_producer(producer_name, country_id)
            else:
                pmatch, wine_name = self.match_producer_from_title(wine_name, country_id)

            if not pmatch:
                self.stats["texsom_producer_miss"] += 1
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

            if (i + 1) % 5000 == 0 and self.verbose:
                print(f"  ... {i+1}/{len(rows)} processed, {matched_wines} wines matched")

        print(f"  Rows with producer match: {matched_producers}/{len(rows)}")
        print(f"  Wines matched: {matched_wines}/{len(rows)}")

        self._write_matches("source_texsom", updates)
        self.stats["texsom_producer_match"] = matched_producers
        self.stats["texsom_wine_match"] = matched_wines
        self.stats["texsom_total"] = len(rows)

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
        "wallys": matcher.run_wallys,
        "specs": matcher.run_specs,
        "enofile": matcher.run_enofile,
        "domestique": matcher.run_domestique,
        "last_bottle": matcher.run_last_bottle,
        "pa": matcher.run_pa,
        "berliner": matcher.run_berliner,
        "texsom": matcher.run_texsom,
        "best_wine_store": matcher.run_best_wine_store,
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
