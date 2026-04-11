"""
Batch 0 pipeline: Create producers and wines from staging data.

LWIN-first approach:
1. Create canonical producers from LWIN data
2. Create canonical wines from LWIN data with cuvée extraction + display names
3. Link to TTB for COLA IDs, vintages, ABV, label images
4. Link to importers for grapes, prices, scores
5. Promote depth data
6. Calculate grades

Usage:
    python -m pipeline.identity.batch_pipeline --dry-run --limit 5    # dry run on 5 producers
    python -m pipeline.identity.batch_pipeline --execute              # run all 48
"""
import argparse
import json
import re
import uuid
from collections import defaultdict
from typing import Optional

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, normalize_producer, slugify
from pipeline.identity.clean_cuvee import extract_cuvee, extract_classifications
from pipeline.identity.build_display_name import build_display_name


# ===== Batch 0 Producer Roster (from IDENTITY_RULES.md Section 8) =====
BATCH_0_PRODUCERS = [
    # (canonical_name, lwin_name, country_code, category)
    # lwin_name is how LWIN stores the producer name
    ("Château Margaux", "Margaux", "FR", "Bordeaux"),
    ("Château Lafite Rothschild", "Lafite Rothschild", "FR", "Bordeaux"),
    ("Château Mouton Rothschild", "Mouton Rothschild", "FR", "Bordeaux"),
    ("Château Haut-Brion", "Haut-Brion", "FR", "Bordeaux"),
    ("Château Latour", "Latour", "FR", "Bordeaux"),
    ("Ridge Vineyards", "Ridge", "US", "Napa"),
    ("Caymus Vineyards", "Caymus", "US", "Napa"),
    ("Stag's Leap Wine Cellars", "Stag's Leap Wine Cellars", "US", "Napa"),
    ("Opus One", "Opus One", "US", "Napa"),
    ("Silver Oak Cellars", "Silver Oak", "US", "Napa"),
    ("Giacomo Conterno", "Giacomo Conterno", "IT", "Italian"),
    ("Marchesi Antinori", "Antinori", "IT", "Italian"),
    ("Gaja", "Gaja", "IT", "Italian"),
    ("Masseto", "Masseto", "IT", "Italian"),
    ("Tenuta San Guido", "San Guido", "IT", "Italian"),
    ("López de Heredia", "R. Lopez de Heredia", "ES", "Spanish"),
    ("La Rioja Alta", "La Rioja Alta", "ES", "Spanish"),
    ("CVNE", "CVNE", "ES", "Spanish"),
    ("Bodegas Muga", "Muga", "ES", "Spanish"),
    ("Marqués de Riscal", "de Riscal", "ES", "Spanish"),
    ("J.J. Prüm", "Joh. Jos. Prum", "DE", "German"),
    ("Dr. Loosen", "Dr. Loosen", "DE", "German"),
    ("Dönnhoff", "Donnhoff", "DE", "German"),
    ("Egon Müller", "Egon Muller", "DE", "German"),
    ("Fritz Haag", "Fritz Haag", "DE", "German"),
    ("Penfolds", "Penfolds", "AU", "Australian"),
    ("Henschke", "Henschke", "AU", "Australian"),
    ("Torbreck", "Torbreck", "AU", "Australian"),
    ("Tyrrell's", "Tyrrell's", "AU", "Australian"),
    ("Yalumba", "Yalumba", "AU", "Australian"),
    ("Barefoot", "Barefoot", "US", "Grocery"),
    ("Josh Cellars", None, "US", "Grocery"),  # Not in LWIN
    ("Meiomi", "Meiomi", "US", "Grocery"),
    ("19 Crimes", "19 Crimes", "AU", "Grocery"),
    ("Yellow Tail", "Yellow Tail", "AU", "Grocery"),
    ("Louis Jadot", "Louis Jadot", "FR", "Negociant"),
    ("Louis Latour", "Louis Latour", "FR", "Negociant"),
    ("Joseph Drouhin", "Joseph Drouhin", "FR", "Negociant"),
    ("Bouchard Père et Fils", "Bouchard Pere et Fils", "FR", "Negociant"),
    ("Maison Champy", "Champy", "FR", "Negociant"),
    ("Dominus Estate", "Dominus", "US", "Single-wine"),
    ("Screaming Eagle", "Screaming Eagle", "US", "Single-wine"),
    ("Harlan Estate", "Harlan", "US", "Single-wine"),
    ("Scarecrow", "Scarecrow", "US", "Single-wine"),
    # Portfolio: use subsidiary brands, skip parent companies
    ("Kendall-Jackson", "Kendall Jackson", "US", "Portfolio"),
    ("Duckhorn Vineyards", "Duckhorn", "US", "Portfolio"),
    # Skip Treasury/Constellation/E&J Gallo — parent companies, not label brands
]

# LWIN colour mapping
LWIN_COLOR_MAP = {
    "Red": "red",
    "White": "white",
    "Rose": "rosé",
    "Rosé": "rosé",
    "Orange": "orange",
}

# LWIN wine_type mapping
LWIN_TYPE_MAP = {
    "Wine": "table",
    "Sparkling": "sparkling",
    "Fortified": "fortified",
}

# (EU_COUNTRIES constant removed in Session 14 Phase B W6. It was only used to
# decide between the 75% US and 85% EU label-regulation minimum when writing
# synthetic wine_grapes.percentage values. Percentages now default to NULL, so
# the constant is dead code.)


class BatchPipeline:
    """End-to-end pipeline for creating producers and wines from staging data."""

    def __init__(self, dry_run: bool = True, limit: Optional[int] = None,
                 skip_ttb: bool = False):
        self.dry_run = dry_run
        self.limit = limit
        self.skip_ttb = skip_ttb
        self.conn = get_conn()
        self.cur = self.conn.cursor()
        self.stats = defaultdict(int)
        self.provenance_log = []

        # Load reference data
        self._load_reference_data()

    def _load_reference_data(self):
        """Pre-load reference tables for FK resolution."""
        print("Loading reference data...")

        # Countries
        self.cur.execute("SELECT id, name, iso_code FROM countries")
        self.countries = {}
        for row in self.cur.fetchall():
            self.countries[row[1].upper()] = {"id": str(row[0]), "name": row[1], "code": row[2]}
            self.countries[row[2].upper()] = {"id": str(row[0]), "name": row[1], "code": row[2]}

        # Regions
        self.cur.execute("SELECT id, name, country_id FROM regions")
        self.regions = {}
        for row in self.cur.fetchall():
            key = (row[1].upper(), str(row[2]))
            self.regions[key] = {"id": str(row[0]), "name": row[1]}
            # Also index by name only (for unique names)
            if row[1].upper() not in self.regions:
                self.regions[row[1].upper()] = {"id": str(row[0]), "name": row[1]}

        # Appellations (name → id, also with country for disambiguation)
        self.cur.execute("SELECT id, name, country_id, region_id FROM appellations")
        self.appellations = {}
        self.appellation_by_name = {}
        self.appellation_by_norm = {}  # accent-stripped index for fallback
        for row in self.cur.fetchall():
            self.appellations[str(row[0])] = {"id": str(row[0]), "name": row[1],
                                               "country_id": str(row[2]) if row[2] else None,
                                               "region_id": str(row[3]) if row[3] else None}
            key = row[1].upper()
            if key not in self.appellation_by_name:
                self.appellation_by_name[key] = []
            self.appellation_by_name[key].append(self.appellations[str(row[0])])
            # Pre-build normalized index
            norm_key = normalize(row[1]).upper()
            if norm_key not in self.appellation_by_norm:
                self.appellation_by_norm[norm_key] = []
            self.appellation_by_norm[norm_key].append(self.appellations[str(row[0])])

        # Grapes (name → id, including synonyms)
        self.cur.execute("SELECT id, name FROM grapes")
        self.grapes = {}
        self.grape_names = set()
        for row in self.cur.fetchall():
            self.grapes[row[1].upper()] = {"id": str(row[0]), "name": row[1]}
            self.grape_names.add(row[1])

        self.cur.execute("SELECT grape_id, synonym FROM grape_synonyms")
        for row in self.cur.fetchall():
            self.grapes[row[1].upper()] = {"id": str(row[0]), "name": row[1]}
            self.grape_names.add(row[1])  # Include synonyms for cuvée stripping

        # Pre-build curated grape list for cuvée stripping
        # Use only common grapes (appear in real wine names), not all 40K VIVC entries
        # This list is for display name clarity — obscure grapes won't appear in LWIN wine names
        COMMON_GRAPES_FOR_STRIP = {
            "CABERNET SAUVIGNON", "MERLOT", "PINOT NOIR", "SYRAH", "SHIRAZ",
            "GRENACHE", "TEMPRANILLO", "SANGIOVESE", "NEBBIOLO", "MALBEC",
            "CABERNET FRANC", "PETIT VERDOT", "MOURVÈDRE", "MOURVEDRE",
            "CARIGNAN", "CARMENÈRE", "CARMENERE", "CINSAULT",
            "CHARDONNAY", "SAUVIGNON BLANC", "RIESLING", "PINOT GRIGIO",
            "PINOT GRIS", "GEWÜRZTRAMINER", "GEWURZTRAMINER", "VIOGNIER",
            "CHENIN BLANC", "SÉMILLON", "SEMILLON", "MARSANNE", "ROUSSANNE",
            "MUSCAT", "MOSCATO", "GARGANEGA", "TREBBIANO", "VERDICCHIO",
            "GRÜNER VELTLINER", "GRUNER VELTLINER", "ALBARIÑO", "ALBARINO",
            "VERDEJO", "TORRONTÉS", "TORRONTES", "GODELLO", "MENCÍA", "MENCIA",
            "BARBERA", "DOLCETTO", "PRIMITIVO", "ZINFANDEL", "AGLIANICO",
            "NERO D'AVOLA", "MONTEPULCIANO", "CORVINA", "GARNACHA",
            "PETITE SIRAH", "PINOTAGE", "TOURIGA NACIONAL", "TOURIGA FRANCA",
            "GAMAY", "BLAUFRÄNKISCH", "BLAUFRANKISCH", "ZWEIGELT",
            "FURMINT", "ASSYRTIKO", "XINOMAVRO", "AGIORGITIKO",
            "SAPERAVI", "FETEASCĂ NEAGRĂ", "FETEASCA NEAGRA",
            "MELON DE BOURGOGNE", "MUSCADET", "ALIGOTÉ", "ALIGOTE",
            "PINOT BLANC", "PINOT MEUNIER", "MÜLLER-THURGAU", "MULLER-THURGAU",
            "SILVANER", "SCHEUREBE", "SPÄTBURGUNDER", "SPATBURGUNDER",
            "WEISSBURGUNDER", "GRAUBURGUNDER", "TROLLINGER", "LEMBERGER",
            "BOBAL", "MONASTRELL", "GRACIANO", "MAZUELO",
            "VERMENTINO", "PECORINO", "FALANGHINA", "FIANO", "GRECO",
            "GLERA", "PROSECCO", "LAMBRUSCO",
        }
        self._grape_names_for_strip = sorted(
            [n for n in self.grape_names if n.upper() in COMMON_GRAPES_FOR_STRIP],
            key=len, reverse=True
        )

        # Pre-build sorted grape names for primary grape identification (sorted longest-first, filtered)
        self._grape_names_sorted = sorted(
            [n for n in self.grape_names if len(n) >= 5 and n.upper() not in BatchPipeline.GRAPE_BLOCKLIST],
            key=len, reverse=True
        )

        # Label designations
        self.cur.execute("SELECT id, canonical_name FROM label_designations")
        self.label_designations = {}
        for row in self.cur.fetchall():
            self.label_designations[row[1].upper()] = {"id": str(row[0]), "name": row[1]}

        print(f"  Loaded: {len(self.countries)//2} countries, {len(self.regions)} regions, "
              f"{len(self.appellation_by_name)} appellations, {len(self.grapes)} grapes, "
              f"{len(self.label_designations)} designations")

    def resolve_appellation(self, text: str, country_id: Optional[str] = None) -> Optional[dict]:
        """Resolve appellation text to canonical appellation (accent-insensitive)."""
        if not text:
            return None
        key = text.strip().upper()
        # Strip common suffixes
        for suffix in [" AOC", " AOP", " DOC", " DOCG", " DO", " AVA", " IGP", " IGT", " DAC", " WO"]:
            if key.endswith(suffix):
                key = key[:-len(suffix)].strip()

        matches = self.appellation_by_name.get(key, [])

        # If no match, try accent-stripped lookup (pre-built index)
        if not matches:
            norm_key = normalize(text).upper()
            matches = self.appellation_by_norm.get(norm_key, [])

        if not matches:
            return None
        if len(matches) == 1:
            return matches[0]
        # Disambiguate by country
        if country_id:
            for m in matches:
                if m.get("country_id") == country_id:
                    return m
        return matches[0]

    def resolve_grape(self, text: str) -> Optional[dict]:
        """Resolve grape text to canonical grape."""
        if not text:
            return None
        key = text.strip().upper()
        return self.grapes.get(key)

    def resolve_country(self, code_or_name: str) -> Optional[dict]:
        """Resolve country code or name to canonical country."""
        if not code_or_name:
            return None
        return self.countries.get(code_or_name.strip().upper())

    def resolve_region(self, name: str, country_id: Optional[str] = None) -> Optional[dict]:
        """Resolve region name to canonical region."""
        if not name:
            return None
        key = name.strip().upper()
        if country_id:
            result = self.regions.get((key, country_id))
            if result:
                return result
        return self.regions.get(key)

    # =========================================================================
    # STEP 1: Create producers
    # =========================================================================

    def create_producer(self, canonical_name: str, lwin_name: Optional[str],
                        country_code: str) -> Optional[str]:
        """Create a canonical producer from staging data. Returns producer UUID."""
        country = self.resolve_country(country_code)
        if not country:
            print(f"  [SKIP] Unknown country: {country_code}")
            return None

        # Search LWIN for region info
        region_id = None
        if lwin_name:
            self.cur.execute(
                "SELECT DISTINCT region FROM source_lwin WHERE UPPER(producer_name) = UPPER(%s)",
                (lwin_name,))
            regions = [r[0] for r in self.cur.fetchall() if r[0]]
            if regions:
                region = self.resolve_region(regions[0], country["id"])
                if region:
                    region_id = region["id"]

        slug = slugify(canonical_name)

        if self.dry_run:
            print(f"  [DRY] Would create producer: {canonical_name} ({country_code})")
            return f"dry-{slug}"

        # Check if already exists
        self.cur.execute("SELECT id FROM producers WHERE slug = %s", (slug,))
        existing = self.cur.fetchone()
        if existing:
            print(f"  [EXISTS] {canonical_name}")
            return str(existing[0])

        producer_id = str(uuid.uuid4())
        self.cur.execute("""
            INSERT INTO producers (id, name, name_normalized, slug, country_id, region_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (producer_id, canonical_name, normalize(canonical_name), slug,
              country["id"], region_id))
        self.conn.commit()
        self.stats["producers_created"] += 1

        # Log provenance
        self._log_provenance("producers", producer_id, "name", canonical_name,
                             "lwin" if lwin_name else "manual", None)

        print(f"  [OK] Created producer: {canonical_name} ({country_code})")
        return producer_id

    # =========================================================================
    # STEP 2: Discover and create wines from LWIN
    # =========================================================================

    def discover_wines_lwin(self, producer_id: str, canonical_name: str,
                            lwin_name: Optional[str], country_code: str) -> list[dict]:
        """Pull wines from LWIN for this producer. Returns list of wine dicts."""
        if not lwin_name:
            return []

        self.cur.execute("""
            SELECT lwin_7, wine_name, country, region, sub_region,
                   colour, wine_type, designation, classification
            FROM source_lwin
            WHERE UPPER(producer_name) = UPPER(%s)
            ORDER BY wine_name
        """, (lwin_name,))

        wines = []
        for row in self.cur.fetchall():
            lwin_7, wine_name, country, region, sub_region, colour, wine_type, designation, classification = row

            # Skip junk entries (assortment cases, gift sets, separate brands)
            if wine_name and any(skip in (wine_name or "").lower()
                                  for skip in ["assortment", "case of", "gift set", "mixed case",
                                               "selection box", "tasting set", "collection",
                                               "advent calendar", "mouton cadet",
                                               "baron de miollis"]):
                continue

            wines.append({
                "lwin_7": str(lwin_7) if lwin_7 else None,
                "raw_name": wine_name,
                "country": country,
                "region": region,
                "sub_region": sub_region,
                "colour": colour,
                "wine_type": wine_type,
                "designation": designation,
                "classification": classification,
                "source": "source_lwin",
            })

        return wines

    def _dedup_lwin_wines(self, wines: list[dict], producer_name: str,
                          country_code: str) -> list[dict]:
        """Deduplicate LWIN wines by computed identity tuple (cuvée + appellation + color)."""
        country = self.resolve_country(country_code)
        country_id = country["id"] if country else None

        seen = {}
        unique = []
        for wine in wines:
            # Compute identity key
            app_text = wine.get("sub_region") or wine.get("designation")
            appellation = self.resolve_appellation(app_text, country_id) if app_text else None
            if not appellation:
                # Try region as appellation (e.g., Rioja)
                region_text = wine.get("region")
                if region_text:
                    appellation = self.resolve_appellation(region_text, country_id)
            app_name = appellation["name"] if appellation else None

            cuvee = extract_cuvee(
                wine.get("raw_name") or "",
                producer_name,
                app_name,
                self._grape_names_for_strip,
                country_code,
            )
            color = LWIN_COLOR_MAP.get(wine.get("colour"))
            wine_type = LWIN_TYPE_MAP.get(wine.get("wine_type"), "table")

            key = (cuvee, app_name, color, wine_type)
            if key not in seen:
                seen[key] = wine
                unique.append(wine)
            else:
                # Keep the one with more data (prefer one with lwin_7)
                existing = seen[key]
                if wine.get("lwin_7") and not existing.get("lwin_7"):
                    seen[key] = wine
                    unique[unique.index(existing)] = wine

        return unique

    def create_wine_from_lwin(self, wine_data: dict, producer_id: str,
                              producer_name: str, country_code: str) -> Optional[str]:
        """Create a canonical wine from a LWIN record."""
        country = self.resolve_country(country_code)
        if not country:
            return None

        raw_name = wine_data.get("raw_name")

        # Resolve appellation from sub_region (LWIN's most specific geographic)
        appellation = None
        region = None
        app_text = wine_data.get("sub_region") or wine_data.get("designation")
        if app_text:
            appellation = self.resolve_appellation(app_text, country["id"])
        if not appellation:
            # Try region text as appellation first (e.g., Rioja is stored in LWIN 'region'
            # but is an appellation in our DB)
            region_text = wine_data.get("region")
            if region_text:
                appellation = self.resolve_appellation(region_text, country["id"])
                if not appellation:
                    region = self.resolve_region(region_text, country["id"])

        # If appellation found, cascade region from it
        if appellation and appellation.get("region_id"):
            region = {"id": appellation["region_id"]}

        # Map color
        color = LWIN_COLOR_MAP.get(wine_data.get("colour"))

        # Map wine type
        wine_type = LWIN_TYPE_MAP.get(wine_data.get("wine_type"), "table")

        # Extract cuvée
        appellation_name = appellation["name"] if appellation else None
        cuvee = extract_cuvee(
            raw_name or "",
            producer_name,
            appellation_name,
            self._grape_names_for_strip,
            country_code,
        )

        # Extract classifications from raw_name
        _, classifications = extract_classifications(raw_name or "", country_code)
        # Also check LWIN classification field
        lwin_class = wine_data.get("classification")
        if lwin_class and lwin_class not in classifications:
            classifications.append(lwin_class)

        # Identify primary grape from wine name (for display name + label regulation)
        primary_grape = self._identify_primary_grape(raw_name, country_code)
        # Title Case grape names for display (grapes table stores ALL CAPS from VIVC)
        primary_grape_name = primary_grape["name"].title() if primary_grape else None

        # Strip primary grape from cuvée to prevent duplication in display name
        # (e.g., "Bin 389 Cabernet" cuvée + "Cabernet Sauvignon" grape → "Cabernet Cabernet")
        if cuvee and primary_grape:
            cuvee = re.sub(
                r'(?<!\w)' + re.escape(primary_grape["name"]) + r'(?!\w)',
                '', cuvee, flags=re.IGNORECASE
            ).strip()
            cuvee = re.sub(r'\s+', ' ', cuvee).strip() or None

        # Build display name
        classification_str = classifications[0] if classifications else None
        # For certain appellations that ARE classifications (Premier Cru, Grand Cru in wine name)
        if lwin_class and lwin_class in ("Premier Cru Classe", "Premier Cru Classé"):
            classification_str = None  # Don't repeat — this is about the chateau, not the wine

        display_name = build_display_name(
            producer_name=producer_name,
            cuvee=cuvee,
            primary_grape=primary_grape_name,
            appellation_name=appellation_name,
            region_name=region["name"] if region and isinstance(region, dict) and "name" in region else
                        (wine_data.get("region")),
            country_code=country_code,
            classification=classification_str,
            wine_type=wine_type,
        )

        # Build slug from display name
        slug = slugify(display_name)

        if self.dry_run:
            print(f"    [DRY] {display_name}")
            print(f"          cuvee={cuvee!r}, grape={primary_grape_name!r}, "
                  f"app={appellation_name!r}, color={color}, type={wine_type}")
            return f"dry-{slug}"

        # Check for duplicate (by slug globally — slug is unique)
        self.cur.execute("SELECT id FROM wines WHERE slug = %s", (slug,))
        existing = self.cur.fetchone()
        if existing:
            self.stats["wines_existed"] += 1
            return str(existing[0])

        wine_id = str(uuid.uuid4())
        # wines.name = cuvée (nullable). NULL is correct when no cuvée.
        wine_name = cuvee
        wine_name_norm = normalize(wine_name) if wine_name else None
        try:
            self.cur.execute("""
                INSERT INTO wines (id, name, name_normalized, slug, producer_id,
                                   country_id, region_id, appellation_id, color, wine_type,
                                   display_name, color_confirmed, appellation_confirmed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                wine_id,
                wine_name,
                wine_name_norm,
                slug,
                producer_id,
                country["id"],
                region["id"] if region and isinstance(region, dict) and "id" in region else None,
                appellation["id"] if appellation else None,
                color,
                wine_type,
                display_name,
                True if color else False,  # LWIN colour is source-confirmed
                True if appellation else False,
            ))
            self.conn.commit()
            self.stats["wines_created"] += 1
        except Exception as e:
            self.conn.rollback()
            if "unique constraint" in str(e).lower() or "duplicate key" in str(e).lower():
                self.stats["wines_slug_conflict"] += 1
                # Try to find existing by slug
                self.cur.execute("SELECT id FROM wines WHERE slug = %s", (slug,))
                existing = self.cur.fetchone()
                if existing:
                    return str(existing[0])
                return None
            raise

        # Log provenance
        self._log_provenance("wines", wine_id, "name", cuvee, "lwin", wine_data.get("lwin_7"))
        self._log_provenance("wines", wine_id, "display_name", display_name, "lwin", wine_data.get("lwin_7"))
        if color:
            self._log_provenance("wines", wine_id, "color", color, "lwin", wine_data.get("lwin_7"))
        if appellation:
            self._log_provenance("wines", wine_id, "appellation_id", appellation["id"], "lwin", wine_data.get("lwin_7"))

        # Create LWIN external_id
        if wine_data.get("lwin_7"):
            self._create_external_id(wine_id, "wine", "lwin_7", wine_data["lwin_7"], "lwin")

        # Link label designations
        for cls in classifications:
            ld = self.label_designations.get(cls.upper())
            if ld:
                self._create_label_designation(wine_id, ld["id"])

        # Promote primary grape (label regulation).
        #
        # Historical note (removed in Session 14 Phase B W6, 2026-04-11): this block
        # used to set `percentage` to 85 (EU) or 75 (US) — the legal minimum for
        # varietal labeling in each jurisdiction. That was a regulatory floor, NOT
        # a blend proportion, and downstream enrichment was reading it as ground-truth
        # composition. 22,664 rows carrying synthetic 75 / 85 values were null'd out
        # during W6. We still create the grape link (we want to know the primary
        # variety), but with NULL percentage. Real blend percentages should only come
        # from TTB `grape_varietals` parsing or producer tech sheets.
        if primary_grape:
            self._create_wine_grape(wine_id, primary_grape["id"], None, "label_regulation")

        return wine_id

    # Grape names that are too ambiguous for wine name matching
    GRAPE_BLOCKLIST = {
        "BARON", "CADET", "ROUGE", "BLANC", "ROSE", "VIN", "WINE",
        "PORT", "RUBY", "RESERVE", "CLASSIC", "PREMIER", "GRAND",
        "NOBLE", "ROYAL", "GOLD", "SILVER", "IMPERIAL",
        "LAFITE",    # VIVC synonym for Cab Sauv — false positive on Lafite Rothschild
        "VINEYARD",  # synonym for Koenigin der Weingaerten
        "ARTEMIS",   # real grape but matches SLW brand name
        "SELECT",    # real grape but matches wine label terms
        "AMBER",     # real grape but matches descriptive terms
        "PEARL",     # synonym for Chasselas Blanc
        "FLORA",     # real grape but matches wine brand names
        "VILLA",     # synonym for Albana Bianca
        "TRIUMPH",   # real grape but matches wine brand names
        "SUMMIT",    # synonym for Sultanina
        "SAN LORENZO",  # real grape but matches Gaja vineyard name
        "MARQUES",   # synonym — false positive on Marqués names
        "CRISTINA",  # real grape but matches Antinori brand
        "EXTRA",     # real grape but matches wine label terms
        "VECCHIA",   # real grape — false positive on "Vigna Vecchia"
        "NEBBIOLO D",  # partial grape name match artifact
        "SELECTION", # real grape but matches wine label terms
        "TINTO",     # color word, not a grape
        "SAUVIGNON", # too ambiguous alone (Sauvignon Blanc is separate)
        "AMABILE",   # real grape but matches sweetness term
        "PINOT BIANCO",  # avoid matching standalone "Pinot" in wine names
        "PINOT",     # too ambiguous alone — Pinot Noir/Grigio/Blanc are separate
        "CLARENCE",  # real grape but matches Haut-Brion cuvée
        "FRANK",     # real grape but matches "Frank Gehry"
        "BRAUNE",    # real grape but matches "Braune Kupp" vineyard
    }

    def _identify_primary_grape(self, wine_name: Optional[str], country_code: str) -> Optional[dict]:
        """Identify grape from wine name using label regulation."""
        if not wine_name:
            return None
        # Use pre-built sorted list (longest first, blocklist already filtered)
        wine_upper = wine_name.upper()
        for grape_name in self._grape_names_sorted:
            # Quick substring check before regex (much faster)
            if grape_name.upper() not in wine_upper:
                continue
            pattern = r'(?<!\w)' + re.escape(grape_name) + r'(?!\w)'
            if re.search(pattern, wine_name, re.IGNORECASE):
                return self.grapes.get(grape_name.upper())
        return None

    # =========================================================================
    # STEP 3: Link TTB for depth data
    # =========================================================================

    def link_ttb_for_producer(self, producer_id: str, producer_name: str,
                              country_code: str) -> int:
        """Link TTB records to canonical wines for depth data. Returns count linked."""
        if self.dry_run:
            return 0

        # Get all wines for this producer
        self.cur.execute("SELECT id, name, display_name, slug FROM wines WHERE producer_id = %s", (producer_id,))
        wines = self.cur.fetchall()
        if not wines:
            return 0

        # Get TTB records for this brand
        ttb_names = [producer_name]
        for prefix in ["Château ", "Chateau ", "Domaine ", "Bodegas ", "Maison "]:
            if producer_name.lower().startswith(prefix.lower()):
                ttb_names.append(producer_name[len(prefix):])
        for suffix in [" Vineyards", " Vineyard", " Winery", " Estate", " Cellars"]:
            if producer_name.lower().endswith(suffix.lower()):
                ttb_names.append(producer_name[:-len(suffix)])

        conditions = " OR ".join(["UPPER(brand_name) = UPPER(%s)" for _ in ttb_names])
        self.cur.execute(f"""
            SELECT ttb_id, fanciful_name, wine_appellation, grape_varietals,
                   class_type_desc, wine_vintage, abv, label_image_urls
            FROM source_ttb_colas
            WHERE ({conditions})
            LIMIT 1000
        """, ttb_names)
        ttb_rows = self.cur.fetchall()

        linked = 0
        for ttb_row in ttb_rows:
            ttb_id, fanciful, appellation, grapes, class_desc, vintage, abv, label_urls = ttb_row

            # Try to match to a canonical wine
            # Simple matching: if fanciful_name matches a wine's cuvée
            matched_wine_id = self._match_ttb_to_wine(wines, fanciful, appellation)
            if not matched_wine_id:
                continue

            # Create vintage if we have one
            if vintage and re.match(r'^(19|20)\d{2}$', str(vintage)):
                self._ensure_vintage(matched_wine_id, int(vintage), abv, label_urls)

            # Create COLA external_id
            if ttb_id:
                self._create_external_id(matched_wine_id, "wine", "cola", str(ttb_id), "ttb_cola")

            linked += 1

        self.stats["ttb_linked"] += linked
        return linked

    def _match_ttb_to_wine(self, wines: list, fanciful: Optional[str],
                           appellation: Optional[str]) -> Optional[str]:
        """Match a TTB record to a canonical wine."""
        if not wines:
            return None

        fanciful_norm = normalize(fanciful) if fanciful else ""

        for wine_id, wine_name, display_name, slug in wines:
            wine_name_norm = normalize(wine_name) if wine_name else ""

            # Exact cuvée match
            if wine_name_norm and fanciful_norm and wine_name_norm == fanciful_norm:
                return str(wine_id)

            # If wine has no cuvée (NULL name), match by appellation
            if not wine_name and not fanciful:
                return str(wine_id)

        # If only one wine for this producer and TTB record has no fanciful, assume match
        if len(wines) == 1 and not fanciful:
            return str(wines[0][0])

        return None

    # =========================================================================
    # STEP 4: Grade calculation
    # =========================================================================

    def calculate_grades(self, producer_id: str):
        """Calculate confirmation, completeness, and identity_complete for all wines of a producer."""
        if self.dry_run:
            return

        self.cur.execute("SELECT id FROM wines WHERE producer_id = %s", (producer_id,))
        wine_ids = [str(r[0]) for r in self.cur.fetchall()]

        for wine_id in wine_ids:
            # Confirmation grade
            confirmation = self._calc_confirmation(wine_id)

            # Completeness score (0-11)
            completeness = self._calc_completeness(wine_id)

            # Identity complete boolean
            identity_complete = self._calc_identity_complete(wine_id)

            self.cur.execute("""
                UPDATE wines SET confirmation = %s, completeness = %s,
                    identity_complete = %s
                WHERE id = %s
            """, (confirmation, completeness, identity_complete, wine_id))

        self.conn.commit()

    def _calc_confirmation(self, wine_id: str) -> str:
        """Calculate confirmation grade D/C/B/A."""
        # Check external_ids
        self.cur.execute("""
            SELECT DISTINCT system FROM external_ids
            WHERE entity_type = 'wine' AND entity_id = %s
        """, (wine_id,))
        systems = {r[0] for r in self.cur.fetchall()}

        backbone_count = sum(1 for s in systems if s in ("lwin_7", "cola", "upc"))

        if backbone_count >= 3:
            return "A"
        elif backbone_count >= 2:
            return "B"
        elif backbone_count >= 1:
            return "C"
        return "D"

    def _calc_completeness(self, wine_id: str) -> int:
        """Calculate completeness score 0-11."""
        score = 0

        self.cur.execute("""
            SELECT producer_id, color, color_confirmed, appellation_id, appellation_confirmed,
                   region_id, country_id, grapes_confirmed
            FROM wines WHERE id = %s
        """, (wine_id,))
        row = self.cur.fetchone()
        if not row:
            return 0
        prod_id, color, color_conf, app_id, app_conf, region_id, country_id, grapes_conf = row

        # 1. Producer
        if prod_id:
            score += 1
        # 2. Color
        if color and color_conf:
            score += 1
        # 3. Grapes
        self.cur.execute("SELECT 1 FROM wine_grapes WHERE wine_id = %s LIMIT 1", (wine_id,))
        if self.cur.fetchone():
            score += 1
        # 4. Appellation
        if app_id or app_conf:
            score += 1
        # 5. Region
        if region_id:
            score += 1
        # 6. Country
        if country_id:
            score += 1
        # 7. Vintage
        self.cur.execute("SELECT 1 FROM wine_vintages WHERE wine_id = %s LIMIT 1", (wine_id,))
        if self.cur.fetchone():
            score += 1
        # 8. UPC
        self.cur.execute("SELECT 1 FROM external_ids WHERE entity_type='wine' AND entity_id=%s AND system='upc' LIMIT 1", (wine_id,))
        if self.cur.fetchone():
            score += 1
        # 9. Price
        self.cur.execute("SELECT 1 FROM wine_vintage_prices wvp JOIN wine_vintages wv ON wvp.wine_vintage_id=wv.id WHERE wv.wine_id=%s LIMIT 1", (wine_id,))
        if self.cur.fetchone():
            score += 1
        # 10. Score
        self.cur.execute("SELECT 1 FROM wine_vintage_scores wvs JOIN wine_vintages wv ON wvs.wine_vintage_id=wv.id WHERE wv.wine_id=%s LIMIT 1", (wine_id,))
        if self.cur.fetchone():
            score += 1
        # 11. Label image
        self.cur.execute("SELECT 1 FROM wine_vintages WHERE wine_id=%s AND label_image_url IS NOT NULL LIMIT 1", (wine_id,))
        if self.cur.fetchone():
            score += 1

        return score

    def _calc_identity_complete(self, wine_id: str) -> bool:
        """Check if core identity is complete."""
        self.cur.execute("""
            SELECT producer_id, color, color_confirmed, appellation_id,
                   region_id, country_id, grapes_confirmed
            FROM wines WHERE id = %s
        """, (wine_id,))
        row = self.cur.fetchone()
        if not row:
            return False
        prod_id, color, color_conf, app_id, region_id, country_id, grapes_conf = row

        has_grapes = False
        self.cur.execute("SELECT 1 FROM wine_grapes WHERE wine_id = %s LIMIT 1", (wine_id,))
        if self.cur.fetchone():
            has_grapes = True

        return bool(
            prod_id and
            color and color_conf and
            has_grapes and grapes_conf and
            (app_id or region_id) and
            country_id
        )

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _log_provenance(self, table_name: str, record_id: str, field_name: str,
                        field_value, source_system: str, source_ref=None):
        """Log a data provenance entry."""
        if self.dry_run:
            return
        self.cur.execute("""
            INSERT INTO data_provenance (table_name, record_id, field_name, field_value,
                                         source_system, provenance_notes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (table_name, record_id, field_name, str(field_value) if field_value else None,
              source_system, f"ref:{source_ref}" if source_ref else None))
        self.stats["provenance_entries"] += 1

    def _create_external_id(self, entity_id: str, entity_type: str,
                             system: str, ext_id: str, source: str):
        """Create an external_id entry (idempotent)."""
        if self.dry_run:
            return
        try:
            self.cur.execute("""
                INSERT INTO external_ids (entity_type, entity_id, system, external_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (entity_type, entity_id, system, ext_id))
            self.conn.commit()
            self.stats["external_ids_created"] += 1
        except Exception:
            self.conn.rollback()

    def _create_label_designation(self, wine_id: str, designation_id: str):
        """Create a wine_label_designation link (idempotent)."""
        if self.dry_run:
            return
        try:
            self.cur.execute("""
                INSERT INTO wine_label_designations (wine_id, label_designation_id)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (wine_id, designation_id))
            self.conn.commit()
        except Exception:
            self.conn.rollback()

    def _create_wine_grape(self, wine_id: str, grape_id: str,
                            percentage: Optional[float], source: str):
        """Create a wine_grapes link (idempotent)."""
        if self.dry_run:
            return
        try:
            self.cur.execute("""
                INSERT INTO wine_grapes (wine_id, grape_id, percentage)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """, (wine_id, grape_id, percentage))
            self.conn.commit()
            self.stats["wine_grapes_created"] += 1
            self._log_provenance("wine_grapes", wine_id, "grape_id",
                                 grape_id, source, None)
        except Exception:
            self.conn.rollback()

    def _ensure_vintage(self, wine_id: str, year: int, abv=None, label_urls=None):
        """Ensure a wine_vintages row exists for this wine+year."""
        if self.dry_run:
            return

        self.cur.execute("""
            SELECT id FROM wine_vintages WHERE wine_id = %s AND vintage_year = %s
        """, (wine_id, year))
        existing = self.cur.fetchone()
        if existing:
            # Update ABV/label if we have new data
            vintage_id = str(existing[0])
            if abv:
                try:
                    abv_val = float(abv)
                    if 3 <= abv_val <= 25:
                        self.cur.execute("UPDATE wine_vintages SET abv = %s WHERE id = %s AND abv IS NULL",
                                         (abv_val, vintage_id))
                        self.conn.commit()
                except (ValueError, TypeError):
                    pass
            return vintage_id

        vintage_id = str(uuid.uuid4())
        abv_val = None
        if abv:
            try:
                abv_val = float(abv)
                if not (3 <= abv_val <= 25):
                    abv_val = None
            except (ValueError, TypeError):
                pass

        label_url = None
        if label_urls:
            try:
                urls = json.loads(label_urls) if isinstance(label_urls, str) else label_urls
                if isinstance(urls, list) and urls:
                    label_url = urls[0]
                elif isinstance(urls, str):
                    label_url = urls
            except (json.JSONDecodeError, TypeError):
                if isinstance(label_urls, str) and label_urls.startswith("http"):
                    label_url = label_urls

        self.cur.execute("""
            INSERT INTO wine_vintages (id, wine_id, vintage_year, abv, label_image_url)
            VALUES (%s, %s, %s, %s, %s)
        """, (vintage_id, wine_id, year, abv_val, label_url))
        self.conn.commit()
        self.stats["vintages_created"] += 1
        return vintage_id

    # =========================================================================
    # Main run
    # =========================================================================

    def run(self, roster_file: Optional[str] = None):
        """Run the full batch pipeline."""
        if roster_file:
            # Load from JSON roster
            with open(roster_file, encoding="utf-8") as f:
                roster_data = json.load(f)
            producers = [
                (p["canonical_name"], p["lwin_name"], p["country_code"],
                 p.get("price_tier", "$30-100"))
                for p in roster_data["producers"]
            ]
            batch_label = f"BATCH 1 ({roster_file})"
        else:
            producers = BATCH_0_PRODUCERS
            batch_label = "BATCH 0"

        if self.limit:
            producers = producers[:self.limit]

        mode = "DRY RUN" if self.dry_run else "EXECUTE"
        print(f"\n{'='*60}")
        print(f"  {batch_label} PIPELINE -- {mode}")
        print(f"  {len(producers)} producers")
        print(f"{'='*60}\n")

        for i, (canonical_name, lwin_name, country_code, category) in enumerate(producers):
            print(f"\n[{i+1}/{len(producers)}] {canonical_name} ({category})")

            try:
                # Step 1: Create producer
                producer_id = self.create_producer(canonical_name, lwin_name, country_code)
                if not producer_id:
                    continue

                # Step 2: Discover and create wines from LWIN
                lwin_wines = self.discover_wines_lwin(producer_id, canonical_name,
                                                       lwin_name, country_code)
                print(f"  Found {len(lwin_wines)} LWIN wines")

                # Deduplicate LWIN wines by identity tuple before creation
                lwin_wines = self._dedup_lwin_wines(lwin_wines, canonical_name, country_code)
                print(f"  After dedup: {len(lwin_wines)} unique wines")

                wine_ids = []
                for wine_data in lwin_wines:
                    wine_id = self.create_wine_from_lwin(wine_data, producer_id,
                                                          canonical_name, country_code)
                    if wine_id:
                        wine_ids.append(wine_id)

                print(f"  Created {len(wine_ids)} wines")

                # Step 3: Link TTB for depth
                if not self.dry_run and wine_ids and not self.skip_ttb:
                    ttb_count = self.link_ttb_for_producer(producer_id, canonical_name, country_code)
                    if ttb_count:
                        print(f"  Linked {ttb_count} TTB records")

                # Step 4: Calculate grades
                if not self.dry_run:
                    self.calculate_grades(producer_id)

            except Exception as e:
                print(f"  [ERROR] {canonical_name}: {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                self.stats["errors"] += 1

        # Print summary
        self._print_summary()

        if not self.dry_run:
            self.conn.commit()
        self.conn.close()

    def _print_summary(self):
        """Print final summary statistics."""
        print(f"\n{'='*60}")
        print(f"  BATCH SUMMARY")
        print(f"{'='*60}")
        for key, val in sorted(self.stats.items()):
            print(f"  {key}: {val}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Batch Pipeline")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Dry run -- don't write to DB")
    parser.add_argument("--execute", action="store_true", default=False,
                        help="Execute -- write to DB")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to first N producers")
    parser.add_argument("--roster", type=str, default=None,
                        help="Path to JSON roster file (e.g., data/batch1_roster.json)")
    parser.add_argument("--skip-ttb", action="store_true", default=False,
                        help="Skip TTB linking step")
    args = parser.parse_args()

    dry_run = not args.execute
    pipeline = BatchPipeline(dry_run=dry_run, limit=args.limit, skip_ttb=args.skip_ttb)
    pipeline.run(roster_file=args.roster)


if __name__ == "__main__":
    main()
