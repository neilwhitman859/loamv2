"""
Reference data resolvers for the Loam pipeline.

Loads all reference data (countries, regions, appellations, grapes, publications,
classifications) into memory and provides fast lookup by name, alias, or normalized form.

Usage:
    from pipeline.lib.resolve import ReferenceResolver

    resolver = ReferenceResolver()
    await resolver.init()
    country_id = resolver.resolve_country("France")
    grape = resolver.resolve_grape("Cab Sauv")
"""

from __future__ import annotations

import time
from .db import fetch_all
from .normalize import normalize


# ── In-Code Alias Maps ──────────────────────────────────────

GRAPE_ALIASES: dict[str, str] = {
    # US names
    "petite sirah": "Durif", "petit sirah": "Durif", "petite syrah": "Durif",
    "cab sauv": "Cabernet Sauvignon", "cab franc": "Cabernet Franc",
    "cab": "Cabernet Sauvignon", "zin": "Zinfandel",
    "petit verdot": "Petit Verdot", "petite verdot": "Petit Verdot",
    # Spanish
    "mazuelo": "Carignan", "mazuela": "Carignan", "cariñena": "Carignan",
    "garnacha": "Grenache", "garnacha tinta": "Grenache",
    "garnacho": "Grenache", "garnacho tinto": "Grenache",
    "malvasía": "Malvasia", "malvasia": "Malvasia",
    # French
    "mourvèdre": "Mourvèdre", "mourvedre": "Mourvèdre",
    "mataro": "Mourvèdre", "mataró": "Mourvèdre",
    "syrah": "Syrah", "shiraz": "Syrah",
    "grenache blanc": "Grenache Blanc", "grenache noir": "Grenache",
    "sémillon": "Sémillon", "semillon": "Sémillon",
    # Hungarian
    "sárgamuskotály": "Muscat Blanc à Petits Grains",
    "sargamuskotaly": "Muscat Blanc à Petits Grains",
    "yellow muscat": "Muscat Blanc à Petits Grains",
    "hárslevelű": "Hárslevelü", "harslevelu": "Hárslevelü",
    # Italian
    "sangiovese grosso": "Sangiovese", "brunello": "Sangiovese",
    "primitivo": "Zinfandel", "nebbiolo": "Nebbiolo",
    "pinot bianco": "Pinot Blanc", "pinot grigio": "Pinot Gris",
    "verdicchio": "Verdicchio Bianco", "lagrein": "Lagrein",
    # German/Austrian
    "müller-thurgau": "Müller-Thurgau", "muller-thurgau": "Müller-Thurgau",
    "muller thurgau": "Müller-Thurgau", "mueller thurgau": "Müller-Thurgau",
    "grüner veltliner": "Grüner Veltliner", "gruner veltliner": "Grüner Veltliner",
    "blaufränkisch": "Blaufränkisch", "blaufrankisch": "Blaufränkisch",
    "lemberger": "Blaufränkisch", "zweigelt": "Zweigelt",
    "st. laurent": "Sankt Laurent", "saint laurent": "Sankt Laurent",
    # Portuguese
    "touriga nacional": "Touriga Nacional", "touriga franca": "Touriga Franca",
    "tinta roriz": "Tempranillo", "aragonez": "Tempranillo",
    "tinto cão": "Tinto Cao", "tinto cao": "Tinto Cao",
    "tinta barroca": "Tinta Barroca",
    # Champagne
    "meunier": "Pinot Meunier", "pinot meunier": "Pinot Meunier",
    # Georgian
    "rkatsiteli": "Rkatsiteli", "saperavi": "Saperavi",
    "mtsvane": "Mtsvane Kakhuri", "mtsvane kakhuri": "Mtsvane Kakhuri",
    # Lebanese
    "obaideh": "Obaideh", "merwah": "Merwah",
    # Madeira
    "sercial": "Sercial", "verdelho": "Verdelho",
    "boal": "Boal", "bual": "Boal", "terrantez": "Terrantez",
    "tinta negra": "Tinta Negra Mole", "tinta negra mole": "Tinta Negra Mole",
    # Common
    "sauv blanc": "Sauvignon Blanc", "sauvignon": "Sauvignon Blanc",
    "chard": "Chardonnay", "pinot noir": "Pinot Noir",
    "pinot gris": "Pinot Gris", "pinot blanc": "Pinot Blanc",
    "gewurztraminer": "Gewürztraminer", "gewürztraminer": "Gewürztraminer",
    "riesling": "Riesling", "merlot": "Merlot",
    "cabernet sauvignon": "Cabernet Sauvignon", "cabernet franc": "Cabernet Franc",
    "malbec": "Malbec", "cot": "Malbec", "côt": "Malbec",
    "tempranillo": "Tempranillo", "tinta de toro": "Tempranillo",
    "sangiovese": "Sangiovese", "morellino": "Sangiovese",
    "prugnolo gentile": "Sangiovese", "nielluccio": "Sangiovese",
    "chenin blanc": "Chenin Blanc", "chenin": "Chenin Blanc", "steen": "Chenin Blanc",
    "viognier": "Viognier", "verdejo": "Verdejo",
    "albariño": "Albariño", "albarino": "Albariño",
    "pinotage": "Pinotage", "colombard": "Colombard",
    "cinsaut": "Cinsaut", "cinsault": "Cinsaut",
    "palomino": "Palomino Fino", "pedro ximenez": "Pedro Ximenez",
    "pedro ximénez": "Pedro Ximenez", "px": "Pedro Ximenez",
    "marsanne": "Marsanne", "roussanne": "Roussanne",
    "trebbiano": "Trebbiano Toscano", "ugni blanc": "Trebbiano Toscano",
    "melon de bourgogne": "Melon", "muscadet": "Melon",
    "cortese": "Cortese", "arneis": "Arneis",
    "dolcetto": "Dolcetto", "barbera": "Barbera",
    "nerello mascalese": "Nerello Mascalese", "carricante": "Carricante",
    "nero d'avola": "Nero d'Avola", "nero davola": "Nero d'Avola",
    "aglianico": "Aglianico", "fiano": "Fiano", "greco": "Greco",
    "falanghina": "Falanghina",
    "picpoul": "Piquepoul Blanc", "piquepoul": "Piquepoul Blanc",
    "clairette": "Clairette", "bourboulenc": "Bourboulenc",
    "rolle": "Vermentino",
    "assyrtiko": "Assyrtiko", "xinomavro": "Xinomavro",
    "agiorgitiko": "Agiorgitiko", "moschofilero": "Moschofilero",
    "país": "País", "pais": "País",
    "torrontés": "Torrontés Riojano", "torrontes": "Torrontés Riojano",
    "bonarda": "Bonarda",
    "ribolla gialla": "Ribolla Gialla", "ribolla": "Ribolla Gialla",
    "friulano": "Sauvignonasse", "tocai friulano": "Sauvignonasse",
}

REGION_ALIASES: dict[str, str] = {
    "piedmont": "Piemonte", "tuscany": "Toscana",
    "lombardy": "Lombardia", "sicily": "Sicilia", "sardinia": "Sardegna",
    "friuli": "Friuli-Venezia Giulia", "friuli venezia giulia": "Friuli-Venezia Giulia",
    "trentino": "Trentino-Alto Adige", "alto adige": "Trentino-Alto Adige",
    "südtirol": "Trentino-Alto Adige", "apulia": "Puglia",
    "burgundy": "Burgundy", "bourgogne": "Burgundy",
    "rhone": "Rhône Valley", "rhône": "Rhône Valley",
    "rhone valley": "Rhône Valley", "rhône valley": "Rhône Valley",
    "northern rhone": "Northern Rhône", "northern rhône": "Northern Rhône",
    "southern rhone": "Southern Rhône", "southern rhône": "Southern Rhône",
    "loire": "Loire Valley", "loire valley": "Loire Valley",
    "languedoc": "Languedoc", "roussillon": "Roussillon",
    "southwest france": "Southwest France", "south west france": "Southwest France",
    "catalonia": "Catalunya", "catalunya": "Catalunya",
    "castile and leon": "Castilla y León", "castilla y leon": "Castilla y León",
    "mosel": "Mosel", "moselle": "Mosel",
    "palatinate": "Pfalz",
    "douro": "Douro", "dão": "Dão", "dao": "Dão",
    "bekaa valley": "Bekaa Valley", "bekaa": "Bekaa Valley",
    "kakheti": "Kakheti", "kartli": "Kartli", "imereti": "Imereti",
    "barossa": "Barossa", "barossa valley": "Barossa Valley",
    "mclaren vale": "McLaren Vale",
    "hawkes bay": "Hawke's Bay", "hawke's bay": "Hawke's Bay",
    "napa valley": "Napa Valley", "sonoma coast": "Sonoma Coast",
    "willamette valley": "Willamette Valley",
    "stellenbosch": "Stellenbosch", "swartland": "Swartland",
    "mendoza": "Mendoza", "salta": "Salta",
    "uco valley": "Uco Valley", "valle de uco": "Uco Valley",
    "lujan de cuyo": "Luján de Cuyo", "luján de cuyo": "Luján de Cuyo",
    "maipo valley": "Maipo Valley", "colchagua valley": "Colchagua Valley",
    "casablanca valley": "Casablanca Valley",
}

PUB_ALIASES: dict[str, str] = {
    "robert parker": "Wine Advocate", "robert parker's wine advocate": "Wine Advocate",
    "the wine advocate": "Wine Advocate", "parker": "Wine Advocate",
    "wine advocate": "Wine Advocate", "wa": "Wine Advocate",
    "jamessuckling.com": "James Suckling", "james suckling": "James Suckling", "js": "James Suckling",
    "vinous media": "Vinous", "vinous": "Vinous", "antonio galloni": "Vinous",
    "wine spectator": "Wine Spectator", "ws": "Wine Spectator",
    "wine enthusiast": "Wine Enthusiast", "we": "Wine Enthusiast",
    "decanter": "Decanter", "decanter magazine": "Decanter",
    "jancis robinson": "Jancis Robinson", "jancisrobinson.com": "Jancis Robinson",
    "guía peñín": "Guía Peñín", "guia penin": "Guía Peñín", "penin": "Guía Peñín",
    "tim atkin": "Tim Atkin MW", "tim atkin mw": "Tim Atkin MW",
    "burghound": "Burghound", "allen meadows": "Burghound",
    "gambero rosso": "Gambero Rosso",
    "jeb dunnuck": "Jeb Dunnuck", "jd": "Jeb Dunnuck",
    "jasper morris": "Jasper Morris MW", "jasper morris mw": "Jasper Morris MW",
}

CLASSIFICATION_SYSTEM_ALIASES: dict[str, str] = {
    "langton's classification": "langton's classification of australian wine",
    "langtons classification": "langton's classification of australian wine",
    "langtons": "langton's classification of australian wine",
    "langton's": "langton's classification of australian wine",
    "bordeaux 1855 sauternes classification": "bordeaux 1855 classification (sauternes)",
    "1855 sauternes": "bordeaux 1855 classification (sauternes)",
    "sauternes classification": "bordeaux 1855 classification (sauternes)",
    "bordeaux 1855 classification": "bordeaux 1855 classification (médoc)",
    "1855 medoc": "bordeaux 1855 classification (médoc)",
    "1855 classification": "bordeaux 1855 classification (médoc)",
    "burgundy classification": "burgundy vineyard classification",
    "burgundy vineyard": "burgundy vineyard classification",
    "champagne classification": "champagne cru classification",
    "champagne cru": "champagne cru classification",
    "saint-emilion classification": "saint-émilion classification",
    "st-emilion classification": "saint-émilion classification",
    "saint emilion": "saint-émilion classification",
    "vdp": "vdp classification",
    "cru bourgeois": "cru bourgeois du médoc",
    "otw erste lagen": "ötw erste lagen",
    "otw": "ötw erste lagen",
}

CONFIDENCE_RANK = {
    "unverified": 0,
    "lwin_matched": 1,
    "cola_matched": 2,
    "upc_matched": 3,
    "manual_verified": 4,
}


class ReferenceResolver:
    """
    Loads all Loam reference data into memory and provides fast resolution
    of countries, regions, appellations, grapes, publications, and classifications.
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._initialized = False

        # Reference data maps
        self.countries: dict[str, str] = {}             # name_lower | iso -> id
        self.regions: dict[str, dict] = {}              # name_lower | "name|country_id" -> row
        self.appellations: dict[str, dict] = {}         # name_lower | normalized -> row
        self.grapes: dict[str, dict] = {}               # display_name_lower -> row
        self.grapes_by_name: dict[str, dict] = {}       # vivc_name_lower -> row
        self.grape_synonyms: dict[str, str] = {}        # synonym_lower -> grape_id
        self.publications: dict[str, str] = {}          # name_lower | slug -> id
        self.source_types: dict[str, str] = {}          # slug -> id
        self.classification_levels: dict[str, dict] = {}  # "system|level" -> entry
        self.classification_systems: dict[str, dict] = {}  # slug -> row

    async def init(self):
        """Load all reference data into memory. Must be called before resolution."""
        if self._initialized:
            return
        t0 = time.time()
        if self.verbose:
            print("ReferenceResolver: loading reference data...")

        # Load all tables concurrently (Python supabase-py is sync, so sequential)
        self._load_countries(await fetch_all("countries", "id,name,iso_code"))
        self._load_regions(
            await fetch_all("regions", "id,name,country_id,parent_id,is_catch_all"),
            await fetch_all("region_aliases", "id,name,region_id"),
        )
        self._load_appellations(
            await fetch_all("appellations", "id,name,designation_type,country_id,region_id"),
            await fetch_all("appellation_aliases", "appellation_id,alias_normalized"),
        )
        self._load_grapes(
            await fetch_all("grapes", "id,name,display_name,color"),
            await fetch_all("grape_synonyms", "grape_id,synonym"),
        )
        self._load_publications(await fetch_all("publications", "id,name,slug"))
        self._load_source_types(await fetch_all("source_types", "id,slug"))
        self._load_classifications(
            await fetch_all("classification_levels", "id,classification_id,level_name,level_rank"),
            await fetch_all("classifications", "id,name,slug,country_id"),
        )

        self._initialized = True
        elapsed = time.time() - t0
        if self.verbose:
            print(f"ReferenceResolver: ready ({elapsed:.1f}s)\n")

    def init_sync(self):
        """Synchronous init — loads reference data without async."""
        if self._initialized:
            return
        t0 = time.time()
        if self.verbose:
            print("ReferenceResolver: loading reference data...")

        from .db import fetch_all as _fetch_all_async
        import asyncio

        # For sync callers, use the sync supabase client directly
        from .db import get_supabase
        sb = get_supabase()

        def _fetch_sync(table, columns="*"):
            all_rows = []
            offset = 0
            batch_size = 1000
            while True:
                result = sb.table(table).select(columns).range(offset, offset + batch_size - 1).execute()
                all_rows.extend(result.data)
                if len(result.data) < batch_size:
                    break
                offset += batch_size
            return all_rows

        self._load_countries(_fetch_sync("countries", "id,name,iso_code"))
        self._load_regions(
            _fetch_sync("regions", "id,name,country_id,parent_id,is_catch_all"),
            _fetch_sync("region_aliases", "id,name,region_id"),
        )
        self._load_appellations(
            _fetch_sync("appellations", "id,name,designation_type,country_id,region_id"),
            _fetch_sync("appellation_aliases", "appellation_id,alias_normalized"),
        )
        self._load_grapes(
            _fetch_sync("grapes", "id,name,display_name,color"),
            _fetch_sync("grape_synonyms", "grape_id,synonym"),
        )
        self._load_publications(_fetch_sync("publications", "id,name,slug"))
        self._load_source_types(_fetch_sync("source_types", "id,slug"))
        self._load_classifications(
            _fetch_sync("classification_levels", "id,classification_id,level_name,level_rank"),
            _fetch_sync("classifications", "id,name,slug,country_id"),
        )

        self._initialized = True
        elapsed = time.time() - t0
        if self.verbose:
            print(f"ReferenceResolver: ready ({elapsed:.1f}s)\n")

    # ── Loaders ──────────────────────────────────────────────────

    def _load_countries(self, rows: list[dict]):
        for c in rows:
            self.countries[c["name"].lower()] = c["id"]
            if c.get("iso_code"):
                self.countries[c["iso_code"].lower()] = c["id"]
        # Common aliases
        us = self.countries.get("united states")
        if us:
            self.countries["usa"] = us
            self.countries["us"] = us
        uk = self.countries.get("united kingdom")
        if uk:
            self.countries["uk"] = uk
            self.countries["england"] = uk
        if self.verbose:
            print(f"  Countries: {len(rows)}")

    def _load_regions(self, regions: list[dict], aliases: list[dict]):
        region_by_id = {r["id"]: r for r in regions}
        for r in regions:
            lower = r["name"].lower()
            norm = normalize(r["name"])
            self.regions[lower] = r
            self.regions[f"{lower}|{r['country_id']}"] = r
            if norm != lower:
                self.regions[norm] = r
                self.regions[f"{norm}|{r['country_id']}"] = r
        for ra in aliases:
            region = region_by_id.get(ra.get("region_id"))
            if not region:
                continue
            norm = normalize(ra["name"])
            lower = ra["name"].lower()
            self.regions[f"{norm}|{region['country_id']}"] = region
            self.regions[norm] = region
            if lower != norm:
                self.regions[f"{lower}|{region['country_id']}"] = region
                self.regions[lower] = region
        if self.verbose:
            print(f"  Regions: {len(regions)} (+{len(aliases)} aliases)")

    def _load_appellations(self, appellations: list[dict], aliases: list[dict]):
        app_by_id = {a["id"]: a for a in appellations}
        for a in appellations:
            lower = a["name"].lower()
            norm = normalize(a["name"])
            self.appellations[lower] = a
            if norm not in self.appellations:
                self.appellations[norm] = a
        alias_count = 0
        for al in aliases:
            key = al.get("alias_normalized", "")
            if key and key not in self.appellations:
                app = app_by_id.get(al.get("appellation_id"))
                if app:
                    self.appellations[key] = app
                    alias_count += 1
        if self.verbose:
            print(f"  Appellations: {len(appellations)} (+{alias_count} alias keys)")

    def _load_grapes(self, grapes: list[dict], synonyms: list[dict]):
        for g in grapes:
            if g.get("display_name"):
                self.grapes[g["display_name"].lower()] = g
            self.grapes_by_name[g["name"].lower()] = g
        for s in synonyms:
            self.grape_synonyms[s["synonym"].lower()] = s["grape_id"]
        if self.verbose:
            print(f"  Grapes: {len(grapes)} (+{len(synonyms)} synonyms)")

    def _load_publications(self, pubs: list[dict]):
        for p in pubs:
            self.publications[p["name"].lower()] = p["id"]
            self.publications[p["slug"]] = p["id"]
        for alias, canonical in PUB_ALIASES.items():
            pid = self.publications.get(canonical.lower())
            if pid:
                self.publications[alias.lower()] = pid
        if self.verbose:
            print(f"  Publications: {len(pubs)}")

    def _load_source_types(self, rows: list[dict]):
        for s in rows:
            self.source_types[s["slug"]] = s["id"]
        if self.verbose:
            print(f"  Source types: {len(rows)}")

    def _load_classifications(self, levels: list[dict], systems: list[dict]):
        sys_by_id = {s["id"]: s for s in systems}
        for s in systems:
            self.classification_systems[s["slug"]] = s
            self.classification_systems[s["name"].lower()] = s
        for cl in levels:
            sys = sys_by_id.get(cl["classification_id"])
            if not sys:
                continue
            entry = {
                "level_id": cl["id"],
                "classification_id": sys["id"],
                "system_name": sys["name"],
                "level_name": cl["level_name"],
                "rank": cl["level_rank"],
            }
            key = f"{sys['name'].lower()}|{cl['level_name'].lower()}"
            self.classification_levels[key] = entry
            self.classification_levels[f"{sys['slug']}|{cl['level_name'].lower()}"] = entry
        # Register aliases
        for alias, canonical in CLASSIFICATION_SYSTEM_ALIASES.items():
            for cl in levels:
                sys = sys_by_id.get(cl["classification_id"])
                if not sys or sys["name"].lower() != canonical:
                    continue
                source_key = f"{canonical}|{cl['level_name'].lower()}"
                entry = self.classification_levels.get(source_key)
                if entry:
                    self.classification_levels[f"{alias}|{cl['level_name'].lower()}"] = entry
        if self.verbose:
            print(f"  Classifications: {len(systems)} systems, {len(levels)} levels")

    # ── Resolution Methods ──────────────────────────────────────

    def resolve_country(self, name: str | None) -> str | None:
        """Resolve a country name or ISO code to a UUID."""
        if not name:
            return None
        return self.countries.get(name.lower().strip())

    def resolve_region(self, name: str | None, country_id: str | None = None) -> dict | None:
        """Resolve a region name to {id, name, country_id, ...}."""
        if not name:
            return None
        lower = name.lower().strip()
        aliased = REGION_ALIASES.get(lower)
        candidates = [lower, aliased.lower()] if aliased else [lower]
        for c in candidates:
            if country_id:
                r = self.regions.get(f"{c}|{country_id}")
                if r:
                    return r
            r = self.regions.get(c)
            if r:
                return r
            norm = normalize(c)
            if country_id:
                r2 = self.regions.get(f"{norm}|{country_id}")
                if r2:
                    return r2
            r3 = self.regions.get(norm)
            if r3:
                return r3
        return None

    def resolve_appellation(self, name: str | None, country_id: str | None = None) -> dict | None:
        """Resolve an appellation name to {id, name, country_id, region_id}."""
        if not name:
            return None
        lower = name.lower().strip()
        a = self.appellations.get(lower)
        if a:
            return a
        norm = normalize(lower)
        return self.appellations.get(norm)

    def resolve_grape(self, name: str | None) -> dict | None:
        """Resolve a grape name to {id, ...} via aliases, display_name, VIVC name, synonyms."""
        if not name:
            return None
        lower = name.lower().strip()
        norm = normalize(lower)

        # 1. In-code alias
        aliased = GRAPE_ALIASES.get(lower) or GRAPE_ALIASES.get(norm)
        if aliased:
            g = self.grapes.get(aliased.lower()) or self.grapes_by_name.get(aliased.lower())
            if g:
                return g
        # 2. Display name
        by_display = self.grapes.get(lower) or self.grapes.get(norm)
        if by_display:
            return by_display
        # 3. VIVC name
        by_vivc = self.grapes_by_name.get(lower) or self.grapes_by_name.get(norm)
        if by_vivc:
            return by_vivc
        # 4. Synonyms table
        syn_id = self.grape_synonyms.get(lower) or self.grape_synonyms.get(norm)
        if syn_id:
            return {"id": syn_id}
        # 5. Common suffixes
        for suffix in [" noir", " blanc", " tinto", " tinta", " blanco"]:
            with_suffix = self.grapes.get(lower + suffix) or self.grapes.get(norm + suffix)
            if with_suffix:
                return with_suffix
        return None

    def resolve_publication(self, name: str | None) -> str | None:
        """Resolve a publication name to a UUID."""
        if not name:
            return None
        return self.publications.get(name.lower().strip())

    def resolve_classification(self, system: str | None, level: str | None) -> dict | None:
        """Resolve a classification system + level to {level_id, classification_id, ...}."""
        if not system or not level:
            return None
        key = f"{system.lower().strip()}|{level.lower().strip()}"
        return self.classification_levels.get(key)
